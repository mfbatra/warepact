"""Filesystem-backed ContractStore.

Contracts are stored as .contract.yaml files under a root directory.
This is the default store — no external dependencies required.
"""

from __future__ import annotations

from pathlib import Path

from datapact.core.contract import Contract
from datapact.core.exceptions import ContractNotFoundError
from datapact.interfaces.store import ContractStore
from datapact.parsers.yaml_parser import YAMLParser

import yaml


class FilesystemContractStore(ContractStore):
    """
    Persists Contract objects as .contract.yaml files.

    Layout::

        root/
            orders.contract.yaml
            users.contract.yaml
            ...
    """

    def __init__(self, root: str | Path = "contracts") -> None:
        self._root = Path(root)
        self._parser = YAMLParser()

    # ── ContractStore interface ────────────────────────────────────────────────

    def save(self, contract: Contract) -> None:
        """Serialise *contract* to <root>/<name>.contract.yaml."""
        self._root.mkdir(parents=True, exist_ok=True)
        path = self._contract_path(contract.name)
        data = contract.model_dump(by_alias=True, exclude_none=True)
        path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )

    def load(self, name: str) -> Contract:
        """Load and return the Contract identified by *name*."""
        path = self._contract_path(name)
        if not path.exists():
            raise ContractNotFoundError(
                f"Contract '{name}' not found in {self._root}."
            )
        return self._parser.parse_file(path)

    def list_names(self) -> list[str]:
        """Return sorted list of contract names stored on disk."""
        if not self._root.is_dir():
            return []
        return sorted(
            p.name.replace(".contract.yaml", "").replace(".contract.yml", "")
            for p in self._root.iterdir()
            if p.suffix in {".yaml", ".yml"} and ".contract." in p.name
        )

    def delete(self, name: str) -> None:
        """Delete the contract file for *name*."""
        path = self._contract_path(name)
        if not path.exists():
            raise ContractNotFoundError(
                f"Contract '{name}' not found in {self._root}."
            )
        path.unlink()

    def exists(self, name: str) -> bool:
        return self._contract_path(name).exists()

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _contract_path(self, name: str) -> Path:
        return self._root / f"{name}.contract.yaml"
