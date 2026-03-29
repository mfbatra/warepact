"""YAML contract parser.

Reads .contract.yaml files and returns validated Contract instances.
All parsing errors are surfaced as ContractValidationError so callers
have a single exception type to handle.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml
from pydantic import ValidationError

from warepact.core.contract import Contract
from warepact.core.exceptions import ContractNotFoundError, ContractValidationError


# ── YAML 1.2-compatible loader ─────────────────────────────────────────────────
# PyYAML defaults to YAML 1.1 which parses `on`/`off`/`yes`/`no` as booleans.
# Warepact contracts use `on:` as a plain string key, so we restrict bool
# resolution to only `true` and `false` (YAML 1.2 behaviour).

class _WarepactLoader(yaml.SafeLoader):
    pass


# Copy resolvers without the bool tag, then re-add a strict bool pattern
_WarepactLoader.yaml_implicit_resolvers = {
    key: [(tag, regexp) for tag, regexp in resolvers
          if tag != "tag:yaml.org,2002:bool"]
    for key, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_WarepactLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|false)$", re.IGNORECASE),
    list("tTfF"),
)


class YAMLParser:
    """
    Parses .contract.yaml files into Contract objects.

    Single responsibility: translate a file path or raw YAML string into
    a validated Contract.  Nothing else.
    """

    # ── Public API ─────────────────────────────────────────────────────────────

    def parse_file(self, path: str | Path) -> Contract:
        """
        Load and parse a .contract.yaml file.

        Raises:
            ContractNotFoundError: file does not exist.
            ContractValidationError: YAML is malformed or fails Contract validation.
        """
        path = Path(path)
        if not path.exists():
            raise ContractNotFoundError(f"Contract file not found: {path}")

        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ContractValidationError(
                f"Could not read contract file '{path}': {exc}"
            ) from exc

        return self.parse_string(raw, source=str(path))

    def parse_string(self, yaml_text: str, source: str = "<string>") -> Contract:
        """
        Parse a YAML string into a Contract.

        Raises:
            ContractValidationError: YAML is malformed or fails validation.
        """
        try:
            data = yaml.load(yaml_text, Loader=_WarepactLoader)
        except yaml.YAMLError as exc:
            raise ContractValidationError(
                f"Invalid YAML in '{source}': {exc}"
            ) from exc

        if data is None:
            raise ContractValidationError(f"Empty contract file: '{source}'")

        if not isinstance(data, dict):
            raise ContractValidationError(
                f"Contract '{source}' must be a YAML mapping, got {type(data).__name__}."
            )

        return self._build_contract(data, source)

    # ── Directory scanning ─────────────────────────────────────────────────────

    def parse_directory(self, directory: str | Path) -> list[Contract]:
        """
        Parse all *.contract.yaml and *.contract.yml files in *directory*
        (non-recursive).

        Skips files that fail validation and collects errors; re-raises as a
        single ContractValidationError listing all failures.
        """
        directory = Path(directory)
        if not directory.is_dir():
            raise ContractNotFoundError(f"Contracts directory not found: {directory}")

        patterns = list(directory.glob("*.contract.yaml")) + list(
            directory.glob("*.contract.yml")
        )

        contracts: list[Contract] = []
        errors: list[str] = []

        for path in sorted(patterns):
            try:
                contracts.append(self.parse_file(path))
            except (ContractValidationError, ContractNotFoundError) as exc:
                errors.append(f"  {path.name}: {exc}")

        if errors:
            raise ContractValidationError(
                f"Failed to parse {len(errors)} contract file(s):\n"
                + "\n".join(errors)
            )

        return contracts

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_contract(data: dict, source: str) -> Contract:
        try:
            return Contract(**data)
        except ValidationError as exc:
            # Pydantic's error messages are already very readable
            raise ContractValidationError(
                f"Contract validation failed for '{source}':\n{exc}"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise ContractValidationError(
                f"Unexpected error parsing contract '{source}': {exc}"
            ) from exc
