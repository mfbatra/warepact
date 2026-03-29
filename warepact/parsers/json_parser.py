"""JSON contract parser — reads .contract.json files."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from warepact.core.contract import Contract
from warepact.core.exceptions import ContractNotFoundError, ContractValidationError


class JSONParser:
    """Parses .contract.json files into Contract objects."""

    def parse_file(self, path: str | Path) -> Contract:
        path = Path(path)
        if not path.exists():
            raise ContractNotFoundError(f"Contract file not found: {path}")
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ContractValidationError(f"Could not read '{path}': {exc}") from exc
        return self.parse_string(raw, source=str(path))

    def parse_string(self, json_text: str, source: str = "<string>") -> Contract:
        try:
            data = json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise ContractValidationError(
                f"Invalid JSON in '{source}': {exc}"
            ) from exc

        if not isinstance(data, dict):
            raise ContractValidationError(
                f"Contract '{source}' must be a JSON object."
            )
        try:
            return Contract(**data)
        except ValidationError as exc:
            raise ContractValidationError(
                f"Contract validation failed for '{source}':\n{exc}"
            ) from exc

    def parse_directory(self, directory: str | Path) -> list[Contract]:
        directory = Path(directory)
        if not directory.is_dir():
            raise ContractNotFoundError(f"Directory not found: {directory}")

        contracts: list[Contract] = []
        errors: list[str] = []
        for path in sorted(directory.glob("*.contract.json")):
            try:
                contracts.append(self.parse_file(path))
            except (ContractValidationError, ContractNotFoundError) as exc:
                errors.append(f"  {path.name}: {exc}")

        if errors:
            raise ContractValidationError(
                f"Failed to parse {len(errors)} contract file(s):\n" + "\n".join(errors)
            )
        return contracts
