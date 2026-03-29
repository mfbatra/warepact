"""warepact publish — publish a validated contract to a local registry or HTTP endpoint."""

from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import typer
import yaml

from warepact.cli._console import console, err_console
from warepact.cli._credentials import load_credentials
from warepact.core.engine import ContractEngine
from warepact.core.exceptions import (
    ContractCheckError,
    ContractNotFoundError,
    ContractValidationError,
)
from warepact.core.registry import PluginRegistry
from warepact.parsers.yaml_parser import YAMLParser


def publish_command(
    contract_name: str = typer.Argument(..., help="Contract name to publish."),
    contracts_dir: str = typer.Option(
        "contracts",
        "--dir", "-d",
        help="Directory containing contract YAML files.",
    ),
    output: str = typer.Option(
        "~/.warepact/registry",
        "--output", "-o",
        help="Local directory to publish the contract YAML to.",
    ),
    registry_url: str = typer.Option(
        "",
        "--registry",
        help="HTTP registry URL. If set, also POSTs to this endpoint.",
        envvar="DATAPACT_REGISTRY_URL",
    ),
    check: bool = typer.Option(
        True,
        "--check/--no-check",
        help="Run the contract check before publishing. Refuses to publish a failing contract.",
    ),
    force: bool = typer.Option(
        False, "--force", help="Publish even if the contract check fails."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Validate and show payload without writing anything."
    ),
    update: bool = typer.Option(
        False, "--update", help="Use PUT instead of POST for the HTTP registry."
    ),
) -> None:
    """Publish a contract to the local registry (and optionally an HTTP registry)."""
    _autodiscover()

    parser = YAMLParser()
    directory = Path(contracts_dir)

    try:
        contract = parser.parse_file(directory / f"{contract_name}.contract.yaml")
    except ContractNotFoundError:
        err_console.print(f"[red]Contract not found:[/] {contract_name}")
        raise typer.Exit(code=1)
    except ContractValidationError as exc:
        err_console.print(f"[red]Invalid contract:[/] {exc}")
        raise typer.Exit(code=1)

    # ── Optional pre-publish check ─────────────────────────────────────────────
    if check:
        try:
            contract.credentials = load_credentials(contract.warehouse)
            engine = ContractEngine(registry=PluginRegistry)
            result = engine.check(contract)
        except ContractCheckError as exc:
            err_console.print(f"[red]Contract check error:[/] {exc}")
            if not force:
                raise typer.Exit(code=1)
            result = None

        if result is not None and not result.passed:
            err_console.print(
                f"[red]Contract '{contract.name}' is failing — refusing to publish.[/]\n"
                "Fix the issues or use [bold]--no-check[/] / [bold]--force[/] to override."
            )
            if not force:
                raise typer.Exit(code=1)

    payload = contract.model_dump(by_alias=True, exclude_none=True)

    if dry_run:
        console.print(
            f"[green]Dry run OK[/] — [cyan]{contract.name}[/] is valid.\n"
            "Payload that would be published:"
        )
        console.print_json(json.dumps(payload, default=str))
        raise typer.Exit(code=0)

    # ── Phase 1: file-based publish ────────────────────────────────────────────
    _publish_to_file(contract_name, payload, output)

    # ── Phase 2: HTTP registry (only if --registry is explicitly set) ──────────
    if registry_url:
        api_key = os.environ.get("DATAPACT_API_KEY", "")
        if not api_key:
            err_console.print(
                "[yellow]Warning:[/] DATAPACT_API_KEY is not set. "
                "Publishing to a private registry may fail."
            )
        url = f"{registry_url.rstrip('/')}/api/v1/contracts/{contract.name}"
        method = "PUT" if update else "POST"
        try:
            ok = _http_publish(url, payload, api_key, method)
        except RuntimeError as exc:
            err_console.print(f"[red]HTTP publish failed:[/] {exc}")
            raise typer.Exit(code=1)
        if ok:
            console.print(f"[green]Published[/] [cyan]{contract.name}[/] → {url}")
        else:
            raise typer.Exit(code=1)


def _publish_to_file(contract_name: str, payload: dict, output_dir: str) -> None:
    """Write *payload* as YAML to *output_dir* with metadata fields."""
    dest_dir = Path(output_dir).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{contract_name}.contract.yaml"

    canonical = json.dumps(payload, sort_keys=True, default=str).encode()
    checksum = hashlib.sha256(canonical).hexdigest()
    published_at = datetime.now(tz=timezone.utc).isoformat()

    enriched = {
        **payload,
        "published_at": published_at,
        "checksum": checksum,
    }
    dest.write_text(yaml.dump(enriched, allow_unicode=True, sort_keys=False))
    console.print(
        f"[green]Published[/] [cyan]{contract_name}[/] → {dest}"
    )


def _http_publish(url: str, payload: dict, api_key: str, method: str) -> bool:
    """POST/PUT the contract payload to *url*. Returns True on success."""
    data = json.dumps(payload, default=str).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in (200, 201, 204):
                return True
            err_console.print(f"[red]Registry returned HTTP {resp.status}[/]")
            return False
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        if exc.code == 409:
            err_console.print(
                f"[yellow]Contract '{payload.get('name')}' already exists.[/] "
                "Use [bold]--update[/] to overwrite."
            )
        else:
            err_console.print(f"[red]HTTP {exc.code}:[/] {body[:200]}")
        return False
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach registry at {url}: {exc.reason}") from exc


def _autodiscover() -> None:
    try:
        import warepact.validators  # noqa: F401
    except ImportError:
        pass
    PluginRegistry.autodiscover()
