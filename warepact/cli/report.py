"""warepact report — generate an HTML health report."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import typer

from warepact.cli._console import console, err_console
from warepact.cli._credentials import load_credentials
from warepact.cli.check import _autodiscover
from warepact.core.engine import ContractEngine, ContractCheckResult
from warepact.core.exceptions import ContractCheckError
from warepact.core.registry import PluginRegistry
from warepact.parsers.yaml_parser import YAMLParser


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DataPact Health Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 960px; margin: 40px auto; padding: 0 20px; color: #1a1a2e; }}
  h1 {{ color: #16213e; }}
  .summary {{ display: flex; gap: 16px; margin: 24px 0; }}
  .card {{ padding: 16px 24px; border-radius: 8px; flex: 1; text-align: center; }}
  .card.pass {{ background: #d4edda; color: #155724; }}
  .card.fail {{ background: #f8d7da; color: #721c24; }}
  .card .num {{ font-size: 2rem; font-weight: bold; }}
  table {{ width: 100%; border-collapse: collapse; margin: 24px 0; }}
  th {{ background: #16213e; color: white; padding: 10px 14px; text-align: left; }}
  td {{ padding: 10px 14px; border-bottom: 1px solid #e9ecef; }}
  tr:hover {{ background: #f8f9fa; }}
  .badge {{ padding: 3px 10px; border-radius: 12px; font-size: 0.8rem; font-weight: bold; }}
  .badge.pass {{ background: #d4edda; color: #155724; }}
  .badge.fail {{ background: #f8d7da; color: #721c24; }}
  .ts {{ color: #6c757d; font-size: 0.9rem; }}
</style>
</head>
<body>
<h1>DataPact Health Report</h1>
<p class="ts">Generated: {generated_at}</p>

<div class="summary">
  <div class="card pass"><div class="num">{passed}</div>Passed</div>
  <div class="card fail"><div class="num">{failed}</div>Failed</div>
</div>

<table>
  <tr><th>Contract</th><th>Table</th><th>Warehouse</th><th>Status</th><th>Failures</th></tr>
  {rows}
</table>
</body>
</html>"""

_ROW_TEMPLATE = """  <tr>
    <td>{name}</td>
    <td><code>{table}</code></td>
    <td>{warehouse}</td>
    <td><span class="badge {cls}">{status}</span></td>
    <td>{failures}</td>
  </tr>"""


def report_command(
    contracts_dir: str = typer.Option(
        "contracts",
        "--dir", "-d",
        help="Directory containing contract YAML files.",
    ),
    output: str = typer.Option(
        "warepact-report.html",
        "--output", "-o",
        help="Output HTML file path.",
    ),
    open_browser: bool = typer.Option(
        False, "--open", help="Open the report in a browser after generation."
    ),
) -> None:
    """Generate an HTML health report for all contracts."""
    _autodiscover()

    parser = YAMLParser()
    engine = ContractEngine(registry=PluginRegistry)
    directory = Path(contracts_dir)

    try:
        contracts = parser.parse_directory(directory)
    except Exception as exc:
        err_console.print(f"[red]Error loading contracts:[/] {exc}")
        raise typer.Exit(code=1)

    if not contracts:
        console.print(f"[yellow]No contracts found in {directory}/[/]")
        raise typer.Exit(code=0)

    results: list[ContractCheckResult] = []
    for contract in contracts:
        contract.credentials = load_credentials(contract.warehouse)
        try:
            results.append(engine.check(contract))
        except ContractCheckError as exc:
            err_console.print(f"[yellow]Warning:[/] {contract.name}: {exc}")

    # Build HTML
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    rows = "\n".join(
        _ROW_TEMPLATE.format(
            name=r.contract.name,
            table=r.contract.table,
            warehouse=r.contract.warehouse,
            cls="pass" if r.passed else "fail",
            status="PASSED" if r.passed else "FAILED",
            failures=", ".join(f.message for f in r.failures) or "—",
        )
        for r in results
    )

    html = _HTML_TEMPLATE.format(
        generated_at=generated_at,
        passed=passed,
        failed=failed,
        rows=rows,
    )

    out_path = Path(output)
    out_path.write_text(html, encoding="utf-8")

    console.print(
        f":bar_chart: Report written to [cyan]{out_path}[/] "
        f"({passed} passed, {failed} failed)"
    )

    if open_browser:
        import webbrowser
        webbrowser.open(out_path.resolve().as_uri())
