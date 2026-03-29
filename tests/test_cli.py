"""Tests for the Warepact CLI using Typer's CliRunner."""

from __future__ import annotations

import json
import textwrap
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from warepact.cli.main import app
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult


runner = CliRunner()


# ── Helpers ────────────────────────────────────────────────────────────────────

VALID_CONTRACT_YAML = textwrap.dedent("""\
    version: 1
    name: orders
    warehouse: mock_wh
    table: analytics.orders
""")


@pytest.fixture(autouse=True)
def clean_registry():
    PluginRegistry._reset()
    yield
    PluginRegistry._reset()


@pytest.fixture
def contracts_dir(tmp_path):
    d = tmp_path / "contracts"
    d.mkdir()
    (d / "orders.contract.yaml").write_text(VALID_CONTRACT_YAML)
    return d


@pytest.fixture
def mock_warehouse(clean_registry):
    """Register a no-op warehouse adapter under 'mock_wh'."""
    @PluginRegistry.register_warehouse("mock_wh")
    class _MockWH:
        def connect(self, credentials): pass
        def get_schema(self, t): return [{"name": "id", "type": "integer"}]
        def get_row_count(self, t): return 100
        def get_last_updated(self, t):
            from datetime import datetime, timezone
            return datetime.now(tz=timezone.utc)
        def run_query(self, sql): return [{"result": 0}]
        def get_null_rates(self, t, cols): return {c: 0.0 for c in cols}


# ── warepact --help ────────────────────────────────────────────────────────────

def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "data contracts" in result.output.lower()


# ── warepact init ──────────────────────────────────────────────────────────────

class TestInitCommand:
    def test_creates_directory_and_file(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        result = runner.invoke(app, [
            "init", str(contracts_dir),
            "--warehouse", "duckdb",
            "--name", "orders",
        ])
        assert result.exit_code == 0
        assert (contracts_dir / "orders.contract.yaml").exists()

    def test_skips_existing_directory(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        result = runner.invoke(app, ["init", str(contracts_dir), "--warehouse", "duckdb"])
        assert result.exit_code == 0
        assert "already exists" in result.output

    def test_skips_existing_file(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        (contracts_dir / "example.contract.yaml").write_text("placeholder")
        result = runner.invoke(app, ["init", str(contracts_dir), "--warehouse", "duckdb"])
        assert result.exit_code == 0
        assert (contracts_dir / "example.contract.yaml").read_text() == "placeholder"

    def test_scaffold_contains_warehouse_name(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        runner.invoke(app, [
            "init", str(contracts_dir),
            "--warehouse", "snowflake",
        ])
        content = (contracts_dir / "example.contract.yaml").read_text()
        assert "snowflake" in content


# ── warepact check ─────────────────────────────────────────────────────────────

class TestCheckCommand:
    def test_check_passes(self, contracts_dir, mock_warehouse):
        result = runner.invoke(app, [
            "check", "orders",
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0

    def test_check_missing_contract(self, contracts_dir, mock_warehouse):
        result = runner.invoke(app, [
            "check", "nonexistent",
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 1

    def test_check_all_passes(self, contracts_dir, mock_warehouse):
        result = runner.invoke(app, [
            "check", "--all",
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0
        assert "passed" in result.output.lower()

    def test_check_all_empty_dir(self, tmp_path, mock_warehouse):
        d = tmp_path / "empty"
        d.mkdir()
        result = runner.invoke(app, ["check", "--all", "--dir", str(d)])
        assert result.exit_code == 0
        assert "no contracts" in result.output.lower()

    def test_check_json_output(self, contracts_dir, mock_warehouse):
        result = runner.invoke(app, [
            "check", "orders",
            "--dir", str(contracts_dir),
            "--json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["contract"] == "orders"
        assert "passed" in data

    def test_check_fails_exits_1(self, contracts_dir, clean_registry):
        @PluginRegistry.register_warehouse("mock_wh")
        class _MockWH:
            def connect(self, c): pass
            def get_schema(self, t): return []
            def get_row_count(self, t): return 0
            def get_last_updated(self, t):
                from datetime import datetime, timezone
                return datetime.now(tz=timezone.utc)
            def run_query(self, sql): return []
            def get_null_rates(self, t, c): return {}

        @PluginRegistry.register_validator
        class _AlwaysFail:
            @property
            def name(self): return "always_fail"
            def validate(self, c, a):
                return ValidationResult(passed=False, message="injected failure")

        result = runner.invoke(app, [
            "check", "orders",
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 1

    def test_check_no_args_exits_1(self, contracts_dir):
        result = runner.invoke(app, ["check", "--dir", str(contracts_dir)])
        assert result.exit_code == 1


# ── warepact diff ──────────────────────────────────────────────────────────────

class TestDiffCommand:
    def test_diff_identical_contracts(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        f = contracts_dir / "orders.contract.yaml"
        f.write_text(VALID_CONTRACT_YAML)

        result = runner.invoke(app, [
            "diff", "orders", "current", "current",
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0
        assert "no differences" in result.output.lower()

    def test_diff_detects_table_change(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()

        v1 = contracts_dir / "v1.yaml"
        v1.write_text(VALID_CONTRACT_YAML)

        v2_yaml = VALID_CONTRACT_YAML.replace("analytics.orders", "analytics.orders_v2")
        v2 = contracts_dir / "v2.yaml"
        v2.write_text(v2_yaml)

        result = runner.invoke(app, [
            "diff", "orders",
            str(v1), str(v2),
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0
        assert "table" in result.output.lower()

    def test_diff_detects_column_removal(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()

        v1 = contracts_dir / "v1.yaml"
        v1.write_text(
            VALID_CONTRACT_YAML + "schema:\n  - column: id\n    type: integer\n"
        )
        v2 = contracts_dir / "v2.yaml"
        v2.write_text(VALID_CONTRACT_YAML)  # no schema

        result = runner.invoke(app, [
            "diff", "orders", str(v1), str(v2),
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0
        assert "removed" in result.output.lower()

    def test_diff_detects_column_addition(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()

        v1 = contracts_dir / "v1.yaml"
        v1.write_text(VALID_CONTRACT_YAML)  # no schema

        v2 = contracts_dir / "v2.yaml"
        v2.write_text(
            VALID_CONTRACT_YAML + "schema:\n  - column: new_col\n    type: string\n"
        )

        result = runner.invoke(app, [
            "diff", "orders", str(v1), str(v2),
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0
        assert "added" in result.output.lower()

    def test_diff_detects_type_change(self, tmp_path):
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()

        v1 = contracts_dir / "v1.yaml"
        v1.write_text(
            VALID_CONTRACT_YAML + "schema:\n  - column: id\n    type: integer\n"
        )
        v2 = contracts_dir / "v2.yaml"
        v2.write_text(
            VALID_CONTRACT_YAML + "schema:\n  - column: id\n    type: string\n"
        )

        result = runner.invoke(app, [
            "diff", "orders", str(v1), str(v2),
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0
        assert "type" in result.output.lower()


# ── warepact publish ───────────────────────────────────────────────────────────

class TestPublishCommand:
    def test_dry_run_passes(self, contracts_dir):
        result = runner.invoke(app, [
            "publish", "orders",
            "--dir", str(contracts_dir),
            "--no-check", "--dry-run",
        ])
        assert result.exit_code == 0
        assert "ok" in result.output.lower()

    def test_missing_contract_exits_1(self, contracts_dir):
        result = runner.invoke(app, [
            "publish", "nonexistent",
            "--dir", str(contracts_dir),
            "--no-check",
        ])
        assert result.exit_code == 1

    def test_publish_file_based_default(self, contracts_dir, tmp_path):
        """Default publish writes to a local directory."""
        out_dir = tmp_path / "registry"
        result = runner.invoke(app, [
            "publish", "orders",
            "--dir", str(contracts_dir),
            "--output", str(out_dir),
            "--no-check",
        ])
        assert result.exit_code == 0
        assert (out_dir / "orders.contract.yaml").exists()

    def test_publish_file_adds_metadata(self, contracts_dir, tmp_path):
        """Published YAML contains published_at and checksum fields."""
        import yaml as _yaml
        out_dir = tmp_path / "registry"
        runner.invoke(app, [
            "publish", "orders",
            "--dir", str(contracts_dir),
            "--output", str(out_dir),
            "--no-check",
        ])
        content = _yaml.safe_load((out_dir / "orders.contract.yaml").read_text())
        assert "published_at" in content
        assert "checksum" in content

    def test_publish_success(self, contracts_dir, tmp_path):
        mock_resp = MagicMock()
        mock_resp.status = 201
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        with patch("urllib.request.urlopen", return_value=mock_resp), \
             patch.dict("os.environ", {"DATAPACT_API_KEY": "test-key"}):
            result = runner.invoke(app, [
                "publish", "orders",
                "--dir", str(contracts_dir),
                "--output", str(tmp_path / "registry"),
                "--registry", "https://registry.example.com",
                "--no-check",
            ])
        assert result.exit_code == 0
        assert "Published" in result.output or "published" in result.output.lower()

    def test_publish_uses_put_with_update_flag(self, contracts_dir, tmp_path):
        captured = []
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured.append(req.method)
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen), \
             patch.dict("os.environ", {"DATAPACT_API_KEY": "key"}):
            runner.invoke(app, [
                "publish", "orders",
                "--dir", str(contracts_dir),
                "--output", str(tmp_path / "registry"),
                "--registry", "https://registry.example.com",
                "--update", "--no-check",
            ])
        assert captured and captured[0] == "PUT"

    def test_publish_409_shows_hint(self, contracts_dir, tmp_path):
        import urllib.error
        exc = urllib.error.HTTPError(
            url="u", code=409, msg="Conflict",
            hdrs=None, fp=MagicMock(read=lambda: b"exists"),  # type: ignore[arg-type]
        )
        with patch("urllib.request.urlopen", side_effect=exc), \
             patch.dict("os.environ", {"DATAPACT_API_KEY": "key"}):
            result = runner.invoke(app, [
                "publish", "orders",
                "--dir", str(contracts_dir),
                "--output", str(tmp_path / "registry"),
                "--registry", "https://registry.example.com",
                "--no-check",
            ])
        assert result.exit_code == 1
        assert "--update" in result.output

    def test_publish_dry_run_shows_json(self, contracts_dir):
        result = runner.invoke(app, [
            "publish", "orders",
            "--dir", str(contracts_dir),
            "--no-check", "--dry-run",
        ])
        assert result.exit_code == 0
        assert "orders" in result.output


# ── warepact report ────────────────────────────────────────────────────────────

class TestReportCommand:
    def test_generates_html(self, contracts_dir, mock_warehouse, tmp_path):
        out = tmp_path / "report.html"
        result = runner.invoke(app, [
            "report",
            "--dir", str(contracts_dir),
            "--output", str(out),
        ])
        assert result.exit_code == 0
        assert out.exists()
        content = out.read_text()
        assert "<html" in content
        assert "orders" in content

    def test_empty_dir_exits_cleanly(self, tmp_path, mock_warehouse):
        d = tmp_path / "empty"
        d.mkdir()
        out = tmp_path / "report.html"
        result = runner.invoke(app, [
            "report",
            "--dir", str(d),
            "--output", str(out),
        ])
        assert result.exit_code == 0


# ── warepact watch ─────────────────────────────────────────────────────────────

class TestWatchCommand:
    """Watch runs an infinite loop — tests use KeyboardInterrupt to stop it."""

    def test_watch_runs_once_then_exits_on_interrupt(
        self, contracts_dir, mock_warehouse
    ):
        call_count = [0]

        def fake_sleep(n):
            call_count[0] += 1
            raise KeyboardInterrupt

        with patch("warepact.cli.watch.time.sleep", fake_sleep):
            result = runner.invoke(app, [
                "watch",
                "--dir", str(contracts_dir),
                "--interval", "5",
            ])

        assert result.exit_code == 0
        assert call_count[0] == 1

    def test_watch_shows_next_check_time(self, contracts_dir, mock_warehouse):
        def fake_sleep(n):
            raise KeyboardInterrupt

        with patch("warepact.cli.watch.time.sleep", fake_sleep):
            result = runner.invoke(app, [
                "watch",
                "--dir", str(contracts_dir),
                "--interval", "60",
            ])

        assert "Next check" in result.output

    def test_watch_cron_flag_accepted(self, contracts_dir, mock_warehouse):
        def fake_sleep(n):
            raise KeyboardInterrupt

        with patch("warepact.cli.watch.time.sleep", fake_sleep):
            result = runner.invoke(app, [
                "watch",
                "--dir", str(contracts_dir),
                "--cron", "0 * * * *",
            ])

        assert result.exit_code == 0
        assert "cron" in result.output

    def test_watch_invalid_cron_shows_error(self, contracts_dir):
        result = runner.invoke(app, [
            "watch",
            "--dir", str(contracts_dir),
            "--cron", "not a cron",
        ])
        assert result.exit_code != 0 or "invalid" in result.output.lower()

    def test_watch_single_contract(self, contracts_dir, mock_warehouse):
        def fake_sleep(n):
            raise KeyboardInterrupt

        with patch("warepact.cli.watch.time.sleep", fake_sleep):
            result = runner.invoke(app, [
                "watch",
                "--dir", str(contracts_dir),
                "--interval", "10",
                "orders",
            ])

        assert result.exit_code == 0

    def test_seconds_until_next_returns_interval_without_cron(self):
        from warepact.cli.watch import _seconds_until_next
        assert _seconds_until_next(None, 300) == 300

    def test_seconds_until_next_cron_returns_positive(self):
        from warepact.cli.watch import _seconds_until_next
        secs = _seconds_until_next("0 * * * *", 300)
        assert 0 < secs <= 3600


# ── warepact generate ──────────────────────────────────────────────────────────

class TestGenerateCommand:
    @pytest.fixture
    def mock_wh_generate(self, clean_registry, tmp_path):
        """Register a warehouse adapter that returns a known schema."""
        @PluginRegistry.register_warehouse("mock_wh")
        class _MockWH:
            def connect(self, credentials): pass
            def get_schema(self, t):
                return [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "score", "type": "DOUBLE"},
                ]
            def get_row_count(self, t): return 1000
            def get_last_updated(self, t):
                from datetime import datetime, timezone
                return datetime.now(tz=timezone.utc)
            def run_query(self, sql): return []
            def get_null_rates(self, t, cols): return {c: 0.0 for c in cols}

        return tmp_path / "contracts"

    def test_generate_creates_yaml_file(self, mock_wh_generate, tmp_path):
        contracts_dir = mock_wh_generate
        result = runner.invoke(app, [
            "generate", "db.schema.orders",
            "--warehouse", "mock_wh",
            "--dir", str(contracts_dir),
        ])
        assert result.exit_code == 0
        out_file = contracts_dir / "orders.contract.yaml"
        assert out_file.exists()

    def test_generate_yaml_contains_all_columns(self, mock_wh_generate, tmp_path):
        import yaml as _yaml
        contracts_dir = mock_wh_generate
        runner.invoke(app, [
            "generate", "db.schema.orders",
            "--warehouse", "mock_wh",
            "--dir", str(contracts_dir),
        ])
        data = _yaml.safe_load((contracts_dir / "orders.contract.yaml").read_text())
        col_names = [c["column"] for c in data["schema"]]
        assert "id" in col_names
        assert "name" in col_names
        assert "score" in col_names

    def test_generate_yaml_has_sla_min_rows(self, mock_wh_generate, tmp_path):
        import yaml as _yaml
        contracts_dir = mock_wh_generate
        runner.invoke(app, [
            "generate", "db.schema.orders",
            "--warehouse", "mock_wh",
            "--dir", str(contracts_dir),
        ])
        data = _yaml.safe_load((contracts_dir / "orders.contract.yaml").read_text())
        assert data["sla"]["min_rows"] == 500  # 1000 // 2

    def test_generate_normalises_types(self, mock_wh_generate, tmp_path):
        import yaml as _yaml
        contracts_dir = mock_wh_generate
        runner.invoke(app, [
            "generate", "db.schema.orders",
            "--warehouse", "mock_wh",
            "--dir", str(contracts_dir),
        ])
        data = _yaml.safe_load((contracts_dir / "orders.contract.yaml").read_text())
        types = {c["column"]: c["type"] for c in data["schema"]}
        assert types["id"] == "integer"
        assert types["name"] == "string"
        assert types["score"] == "float"

    def test_generate_unknown_warehouse_exits_1(self, clean_registry, tmp_path):
        result = runner.invoke(app, [
            "generate", "db.schema.orders",
            "--warehouse", "nonexistent_wh",
            "--dir", str(tmp_path),
        ])
        assert result.exit_code == 1

    def test_generate_custom_output_path(self, mock_wh_generate, tmp_path):
        contracts_dir = mock_wh_generate
        result = runner.invoke(app, [
            "generate", "db.schema.orders",
            "--warehouse", "mock_wh",
            "--dir", str(contracts_dir),
            "--output", "custom_name.contract.yaml",
        ])
        assert result.exit_code == 0
        assert (contracts_dir / "custom_name.contract.yaml").exists()


# ── warepact init (env detection) ─────────────────────────────────────────────

class TestInitCommandEnvDetection:
    def test_init_scaffold_contains_default_warehouse(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("BIGQUERY_PROJECT", raising=False)

        contracts_dir = tmp_path / "contracts"
        result = runner.invoke(app, ["init", str(contracts_dir)])
        assert result.exit_code == 0
        yaml_files = list(contracts_dir.glob("*.contract.yaml"))
        assert yaml_files, "init should create at least one example contract"
        content = yaml_files[0].read_text()
        assert "warehouse:" in content

    def test_init_detects_snowflake_from_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myaccount")
        contracts_dir = tmp_path / "contracts2"
        result = runner.invoke(app, ["init", str(contracts_dir)])
        assert result.exit_code == 0
        yaml_files = list(contracts_dir.glob("*.contract.yaml"))
        content = yaml_files[0].read_text()
        assert "snowflake" in content


# ── warepact mcp ───────────────────────────────────────────────────────────────

class TestMcpCommand:
    def test_mcp_starts_server(self):
        with patch("warepact.mcp.server.run_server") as mock_run:
            result = runner.invoke(app, ["mcp"])
            assert result.exit_code == 0
            mock_run.assert_called_once()

    def test_mcp_passes_host_and_port(self):
        with patch("warepact.mcp.server.run_server") as mock_run:
            result = runner.invoke(app, ["mcp", "--host", "0.0.0.0", "--port", "9000"])
            assert result.exit_code == 0
            mock_run.assert_called_once_with(host="0.0.0.0", port=9000)

    def test_mcp_import_error_exits_nonzero(self):
        import sys
        with patch.dict(sys.modules, {"warepact.mcp.server": None}):
            result = runner.invoke(app, ["mcp"])
            assert result.exit_code == 1

    def test_mcp_import_error_prints_message(self):
        import sys
        with patch.dict(sys.modules, {"warepact.mcp.server": None}):
            result = runner.invoke(app, ["mcp"])
            assert "not installed" in result.output.lower() or result.exit_code == 1


# ── main() entry point ─────────────────────────────────────────────────────────

class TestMainEntryPoint:
    def test_main_invokes_app(self):
        from unittest.mock import patch as _patch
        with _patch("warepact.cli.main.app") as mock_app:
            from warepact.cli.main import main
            main()
            mock_app.assert_called_once()
