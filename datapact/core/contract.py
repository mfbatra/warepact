"""Contract dataclass — pure data, no business logic.

Parsed from .contract.yaml by parsers/yaml_parser.py.
Validated by Pydantic v2 on construction.
"""

from __future__ import annotations

import os
import re
from typing import Any, Literal, cast

from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator


# ── Sub-models ─────────────────────────────────────────────────────────────────

class ColumnSpec(BaseModel):
    """Definition of a single expected column."""

    column: str
    type: str
    not_null: bool = False
    unique: bool = False
    allowed_values: list[Any] | None = None
    min: float | None = None
    max: float | None = None


class SLASpec(BaseModel):
    """Service-level agreement thresholds."""

    freshness_hours: float | None = None
    min_rows: int | None = None
    max_rows: int | None = None
    max_null_rate: float | None = Field(default=None, ge=0.0, le=1.0)


class CustomCheckSpec(BaseModel):
    """User-defined SQL assertion."""

    name: str
    sql: str
    expected: Any  # typically 0 (no bad rows)


class ConsumerSpec(BaseModel):
    """A downstream team or dashboard that depends on this contract."""

    team: str | None = None
    dashboard: str | None = None


class ScheduleSpec(BaseModel):
    """Expected delivery schedule."""

    frequency: Literal["hourly", "daily", "weekly", "monthly"] | None = None
    expected_by: str | None = None  # e.g. "06:00 UTC"


class AlertSpec(BaseModel):
    """Alert destination config as written in the YAML."""

    channel: str
    on: list[Literal["breach", "recovery"]] = Field(
        default_factory=lambda: cast(list[Literal["breach", "recovery"]], ["breach"])
    )
    # All other keys (webhook_url, to, …) are kept in extra fields.
    model_config = {"extra": "allow"}


# ── Root contract model ────────────────────────────────────────────────────────

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _expand_env(value: str) -> str:
    """Replace ${VAR} placeholders with environment variable values."""
    def _replace(match: re.Match[str]) -> str:
        var = match.group(1)
        result = os.environ.get(var)
        if result is None:
            raise ValueError(f"Environment variable '{var}' is not set")
        return result
    return _ENV_VAR_RE.sub(_replace, value)


class Contract(BaseModel):
    """
    The central data object — what a data contract IS.

    No methods beyond validation. Business logic lives in validators/ and
    core/engine.py.
    """

    version: int = 1
    name: str
    description: str | None = None
    owner: EmailStr | None = None

    # Warehouse
    warehouse: str  # matches a PluginRegistry key, e.g. "snowflake"
    table: str

    # Optional structure
    consumers: list[ConsumerSpec] = Field(default_factory=list)
    schedule: ScheduleSpec | None = None
    schema_: list[ColumnSpec] | None = Field(default=None, alias="schema")
    sla: SLASpec | None = None
    custom_checks: list[CustomCheckSpec] = Field(default_factory=list)
    alerts: list[AlertSpec] = Field(default_factory=list)

    # Metadata
    tags: list[str] = Field(default_factory=list)
    pii: bool = False

    # Credentials are injected at runtime — never stored in the YAML
    credentials: dict[str, Any] = Field(default_factory=dict, exclude=True)

    model_config = {"populate_by_name": True}

    @field_validator("name")
    @classmethod
    def _name_must_be_slug(cls, v: str) -> str:
        if not re.match(r"^[a-z0-9_-]+$", v):
            raise ValueError(
                f"Contract name '{v}' must contain only lowercase letters, "
                "digits, hyphens, and underscores."
            )
        return v

    @field_validator("version")
    @classmethod
    def _version_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("version must be >= 1")
        return v

    @model_validator(mode="after")
    def _expand_alert_env_vars(self) -> Contract:
        """Expand ${ENV_VAR} placeholders in alert configs at parse time."""
        for alert in self.alerts:
            extra = alert.model_extra or {}
            if not extra:
                continue
            updated = {
                k: (_expand_env(v) if isinstance(v, str) else v)
                for k, v in extra.items()
            }
            # Pydantic v2 stores extra fields in __pydantic_extra__
            if alert.__pydantic_extra__ is not None:
                alert.__pydantic_extra__.update(updated)
        return self

    # ── Convenience properties ────────────────────────────────────────────────

    @property
    def alert_channels(self) -> list[str]:
        """Unique list of channel types configured for this contract."""
        return list(dict.fromkeys(a.channel for a in self.alerts))

    @property
    def columns(self) -> list[ColumnSpec]:
        """Shorthand for schema_ (avoids the alias in call-sites)."""
        return self.schema_ or []
