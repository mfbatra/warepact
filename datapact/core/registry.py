"""PluginRegistry — the plug-and-play extension point for DataPact.

Adapters self-register by decorating their class:

    @PluginRegistry.register_warehouse("snowflake")
    class SnowflakeAdapter(WarehouseAdapter): ...

Core code never imports adapters directly. It asks the registry:

    adapter = PluginRegistry.get_warehouse("snowflake")
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import TYPE_CHECKING, Callable, Type

from datapact.core.exceptions import UnknownAlertChannelError, UnknownWarehouseError

if TYPE_CHECKING:
    from datapact.interfaces.alerting import AlertChannel
    from datapact.interfaces.validator import Validator
    from datapact.interfaces.warehouse import WarehouseAdapter


class PluginRegistry:
    """
    Central registry for all runtime-swappable components.

    All state is held at class level — the registry is effectively a
    singleton namespace, not an instance.
    """

    _warehouse_adapters: dict[str, Type[WarehouseAdapter]] = {}
    _alert_channels: dict[str, Type[AlertChannel]] = {}
    _validators: list[Type[Validator]] = []

    # ── Registration decorators ───────────────────────────────────────────────

    @classmethod
    def register_warehouse(cls, name: str) -> Callable[[Type[WarehouseAdapter]], Type[WarehouseAdapter]]:
        """
        Class decorator that registers a WarehouseAdapter under *name*.

        Usage::

            @PluginRegistry.register_warehouse("duckdb")
            class DuckDBAdapter(WarehouseAdapter): ...
        """
        def decorator(adapter_class: Type[WarehouseAdapter]) -> Type[WarehouseAdapter]:
            cls._warehouse_adapters[name] = adapter_class
            return adapter_class
        return decorator

    @classmethod
    def register_alert_channel(cls, name: str) -> Callable[[Type[AlertChannel]], Type[AlertChannel]]:
        """
        Class decorator that registers an AlertChannel under *name*.

        Usage::

            @PluginRegistry.register_alert_channel("slack")
            class SlackChannel(AlertChannel): ...
        """
        def decorator(channel_class: Type[AlertChannel]) -> Type[AlertChannel]:
            cls._alert_channels[name] = channel_class
            return channel_class
        return decorator

    @classmethod
    def register_validator(cls, validator_class: Type[Validator]) -> Type[Validator]:
        """
        Class decorator that appends a Validator to the global list.

        Usage::

            @PluginRegistry.register_validator
            class SchemaValidator(Validator): ...
        """
        if validator_class not in cls._validators:
            cls._validators.append(validator_class)
        return validator_class

    # ── Lookup ────────────────────────────────────────────────────────────────

    @classmethod
    def get_warehouse(cls, name: str) -> WarehouseAdapter:
        """Return a fresh instance of the adapter registered under *name*."""
        if name not in cls._warehouse_adapters:
            raise UnknownWarehouseError(
                f"No adapter registered for warehouse '{name}'. "
                f"Available: {sorted(cls._warehouse_adapters)}"
            )
        return cls._warehouse_adapters[name]()

    @classmethod
    def get_alert_channel(cls, name: str) -> AlertChannel:
        """Return a fresh instance of the channel registered under *name*."""
        if name not in cls._alert_channels:
            raise UnknownAlertChannelError(
                f"No alert channel registered for '{name}'. "
                f"Available: {sorted(cls._alert_channels)}"
            )
        return cls._alert_channels[name]()

    @classmethod
    def get_validators(cls) -> list[Type[Validator]]:
        """Return all registered validator classes."""
        return list(cls._validators)

    # ── Introspection ─────────────────────────────────────────────────────────

    @classmethod
    def list_warehouses(cls) -> list[str]:
        return sorted(cls._warehouse_adapters)

    @classmethod
    def list_alert_channels(cls) -> list[str]:
        return sorted(cls._alert_channels)

    # ── Autodiscovery ─────────────────────────────────────────────────────────

    @classmethod
    def autodiscover(cls) -> None:
        """
        Import every module under datapact/adapters/ and datapact/plugins/.

        Importing a module that contains @register_* decorators is enough to
        trigger registration — no explicit calls needed.
        """
        for package_name in ("datapact.adapters.warehouses", "datapact.adapters.alerting"):
            cls._import_package(package_name)

        # Community plugins installed as datapact.plugins.*
        cls._import_package("datapact.plugins")

    @classmethod
    def _import_package(cls, package_name: str) -> None:
        import sys
        try:
            package = importlib.import_module(package_name)
        except ModuleNotFoundError:
            return
        package_path = getattr(package, "__path__", None)
        if package_path is None:
            return
        for _, module_name, _ in pkgutil.iter_modules(package_path):
            full_name = f"{package_name}.{module_name}"
            if full_name in sys.modules:
                # Already imported — reload so @register_* decorators re-run
                importlib.reload(sys.modules[full_name])
            else:
                importlib.import_module(full_name)

    # ── Test helpers (only call from tests) ──────────────────────────────────

    @classmethod
    def _reset(cls) -> None:
        """Clear all registrations. Used between tests to avoid leakage."""
        cls._warehouse_adapters = {}
        cls._alert_channels = {}
        cls._validators = []
