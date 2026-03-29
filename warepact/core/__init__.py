"""DataPact core — Contract dataclass, PluginRegistry, and typed exceptions."""

from warepact.core.contract import (
    AlertSpec,
    ColumnSpec,
    ConsumerSpec,
    Contract,
    CustomCheckSpec,
    ScheduleSpec,
    SLASpec,
)
from warepact.core.exceptions import (
    ContractCheckError,
    ContractNotFoundError,
    ContractValidationError,
    DataPactError,
    UnknownAlertChannelError,
    UnknownWarehouseError,
    WarehouseConnectionError,
)
from warepact.core.engine import ContractCheckResult, ContractEngine, LLMExplainer
from warepact.core.registry import PluginRegistry

__all__ = [
    "ContractCheckResult",
    "ContractEngine",
    "LLMExplainer",
    "AlertSpec",
    "ColumnSpec",
    "ConsumerSpec",
    "Contract",
    "ContractCheckError",
    "ContractNotFoundError",
    "ContractValidationError",
    "CustomCheckSpec",
    "DataPactError",
    "PluginRegistry",
    "ScheduleSpec",
    "SLASpec",
    "UnknownAlertChannelError",
    "UnknownWarehouseError",
    "WarehouseConnectionError",
]
