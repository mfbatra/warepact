"""DataPact core — Contract dataclass, PluginRegistry, and typed exceptions."""

from datapact.core.contract import (
    AlertSpec,
    ColumnSpec,
    ConsumerSpec,
    Contract,
    CustomCheckSpec,
    ScheduleSpec,
    SLASpec,
)
from datapact.core.exceptions import (
    ContractCheckError,
    ContractNotFoundError,
    ContractValidationError,
    DataPactError,
    UnknownAlertChannelError,
    UnknownWarehouseError,
    WarehouseConnectionError,
)
from datapact.core.engine import ContractCheckResult, ContractEngine, LLMExplainer
from datapact.core.registry import PluginRegistry

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
