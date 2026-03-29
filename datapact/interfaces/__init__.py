"""DataPact abstract interfaces — the contracts that every plugin must honour."""

from datapact.interfaces.alerting import AlertChannel
from datapact.interfaces.store import ContractStore
from datapact.interfaces.validator import ValidationResult, Validator
from datapact.interfaces.warehouse import (
    IFreshnessCheckable,
    INullCheckable,
    IQueryable,
    ISchemaValidatable,
    IVolumeCheckable,
    WarehouseAdapter,
)

__all__ = [
    "AlertChannel",
    "ContractStore",
    "IFreshnessCheckable",
    "INullCheckable",
    "IQueryable",
    "ISchemaValidatable",
    "IVolumeCheckable",
    "ValidationResult",
    "Validator",
    "WarehouseAdapter",
]
