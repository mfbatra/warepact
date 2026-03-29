"""DataPact abstract interfaces — the contracts that every plugin must honour."""

from warepact.interfaces.alerting import AlertChannel
from warepact.interfaces.store import ContractStore
from warepact.interfaces.validator import ValidationResult, Validator
from warepact.interfaces.warehouse import (
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
