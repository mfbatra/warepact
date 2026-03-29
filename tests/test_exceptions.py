"""Tests for core/exceptions.py."""

import pytest

from warepact.core.exceptions import (
    ContractCheckError,
    ContractNotFoundError,
    ContractValidationError,
    DataPactError,
    UnknownAlertChannelError,
    UnknownWarehouseError,
    WarehouseConnectionError,
)


ALL_EXCEPTIONS = [
    UnknownWarehouseError,
    UnknownAlertChannelError,
    ContractNotFoundError,
    ContractValidationError,
    WarehouseConnectionError,
    ContractCheckError,
]


class TestExceptionHierarchy:
    @pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
    def test_inherits_from_warepact_error(self, exc_class):
        assert issubclass(exc_class, DataPactError)

    @pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
    def test_inherits_from_base_exception(self, exc_class):
        assert issubclass(exc_class, Exception)

    @pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
    def test_can_be_raised_and_caught_as_warepact_error(self, exc_class):
        with pytest.raises(DataPactError):
            raise exc_class("test message")

    @pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
    def test_message_preserved(self, exc_class):
        try:
            raise exc_class("something went wrong")
        except exc_class as e:
            assert "something went wrong" in str(e)
