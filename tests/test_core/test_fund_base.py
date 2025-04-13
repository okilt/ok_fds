# tests/test_core/test_fund_base.py
# -*- coding: utf-8 -*-
"""
Tests for the BaseFund abstract base class.
"""
import pytest
import abc
import pandas as pd
from typing import Any
from src.core.fund_base import BaseFund
from src.dataprovider.interfaces import IDataProvider # Import for type hinting

# Create a dummy concrete implementation of BaseFund for testing purposes
class ConcreteFund(BaseFund):
    """A minimal concrete implementation for testing BaseFund."""

    async def fund_guidelines(self, data_provider: IDataProvider[Any]) -> dict[str, Any]:
        return {"test_guideline": "value"}

    async def fund_events(self, data_provider: IDataProvider[Any]) -> pd.DataFrame:
        return pd.DataFrame({'event': ['test']})

    async def aum_nav(self, data_provider: IDataProvider[Any], calculation_date: pd.Timestamp) -> pd.DataFrame:
        return pd.DataFrame({'aum': [100]})

    async def positions(self, data_provider: IDataProvider[Any], position_date: pd.Timestamp) -> pd.DataFrame:
        return pd.DataFrame({'asset': ['test_asset']})

# --- Test Cases ---

def test_base_fund_is_abc():
    """Verify that BaseFund is an Abstract Base Class."""
    assert issubclass(BaseFund, abc.ABC)

def test_cannot_instantiate_base_fund_directly():
    """Verify that BaseFund cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseFund(fund_id="abc", config={}) # Should fail

def test_concrete_fund_instantiation():
    """Verify that a concrete implementation can be instantiated."""
    fund_id = "concrete_test_001"
    config = {"name": "Test Fund", "currency": "USD"}
    fund = ConcreteFund(fund_id=fund_id, config=config)
    assert fund.fund_id == fund_id
    assert fund.config == config # Check if config is accessible

def test_base_fund_init_requires_fund_id():
    """Test that fund_id is required during initialisation."""
    with pytest.raises(ValueError, match="fund_id cannot be empty"):
        ConcreteFund(fund_id="", config={})

def test_base_fund_init_requires_non_none_config():
    """Test that config cannot be None."""
    with pytest.raises(ValueError, match="config dictionary cannot be None"):
        ConcreteFund(fund_id="test", config=None) # type: ignore

def test_base_fund_config_is_a_copy():
    """Test that the config property returns a copy."""
    fund_id = "copy_test"
    original_config = {"key": "value"}
    fund = ConcreteFund(fund_id=fund_id, config=original_config)
    
    config_copy = fund.config
    assert config_copy == original_config
    assert id(config_copy) != id(original_config) # Ensure it's a different object
    
    # Modify the returned copy and check the original is unchanged
    config_copy["key"] = "new_value"
    assert fund.config["key"] == "value" 

# We cannot easily test the abstract methods themselves here,
# as they are designed to be implemented by subclasses.
# Testing of their *behaviour* happens in the tests for concrete classes.