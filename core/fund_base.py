# core/fund_base.py
# -*- coding: utf-8 -*-
"""
Defines the Abstract Base Class (ABC) for all Fund implementations.

All concrete Fund classes (located in the 'funds/' subdirectories) must inherit
from BaseFund and implement its abstract methods.
"""
from __future__ import annotations
import abc
import pandas as pd  # Assuming pandas DataFrames are commonly used
from typing import Any, Protocol # Use Protocol for structural typing of provider
from dataprovider.interfaces import IDataProvider # Import the high-level interface

class BaseFund(abc.ABC):
    """
    Abstract Base Class for all investment fund representations.

    Provides a common structure and enforces implementation of key functionalities.
    Each concrete fund implementation will inherit from this class (or an
    intermediate class that inherits from this).
    """

    def __init__(self, fund_id: str, config: dict[str, Any]):
        """
        Initialise the BaseFund.

        Args:
            fund_id: A unique identifier for the fund instance.
            config: A dictionary containing fund-specific configuration loaded
                    from its corresponding YAML file.
        """
        if not fund_id:
            raise ValueError("fund_id cannot be empty.")
        if config is None: # Allow empty config, but not None
             raise ValueError("config dictionary cannot be None.")

        self._fund_id = fund_id
        self._config = config
        # You might want to validate the config structure here, e.g. using Pydantic

    @property
    def fund_id(self) -> str:
        """Return the unique identifier of the fund."""
        return self._fund_id

    @property
    def config(self) -> dict[str, Any]:
        """Return the fund-specific configuration."""
        # Return a copy to prevent external modification of the internal state
        return self._config.copy()

    # --- Abstract methods to be implemented by concrete fund classes ---

    @abc.abstractmethod
    async def fund_guidelines(self, data_provider: IDataProvider[Any]) -> dict[str, Any]:
        """
        Retrieve or calculate the fund's investment guidelines.

        Args:
            data_provider: An instance confirming to the IDataProvider interface,
                           used to fetch any required underlying data.

        Returns:
            A dictionary representing the fund's guidelines.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def fund_events(self, data_provider: IDataProvider[Any]) -> pd.DataFrame:
        """
        Retrieve significant events related to the fund (e.g., capital calls, distributions).

        Args:
            data_provider: An IDataProvider instance.

        Returns:
            A pandas DataFrame containing fund event data.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def aum_nav(self, data_provider: IDataProvider[Any], calculation_date: pd.Timestamp) -> pd.DataFrame:
        """
        Calculate or retrieve the fund's Assets Under Management (AUM) and Net Asset Value (NAV).

        Args:
            data_provider: An IDataProvider instance.
            calculation_date: The date for which to calculate AUM/NAV.

        Returns:
            A pandas DataFrame containing AUM/NAV information, potentially per share class.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def positions(self, data_provider: IDataProvider[Any], position_date: pd.Timestamp) -> pd.DataFrame:
        """
        Retrieve the fund's investment positions as of a specific date.

        Args:
            data_provider: An IDataProvider instance.
            position_date: The date for which to retrieve positions.

        Returns:
            A pandas DataFrame detailing the fund's positions.
        """
        raise NotImplementedError

    # Add other common abstract methods as needed...
    # async def calculate_performance(...) -> ...
    # async def check_compliance(...) -> ...