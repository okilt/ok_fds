# dataprovider/interfaces.py
# -*- coding: utf-8 -*-
"""
Defines Abstract Base Classes for Data Providers.

- IDataProvider: High-level interface used by Funds and FundFactory.
- ILowLevelProvider: Interface for specific data source connectors (SQL, API etc.).
"""
from __future__ import annotations
import abc
from typing import Any, Protocol, runtime_checkable, TypeVar, Generic

# Define a type variable for the data returned by providers
DataType = TypeVar('DataType')

@runtime_checkable
class ILowLevelProvider(Protocol):
    """
    Interface for low-level data providers that connect to specific sources.
    e.g., a specific SQL database, a particular REST API endpoint.
    """

    @abc.abstractmethod
    async def fetch(self, request_details: Any) -> Any:
        """
        Fetch raw data from the specific source based on request details.

        Args:
            request_details: An object or dictionary containing all necessary
                             information to perform the data fetch operation
                             (e.g., query parameters, API endpoint, SQL query).

        Returns:
            The raw data fetched from the source. The exact type depends
            on the source and implementation (e.g., list of dicts, DataFrame).

        Raises:
            Various exceptions depending on potential issues like network errors,
            authentication failures, database errors, etc.
        """
        raise NotImplementedError

@runtime_checkable
class IDataProvider(Protocol, Generic[DataType]):
    """
    High-level data provider interface used by the application logic (Funds, Factory).

    This interface abstracts away the complexities of caching, data source selection,
    and request optimisation.
    """

    @abc.abstractmethod
    async def get_data(self, data_key: Any, **kwargs: Any) -> DataType:
        """
        Retrieve processed or raw data based on a logical data key and parameters.

        This method is responsible for checking caches, managing in-flight requests,
        resolving dependencies (potentially), and delegating to appropriate
        lower-level providers if data needs to be fetched.

        Args:
            data_key: A logical identifier for the type of data needed
                      (e.g., 'positions', 'aum_nav', 'market_price').
                      The exact structure can be defined (e.g., Enum, str, tuple).
            **kwargs: Additional parameters required for the specific data request,
                      such as 'fund_id', 'date', 'asset_ids', 'start_date', etc.

        Returns:
            The requested data, potentially processed or structured.

        Raises:
            DataNotFoundError: If the requested data cannot be found or computed.
            DataProviderError: For general errors during data retrieval or processing.
        """
        raise NotImplementedError

    # Potentially add batch methods later for optimisation
    # @abc.abstractmethod
    # async def get_batch_data(self, requests: list[tuple[Any, dict]]) -> list[Any]:
    #     """ Fetch multiple data points in a potentially optimised way. """
    #     raise NotImplementedError

# Define potential custom exceptions
class DataProviderError(Exception):
    """Base exception for data provider errors."""
    pass

class DataNotFoundError(DataProviderError):
    """Exception raised when requested data cannot be found."""
    pass