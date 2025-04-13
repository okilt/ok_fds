# caching/interface.py
# -*- coding: utf-8 -*-
"""
Defines the Abstract Base Class for all cache provider implementations.
Ensures a consistent interface for getting, setting, and deleting cache entries.
"""
from __future__ import annotations
import abc
from typing import Any, Optional, TypeVar, Protocol, runtime_checkable

# Define a type variable for cached values
ValueType = TypeVar('ValueType')
# Define a type variable for cache keys (typically string or tuple)
KeyType = TypeVar('KeyType')

class ICacheProvider(abc.ABC, Protocol[KeyType, ValueType]):
    """
    Abstract Base Class (Interface) for cache providers.

    Defines the essential asynchronous methods required for cache operations.
    This uses Protocol to allow for structural subtyping if needed,
    alongside ABC for explicit inheritance checks.
    """

    @abc.abstractmethod
    async def get(self, key: KeyType) -> Optional[ValueType]:
        """
        Retrieve an item from the cache.

        Args:
            key: The key identifying the item in the cache.

        Returns:
            The cached value if found and valid, otherwise None.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def set(
        self,
        key: KeyType,
        value: ValueType,
        ttl: Optional[int] = None,
        dependencies: Optional[list[Any]] = None
    ) -> None:
        """
        Store an item in the cache.

        Args:
            key: The key under which to store the item.
            value: The value to store.
            ttl: Time To Live in seconds. If None, potentially use a default
                 or cache indefinitely (behaviour depends on implementation).
            dependencies: An optional list of dependency keys or identifiers
                          used for potential dependency-based invalidation.
                          The exact structure is implementation-dependent.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete(self, key: KeyType) -> bool:
        """
        Remove an item from the cache.

        Args:
            key: The key of the item to remove.

        Returns:
            True if an item was deleted, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def clear(self) -> bool:
        """
        Remove all items from the cache instance.

        Returns:
            True if the cache was cleared successfully, False otherwise.
        """
        raise NotImplementedError

    # Optional: Add methods for invalidation later if needed
    # @abc.abstractmethod
    # async def invalidate_by_dependency(self, dependency_key: Any) -> int:
    #     """
    #     Invalidate cache entries based on a dependency.
    #     Returns the number of invalidated entries.
    #     """
    #     raise NotImplementedError