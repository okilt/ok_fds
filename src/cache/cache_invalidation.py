# src/cache/cache_invalidation.py
"""
Cache Invalidation Module.

This module defines strategies and mechanisms for intelligently invalidating
cached data based on dependencies and potential source data changes.
"""

from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Set, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .cache_manager import CacheManager  # Prevent circular import

logger = logging.getLogger(__name__)


class BaseInvalidationStrategy(ABC):
    """
    Abstract Base Class for cache invalidation strategies.

    Defines the interface for determining which cache keys should be
    invalidated based on certain triggers or conditions.
    """

    def __init__(self, cache_manager: "CacheManager"):
        """
        Initialise the invalidation strategy.

        Args:
            cache_manager: The central cache manager instance.
        """
        self._cache_manager = cache_manager
        logger.info(f"Initialising invalidation strategy: {self.__class__.__name__}")

    @abstractmethod
    def invalidate(self, trigger_info: Any) -> Set[Any]:
        """
        Determine and return the set of cache keys to invalidate.

        Args:
            trigger_info: Information about the event or data change
                          that triggered the invalidation check. This could be
                          a specific data key, a source name, or other relevant data.

        Returns:
            A set of cache keys that should be invalidated.
        """
        pass


class DependencyBasedInvalidation(BaseInvalidationStrategy):
    """
    Invalidation strategy based on tracked data dependencies.

    Invalidates cache entries that depend directly or indirectly on the
    data specified in the trigger information.
    """

    def invalidate(self, trigger_info: Any) -> Set[Any]:
        """
        Invalidate keys based on dependency tracking.

        Args:
            trigger_info: Typically, the cache key of the data that has changed
                          or become stale.

        Returns:
            A set of dependent cache keys to invalidate.
        """
        stale_key = trigger_info
        logger.info(f"Initiating dependency-based invalidation for key: {stale_key}")

        # Dependency tracking logic will reside elsewhere (e.g., DependencyUtils)
        # and will be accessed via the cache manager or a dedicated dependency manager.
        # For now, this is a placeholder.
        dependent_keys = self._cache_manager.get_dependents(stale_key)

        if dependent_keys:
            logger.info(
                f"Found {len(dependent_keys)} dependent keys to invalidate for {stale_key}: {dependent_keys}"
            )
        else:
            logger.debug(f"No dependent keys found for {stale_key}")

        # The initial key itself might also need invalidation
        keys_to_invalidate = dependent_keys.union({stale_key})

        return keys_to_invalidate


class TimeBasedInvalidation(BaseInvalidationStrategy):
    """
    Invalidation strategy based on Time-To-Live (TTL).

    This strategy might be less about *triggering* invalidation and more
    about how the cache manager handles expiry, but could be used
    to proactively clean up expired entries.
    """

    def invalidate(self, trigger_info: Any = None) -> Set[Any]:
        """
        Identify keys that have expired based on their TTL.

        Args:
            trigger_info: Currently unused for this strategy, but kept for
                          interface consistency.

        Returns:
            A set of expired cache keys.
        """
        logger.info("Initiating time-based invalidation check (scan for expired keys)")
        # This logic would typically be handled directly by the cache stores (memory, Redis)
        # based on expiry times set during caching. This method could potentially
        # trigger a cleanup scan if needed, though often TTL expiry is passive.
        # Returning an empty set as proactive TTL scan might be complex/costly.
        # Actual expiry check happens during cache retrieval.
        expired_keys = set()  # Placeholder
        logger.debug(
            "Time-based invalidation check complete (passive expiry handled by stores)."
        )
        return expired_keys


class CacheInvalidator:
    """
    Manages the cache invalidation process using configured strategies.

    It receives notifications about potential data changes and orchestrates
    the invalidation process by invoking the appropriate strategies.
    """

    def __init__(
        self,
        cache_manager: "CacheManager",
        strategies: list[BaseInvalidationStrategy] | None = None,
    ):
        """
        Initialise the CacheInvalidator.

        Args:
            cache_manager: The central CacheManager instance.
            strategies: A list of invalidation strategies to use. If None,
                        a default strategy (e.g., DependencyBased) might be used.
        """
        self._cache_manager = cache_manager
        if strategies is None:
            # Default to dependency-based invalidation if available/configured
            self._strategies = [DependencyBasedInvalidation(cache_manager)]
            logger.info(
                "Initialising CacheInvalidator with default strategy: DependencyBasedInvalidation"
            )
        else:
            self._strategies = strategies
            logger.info(
                f"Initialising CacheInvalidator with strategies: {[s.__class__.__name__ for s in strategies]}"
            )

    def trigger_invalidation(self, trigger_info: Any):
        """
        Trigger the invalidation process based on some event or data change.

        Args:
            trigger_info: Information about the trigger (e.g., updated data key).
        """
        logger.info(f"Invalidation triggered with info: {trigger_info}")
        keys_to_invalidate: Set[Any] = set()

        for strategy in self._strategies:
            try:
                keys_from_strategy = strategy.invalidate(trigger_info)
                keys_to_invalidate.update(keys_from_strategy)
                logger.debug(
                    f"Strategy {strategy.__class__.__name__} identified {len(keys_from_strategy)} keys for invalidation."
                )
            except Exception as e:
                logger.error(
                    f"Error executing invalidation strategy {strategy.__class__.__name__}: {e}",
                    exc_info=True,
                )

        if keys_to_invalidate:
            logger.info(
                f"Total keys identified for invalidation: {len(keys_to_invalidate)}. Sending to CacheManager."
            )
            # Pass the keys to the CacheManager to perform the actual removal
            self._cache_manager.invalidate_keys(keys_to_invalidate)
        else:
            logger.info("No keys identified for invalidation.")
