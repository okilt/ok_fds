# src/cache/cache_manager.py
"""
Cache Manager Module.

Provides a central interface for managing caching operations, coordinating
between different cache levels (memory, Redis, disk), handling request
coalescing, and interacting with the invalidation system.
"""

from __future__ import annotations
import asyncio
import functools
import logging
from typing import Any, Callable, Set, Optional, Type
from contextlib import asynccontextmanager

from .distributed_cache import DistributedCache
from .cache_key import generate_cache_key
from .request_registry import RequestRegistry

# Use forward reference for CacheInvalidator to avoid circular import
from typing import TYPE_CHECKING
from ..utils.dependency_utils import DependencyTracker, DependencyContext

if TYPE_CHECKING:
    from .cache_invalidation import CacheInvalidator

logger = logging.getLogger(__name__)

# Simple Singleton implementation using a module-level variable
_instance = None


class CacheManager:
    """
    Central manager for caching operations.

    Coordinates data storage and retrieval across multiple cache layers
    (memory, Redis, disk), manages request coalescing for concurrent requests,
    and interfaces with the dependency tracking and invalidation systems.

    This class is intended to be used as a Singleton.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            logger.info("Creating CacheManager instance")
            cls._instance = super(CacheManager, cls).__new__(cls)
            # Flag to indicate initialisation is needed
            cls._instance._initialised = False
        return cls._instance

    def __init__(
        self,
        distributed_cache: Optional[DistributedCache] = None,
        request_registry: Optional[RequestRegistry] = None,
        dependency_tracker: Optional[DependencyTracker] = None,
        cache_key_func: Callable[..., str] = generate_cache_key,
    ):
        """
        Initialise the CacheManager Singleton.

        Args:
            distributed_cache: The DistributedCache instance for multi-level caching.
                               If None, a default instance will be created.
            request_registry: The RequestRegistry instance for handling concurrent requests.
                              If None, a default instance will be created.
            dependency_tracker: The DependencyTracker instance. If None, a default is created.
            cache_key_func: The function used to generate cache keys.
        """
        # Singleton initialisation guard
        if hasattr(self, "_initialised") and self._initialised:
            return

        logger.info("Initialising CacheManager...")
        self.cache = distributed_cache or DistributedCache()
        self.registry = request_registry or RequestRegistry()
        self.dependency_tracker = dependency_tracker or DependencyTracker()
        self.cache_key_func = cache_key_func
        # The CacheInvalidator needs a CacheManager instance, creating a potential
        # circular dependency if created directly here. It should be set later
        # or injected.
        self.invalidator: Optional["CacheInvalidator"] = None
        logger.info("CacheManager initialised.")
        self._initialised = True

    def set_invalidator(self, invalidator: "CacheInvalidator"):
        """
        Set the CacheInvalidator instance.

        This is called after CacheInvalidator is initialised to break the
        circular dependency during initialisation.

        Args:
            invalidator: The CacheInvalidator instance.
        """
        if self.invalidator is None:
            logger.info(f"Setting CacheInvalidator: {invalidator.__class__.__name__}")
            self.invalidator = invalidator
        else:
            logger.warning("CacheInvalidator already set.")

    async def get(self, key: Any) -> Any:
        """
        Retrieve an item from the cache.

        Checks memory, Redis, and disk caches sequentially.

        Args:
            key: The cache key.

        Returns:
            The cached data, or None if not found or expired.
        """
        # logger.debug(f"Attempting to get data for key: {key}")
        data = await self.cache.get(key)
        if data is not None:
            # logger.debug(f"Cache hit for key: {key}")
            # Data might be wrapped with metadata (e.g., dependencies)
            # For simplicity now, assume DistributedCache handles unwrapping if needed
            # or that wrapping/unwrapping happens in the decorator/set method.
            return data
        else:
            # logger.debug(f"Cache miss for key: {key}")
            return None

    async def set(
        self,
        key: Any,
        value: Any,
        ttl: Optional[int] = None,
        dependencies: Optional[Set[Any]] = None,
    ):
        """
        Store an item in the cache.

        Stores the item in the configured cache levels with an optional TTL.
        Also records dependencies if provided.

        Args:
            key: The cache key.
            value: The value to store.
            ttl: Time-to-live in seconds. Uses default if None.
            dependencies: A set of keys that this entry depends on.
        """
        logger.debug(
            f"Setting cache for key: {key}, TTL: {ttl}, Dependencies: {dependencies}"
        )
        await self.cache.set(key, value, ttl=ttl)
        if dependencies:
            self.dependency_tracker.add_dependencies(key, dependencies)
            logger.debug(f"Recorded dependencies for key {key}: {dependencies}")

    async def delete(self, key: Any):
        """
        Delete an item from all cache levels.

        Args:
            key: The cache key to delete.
        """
        logger.info(f"Deleting cache key: {key}")
        await self.cache.delete(key)
        # Also remove dependency information related to this key
        self.dependency_tracker.remove_key(key)
        logger.debug(f"Removed dependencies associated with key {key}")

    async def invalidate_keys(self, keys: Set[Any]):
        """
        Invalidate (delete) a set of keys from the cache.

        Usually called by the CacheInvalidator. This method also triggers
        the removal of associated dependency tracking information.

        Args:
            keys: A set of cache keys to invalidate.
        """
        if not keys:
            return
        logger.info(f"Invalidating {len(keys)} keys: {keys}")
        # Invalidate concurrently for better performance
        delete_tasks = [self.delete(key) for key in keys]
        await asyncio.gather(*delete_tasks)
        logger.info(f"Finished invalidating {len(keys)} keys.")

    def get_dependents(self, key: Any) -> Set[Any]:
        """
        Retrieve the set of keys that depend on the given key.

        Used by the DependencyBasedInvalidation strategy.

        Args:
            key: The key whose dependents are needed.

        Returns:
            A set of keys that depend on the input key.
        """
        return self.dependency_tracker.get_dependents(key)

    def cached(
        self,
        ttl: Optional[int] = None,
        cache_key_func: Optional[Callable[..., str]] = None,
        track_dependencies: bool = True,
    ):
        """
        Decorator to cache the results of a function (sync or async).

        Args:
            ttl: Cache Time-To-Live in seconds for the result. Uses cache default if None.
            cache_key_func: Custom function to generate the cache key. Defaults to
                              the CacheManager's configured function.
            track_dependencies: If True, track dependencies automatically using
                                DependencyContext while the decorated function executes.
        """
        key_generator = cache_key_func or self.cache_key_func

        def decorator(func):
            is_async = asyncio.iscoroutinefunction(func)

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                # logger.debug(f"Decorator invoked for async func: {func.__name__}")
                cache_key = key_generator(func, *args, **kwargs)
                # logger.debug(f"Generated cache key for {func.__name__}: {cache_key}")

                # 1. Check cache first
                cached_value = await self.get(cache_key)
                if cached_value is not None:
                    # logger.debug(f"Cache hit for {func.__name__} with key {cache_key}")
                    return cached_value

                # 2. Check request registry (prevent dogpiling)
                async with self.registry.get_lock(cache_key):
                    # Re-check cache after acquiring lock (another request might have finished)
                    cached_value = await self.get(cache_key)
                    if cached_value is not None:
                        # logger.debug(f"Cache hit after acquiring lock for {func.__name__} with key {cache_key}")
                        return cached_value

                    # logger.debug(f"Cache miss for {func.__name__} with key {cache_key}. Executing function.")
                    # 3. Execute the function
                    result = None
                    dependencies = set()
                    try:
                        if track_dependencies:
                            with DependencyContext(self.dependency_tracker) as context:
                                result = await func(*args, **kwargs)
                                dependencies = context.get_dependencies()
                                logger.debug(
                                    f"Tracked dependencies for {cache_key}: {dependencies}"
                                )
                        else:
                            result = await func(*args, **kwargs)
                            logger.debug(
                                f"Dependency tracking disabled for {cache_key}."
                            )

                        # 4. Store result in cache
                        if (
                            result is not None
                        ):  # Avoid caching None unless explicitly desired
                            await self.set(
                                cache_key,
                                result,
                                ttl=ttl,
                                dependencies=(
                                    dependencies if track_dependencies else None
                                ),
                            )
                            logger.debug(
                                f"Stored result for {func.__name__} with key {cache_key} in cache."
                            )
                        else:
                            logger.debug(
                                f"Function {func.__name__} returned None. Not caching."
                            )
                        return result
                    except Exception as e:
                        logger.error(
                            f"Error executing cached function {func.__name__} for key {cache_key}: {e}",
                            exc_info=True,
                        )
                        # Do not cache errors, re-raise
                        raise

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                # logger.debug(f"Decorator invoked for sync func: {func.__name__}")
                cache_key = key_generator(func, *args, **kwargs)
                # logger.debug(f"Generated cache key for {func.__name__}: {cache_key}")

                # Note: For simplicity, using asyncio run_until_complete here.
                # In a truly mixed sync/async application, managing the event loop
                # and potential blocking calls needs careful consideration.
                # A better approach might involve running sync functions in a thread pool
                # managed by the async framework. This implementation assumes the sync
                # function can be awaited somehow if called from an async context,
                # or that this manager primarily operates within an async context.
                # Let's assume we need to run the async logic even for sync functions.
                # This requires an event loop to be running.

                # Check if an event loop is running
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # If no loop is running, we cannot use async get/set/registry.
                    # This indicates a design issue: the CacheManager with async
                    # features needs to operate within an async environment.
                    # Fallback: bypass caching for sync calls outside an event loop? Or start a loop?
                    # Starting a loop might have side effects. Logging a warning and bypassing cache for now.
                    logger.warning(
                        f"No running asyncio event loop. Bypassing cache for sync function {func.__name__}. Key: {cache_key}"
                    )
                    # Alternative: Use sync versions of cache/registry if available
                    # return func(*args, **kwargs) # Bypass cache

                    # Or, try running the async wrapper in a new event loop (can be problematic)
                    # return asyncio.run(async_wrapper(*args, **kwargs))

                    # Let's assume for OKFundDataSystem, operations will likely be async-driven.
                    # If sync usage outside a loop is critical, the cache architecture needs revisiting.
                    # For now, raise an error to highlight the issue.
                    raise RuntimeError(
                        f"CacheManager.cached used on sync function '{func.__name__}' outside of a running asyncio event loop."
                    )

                # If a loop is running, run the async logic
                # Using create_task and await ensures it integrates with the existing loop.
                # Note: This blocks the sync function until the async operations complete.
                # Consider using thread pools for true non-blocking sync execution.
                task = loop.create_task(
                    async_wrapper_sync_executor(
                        func, args, kwargs, cache_key, self, ttl, track_dependencies
                    )
                )
                # Running the task and getting the result (blocking)
                return loop.run_until_complete(task)

            async def async_wrapper_sync_executor(
                sync_func,
                args,
                kwargs,
                cache_key,
                manager_instance,
                ttl_val,
                track_deps,
            ):
                """Helper to execute sync function logic within the async wrapper structure."""
                # 1. Check cache first (re-implement async_wrapper logic slightly adjusted for sync func)
                cached_value = await manager_instance.get(cache_key)
                if cached_value is not None:
                    return cached_value

                # 2. Check request registry
                async with manager_instance.registry.get_lock(cache_key):
                    cached_value = await manager_instance.get(cache_key)
                    if cached_value is not None:
                        return cached_value

                    # 3. Execute the function (in the current thread/context, as it's sync)
                    result = None
                    dependencies = set()
                    try:
                        if track_deps:
                            # DependencyContext needs to work for sync functions too
                            # Assume DependencyContext supports sync 'with' usage
                            with DependencyContext(
                                manager_instance.dependency_tracker
                            ) as context:
                                result = sync_func(*args, **kwargs)  # Direct call
                                dependencies = context.get_dependencies()
                                logger.debug(
                                    f"Tracked dependencies for sync {sync_func.__name__} {cache_key}: {dependencies}"
                                )
                        else:
                            result = sync_func(*args, **kwargs)  # Direct call
                            logger.debug(
                                f"Dependency tracking disabled for sync {sync_func.__name__} {cache_key}."
                            )

                        # 4. Store result in cache
                        if result is not None:
                            await manager_instance.set(
                                cache_key,
                                result,
                                ttl=ttl_val,
                                dependencies=dependencies if track_deps else None,
                            )
                            logger.debug(
                                f"Stored result for sync {sync_func.__name__} with key {cache_key} in cache."
                            )
                        else:
                            logger.debug(
                                f"Sync function {sync_func.__name__} returned None. Not caching."
                            )

                        return result
                    except Exception as e:
                        logger.error(
                            f"Error executing cached sync function {sync_func.__name__} for key {cache_key}: {e}",
                            exc_info=True,
                        )
                        raise  # Re-raise

            return async_wrapper if is_async else sync_wrapper

        return decorator


# Function to get the Singleton instance
def get_cache_manager() -> CacheManager:
    """
    Returns the Singleton instance of the CacheManager.

    Initialises it if it doesn't exist yet.
    """
    instance = CacheManager()
    if not instance._initialised:
        # Ensure initialisation happens if accessed via get_cache_manager first
        # Pass default dependencies; these can be configured later if needed
        # by accessing the instance directly or through a configuration mechanism.
        instance.__init__()
    return instance
