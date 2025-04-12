import asyncio
import logging

from src.cache.cache_manager import get_cache_manager
from src.utils.dependency_utils import DependencyContext


async def example_async_task(x: int) -> str:
    """An example async function to be cached."""
    print(f"--- Running example_async_task({x}) ---")
    await asyncio.sleep(0.1)  # Simulate work
    # Simulate dependency
    manager = get_cache_manager()
    # Assume 'base_data' is another cached key this task depends on
    base_key = manager.cache_key_func(None, "base_data", x)
    DependencyContext.add_dependency(base_key)
    return f"Result for {x}"


def example_sync_task(y: str) -> int:
    """An example sync function to be cached."""
    print(f"--- Running example_sync_task({y}) ---")
    # Simulate work
    import time

    time.sleep(0.1)
    # Simulate dependency
    manager = get_cache_manager()
    # Assume 'config_param' is another cached key this task depends on
    config_key = manager.cache_key_func(None, "config_param", y)
    DependencyContext.add_dependency(config_key)
    return len(y)


async def main():
    logging.basicConfig(level=logging.DEBUG)
    # Configure CacheManager (e.g., with mock or real dependencies)
    # For simplicity, use defaults which create basic instances
    manager = get_cache_manager()

    # We need to manually set the invalidator if we want to use it
    # from .cache_invalidation import CacheInvalidator, DependencyBasedInvalidation
    # invalidator = CacheInvalidator(manager, strategies=[DependencyBasedInvalidation(manager)])
    # manager.set_invalidator(invalidator) # Setup invalidator link

    # Decorate functions
    cached_async_task = manager.cached(ttl=60)(example_async_task)
    cached_sync_task = manager.cached(ttl=120)(example_sync_task)

    print("--- Testing Async Caching ---")
    print(f"1st call async_task(1): {await cached_async_task(1)}")
    print(f"2nd call async_task(1): {await cached_async_task(1)}")  # Should be cached
    print(f"1st call async_task(2): {await cached_async_task(2)}")

    print("\n--- Testing Sync Caching (within async context) ---")
    print(f"1st call sync_task('hello'): {cached_sync_task('hello')}")  # Run via helper
    print(
        f"2nd call sync_task('hello'): {cached_sync_task('hello')}"
    )  # Should be cached
    print(f"1st call sync_task('world'): {cached_sync_task('world')}")

    # Demonstrate dependency tracking (conceptual)
    key_async_1 = manager.cache_key_func(example_async_task, 1)
    key_sync_hello = manager.cache_key_func(example_sync_task, "hello")
    base_key_1 = manager.cache_key_func(None, "base_data", 1)
    config_key_hello = manager.cache_key_func(None, "config_param", "hello")

    print(
        f"\nDependencies recorded for {key_async_1}: {manager.dependency_tracker.adj.get(key_async_1)}"
    )
    print(
        f"Dependents recorded for {base_key_1}: {manager.dependency_tracker.rev_adj.get(base_key_1)}"
    )
    print(
        f"Dependencies recorded for {key_sync_hello}: {manager.dependency_tracker.adj.get(key_sync_hello)}"
    )
    print(
        f"Dependents recorded for {config_key_hello}: {manager.dependency_tracker.rev_adj.get(config_key_hello)}"
    )

    # --- Test invalidation (Requires Invalidator setup) ---
    # if manager.invalidator:
    #    print(f"\nInvalidating base_key_1: {base_key_1}")
    #    await manager.invalidator.trigger_invalidation(base_key_1) # Note: trigger_invalidation needs to be async if invalidate_keys is async
    # Need to make trigger_invalidation async or run invalidate_keys in a task
    # Let's assume invalidate_keys can be called from sync context if needed, or trigger becomes async
    #    print(f"3rd call async_task(1) after invalidation: {await cached_async_task(1)}") # Should re-run


if __name__ == "__main__":
    # To run the example main function
    try:
        asyncio.run(main())
    except RuntimeError as e:
        print(f"Error running main: {e}")
        print(
            "Note: Running sync cached functions requires an active asyncio event loop."
        )
