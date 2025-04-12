import asyncio
import logging

from src.cache.distributed_cache import DistributedCache


# Example Usage (Conceptual - requires running event loop and dependencies)
# ... (Example usage remains largely the same, but ensure it uses async calls correctly)
async def main():
    logging.basicConfig(level=logging.DEBUG)
    # Ensure Redis server is running locally for this example to work fully
    # Start one with Docker: docker run -d -p 6379:6379 redis

    cache = DistributedCache(
        memory_maxsize=10,
        memory_ttl=5,  # 5 second TTL for memory
        redis_ttl=10,  # 10 second TTL for Redis
        disk_ttl=20,  # 20 second TTL for disk
    )

    # Check connection explicitly if desired (optional)
    await cache.check_redis_connection_async()

    key1 = "my_test_key_async"
    value1 = {"a": 1, "b": [1, 2, 3], "c": "hello async"}

    try:  # Import pandas locally for example
        import pandas as pd

        key2 = "another_key_async"
        value2 = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    except ImportError:
        key2 = "another_key_async_nopd"
        value2 = "Pandas not installed"  # Fallback value

    print("\n--- Setting values (Async Redis) ---")
    await cache.set(key1, value1)
    await cache.set(key2, value2, ttl=15)  # Override default TTLs

    # ... rest of the example logic from before should work,
    # just verify await calls are used for cache.get/set/delete ...

    print("\n--- Getting values (should hit memory/be fast) ---")
    ret_val1 = await cache.get(key1)
    print(f"Get {key1}: {ret_val1}")
    ret_val2 = await cache.get(key2)
    print(f"Get {key2}: {type(ret_val2)}")  # Print type for DataFrame

    # ... (rest of example: clear memory, test promotion, wait for TTL, delete, etc.)

    await cache.close()


if __name__ == "__main__":
    # Setup basic logging for the example
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("src.cache.distributed_cache").setLevel(logging.DEBUG)

    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\nAn error occurred during the example run: {e}")
        print(
            "Ensure Redis server is running and required libraries (redis>=4.0, cachetools, diskcache, pandas) are installed."
        )
