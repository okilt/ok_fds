# src/cache/distributed_cache.py
"""
Distributed Multi-Level Cache Module.

Implements a caching system that utilises multiple layers:
1. In-Memory Cache (Fastest, local to process)
2. Redis Cache (Shared, fast, distributed)
3. Disk Cache (Slower, persistent, larger capacity)

Provides a unified interface (`get`, `set`, `delete`) that automatically
manages data across these layers based on configuration and availability.
"""

import asyncio
import logging
import pickle  # Consider cloudpickle for wider object support, but pickle is built-in
import functools  # Keep for diskcache executor usage
from pathlib import Path
from typing import Any, Optional

# --- Dependencies ---
# Redis (Async version >= 4.0 recommended for Python 3.8+)
try:
    import redis.asyncio as aioredis  # USE ALIAS 'aioredis'
    from redis.exceptions import (
        ConnectionError as RedisConnectionError,
    )  # IMPORT EXCEPTION
    from redis.exceptions import TimeoutError as RedisTimeoutError  # IMPORT EXCEPTION

    # Import other needed exceptions like redis.exceptions.ResponseError if necessary
    REDIS_AVAILABLE = True
except ImportError:
    aioredis = None
    # Define fallbacks so type hints don't break easily, though functionality is lost
    RedisConnectionError = ConnectionError
    RedisTimeoutError = TimeoutError
    REDIS_AVAILABLE = False
    logging.warning(
        "Redis library >= 4.0 with asyncio support not found. Redis cache layer will be disabled."
    )

# Disable Redis for now
REDIS_AVAILABLE = False

# Cachetools
try:
    from cachetools import TTLCache
except ImportError:
    TTLCache = None
    logging.warning(
        "cachetools library not found. In-memory TTL cache will use a basic dictionary (no TTL)."
    )

# Diskcache
try:
    import diskcache
except ImportError:
    diskcache = None
    logging.warning("diskcache library not found. Disk cache layer will be disabled.")


logger = logging.getLogger(__name__)

# --- Configuration Defaults ---
DEFAULT_MEMORY_MAXSIZE = 1024
DEFAULT_MEMORY_TTL = 300
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_REDIS_TTL = 3600
DEFAULT_DISK_CACHE_DIR = "./data/cache"
DEFAULT_DISK_TTL = 86400

CACHE_MISS_SENTINEL = object()


class DistributedCache:
    """
    Manages multi-level caching (Memory, Redis, Disk).

    Provides asynchronous methods to interact with the cache layers.
    Uses native asyncio client for Redis (version >= 4.0).
    """

    def __init__(
        self,
        memory_maxsize: int = DEFAULT_MEMORY_MAXSIZE,
        memory_ttl: int = DEFAULT_MEMORY_TTL,
        redis_url: Optional[str] = DEFAULT_REDIS_URL,
        redis_ttl: int = DEFAULT_REDIS_TTL,
        disk_cache_dir: Optional[str] = DEFAULT_DISK_CACHE_DIR,
        disk_ttl: int = DEFAULT_DISK_TTL,
        enable_memory: bool = True,
        enable_redis: bool = True,
        enable_disk: bool = True,
    ):
        """
        Initialise the DistributedCache.
        (See previous version for detailed args)
        """
        logger.info("Initialising DistributedCache...")

        # --- Memory Cache ---
        self.memory_cache = None
        self.memory_ttl = memory_ttl
        # ... (memory cache init as before using TTLCache) ...
        if enable_memory:
            if TTLCache:
                self.memory_cache = TTLCache(maxsize=memory_maxsize, ttl=memory_ttl)
                logger.info(
                    f"In-memory TTL cache enabled (maxsize={memory_maxsize}, ttl={memory_ttl}s)."
                )
            else:
                self.memory_cache = {}
                logger.warning(
                    "cachetools not found. Using basic dictionary for memory cache (no TTL/size limit)."
                )
        else:
            logger.info("In-memory cache disabled.")

        # --- Redis Cache (Async) ---
        self.redis_client: Optional[aioredis.Redis] = None
        self.redis_ttl = redis_ttl
        self.redis_enabled = False
        self.redis_pool = None
        if enable_redis and REDIS_AVAILABLE and redis_url:
            try:
                # Use ASYNC connection pool
                self.redis_pool = aioredis.ConnectionPool.from_url(
                    redis_url,
                    decode_responses=False,  # Store bytes for pickle compatibility
                )
                # Create ASYNC client using the pool
                self.redis_client = aioredis.Redis(connection_pool=self.redis_pool)
                # Connection check is deferred until first use or explicit check method
                self.redis_enabled = True
                logger.info(
                    f"Redis cache enabled (ASYNC CLIENT, URL: {redis_url}, default TTL: {redis_ttl}s)."
                )
            except Exception as e:
                logger.error(
                    f"Failed to initialise ASYNC Redis client at {redis_url}: {e}",
                    exc_info=True,
                )
                self.redis_client = None
                self.redis_pool = None
        elif enable_redis and not REDIS_AVAILABLE:
            logger.warning(
                "Redis cache was enabled, but 'redis>=4.0' library with asyncio support is not installed."
            )
        elif enable_redis and not redis_url:
            logger.warning("Redis cache was enabled, but no redis_url was provided.")
        else:
            logger.info("Redis cache disabled.")

        # --- Disk Cache ---
        self.disk_cache = None
        self.disk_ttl = disk_ttl
        # ... (disk cache init as before using diskcache) ...
        if enable_disk and diskcache and disk_cache_dir:
            try:
                cache_path = Path(disk_cache_dir)
                cache_path.mkdir(parents=True, exist_ok=True)
                self.disk_cache = diskcache.Cache(str(cache_path), expire=disk_ttl)
                logger.info(
                    f"Disk cache enabled (Directory: {disk_cache_dir}, default TTL: {disk_ttl}s)."
                )
            except Exception as e:
                logger.error(
                    f"Failed to initialise DiskCache at {disk_cache_dir}: {e}",
                    exc_info=True,
                )
                self.disk_cache = None
        # ... (rest of disk disabling logic) ...

        # --- Serialization ---
        self.serializer = pickle

    async def check_redis_connection_async(self) -> bool:
        """Helper coroutine to check Redis connection using async PING."""
        if self.redis_client and self.redis_enabled:
            try:
                logger.debug("Pinging Redis asynchronously...")
                pong = await self.redis_client.ping()
                if pong:
                    logger.info("Redis ASYNC connection successful (PING successful).")
                    return True
                else:
                    logger.warning(
                        "Redis ASYNC PING command returned unexpected response."
                    )
                    return False
            except RedisConnectionError as e:  # USE IMPORTED EXCEPTION
                logger.error(f"Redis ASYNC connection failed: {e}")
                return False
            except RedisTimeoutError as e:  # USE IMPORTED EXCEPTION
                logger.error(f"Redis ASYNC PING command timed out: {e}")
                return False
            except Exception as e:
                logger.error(
                    f"Error during Redis ASYNC connection check: {e}", exc_info=True
                )
                return False
        return False

    async def get(self, key: Any) -> Any:
        """
        Retrieve data from the cache layers (Memory -> Redis -> Disk).
        """
        cache_key_str = str(key)

        # 1. Try Memory Cache
        # ... (memory cache logic as before) ...
        if self.memory_cache is not None:
            try:
                value = self.memory_cache[cache_key_str]
                # logger.debug(f"Memory cache hit for key: {cache_key_str}")
                return value
            except KeyError:
                # logger.debug(f"Memory cache miss for key: {cache_key_str}")
                pass
            except Exception as e:
                logger.error(
                    f"Error reading from memory cache for key {cache_key_str}: {e}",
                    exc_info=True,
                )

        # 2. Try Redis Cache (Async)
        if self.redis_enabled and self.redis_client:
            try:
                # logger.debug(f"Checking Redis cache for key: {cache_key_str}")
                cached_data_bytes = await self.redis_client.get(
                    cache_key_str
                )  # Use await
                if cached_data_bytes is not None:
                    # logger.debug(f"Redis cache hit for key: {cache_key_str}")
                    value = self.serializer.loads(cached_data_bytes)
                    # Promote to memory cache
                    if self.memory_cache is not None:
                        try:
                            self.memory_cache[cache_key_str] = value
                            # logger.debug(f"Promoted key {cache_key_str} from Redis to memory cache.")
                        except Exception as e:
                            logger.error(
                                f"Error promoting key {cache_key_str} to memory cache: {e}",
                                exc_info=True,
                            )
                    return value
                else:
                    # logger.debug(f"Redis cache miss for key: {cache_key_str}")
                    pass  # Continue to next layer
            except RedisConnectionError as e:  # USE IMPORTED EXCEPTION
                logger.error(
                    f"Redis connection error during GET for key {cache_key_str}: {e}"
                )
                # Potentially disable Redis temporarily (Circuit Breaker pattern needed)
            except RedisTimeoutError as e:  # USE IMPORTED EXCEPTION
                logger.error(f"Redis timeout during GET for key {cache_key_str}: {e}")
            except Exception as e:
                logger.error(
                    f"Error reading from Redis cache for key {cache_key_str}: {e}",
                    exc_info=True,
                )

        # 3. Try Disk Cache
        # ... (disk cache logic as before - uses run_in_executor internally via diskcache lib) ...
        if self.disk_cache is not None:
            loop = asyncio.get_running_loop()
            try:
                # logger.debug(f"Checking disk cache for key: {cache_key_str}")
                # Run diskcache get in executor as it might block on I/O or CPU (pickle)
                value = await loop.run_in_executor(
                    None,  # Use default executor
                    functools.partial(
                        self.disk_cache.get,
                        cache_key_str,
                        default=CACHE_MISS_SENTINEL,
                        read=True,
                    ),
                )
                if value is not CACHE_MISS_SENTINEL:
                    # logger.debug(f"Disk cache hit for key: {cache_key_str}")
                    # Promotion logic (Memory Cache)
                    if self.memory_cache is not None:
                        try:
                            self.memory_cache[cache_key_str] = value
                            # logger.debug(f"Promoted key {cache_key_str} from disk to memory cache.")
                        except Exception as e:
                            logger.error(
                                f"Error promoting key {cache_key_str} from disk to memory cache: {e}",
                                exc_info=True,
                            )

                    # Promotion logic (Redis Cache - run async set)
                    if self.redis_enabled and self.redis_client:
                        try:
                            serialized_value = self.serializer.dumps(value)
                            # Use calculated Redis TTL (rds_ttl defined in 'set' but needed here conceptually)
                            rds_ttl = (
                                self.redis_ttl
                            )  # Use default or get from somewhere if promotion TTL differs
                            if rds_ttl > 0:
                                await self.redis_client.set(
                                    cache_key_str, serialized_value, ex=rds_ttl
                                )
                            else:
                                await self.redis_client.set(
                                    cache_key_str, serialized_value
                                )  # Persist
                            # logger.debug(f"Promoted key {cache_key_str} from disk to Redis cache.")
                        except RedisConnectionError as e:  # USE IMPORTED EXCEPTION
                            logger.error(
                                f"Redis connection error during disk promotion SET for key {cache_key_str}: {e}"
                            )
                        except RedisTimeoutError as e:  # USE IMPORTED EXCEPTION
                            logger.error(
                                f"Redis timeout during disk promotion SET for key {cache_key_str}: {e}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error promoting key {cache_key_str} from disk to Redis cache: {e}",
                                exc_info=True,
                            )

                    return value
                else:
                    # logger.debug(f"Disk cache miss for key: {cache_key_str}")
                    pass  # Cache miss overall
            except Exception as e:
                logger.error(
                    f"Error reading from disk cache (executor) for key {cache_key_str}: {e}",
                    exc_info=True,
                )

        # 4. Cache Miss
        # logger.debug(f"Cache miss overall for key: {cache_key_str}")
        return None

    async def set(self, key: Any, value: Any, ttl: Optional[int] = None):
        """
        Store data in the enabled cache layers (Memory, Redis, Disk).
        """
        # ... (value check, cache_key_str logic as before) ...

        cache_key_str = str(key)
        serialized_value = None

        # Determine TTLs for each layer
        mem_ttl = ttl if ttl is not None else self.memory_ttl
        rds_ttl = ttl if ttl is not None else self.redis_ttl
        dsk_ttl = ttl if ttl is not None else self.disk_ttl

        # 1. Set Memory Cache
        # ... (memory cache logic as before) ...
        if self.memory_cache is not None:
            try:
                # Use default TTL for TTLCache unless complex per-item TTL is implemented
                self.memory_cache[cache_key_str] = value
                # logger.debug(f"Stored key {cache_key_str} in memory cache.")
            except Exception as e:
                logger.error(
                    f"Error writing to memory cache for key {cache_key_str}: {e}",
                    exc_info=True,
                )

        # Prepare serialized value for Redis/Disk
        try:
            serialized_value = self.serializer.dumps(value)
        except Exception as e:
            logger.error(
                f"Failed to serialize value for key {cache_key_str}: {e}. Cannot cache in Redis/Disk.",
                exc_info=True,
            )
            return  # Cannot proceed to Redis/Disk

        # Create tasks for concurrent Redis/Disk writes
        write_tasks = []

        # 2. Set Redis Cache (Async)
        if self.redis_enabled and self.redis_client and serialized_value is not None:

            async def _set_redis():
                try:
                    if rds_ttl is not None and rds_ttl > 0:
                        await self.redis_client.set(
                            cache_key_str, serialized_value, ex=rds_ttl
                        )
                    else:
                        await self.redis_client.set(
                            cache_key_str, serialized_value
                        )  # Persist
                    # logger.debug(f"Stored key {cache_key_str} in Redis cache (TTL: {rds_ttl}s).")
                except RedisConnectionError as e:  # USE IMPORTED EXCEPTION
                    logger.error(
                        f"Redis connection error during SET for key {cache_key_str}: {e}"
                    )
                except RedisTimeoutError as e:  # USE IMPORTED EXCEPTION
                    logger.error(
                        f"Redis timeout during SET for key {cache_key_str}: {e}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error writing to Redis cache for key {cache_key_str}: {e}",
                        exc_info=True,
                    )

            write_tasks.append(asyncio.create_task(_set_redis()))

        # 3. Set Disk Cache (run in executor)
        disk_task = None
        if (
            self.disk_cache is not None and serialized_value is not None
        ):  # serialized_value check might be redundant if diskcache pickles raw value

            async def _set_disk():
                loop = asyncio.get_running_loop()
                try:
                    await loop.run_in_executor(
                        None,  # Use default executor
                        functools.partial(
                            self.disk_cache.set,
                            cache_key_str,
                            value,
                            expire=(
                                dsk_ttl if dsk_ttl is not None and dsk_ttl > 0 else None
                            ),
                            read=False,
                            tag=None,
                            retry=False,
                        ),  # Pass raw value to diskcache
                    )
                    # logger.debug(f"Stored key {cache_key_str} in disk cache (TTL: {dsk_ttl}s).")
                except Exception as e:
                    logger.error(
                        f"Error writing to disk cache (executor) for key {cache_key_str}: {e}",
                        exc_info=True,
                    )

            write_tasks.append(asyncio.create_task(_set_disk()))

        # Wait for Redis/Disk writes to complete
        if write_tasks:
            await asyncio.gather(*write_tasks)

    async def delete(self, key: Any):
        """
        Delete data from all enabled cache layers (Memory, Redis, Disk).
        """
        cache_key_str = str(key)
        # logger.debug(f"Deleting key {cache_key_str} from all cache layers.")

        # 1. Delete from Memory Cache
        # ... (memory cache logic as before) ...
        if self.memory_cache is not None:
            try:
                if cache_key_str in self.memory_cache:
                    del self.memory_cache[cache_key_str]
                    # logger.debug(f"Deleted key {cache_key_str} from memory cache.")
            except Exception as e:
                logger.error(
                    f"Error deleting from memory cache for key {cache_key_str}: {e}",
                    exc_info=True,
                )

        # Create tasks for concurrent Redis/Disk deletes
        delete_tasks = []

        # 2. Delete from Redis Cache (Async)
        if self.redis_enabled and self.redis_client:

            async def _delete_redis():
                try:
                    await self.redis_client.delete(cache_key_str)
                    # logger.debug(f"Deleted key {cache_key_str} from Redis cache.")
                except RedisConnectionError as e:  # USE IMPORTED EXCEPTION
                    logger.error(
                        f"Redis connection error during DELETE for key {cache_key_str}: {e}"
                    )
                except RedisTimeoutError as e:  # USE IMPORTED EXCEPTION
                    logger.error(
                        f"Redis timeout during DELETE for key {cache_key_str}: {e}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error deleting from Redis cache for key {cache_key_str}: {e}",
                        exc_info=True,
                    )

            delete_tasks.append(asyncio.create_task(_delete_redis()))

        # 3. Delete from Disk Cache (run in executor)
        if self.disk_cache is not None:

            async def _delete_disk():
                loop = asyncio.get_running_loop()
                try:
                    await loop.run_in_executor(
                        None,  # Use default executor
                        functools.partial(
                            self.disk_cache.delete, cache_key_str, retry=False
                        ),
                    )
                    # logger.debug(f"Deleted key {cache_key_str} from disk cache.")
                except Exception as e:
                    logger.error(
                        f"Error deleting from disk cache (executor) for key {cache_key_str}: {e}",
                        exc_info=True,
                    )

            delete_tasks.append(asyncio.create_task(_delete_disk()))

        # Wait for Redis/Disk deletes to complete
        if delete_tasks:
            await asyncio.gather(*delete_tasks)

    async def clear_all(self):
        """Clear all caches (Memory, Redis, Disk). Use with caution!"""
        logger.warning("Clearing ALL cache layers...")

        # 1. Clear Memory Cache
        # ... (memory cache logic as before) ...
        if self.memory_cache is not None:
            try:
                self.memory_cache.clear()
                logger.info("Memory cache cleared.")
            except Exception as e:
                logger.error(f"Error clearing memory cache: {e}", exc_info=True)

        clear_tasks = []
        # 2. Clear Redis Cache (Flush DB - async)
        if self.redis_enabled and self.redis_client:
            logger.warning(
                f"Flushing Redis DB (ASYNC CLIENT) associated with pool {self.redis_pool.connection_kwargs if self.redis_pool else 'N/A'}"
            )

            async def _flush_redis():
                try:
                    await self.redis_client.flushdb()
                    logger.info("Redis cache flushed (FLUSHDB).")
                except RedisConnectionError as e:  # USE IMPORTED EXCEPTION
                    logger.error(f"Redis connection error during FLUSHDB: {e}")
                except RedisTimeoutError as e:  # USE IMPORTED EXCEPTION
                    logger.error(f"Redis timeout during FLUSHDB: {e}")
                except Exception as e:
                    logger.error(f"Error flushing Redis cache: {e}", exc_info=True)

            clear_tasks.append(asyncio.create_task(_flush_redis()))

        # 3. Clear Disk Cache (run in executor)
        if self.disk_cache is not None:
            logger.warning(f"Clearing Disk cache at {self.disk_cache.directory}")

            async def _clear_disk():
                loop = asyncio.get_running_loop()
                try:
                    await loop.run_in_executor(None, self.disk_cache.clear)
                    logger.info("Disk cache cleared.")
                except Exception as e:
                    logger.error(
                        f"Error clearing disk cache (executor): {e}", exc_info=True
                    )

            clear_tasks.append(asyncio.create_task(_clear_disk()))

        # Wait for async clears
        if clear_tasks:
            await asyncio.gather(*clear_tasks)
        logger.warning("Cache clearing process finished.")

    async def close(self):
        """Cleanly close connections (Redis Pool, Disk)."""
        logger.info("Closing DistributedCache connections (ASYNC Redis)...")
        # Close Redis connection pool (async)
        if self.redis_client:
            try:
                await self.redis_client.close()  # Close the client instance
                logger.debug("Redis client closed.")
            except Exception as e:
                logger.error(f"Error closing Redis client: {e}", exc_info=True)

        if self.redis_pool:
            try:
                # Close the underlying pool connections
                await self.redis_pool.disconnect()
                logger.info("Redis connection pool disconnected.")
            except Exception as e:
                logger.error(
                    f"Error disconnecting Redis connection pool: {e}", exc_info=True
                )

        # Reset attributes
        self.redis_client = None
        self.redis_pool = None

        # Close Disk Cache (remains the same)
        if self.disk_cache is not None:
            try:
                # Run close in executor as it might do I/O
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.disk_cache.close)
                logger.info("Disk cache closed.")
            except Exception as e:
                logger.error(f"Error closing disk cache (executor): {e}", exc_info=True)
            self.disk_cache = None

        logger.info("DistributedCache closed.")
