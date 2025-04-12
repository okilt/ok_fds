# src/cache/cache_key.py
"""
Cache Key Generation Module.

Provides functions to generate consistent and hashable cache keys
from various Python objects, including function arguments and keyword arguments.
Ensures that the same logical input results in the same cache key.
"""

from __future__ import annotations
import hashlib
import inspect
import json
import logging
from typing import Any, Callable
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Consider using a more robust serialization method if complex objects are common,
# e.g., cloudpickle, but json is often sufficient and safer.
# Using json.dumps with sorting ensures consistent key order.


def _stable_json_serializer(
    obj: Any,
) -> Any:  # Return type changed to Any, but must be JSON-serializable
    """
    Custom JSON serializer to handle common data types including DataFrames,
    numpy arrays, sets, functions, and objects with __cache_key__.

    Args:
        obj: The Python object to serialize.

    Returns:
        A JSON-compatible representation of the object.

    Raises:
        TypeError: If an object type cannot be reliably serialized for caching.
    """
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        # Represent DataFrame/Series by shape, columns, dtypes and maybe a content hash.
        # Hashing full content can be expensive. A simpler approach for now:
        # use a descriptive representation. For stronger guarantees, hash obj.to_msgpack() or similar.
        # Using shape, columns, and dtypes provides a reasonable balance.
        if isinstance(obj, pd.DataFrame):
            return {
                "__type__": "pandas.DataFrame",
                "shape": obj.shape,
                "columns": sorted(list(obj.columns)),
                "dtypes": {col: str(dtype) for col, dtype in obj.dtypes.items()},
                # Optionally add a content hash (can be slow):
                # "content_hash": hashlib.sha256(pd.util.hash_pandas_object(obj, index=True).values).hexdigest()
            }
        else:  # pd.Series
            return {
                "__type__": "pandas.Series",
                "shape": obj.shape,
                "name": obj.name,
                "dtype": str(obj.dtype),
                # Optionally add a content hash:
                # "content_hash": hashlib.sha256(pd.util.hash_pandas_object(obj, index=True).values).hexdigest()
            }
    elif isinstance(obj, np.ndarray):
        # Similar approach for numpy arrays
        return {
            "__type__": "numpy.ndarray",
            "shape": obj.shape,
            "dtype": str(obj.dtype),
            # Optionally add a content hash (can be slow):
            # "content_hash": hashlib.sha256(obj.tobytes()).hexdigest()
        }
    elif isinstance(obj, (set, frozenset)):
        # Convert sets to sorted lists for stable representation
        return sorted(list(obj))
    elif hasattr(obj, "__cache_key__"):
        # Allow objects to define their own cache key representation
        try:
            key_repr = obj.__cache_key__()
            # Basic check: ensure the custom key is serializable
            json.dumps(key_repr)
            return key_repr
        except Exception as e:
            logger.error(
                f"Error calling or serializing __cache_key__ for object {obj}: {e}",
                exc_info=True,
            )
            raise TypeError(
                f"Object's __cache_key__ method failed or returned non-serializable data for {type(obj)}"
            ) from e
    elif inspect.isfunction(obj) or inspect.ismethod(obj):
        # Represent functions/methods by their qualified name
        try:
            return f"{obj.__module__}.{obj.__qualname__}"
        except AttributeError:
            return repr(obj)  # Fallback if introspection fails
    else:
        # Explicitly raise TypeError for unhandled types
        # Instead of returning obj, raise error because json.dumps default won't handle it reliably for caching.
        # We rely on json.dumps to handle built-in types (dict, list, str, int, float, bool, None) directly *before* calling this default function.
        # If this function is called, it means json.dumps couldn't handle the type itself.
        raise TypeError(
            f"Object of type {type(obj).__name__} is not JSON serializable for cache key generation. "
            f"Consider adding a __cache_key__ method or using simpler types."
        )


def generate_cache_key(func: Callable | None = None, *args: Any, **kwargs: Any) -> str:
    """
    Generates a stable cache key from a function, arguments, and keyword arguments.

    Handles various data types including primitives, lists, dicts, sets,
    pandas DataFrames/Series, and numpy arrays. Sorts dictionary keys and set elements
    for consistency. Uses JSON serialization with a custom handler and then hashes
    the result using SHA256 for a robust key.

    Args:
        func: The function being called (optional, include for function caching).
        *args: Positional arguments passed to the function.
        **kwargs: Keyword arguments passed to the function.

    Returns:
        A string representing the generated cache key (SHA256 hash).

    Raises:
        TypeError: If any argument contains a type that cannot be serialized
                   stably by the custom JSON encoder.
    """
    key_elements = []
    func_part = ""
    args_part = ""
    kwargs_part = ""

    # 1. Include function identifier if provided
    if func:
        try:
            # No need to pass func through json.dumps, just serialize it directly
            # If func itself is complex and needs __cache_key__, _stable_json_serializer handles it.
            func_part = _stable_json_serializer(func)
            key_elements.append(func_part)
            # logger.debug(f"Cache key element (func): {func_part}")
        except TypeError as e:
            logger.error(
                f"Failed to serialize function/method for cache key: {func}. Error: {e}"
            )
            raise TypeError(
                f"Cannot generate cache key: Unserializable function/method type. Error: {e}"
            ) from e

    # 2. Process positional arguments
    try:
        args_part = json.dumps(
            args, default=_stable_json_serializer, sort_keys=True, separators=(",", ":")
        )
        key_elements.append(args_part)
        # logger.debug(f"Cache key element (args): {args_part}")
    except TypeError as e:
        # This will now catch TypeErrors raised by _stable_json_serializer for unknown types
        logger.error(
            f"Failed to serialize positional arguments for cache key: {args}. Error: {e}"
        )
        raise TypeError(
            f"Cannot generate cache key: Unserializable type found in positional arguments (*args). Error: {e}"
        ) from e
    except Exception as e:
        # Catch other unexpected errors during serialization
        logger.error(
            f"Unexpected error serializing positional arguments for cache key: {args}. Error: {e}",
            exc_info=True,
        )
        # Don't raise the confusing circular reference error if that was the underlying cause
        if "Circular reference detected" in str(e):
            raise TypeError(
                "Cannot generate cache key due to potential circular reference or complex object structure in positional arguments (*args)."
            ) from e
        raise RuntimeError(
            f"Cannot generate cache key due to unexpected error in *args serialization. Error: {e}"
        ) from e

    # 3. Process keyword arguments (ensure stable order)
    try:
        # Sort kwargs by key before serializing
        sorted_kwargs_items = sorted(kwargs.items())
        kwargs_part = json.dumps(
            sorted_kwargs_items,
            default=_stable_json_serializer,
            sort_keys=True,
            separators=(",", ":"),
        )
        key_elements.append(kwargs_part)
        # logger.debug(f"Cache key element (kwargs): {kwargs_part}")
    except TypeError as e:
        # Catch TypeErrors from _stable_json_serializer
        logger.error(
            f"Failed to serialize keyword arguments for cache key: {kwargs}. Error: {e}"
        )
        raise TypeError(
            f"Cannot generate cache key: Unserializable type found in keyword arguments (**kwargs). Error: {e}"
        ) from e
    except Exception as e:
        logger.error(
            f"Unexpected error serializing keyword arguments for cache key: {kwargs}. Error: {e}",
            exc_info=True,
        )
        if "Circular reference detected" in str(e):
            raise TypeError(
                "Cannot generate cache key due to potential circular reference or complex object structure in keyword arguments (**kwargs)."
            ) from e
        raise RuntimeError(
            f"Cannot generate cache key due to unexpected error in **kwargs serialization. Error: {e}"
        ) from e

    # 4. Combine elements and hash
    combined_repr = "|".join(key_elements)
    # Use SHA256 for a robust hash
    hash_object = hashlib.sha256(combined_repr.encode("utf-8"))
    cache_key = hash_object.hexdigest()

    # logger.debug(f"Generated cache key: {cache_key} from representation: {combined_repr}")
    # Debug log showing the parts:
    # logger.debug(f"Key parts: Func='{func_part}', Args='{args_part}', Kwargs='{kwargs_part}' -> Hash={cache_key}")
    return cache_key
