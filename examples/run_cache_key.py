import logging
import pandas as pd
import numpy as np

from src.cache.cache_key import generate_cache_key


logging.basicConfig(level=logging.DEBUG)


def example_function(a: int, b: str, c: list, d: dict, e: pd.DataFrame = None):
    pass


df = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
arr = np.array([[1, 2], [3, 4]])

print("--- Generating Cache Keys ---")

key1 = generate_cache_key(
    example_function, 1, "hello", [1, 2], {"x": 10, "y": 20}, e=df
)
print(f"Key 1: {key1}")

# Same logical inputs, different dict order - should yield same key
key2 = generate_cache_key(
    example_function, 1, "hello", [1, 2], {"y": 20, "x": 10}, e=df
)
print(f"Key 2 (same as 1?): {key2} -> {key1 == key2}")

# Different argument value
key3 = generate_cache_key(
    example_function, 1, "world", [1, 2], {"x": 10, "y": 20}, e=df
)
print(f"Key 3 (different): {key3}")

# Key without function context
key4 = generate_cache_key(None, "data_id_1", {"param": True})
print(f"Key 4 (no func): {key4}")

# Key with set and numpy array
key5 = generate_cache_key(None, {3, 1, 2}, arr)
print(f"Key 5 (set, array): {key5}")

# Key with slightly different DataFrame (e.g., different column name)
df_alt = pd.DataFrame({"col_A": [1, 2], "col_B": ["A", "B"]})
key6 = generate_cache_key(
    example_function, 1, "hello", [1, 2], {"x": 10, "y": 20}, e=df_alt
)
print(f"Key 6 (alt df): {key6} -> {key1 == key6}")


# Example of unserializable type
class Unserializable:
    pass


print("\n--- Testing Unserializable Type ---")
try:
    # This should now raise TypeError originating from _stable_json_serializer
    generate_cache_key(None, Unserializable())
except TypeError as e:
    print(f"Caught expected TypeError for unserializable type: {e}")
except Exception as e:
    print(f"Caught unexpected error for unserializable type: {type(e).__name__} - {e}")


# Example demonstrating __cache_key__
class SerializableWithMethod:
    def __init__(self, id, value):
        self.id = id
        self.value = value

    def __cache_key__(self):
        # Define how this object should be represented in a cache key
        return {
            "__type__": "SerializableWithMethod",
            "id": self.id,
        }  # Only use ID for key


obj_serializable = SerializableWithMethod("obj1", {"complex": "data"})
key7 = generate_cache_key(None, obj_serializable, 123)
print(f"\nKey 7 (custom __cache_key__): {key7}")

obj_serializable_diff_value = SerializableWithMethod("obj1", {"different": "stuff"})
key8 = generate_cache_key(None, obj_serializable_diff_value, 123)
print(
    f"Key 8 (custom __cache_key__, same ID): {key8} -> {key7 == key8}"
)  # Should be True
