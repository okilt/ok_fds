from src.cache.cache_invalidation import (
    CacheInvalidator,
    DependencyBasedInvalidation,
    TimeBasedInvalidation,
)

# This block is for demonstration/testing purposes and wouldn't run in production.
# Requires a mock or real CacheManager instance.


# Mock CacheManager for demonstration
class MockCacheManager:
    def get_dependents(self, key):
        print(f"[MockCacheManager] Getting dependents for key: {key}")
        # Simulate dependency: key 'B' depends on 'A', 'C' depends on 'A'
        if key == "A":
            return {"B", "C"}
        elif key == "B":
            return {"D"}  # D depends on B
        return set()

    def invalidate_keys(self, keys):
        print(f"[MockCacheManager] Invalidating keys: {keys}")

    def get(self, key):
        pass

    def set(self, key, value, ttl=None, dependencies=None):
        pass


print("--- Demonstrating Cache Invalidation ---")
mock_cache_manager = MockCacheManager()
invalidator = CacheInvalidator(mock_cache_manager)

print("\nTriggering invalidation for key 'A':")
invalidator.trigger_invalidation("A")
# Expected output: Should identify A, B, C for invalidation

print("\nTriggering invalidation for key 'B':")
invalidator.trigger_invalidation("B")
# Expected output: Should identify B, D for invalidation

print("\nTriggering invalidation for key 'X' (no dependents):")
invalidator.trigger_invalidation("X")
# Expected output: Should identify only X for invalidation

print("\n--- Demonstrating with multiple strategies (conceptual) ---")
# Add a TimeBased strategy (though its effect is passive/demonstrative here)
invalidator_multi = CacheInvalidator(
    mock_cache_manager,
    strategies=[
        DependencyBasedInvalidation(mock_cache_manager),
        TimeBasedInvalidation(
            mock_cache_manager
        ),  # This won't do much in this mock setup
    ],
)
print("\nTriggering invalidation for key 'A' with multiple strategies:")
invalidator_multi.trigger_invalidation("A")
