# src/utils/dependency_utils.py
"""
Dependency Tracking Utilities Module.

Provides classes for tracking dependencies between data items or function calls,
and a context manager to facilitate automatic dependency discovery within code blocks.
Essential for the cache invalidation mechanism.
"""

from __future__ import annotations
import asyncio
import contextvars
import logging
import threading
from collections import defaultdict, deque  # Import deque for BFS queue
from contextlib import contextmanager, asynccontextmanager
from typing import Any, Set, DefaultDict, Iterator, AsyncIterator, Optional, Dict

logger = logging.getLogger(__name__)

# --- Dependency Tracker ---


class DependencyTracker:
    """
    Stores and manages the dependency graph between tracked keys.

    Maintains relationships where one key (e.g., a cached result) depends
    on other keys (e.g., source data). Provides methods to add dependencies,
    find dependents for invalidation, and clean up nodes.

    This class should generally be treated as a singleton or a shared instance
    managed centrally (e.g., by CacheManager). Thread-safety is handled via locks.
    """

    _instance = None
    _lock = threading.RLock()  # Reentrant lock for thread-safe modifications

    def __new__(cls, *args, **kwargs):
        # Singleton pattern
        with cls._lock:
            if cls._instance is None:
                logger.info("Creating DependencyTracker instance")
                cls._instance = super(DependencyTracker, cls).__new__(cls)
                cls._instance._initialised = False
            return cls._instance

    def __init__(self):
        """Initialise the DependencyTracker Singleton."""
        # Singleton initialisation guard
        if hasattr(self, "_initialised") and self._initialised:
            return

        with self._lock:
            if not self._initialised:
                # adj: key -> set of keys it directly depends on
                self.adj: DefaultDict[Any, Set[Any]] = defaultdict(set)
                # rev_adj: key -> set of keys that directly depend on it
                self.rev_adj: DefaultDict[Any, Set[Any]] = defaultdict(set)
                logger.info("DependencyTracker initialised.")
                self._initialised = True

    def add_dependencies(self, dependent_key: Any, dependency_keys: Set[Any]):
        """
        Record that `dependent_key` depends on the keys in `dependency_keys`.

        Args:
            dependent_key: The key that has dependencies.
            dependency_keys: A set of keys that `dependent_key` depends on.
        """
        if not dependency_keys:
            return

        with self._lock:
            # logger.debug(f"Adding dependencies: {dependent_key} depends on {dependency_keys}")
            # Make defensive copies? Not strictly needed if caller doesn't modify set later.
            current_dependencies = self.adj[dependent_key]
            new_dependencies = (
                dependency_keys - current_dependencies
            )  # Find only new ones to add reverse links for

            if not new_dependencies:  # No change in dependencies
                return

            self.adj[dependent_key].update(new_dependencies)

            # Update reverse dependencies only for the new ones added
            for dep_key in new_dependencies:
                self.rev_adj[dep_key].add(dependent_key)

            # logger.debug(f"Adj list for {dependent_key}: {self.adj.get(dependent_key)}")
            # for dep_key in new_dependencies:
            #    logger.debug(f"Rev Adj list for {dep_key}: {self.rev_adj.get(dep_key)}")

    def set_dependencies(self, dependent_key: Any, dependency_keys: Set[Any]):
        """
        Sets the dependencies for `dependent_key`, removing any previous dependencies.

        Args:
            dependent_key: The key whose dependencies are being set.
            dependency_keys: The complete set of keys that `dependent_key` now depends on.
        """
        with self._lock:
            logger.debug(
                f"Setting dependencies for {dependent_key} to: {dependency_keys}"
            )
            old_dependencies = self.adj.get(dependent_key, set()).copy()
            new_dependencies = set(dependency_keys)  # Ensure it's a set

            # --- Update Forward List (adj) ---
            self.adj[dependent_key] = new_dependencies
            if not new_dependencies:  # Clean up if no dependencies anymore
                del self.adj[dependent_key]

            # --- Update Reverse List (rev_adj) ---
            # 1. Remove links from dependencies that are no longer depended upon
            removed_deps = old_dependencies - new_dependencies
            for removed_key in removed_deps:
                if removed_key in self.rev_adj:
                    self.rev_adj[removed_key].discard(dependent_key)
                    if not self.rev_adj[removed_key]:  # Clean up empty entries
                        del self.rev_adj[removed_key]

            # 2. Add links for dependencies that are newly added
            added_deps = new_dependencies - old_dependencies
            for added_key in added_deps:
                self.rev_adj[added_key].add(dependent_key)

    def get_dependents(self, key: Any) -> Set[Any]:
        """
        Find all keys that directly OR INDIRECTLY depend on the given key (recursive).

        Uses Breadth-First Search (BFS) on the reverse dependency graph (`rev_adj`).
        This is crucial for cache invalidation: if `key` changes, all keys returned
        by this method (its dependents) might become stale.

        Args:
            key: The key whose dependents are to be found.

        Returns:
            A set containing all direct and indirect dependent keys. Returns
            an empty set if the key has no dependents or is not tracked.
        """
        with self._lock:
            if (
                key not in self.rev_adj and key not in self.adj
            ):  # Check both maps to see if key exists at all
                logger.debug(f"Key {key} not found in tracker dependency graph.")
                return set()

            dependents: Set[Any] = set()
            queue: deque[Any] = deque()  # Queue for BFS

            # Start BFS from the direct dependents of the input key
            direct_dependents = self.rev_adj.get(key, set())
            visited: Set[Any] = set(direct_dependents)  # Keep track of visited nodes
            dependents.update(direct_dependents)
            queue.extend(direct_dependents)

            # logger.debug(f"Starting recursive BFS traversal to find dependents of {key}")
            # logger.debug(f"  Direct dependents: {direct_dependents}")

            while queue:
                current_key = queue.popleft()
                # logger.debug(f"  Processing node: {current_key}")

                # Find direct dependents of the current key
                next_level_dependents = self.rev_adj.get(current_key, set())
                # logger.debug(f"    Dependents of {current_key}: {next_level_dependents}")

                for dependent in next_level_dependents:
                    if dependent not in visited:
                        # logger.debug(f"      Adding unvisited {dependent} to dependents set and queue.")
                        visited.add(dependent)
                        dependents.add(dependent)
                        queue.append(dependent)
                    # else:
                    # logger.debug(f"      Skipping already visited dependent: {dependent}")

            logger.debug(
                f"Found {len(dependents)} total dependents for key {key}: {dependents}"
            )
            return dependents

    def remove_key(self, key: Any):
        """
        Remove a key and all its associated dependency information from the tracker.

        This should be called when a cached item associated with `key` is explicitly
        deleted or invalidated permanently.

        Args:
            key: The key to remove from the dependency graph.
        """
        with self._lock:
            if key not in self.adj and key not in self.rev_adj:
                # logger.debug(f"Key {key} not found in tracker. Nothing to remove.")
                return

            # logger.info(f"Removing key {key} and its dependencies from tracker.")

            # 1. Remove outgoing dependencies (from adj list)
            dependencies = self.adj.pop(key, set())
            # logger.debug(f"  Key {key} depended on: {dependencies}")
            # For each key it depended on, remove 'key' from their reverse list
            for dep_key in dependencies:
                if dep_key in self.rev_adj:
                    # logger.debug(f"    Removing {key} from rev_adj of {dep_key}")
                    self.rev_adj[dep_key].discard(key)
                    # Clean up empty entries in rev_adj
                    if not self.rev_adj[dep_key]:
                        # logger.debug(f"      Removing empty rev_adj entry for {dep_key}")
                        del self.rev_adj[dep_key]

            # 2. Remove incoming dependencies (from rev_adj list)
            dependents = self.rev_adj.pop(key, set())
            # logger.debug(f"  Keys dependent on {key}: {dependents}")
            # For each key that depended on 'key', remove 'key' from their forward list
            for dep_key in dependents:
                if dep_key in self.adj:
                    # logger.debug(f"    Removing {key} from adj of {dep_key}")
                    self.adj[dep_key].discard(key)
                    # Clean up empty entries in adj
                    if not self.adj[dep_key]:
                        # logger.debug(f"      Removing empty adj entry for {dep_key}")
                        del self.adj[dep_key]

            # logger.info(f"Finished removing key {key}.")

    def clear_all(self):
        """Removes all keys and dependencies from the tracker."""
        with self._lock:
            logger.warning("Clearing all dependency tracking information.")
            self.adj.clear()
            self.rev_adj.clear()

    def get_graph_data(self) -> Dict[str, DefaultDict[Any, Set[Any]]]:
        """Returns a copy of the internal graph data for inspection."""
        with self._lock:
            # Return copies to prevent modification
            adj_copy = defaultdict(set, {k: v.copy() for k, v in self.adj.items()})
            rev_adj_copy = defaultdict(
                set, {k: v.copy() for k, v in self.rev_adj.items()}
            )
            return {"adj": adj_copy, "rev_adj": rev_adj_copy}


# --- Dependency Context --- (No changes needed here)

_current_dependencies_var: contextvars.ContextVar[Optional[Set[Any]]] = (
    contextvars.ContextVar("current_dependencies", default=None)
)


class DependencyContext:
    """
    A context manager (sync/async) to track dependencies within a code block.
    See previous implementation.
    """

    def __init__(self, tracker: Optional[DependencyTracker] = None):
        self._collected_dependencies: Set[Any] = set()
        self._token: Optional[contextvars.Token] = None

    @staticmethod
    def add_dependency(key: Any):
        current_deps_set = _current_dependencies_var.get()
        if current_deps_set is not None:
            current_deps_set.add(key)

    def get_dependencies(self) -> Set[Any]:
        return self._collected_dependencies

    def __enter__(self):
        self._collected_dependencies = set()
        self._token = _current_dependencies_var.set(self._collected_dependencies)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._token:
            _current_dependencies_var.reset(self._token)
        return False

    async def __aenter__(self):
        self._collected_dependencies = set()
        self._token = _current_dependencies_var.set(self._collected_dependencies)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._token:
            _current_dependencies_var.reset(self._token)
        return False


# --- End Dependency Context ---


# Function to get the Singleton instance
def get_dependency_tracker() -> DependencyTracker:
    """Returns the Singleton instance of the DependencyTracker."""
    instance = DependencyTracker()
    return instance


# --- Example Usage (Updated for recursive check) ---


async def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - [%(threadName)s] %(name)s - %(message)s",
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    tracker = get_dependency_tracker()
    tracker.clear_all()  # Start fresh for example

    # Setup dependencies: A -> B, A -> C, B -> D
    key_A = "data:source:A"
    key_B = "calc:step1:B"
    key_C = "calc:step1:C"
    key_D = "calc:step2:D"

    # Simulate caching B (depends on A)
    tracker.add_dependencies(key_B, {key_A})
    # Simulate caching C (depends on A)
    tracker.add_dependencies(key_C, {key_A})
    # Simulate caching D (depends on B)
    tracker.add_dependencies(key_D, {key_B})

    print("\n--- Current Dependency Graph ---")
    graph_data = tracker.get_graph_data()
    print("Forward dependencies (adj):")
    for k, v in graph_data["adj"].items():
        print(f"  {k}: {v}")
    print("Reverse dependencies (rev_adj):")
    for k, v in graph_data["rev_adj"].items():
        print(f"  {k}: {v}")
    # Expected rev_adj: A: {B, C}, B: {D}

    print(f"\n--- Finding all dependents of '{key_A}' (Should be recursive) ---")
    dependents_of_a = tracker.get_dependents(key_A)
    print(f"Dependents of A: {dependents_of_a}")
    # *** Expected: {key_B, key_C, key_D} ***

    print(f"\n--- Finding all dependents of '{key_B}' ---")
    dependents_of_b = tracker.get_dependents(key_B)
    print(f"Dependents of B: {dependents_of_b}")
    # Expected: {key_D}

    print(f"\n--- Finding all dependents of '{key_C}' ---")
    dependents_of_c = tracker.get_dependents(key_C)
    print(f"Dependents of C: {dependents_of_c}")
    # Expected: set()

    print(f"\n--- Finding all dependents of '{key_D}' ---")
    dependents_of_d = tracker.get_dependents(key_D)
    print(f"Dependents of D: {dependents_of_d}")
    # Expected: set()


if __name__ == "__main__":
    import asyncio

    # Add local imports needed only for main example
    from collections import deque  # Needed for BFS

    asyncio.run(main())
