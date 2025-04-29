import asyncio
from rich.progress import Progress
from types import TracebackType  # For type hinting __aexit__
from typing import Optional, Type, Any # Added Any for __init__ args
import logging

log = logging.getLogger("progress_context_wrapper")

class AsyncProgressContext:
    """
    An asynchronous context manager wrapper for rich.progress.Progress.
    Handles manual start() and stop() for use with 'async with'.
    """
    def __init__(self, *args: Any, **kwargs: Any):
        """
        Initializes the underlying Progress object.
        Accepts the same arguments as rich.progress.Progress.
        """
        # Create the actual Progress instance internally
        self._progress = Progress(*args, **kwargs)
        self._started = False # Track if start was called successfully
        log.debug(f"AsyncProgressContext created. Internal Progress ID: {id(self._progress)}")

    async def __aenter__(self) -> Progress:
        """Starts the underlying Progress display when entering the context."""
        log.debug(f"Entering AsyncProgressContext.__aenter__ for Progress ID: {id(self._progress)}")
        try:
            # Start the progress display (idempotent in recent rich versions)
            # Await might not strictly be needed if start() is sync, but safe.
            await self._progress.start()
            self._started = True
            log.debug(f"Internal Progress {id(self._progress)} started.")
            # Return the underlying Progress object so it can be used with 'as'
            return self._progress
        except Exception as e:
            log.error(f"Error starting Progress {id(self._progress)} in __aenter__: {e}", exc_info=True)
            # Re-raise to prevent entering the 'with' block in a bad state
            raise

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        """Stops the underlying Progress display when exiting the context."""
        log.debug(f"Entering AsyncProgressContext.__aexit__ for Progress ID: {id(self._progress)}")
        # Stop the progress display only if it was successfully started
        if self._started:
            try:
                # Await might not strictly be needed if stop() is sync, but safe.
                await self._progress.stop()
                log.debug(f"Internal Progress {id(self._progress)} stopped.")
            except Exception as e:
                 log.error(f"Error stopping Progress {id(self._progress)} in __aexit__: {e}", exc_info=True)
                 # Decide if we should suppress the original exception (if any)
                 # Typically return False or None to let the original exception propagate
                 return False # Do not suppress exception
        # Return False or None to indicate exceptions (if any) should be propagated
        return False


from typing import Sequence, Dict # Added Dict

# Previous definition of run_coroutines_with_progress
async def run_coroutines_with_progress(
    coroutines: Sequence[Coroutine],
    description: str,
    progress: Progress, # Expects an already started Progress object
    return_exceptions: bool = False
) -> List[Any]:
    """
    Runs a sequence of coroutines concurrently, updating a provided (and
    already started) rich.progress.Progress object. Handles task creation
    internally. The caller is responsible for starting/stopping the Progress object
    (or using a context manager like AsyncProgressContext).

    (Implementation details omitted for brevity - see previous answers
     for the full code of this function. It adds a task line, creates tasks
     internally, uses as_completed, updates the task line, finalizes the
     description, and returns results/exceptions).
    """
    num_tasks = len(coroutines)
    if num_tasks == 0:
        log.info(f"'{description}': No coroutines to run.")
        return []

    task_id: Optional[TaskID] = None
    results = [None] * num_tasks
    task_futures = []
    original_indices: Dict[asyncio.Task, int] = {}
    completed_count = 0
    exceptions_caught = []
    first_exception = None

    # --- Step 1: Add the task line to the progress bar ---
    try:
        task_id = progress.add_task(f"{description} (0/{num_tasks})", total=num_tasks, start=False, visible=True)
    except Exception as e:
        log.error(f"Failed to add progress task '{description}': {e}")
        return [] # Cannot proceed

    # --- Step 2: Create asyncio Tasks Internally ---
    for i, coro in enumerate(coroutines):
        if not asyncio.iscoroutine(coro):
            if task_id is not None:
                progress.update(task_id, description=f"[red]✗ {description} (Error: Invalid input)")
            raise TypeError(f"Item at index {i} is not a coroutine: {type(coro)}")
        task = asyncio.create_task(coro, name=f"{description}_item_{i}")
        task_futures.append(task)
        original_indices[task] = i

    # --- Step 3: Monitor Completion using as_completed ---
    for future in asyncio.as_completed(task_futures):
        original_index = original_indices[future]
        try:
            result = await future
            results[original_index] = result
        except Exception as e:
            exceptions_caught.append(e)
            results[original_index] = e
            if not return_exceptions and first_exception is None:
                first_exception = e
            log.debug(f"Task item {original_index} in '{description}' failed: {e}")
        finally:
            completed_count += 1
            # --- Step 4: Update Progress Bar Line ---
            current_desc = f"{description} ({completed_count}/{num_tasks})"
            if exceptions_caught:
                 current_desc += f" - {len(exceptions_caught)} errors!"
            try:
                 if task_id is not None:
                     progress.update(task_id, completed=completed_count, description=current_desc)
            except Exception as e:
                 log.warning(f"Failed to update progress bar for task {task_id}: {e}")

    # --- Step 5: Finalize Progress Bar Line Description ---
    final_desc = ""
    if exceptions_caught:
         final_desc = f"[red]✗ {description} ({len(exceptions_caught)} errors, {num_tasks - len(exceptions_caught)}/{num_tasks} success)"
    else:
         final_desc = f"[green]✓ {description} ({completed_count}/{num_tasks} success)"

    try:
        if task_id is not None:
            progress.update(task_id, description=final_desc, completed=num_tasks)
            # We don't stop the individual task line here, just update it
            # progress.stop_task(task_id) # Optional: stop the spinner on this line
    except Exception as e:
        log.warning(f"Failed to finalize progress bar description for task {task_id}: {e}")

    # --- Step 6: Handle Exceptions / Return Results ---
    if not return_exceptions and first_exception:
        raise first_exception
    else:
        return results


import random # Make sure random is imported
from rich.progress import SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
from rich.logging import RichHandler
from rich.console import Console

# --- Logger Setup (stderr recommended) ---
log_console = Console(stderr=True)
logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]",
                    handlers=[RichHandler(console=log_console, show_path=False, markup=True)])
log = logging.getLogger("wrapper_context_user")


# --- Original Task Function (Example) ---
async def async_task(id, delay):
    log.debug(f"Task {id} starting (sleep {delay:.2f}s)")
    await asyncio.sleep(delay)
    if id == 'A-2': raise ValueError(f"Error in async {id}")
    log.debug(f"Task {id} finished")
    return f"Async Result {id}"


# --- Main Orchestration using the AsyncProgressContext Wrapper ---
async def main_with_context_wrapper():
    # --- Progress Bar Columns (defined once) ---
    progress_columns = [
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()
    ]

    # --- Create lists of COROUTINES ---
    log.info("Preparing coroutines...")
    coroutines_group_a = [async_task(f"A-{i}", random.random() * 0.6) for i in range(5)]
    coroutines_group_b = [async_task(f"B-{i}", random.random() * 0.8) for i in range(7)]

    # --- Use async with on the custom context wrapper ---
    log.info("Entering async context with AsyncProgressContext...")
    # Pass the column definitions etc. to the wrapper's constructor
    async with AsyncProgressContext(*progress_columns, transient=False) as progress:
        # 'progress' here is the actual rich.progress.Progress instance,
        # started by AsyncProgressContext.__aenter__
        log.info(f"Inside async context. Using Progress object ID: {id(progress)}")

        log.info("Launching concurrent group processing...")
        wrapper_results = await asyncio.gather(
            run_coroutines_with_progress( # Use the version taking coroutines
                coroutines_group_a, "Processing Group A", progress, return_exceptions=True
            ),
            run_coroutines_with_progress(
                coroutines_group_b, "Processing Group B", progress, return_exceptions=True
            ),
            return_exceptions=True # For gather itself
        )
        log.info("Concurrent group processing finished within async context.")

    # --- Exited async context, progress.stop() was called by __aexit__ ---
    log.info("Exited async context.")

    # --- Process results ---
    log.info("Processing results...")
    for i, group_result_or_exc in enumerate(wrapper_results):
        group_name = f"Group {'A' if i == 0 else 'B'}"
        if isinstance(group_result_or_exc, Exception):
            log.error(f"{group_name} wrapper itself failed: {group_result_or_exc}")
        else:
            log.info(f"Results for {group_name}:")
            exceptions_in_group = []
            for item in group_result_or_exc:
                if isinstance(item, Exception):
                    exceptions_in_group.append(item)
                    log.warning(f"  - Task Error: {item}")
                # else:
                #     log.info(f"  - Success: {item}")
            if exceptions_in_group:
                log.warning(f"  ({len(exceptions_in_group)} errors detected in group)")
            else:
                log.info(f"  (All tasks succeeded in group)")

    log.info("Main function finished.")


# --- Run ---
if __name__ == "__main__":
    asyncio.run(main_with_context_wrapper())
