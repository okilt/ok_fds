from typing import Any, List, Coroutine, Callable, Union, Optional, Sequence
import asyncio
from rich.progress import Progress, TaskID
import logging

# Assume logger 'log' is already configured
log = logging.getLogger("rich_progress_wrapper")

async def run_with_progress(
    work: Union[Coroutine, Callable[[], Any], Sequence[Union[Coroutine, Callable[[], Any]]]],
    description: str,
    progress: Progress,
    total: Optional[int] = None, # Explicit total needed if work is not a sequence or is an iterator
    return_exceptions: bool = False # Similar to asyncio.gather
) -> Any:
    """
    Runs async or blocking work with rich progress reporting.

    Handles:
        - A single awaitable (Coroutine).
        - A single blocking function (runs in asyncio.to_thread).
        - A sequence (e.g., list) of awaitables or blocking functions
          (runs concurrently, updates progress incrementally like as_completed,
           returns results like gather).

    Args:
        work: The work to perform. Can be a coroutine, a blocking function,
              or a sequence of coroutines/blocking functions.
        description: Text description for the progress bar task.
        progress: The rich.progress.Progress object to use.
        total: The total number of items (required if 'work' is a sequence
               and needed for accurate MofN/Bar columns). If 'work' is a
               sequence and total is None, it defaults to len(work).
        return_exceptions: If True and 'work' is a sequence, exceptions
                           are returned in the result list instead of being
                           raised. If False (default), the first exception
                           encountered during concurrent execution is raised.

    Returns:
        - The result of the single coroutine or function.
        - A list containing results (and potentially exceptions if
          return_exceptions=True) in the original order if 'work' was a sequence.

    Raises:
        Exception: If return_exceptions is False and an exception occurs
                   during concurrent execution of a sequence, the first
                   exception is raised. Also raises TypeError for invalid 'work' input.
    """
    task_id: Optional[TaskID] = None # Initialize task_id

    # --- Helper to safely add/start task ---
    def _add_and_start_task(desc: str, task_total: Optional[float]):
        nonlocal task_id
        try:
            task_id = progress.add_task(desc, total=task_total, start=False, visible=True)
            progress.start_task(task_id)
            return task_id
        except Exception as e:
            log.error(f"Failed to add/start progress task '{desc}': {e}")
            return None # Indicate failure

    # --- Helper to update task description safely ---
    def _update_description(new_description: str):
        if task_id is not None:
            try:
                progress.update(task_id, description=new_description)
            except Exception as e:
                log.warning(f"Failed to update progress description for task {task_id}: {e}")

    # --- Helper to stop task safely ---
    def _stop_task():
         if task_id is not None:
            try:
                progress.stop_task(task_id)
            except Exception as e:
                 log.warning(f"Failed to stop progress task {task_id}: {e}")


    # --- Handle Sequence (gather/as_completed like behavior) ---
    if isinstance(work, Sequence) and not isinstance(work, (str, bytes)):
        if total is None:
            total = len(work)
        if total == 0:
            log.info(f"'{description}': No tasks to run.")
            return [] # Return empty list for empty sequence

        task_id = _add_and_start_task(f"{description} (0/{total})", total)
        if task_id is None: return [] # Failed to add task

        results = [None] * total
        exceptions = []
        completed_count = 0
        futures = []
        original_indices = {} # Map future back to original index

        # Create asyncio Tasks for concurrent execution
        for i, item in enumerate(work):
            future: asyncio.Future
            if asyncio.iscoroutine(item):
                future = asyncio.create_task(item)
            elif callable(item) and not asyncio.iscoroutinefunction(item):
                # Wrap blocking function call in to_thread
                future = asyncio.create_task(asyncio.to_thread(item), name=f"{description}_item_{i}")
            else:
                _update_description(f"[red]✗ {description} (Invalid item at index {i})")
                _stop_task()
                raise TypeError(f"Item at index {i} in 'work' sequence is not an awaitable or callable: {type(item)}")

            original_indices[future] = i
            futures.append(future)

        # Process tasks as they complete to update progress
        first_exception = None
        for future in asyncio.as_completed(futures):
            original_index = original_indices[future]
            try:
                result = await future
                results[original_index] = result
            except Exception as e:
                exceptions.append(e)
                results[original_index] = e # Store exception if return_exceptions=True
                if not return_exceptions and first_exception is None:
                    first_exception = e # Store the first exception to potentially re-raise
                log.debug(f"Task item {original_index} in '{description}' failed: {e}")
            finally:
                completed_count += 1
                # Update progress description and bar
                current_desc = f"{description} ({completed_count}/{total})"
                if exceptions:
                     current_desc += f" - {len(exceptions)} errors!"

                if task_id is not None: # Check if task was added successfully
                    try:
                        progress.update(task_id, completed=completed_count, description=current_desc)
                    except Exception as e:
                         log.warning(f"Failed to update progress bar for task {task_id}: {e}")


        # Final status update
        if exceptions:
             final_desc = f"[red]✗ {description} ({len(exceptions)} errors, {total - len(exceptions)}/{total} success)"
        else:
             final_desc = f"[green]✓ {description} ({completed_count}/{total} success)"
        _update_description(final_desc)
        _stop_task()

        # Handle exceptions based on return_exceptions flag
        if not return_exceptions and first_exception:
            raise first_exception
        else:
            return results # Return list of results (may contain exceptions)

    # --- Handle Single Coroutine ---
    elif asyncio.iscoroutine(work):
        task_id = _add_and_start_task(description, total=1.0) # Use float total for single indeterminate bar
        if task_id is None: return None
        try:
            result = await work
            _update_description(f"[green]✓ {description}")
            progress.update(task_id, completed=1.0) # Mark as complete
            return result
        except Exception as e:
            _update_description(f"[red]✗ {description} (Error)")
            progress.update(task_id, completed=1.0) # Still mark as "finished" in terms of progress
            raise e # Re-raise the exception
        finally:
            _stop_task()


    # --- Handle Single Blocking Callable ---
    elif callable(work) and not asyncio.iscoroutinefunction(work):
        task_id = _add_and_start_task(description, total=1.0)
        if task_id is None: return None
        try:
            result = await asyncio.to_thread(work)
            _update_description(f"[green]✓ {description}")
            progress.update(task_id, completed=1.0)
            return result
        except Exception as e:
            _update_description(f"[red]✗ {description} (Error)")
            progress.update(task_id, completed=1.0)
            raise e
        finally:
            _stop_task()

    # --- Handle Invalid Input ---
    else:
        raise TypeError(f"Unsupported type for 'work' parameter: {type(work)}")


# --- Example Usage ---

import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
from rich.logging import RichHandler
from rich.console import Console

# --- Logging Settings (stderr) ---
log_console = Console(stderr=True)
logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]",
                    handlers=[RichHandler(console=log_console, show_path=False, markup=True)])
log = logging.getLogger("wrapper_user")


# --- Original Task Functions (unchanged) ---
async def async_task(id, delay):
    await asyncio.sleep(delay)
    if id == 3: raise ValueError("Simulated error in async task 3")
    return f"Async Result {id}"

def blocking_task(id, delay):
    time.sleep(delay)
    if id == 1: raise ValueError("Simulated error in blocking task 1")
    return f"Blocking Result {id}"

# --- Main Orchestration using the Wrapper ---
async def main():
    # --- プログレスバー設定 ---
    progress_columns = [
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()
    ]

    async with Progress(*progress_columns, transient=False) as progress:

        # --- Example 1: Running a list like gather ---
        log.info("Example 1: Running list like gather (errors included in result)")
        tasks_for_gather = [
            async_task(1, 0.8),
            async_task(2, 0.3),
            async_task(3, 0.5), # This one will raise ValueError
            lambda: blocking_task(0, 0.6), # Blocking task
            lambda: blocking_task(1, 0.4), # This one will raise ValueError
        ]
        gather_results = await run_with_progress(
            tasks_for_gather,
            "Processing Batch A (gather-like)",
            progress,
            return_exceptions=True # Get exceptions in the list
        )
        log.info(f"Batch A Results (incl. exceptions): {gather_results}")
        print("-" * 20)

        # --- Example 2: Running a list like gather (raising first error) ---
        log.info("Example 2: Running list like gather (raising first error)")
        tasks_raise_error = [
             async_task(10, 0.2),
             lambda: blocking_task(10, 0.5),
             async_task(11, 0.1), # Might not run if error happens before
             async_task(3, 0.3),  # The one that errors
        ]
        try:
             await run_with_progress(
                 tasks_raise_error,
                 "Processing Batch B (raise error)",
                 progress,
                 return_exceptions=False # Default
             )
        except Exception as e:
             log.error(f"Batch B failed as expected: {type(e).__name__}: {e}")
        print("-" * 20)

        # --- Example 3: Running a single async task ---
        log.info("Example 3: Running single async task")
        single_async_result = await run_with_progress(
            async_task(99, 0.7),
            "Running Single Async",
            progress
        )
        log.info(f"Single Async Result: {single_async_result}")
        print("-" * 20)

        # --- Example 4: Running a single blocking task ---
        log.info("Example 4: Running single blocking task")
        single_blocking_result = await run_with_progress(
            lambda: blocking_task(55, 0.6), # Use lambda to pass args
            "Running Single Blocking",
            progress
        )
        log.info(f"Single Blocking Result: {single_blocking_result}")
        print("-" * 20)

        # Example 5: Empty list
        log.info("Example 5: Running empty list")
        empty_results = await run_with_progress([], "Processing Empty Batch", progress)
        log.info(f"Empty Batch Results: {empty_results}")


    log.info("Main function finished.")

# --- Execute ---
if __name__ == "__main__":
    asyncio.run(main())
