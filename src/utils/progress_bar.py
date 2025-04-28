from typing import Any, List, Coroutine, Union, Optional, Sequence
import asyncio
from rich.progress import Progress, TaskID
import logging

log = logging.getLogger("coroutine_wrapper") # Logger instance

async def run_coroutines_with_progress(
    coroutines: Sequence[Coroutine], # ★ Takes a sequence of Coroutine objects
    description: str,
    progress: Progress,
    return_exceptions: bool = False
) -> List[Any]:
    """
    Runs a sequence of coroutines concurrently with rich progress reporting.
    Handles task creation internally.

    Args:
        coroutines: The sequence of awaitable coroutine objects to run.
        description: Text description for the progress bar task.
        progress: The rich.progress.Progress object to use.
        return_exceptions: If True, exceptions are returned in the result list.
                           If False (default), the first exception is raised.

    Returns:
        A list containing results (and potentially exceptions if
        return_exceptions=True) in the original order.

    Raises:
        Exception: If return_exceptions is False and an exception occurs.
        TypeError: If any item in 'coroutines' is not awaitable.
    """
    num_tasks = len(coroutines)
    if num_tasks == 0:
        log.info(f"'{description}': No coroutines to run.")
        return []

    task_id: Optional[TaskID] = None
    results = [None] * num_tasks
    task_futures = [] # To store the created asyncio.Task objects
    original_indices: Dict[asyncio.Task, int] = {} # Map task back to original index
    completed_count = 0
    exceptions_caught = []
    first_exception = None

    # Add the overall progress task for this group
    try:
        task_id = progress.add_task(f"{description} (0/{num_tasks})", total=num_tasks, start=False)
        progress.start_task(task_id)
    except Exception as e:
        log.error(f"Failed to add/start progress task '{description}': {e}")
        return [] # Cannot proceed without a progress task

    # --- Step 2: Create asyncio Tasks Internally ---
    for i, coro in enumerate(coroutines):
        if not asyncio.iscoroutine(coro):
            # Clean up progress bar before raising error
            if task_id is not None:
                progress.update(task_id, description=f"[red]✗ {description} (Error: Invalid input)")
                progress.stop_task(task_id)
            raise TypeError(f"Item at index {i} is not a coroutine: {type(coro)}")
        # Create the task
        task = asyncio.create_task(coro, name=f"{description}_item_{i}")
        task_futures.append(task)
        original_indices[task] = i

    # --- Step 3: Monitor Completion using as_completed ---
    for future in asyncio.as_completed(task_futures): # future is a completed Task object
        original_index = original_indices[future]
        try:
            result = await future # Retrieve result or exception from the completed task
            results[original_index] = result
        except Exception as e:
            exceptions_caught.append(e)
            results[original_index] = e # Store exception if return_exceptions=True
            if not return_exceptions and first_exception is None:
                first_exception = e
            log.debug(f"Task item {original_index} in '{description}' failed: {e}")
        finally:
            completed_count += 1
            # --- Step 4: Update Progress Bar ---
            current_desc = f"{description} ({completed_count}/{num_tasks})"
            if exceptions_caught:
                 current_desc += f" - {len(exceptions_caught)} errors!"
            try:
                progress.update(task_id, completed=completed_count, description=current_desc)
            except Exception as e:
                 log.warning(f"Failed to update progress bar for task {task_id}: {e}")


    # --- Step 5: Finalize Progress Bar ---
    final_desc = ""
    if exceptions_caught:
         final_desc = f"[red]✗ {description} ({len(exceptions_caught)} errors, {num_tasks - len(exceptions_caught)}/{num_tasks} success)"
    else:
         final_desc = f"[green]✓ {description} ({completed_count}/{num_tasks} success)"

    try:
        progress.update(task_id, description=final_desc)
        progress.stop_task(task_id)
    except Exception as e:
        log.warning(f"Failed to finalize progress bar for task {task_id}: {e}")

    # --- Step 6: Handle Exceptions / Return Results ---
    if not return_exceptions and first_exception:
        raise first_exception
    else:
        return results

import asyncio
import random
import logging
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
from rich.logging import RichHandler
from rich.console import Console

# --- Logger Setup (stderr recommended) ---
log_console = Console(stderr=True)
logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]",
                    handlers=[RichHandler(console=log_console, show_path=False, markup=True)])
log = logging.getLogger("coroutine_wrapper_user")

# --- Original Task Function (unchanged) ---
async def async_task(id, delay):
    log.debug(f"Task {id} starting (sleep {delay:.2f}s)")
    await asyncio.sleep(delay)
    if id == 'A-2': raise ValueError(f"Error in async {id}")
    log.debug(f"Task {id} finished")
    return f"Async Result {id}"

# --- Main Orchestration using the Wrapper ---
async def main_simple_wrapper():
    # --- Progress Bar Setup ---
    progress_columns = [
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()
    ]

    async with Progress(*progress_columns, transient=False) as progress:

        # --- Create lists of COROUTINES (not Tasks) ---
        log.info("Preparing coroutines...")
        coroutines_group_a = [
            async_task(f"A-{i}", random.random() * 0.6)
            for i in range(5) # A-2 will raise error
        ]
        coroutines_group_b = [
            async_task(f"B-{i}", random.random() * 0.8)
            for i in range(7)
        ]

        # --- Call the wrapper with the coroutine lists ---
        # Run them concurrently using asyncio.gather on the wrapper calls
        log.info("Launching wrappers concurrently...")
        wrapper_results = await asyncio.gather(
            run_coroutines_with_progress(
                coroutines_group_a, "Processing Group A", progress, return_exceptions=True
            ),
            run_coroutines_with_progress(
                coroutines_group_b, "Processing Group B", progress, return_exceptions=True # Set to True to see errors from B too
            ),
            # Add more groups here if needed
            return_exceptions=True # Gather itself should also return exceptions from wrappers if they fail catastrophically
        )
        log.info("All wrapper calls finished.")

        # Process results from each wrapper call
        for i, group_result_or_exc in enumerate(wrapper_results):
            group_name = f"Group {'A' if i == 0 else 'B'}" # Adjust if more groups
            if isinstance(group_result_or_exc, Exception):
                log.error(f"{group_name} wrapper itself failed: {group_result_or_exc}")
            else:
                # Process the list returned by the wrapper
                log.info(f"Results for {group_name}:")
                exceptions_in_group = []
                for item in group_result_or_exc: # This is the list from run_coroutines_with_progress
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
    asyncio.run(main_simple_wrapper())
