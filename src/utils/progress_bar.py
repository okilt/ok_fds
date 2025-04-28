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
