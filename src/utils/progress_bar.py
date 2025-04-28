from typing import Any, List, Coroutine, Union, Optional, Sequence, Dict # Added Dict
import asyncio
from rich.progress import Progress, TaskID
import logging

log = logging.getLogger("coroutine_wrapper_manual_start") # Logger instance

async def run_coroutines_with_progress_manual( # Renamed slightly for clarity
    coroutines: Sequence[Coroutine],
    description: str,
    progress: Progress,                # Expects an already started Progress object
    return_exceptions: bool = False
) -> List[Any]:
    """
    Runs a sequence of coroutines concurrently, updating a provided (and
    already started) rich.progress.Progress object. Handles task creation
    internally. The caller is responsible for starting/stopping the Progress object.

    Args:
        coroutines: The sequence of awaitable coroutine objects to run.
        description: Text description for the progress bar task line.
        progress: The rich.progress.Progress object (assumed to be started).
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
    task_futures = []
    original_indices: Dict[asyncio.Task, int] = {}
    completed_count = 0
    exceptions_caught = []
    first_exception = None

    # --- Step 1: Add the task line to the progress bar ---
    # We add the task, but DO NOT call progress.start_task() here,
    # as the overall progress display is managed externally.
    try:
        # Set initial description, total, but leave it 'stopped' initially
        task_id = progress.add_task(f"{description} (0/{num_tasks})", total=num_tasks, start=False, visible=True)
        # We might want to immediately update description to 'Running...' if needed
        # progress.update(task_id, description=f"{description} (Running 0/{num_tasks})")
    except Exception as e:
        log.error(f"Failed to add progress task '{description}': {e}")
        return [] # Cannot proceed

    # --- Step 2: Create asyncio Tasks Internally ---
    for i, coro in enumerate(coroutines):
        if not asyncio.iscoroutine(coro):
            # Clean up the added task line before raising error
            if task_id is not None:
                progress.update(task_id, description=f"[red]✗ {description} (Error: Invalid input)")
                # We don't stop the task here as stop is managed externally
                # but we might want to mark it completed if the bar should fill
                # progress.update(task_id, completed=num_tasks)
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
                 # Only update the specific task line's progress and description
                 if task_id is not None:
                     progress.update(task_id, completed=completed_count, description=current_desc)
            except Exception as e:
                 log.warning(f"Failed to update progress bar for task {task_id}: {e}")

    # --- Step 5: Finalize Progress Bar Line Description ---
    # Update the description one last time, but DO NOT call progress.stop_task()
    final_desc = ""
    if exceptions_caught:
         final_desc = f"[red]✗ {description} ({len(exceptions_caught)} errors, {num_tasks - len(exceptions_caught)}/{num_tasks} success)"
    else:
         final_desc = f"[green]✓ {description} ({completed_count}/{num_tasks} success)"

    try:
        if task_id is not None:
            # Ensure the bar fills completely upon finishing, even if description is set
            progress.update(task_id, description=final_desc, completed=num_tasks)
    except Exception as e:
        log.warning(f"Failed to finalize progress bar description for task {task_id}: {e}")

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
log = logging.getLogger("manual_wrapper_user")

# --- Original Task Function (unchanged) ---
async def async_task(id, delay):
    log.debug(f"Task {id} starting (sleep {delay:.2f}s)")
    await asyncio.sleep(delay)
    if id == 'A-2': raise ValueError(f"Error in async {id}")
    log.debug(f"Task {id} finished")
    return f"Async Result {id}"

# --- Main Orchestration using the Wrapper ---
async def main_manual_wrapper():
    # --- Progress Bar Setup ---
    progress_columns = [
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()
    ]
    # Create Progress instance BUT DO NOT USE async with
    progress = Progress(*progress_columns, transient=False)

    # --- Create lists of COROUTINES ---
    log.info("Preparing coroutines...")
    coroutines_group_a = [async_task(f"A-{i}", random.random() * 0.6) for i in range(5)]
    coroutines_group_b = [async_task(f"B-{i}", random.random() * 0.8) for i in range(7)]

    # --- Manual Start/Stop Block ---
    await progress.start() # Start the progress display
    log.info("Progress started manually.")
    wrapper_results = []
    try:
        # --- Call the wrapper with the started progress object ---
        log.info("Launching wrappers concurrently...")
        wrapper_results = await asyncio.gather(
            run_coroutines_with_progress_manual( # Use the updated wrapper name
                coroutines_group_a, "Processing Group A", progress, return_exceptions=True
            ),
            run_coroutines_with_progress_manual(
                coroutines_group_b, "Processing Group B", progress, return_exceptions=True
            ),
            return_exceptions=True
        )
        log.info("All wrapper calls finished within try block.")

    except Exception as e:
        log.error(f"An error occurred outside the wrappers: {e}", exc_info=True)
    finally:
        await progress.stop() # ★★★ Ensure progress is stopped ★★★
        log.info("Progress stopped manually.")

    # --- Process results ---
    log.info("Processing results...")
    # (Result processing logic remains the same as before)
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
            if exceptions_in_group:
                log.warning(f"  ({len(exceptions_in_group)} errors detected in group)")
            else:
                log.info(f"  (All tasks succeeded in group)")

    log.info("Main function finished.")

# --- Run ---
if __name__ == "__main__":
    asyncio.run(main_manual_wrapper())
