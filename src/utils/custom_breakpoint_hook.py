import sys
import inspect # For getting module and function name reliably

# --- Mock Progress Helper (for demonstration) ---
# In your actual code, this would be your Rich progress bar manager
class MockProgressHelper:
    def __init__(self, name="default_progress"):
        self.name = name
        self.is_active = False
        self.is_stopped_for_debug = False
        print(f"ProgressHelper '{self.name}' initialized.")

    def start_active_progress(self):
        if not self.is_active:
            print(f"ProgressHelper '{self.name}': Progress becoming ACTIVE.")
            self.is_active = True
            self.is_stopped_for_debug = False

    def stop(self):
        """Stops the progress bar display for debugging. Returns True if stopped."""
        if self.is_active and not self.is_stopped_for_debug:
            print(f"ProgressHelper '{self.name}': STOPPING for debugger.")
            self.is_stopped_for_debug = True
            return True
        elif not self.is_active and not self.is_stopped_for_debug: # Not active, but mark as "stopped" for consistency
            print(f"ProgressHelper '{self.name}': Not active, conceptually stopping for debugger.")
            self.is_stopped_for_debug = True
            return True # Conceptually stopped
        return False # Already stopped or no change

    def resume(self):
        """Resumes the progress bar display after debugging."""
        if self.is_stopped_for_debug: # Resume if it was stopped for debugging
            print(f"ProgressHelper '{self.name}': RESUMING after debugger.")
            if self.is_active:
                 # Potentially re-render or restart Rich components
                 pass
            self.is_stopped_for_debug = False
        else:
            print(f"ProgressHelper '{self.name}': No resume needed (wasn't stopped for debug).")


    def finish_active_progress(self):
        if self.is_active:
            print(f"ProgressHelper '{self.name}': Progress FINISHED (all tasks done).")
            self.is_active = False
            self.is_stopped_for_debug = False # Ensure reset

progress_helper = MockProgressHelper()
# --- End Mock ---

# Global state to manage restoration of original skip lists
_original_skip_lists = {} # Stores {'ipdb': original_list, 'pdb': original_list}
_glob_patterns_added = {} # Stores {'ipdb': pattern, 'pdb': pattern}

def _get_pdb_class_and_skip_list(debugger_name):
    """Helper to get Pdb class and its skip list for 'ipdb' or 'pdb'."""
    try:
        if debugger_name == 'ipdb':
            import ipdb
            # Try common locations for Pdb in ipdb/IPython
            pdb_class = getattr(ipdb, 'Pdb', None)
            if pdb_class is None:
                from IPython.core.debugger import Pdb as IPDB_Pdb_Class_fallback
                pdb_class = IPDB_Pdb_Class_fallback
            if pdb_class and hasattr(pdb_class, 'skip') and isinstance(pdb_class.skip, list):
                return pdb_class, pdb_class.skip
        elif debugger_name == 'pdb':
            import pdb
            if hasattr(pdb.Pdb, 'skip') and isinstance(pdb.Pdb.skip, list):
                return pdb.Pdb, pdb.Pdb.skip
    except ImportError:
        pass # Module not found
    except AttributeError:
        pass # Pdb class or skip attribute not found as expected
    return None, None

def custom_breakpointhook(*args, **kwargs):
    global progress_helper, _original_skip_lists, _glob_patterns_added

    # Determine the module and function name of this hook for precise skipping
    hook_frame_obj = sys._getframe(0)
    hook_module_name = inspect.getmodule(hook_frame_obj).__name__
    hook_function_name = hook_frame_obj.f_code.co_name
    glob_pattern_to_hide = f"{hook_module_name}.{hook_function_name}"

    caller_frame = sys._getframe(1) # Frame where breakpoint() was called
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    debugger_to_use = None
    pdb_class_modified = None

    # Try to modify skip list for ipdb first, then pdb
    for dbg_name in ['ipdb', 'pdb']:
        if dbg_name in _original_skip_lists: # Already modified in a previous call (should not happen if breakpoint exits)
            continue

        pdb_class, current_skip_list = _get_pdb_class_and_skip_list(dbg_name)
        if pdb_class and current_skip_list is not None:
            _original_skip_lists[dbg_name] = list(current_skip_list) # Save copy
            if glob_pattern_to_hide not in current_skip_list:
                current_skip_list.append(glob_pattern_to_hide)
            _glob_patterns_added[dbg_name] = glob_pattern_to_hide
            debugger_to_use = dbg_name
            pdb_class_modified = pdb_class # Store the class whose skip list was changed
            break # Found and modified a debugger's skip list

    if progress_helper:
        progress_helper.stop()

    try:
        print(f"--- Debugger (custom hook) starting at: "
              f"{caller_frame.f_code.co_filename}:{caller_frame.f_lineno} ---",
              file=original_stdout)

        if debugger_to_use == 'ipdb':
            import ipdb
            ipdb_context = kwargs.get('context')
            if ipdb_context is not None:
                ipdb.set_trace(frame=caller_frame, context=int(ipdb_context))
            else:
                ipdb.set_trace(frame=caller_frame)
        elif debugger_to_use == 'pdb':
            import pdb
            pdb_instance = pdb.Pdb(stdout=original_stdout, stdin=sys.stdin)
            # Pass the modified Pdb class if we have it, else default
            # This is tricky as pdb.Pdb() instantiates. If we modified pdb.Pdb.skip, it's already set.
            pdb_instance.set_trace(frame=caller_frame)
        else:
            # Fallback if no skip list could be modified (e.g., neither ipdb nor pdb found, or structure changed)
            # Or if PYTHONBREAKPOINT is set to something else that doesn't use Pdb.skip
            print("INFO: Using fallback debugger invocation (frame hiding might not occur).", file=sys.stderr)
            try:
                import ipdb
                ipdb.set_trace(frame=caller_frame, context=kwargs.get('context'))
            except ImportError:
                try:
                    import pdb
                    pdb.Pdb(stdout=original_stdout, stdin=sys.stdin).set_trace(frame=caller_frame)
                except ImportError:
                    print("ERROR: No debugger (ipdb or pdb) found.", file=sys.stderr)
    finally:
        # Restore skip lists
        for dbg_name, original_list in list(_original_skip_lists.items()): # Iterate over a copy
            pdb_class, current_skip_list = _get_pdb_class_and_skip_list(dbg_name)
            if pdb_class and current_skip_list is not None:
                # Restore by direct assignment to the class attribute's slice
                pdb_class.skip[:] = original_list
            del _original_skip_lists[dbg_name]
            if dbg_name in _glob_patterns_added:
                del _glob_patterns_added[dbg_name]

        # Restore stdout/stderr and resume progress bar
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        if progress_helper:
            progress_helper.resume()
        print("--- Debugger session finished. Resuming execution. ---", file=sys.stdout)

# Set the custom hook. This is effective if PYTHONBREAKPOINT is not set
# or is set to 'sys.breakpointhook'.
sys.breakpointhook = custom_breakpointhook

# --- Example Usage ---
def process_item(item_id):
    print(f"Processing item {item_id}...")
    x = item_id * 100
    # Simulate some work that might update a progress bar
    for i in range(item_id * 2):
        y = i * x

    if item_id == 2:
        print(f"Special condition for item {item_id}, triggering breakpoint.")
        # When debugger starts here, the "custom_breakpointhook" frame should be hidden in `bt`.
        # The context (variables, code) will be for this line in process_item.
        breakpoint() # Example: type 'bt' in ipdb

    print(f"Finished processing item {item_id}. Result: {x}")
    return x

if __name__ == "__main__":
    print("Main script starting. Ensure PYTHONBREAKPOINT is not overriding sys.breakpointhook for this test.")
    # To test this:
    # 1. Run the script.
    # 2. When ipdb/pdb starts for item_id == 2:
    #    - Type `w` (where) or `bt` (backtrace).
    #    - `custom_breakpointhook` should not appear in the list.
    #    - The current frame arrow should point to the `breakpoint()` line in `process_item`.
    #    - Type `x` or `item_id` to see local variables.

    progress_helper.start_active_progress() # Simulate progress bar starting
    results = []
    for i in range(1, 4):
        # In a real Rich app, you'd update progress_helper.progress.update(...) here
        results.append(process_item(i))
    progress_helper.finish_active_progress() # Simulate progress bar finishing

    print(f"\nAll items processed. Results: {results}")
    print("Main script finished.")
