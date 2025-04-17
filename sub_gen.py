#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dynamically generates .pyi stub files for classes with async methods.

Finds async methods starting with 'async_' and generates corresponding sync method
stubs (without the 'async_' prefix) in the .pyi file.
"""

import inspect
import importlib
import os
import sys
import typing
from pathlib import Path
from collections import defaultdict

# --- Configuration ---
# Adjust these paths according to your project structure.
# This script assumes it's located in '<PROJECT_ROOT>/scripts/'.
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
# List of directories containing the source code to scan (relative to PROJECT_ROOT)
SOURCE_DIRS_RELATIVE = ["your_library"]
# Specific files to process (relative to SOURCE_DIRS)
# Example: ["module_a.py", "core/module_b.py"]
SPECIFIC_FILES_RELATIVE: list[str] = [
    "module_a.py",
    "module_b.py",
]
# Specific folders where all direct .py files should be processed (relative to SOURCE_DIRS)
# Example: ["subfolder", "another_folder"]
SPECIFIC_FOLDERS_RELATIVE: list[str] = [
    "subfolder",
]
# Place generated .pyi files next to the corresponding .py files?
STUBS_IN_SAME_DIR = True
# --- End Configuration ---

# Type aliases for clarity
Annotation = typing.Any
TypeName = str


def _format_annotation(annotation: Annotation, module_context: str) -> TypeName:
    """Converts a type annotation to its string representation for stubs."""
    if annotation is inspect.Parameter.empty or annotation is None:
        return "None" # Treat missing annotation as None in return, Any in param? Be explicit.
        # For params, inspect.Parameter.empty might be better as Any? Let's use Any for params.
    if annotation is type(None):
        return "None"

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)

    if origin:  # Generic types like List[int], Optional[str], Union[int, str]
        origin_name = getattr(origin, "__name__", str(origin))
        # Handle common typing aliases
        if origin is typing.Union and len(args) == 2 and args[1] is type(None):
             # Convert Union[T, None] to Optional[T]
             # Ensure Optional is imported or use typing.Optional
             return f"typing.Optional[{_format_annotation(args[0], module_context)}]"
        if hasattr(typing, origin_name) and getattr(typing, origin_name) == origin:
             origin_name = f"typing.{origin_name}" # Qualify with 'typing.'

        if args:
            formatted_args = ", ".join(_format_annotation(arg, module_context) for arg in args)
            return f"{origin_name}[{formatted_args}]"
        else:
            return origin_name # e.g., typing.List, typing.Dict
    else:  # Simple types like int, str, or custom classes
        type_name = getattr(annotation, "__qualname__", getattr(annotation, "__name__", None))
        if type_name:
             type_module = getattr(annotation, "__module__", None)
             # Add module prefix if it's not a builtin and not in the current module context
             if type_module and type_module != "builtins" and type_module != module_context and type_module != 'typing':
                  return f"{type_module}.{type_name}"
             return type_name
        else:
            # Fallback for complex or unrepresentable types
            return "typing.Any" # Fallback to Any

def get_sync_return_type(annotation: Annotation, module_context: str) -> TypeName:
    """Gets the sync equivalent return type string from an async annotation."""
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)

    # Check for Awaitable[T] or Coroutine[..., T]
    if origin in (typing.Awaitable, typing.Coroutine) and args:
        sync_type_annotation = args[-1] # Return type T is the last argument
        return _format_annotation(sync_type_annotation, module_context)
    else:
        # If not Awaitable/Coroutine, format it as is
        return _format_annotation(annotation, module_context)

def get_param_type(annotation: Annotation, module_context: str) -> TypeName:
     """Formats parameter type annotation, defaulting to Any if missing."""
     if annotation is inspect.Parameter.empty:
         return "typing.Any"
     return _format_annotation(annotation, module_context)


def generate_stubs_for_module(module_path: Path, source_root: Path):
    """Generates a .pyi file for a given Python module file."""
    if not module_path.is_file() or module_path.suffix != ".py":
        print(f"  Skipping non-python file: {module_path}", file=sys.stderr)
        return

    # Calculate module's dotted path (e.g., your_library.module_a)
    try:
        relative_path = module_path.relative_to(source_root)
        module_dot_path = str(relative_path.with_suffix('')).replace(os.path.sep, '.')
    except ValueError:
        print(f"  Error: Cannot determine module path for {module_path} relative to {source_root}", file=sys.stderr)
        return

    # --- Import the module ---
    try:
        # Add source root to sys.path temporarily if not already present
        source_root_str = str(source_root)
        original_sys_path = list(sys.path)
        if source_root_str not in sys.path:
            sys.path.insert(0, source_root_str)
        module = importlib.import_module(module_dot_path)
    except ImportError as e:
        print(f"  Error importing module {module_dot_path}: {e}", file=sys.stderr)
        return
    except Exception as e:
        print(f"  Unexpected error importing {module_dot_path}: {e}", file=sys.stderr)
        return
    finally:
        # Restore sys.path
        sys.path = original_sys_path


    # --- Prepare stub content ---
    class_stubs: dict[str, list[str]] = defaultdict(list)
    module_level_imports = {"typing"} # Track necessary imports

    # --- Find classes defined directly in this module ---
    classes_to_stub: list[tuple[str, type]] = []
    try:
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and obj.__module__ == module.__name__:
                classes_to_stub.append((name, obj))
    except Exception as e:
        print(f"  Error inspecting members of {module_dot_path}: {e}", file=sys.stderr)
        # Continue if possible, might miss some classes

    if not classes_to_stub:
        print(f"  No classes found directly defined in {module_dot_path}. Skipping stub generation.")
        # Optionally create an empty .pyi or handle module-level functions if needed
        # We might still want to create the file if it contains module-level async funcs
        # For now, we focus on classes as per the original request.
        # return # Uncomment if you ONLY want stubs for files with classes

    # --- Process each class ---
    for class_name, cls in sorted(classes_to_stub):
        print(f"  Processing class: {class_name}")
        current_class_lines = []
        class_body_present = False

        # Class definition line
        base_classes_str = ", ".join(
             _format_annotation(b, module.__name__) for b in cls.__bases__ if b is not object
        )
        class_def = f"class {class_name}"
        if base_classes_str:
            class_def += f"({base_classes_str})"
        current_class_lines.append(f"{class_def}:")

        # Process __init__
        if "__init__" in cls.__dict__:
             try:
                 init_sig = inspect.signature(cls.__init__)
                 params_str = []
                 for pname, param in init_sig.parameters.items():
                      ptype = get_param_type(param.annotation, module.__name__)
                      param_str = f"{pname}: {ptype}"
                      if param.default is not inspect.Parameter.empty:
                           param_str += f" = {repr(param.default)}"
                      params_str.append(param_str)
                 current_class_lines.append(f"    def __init__({', '.join(params_str)}) -> None: ...")
                 class_body_present = True
             except (ValueError, TypeError) as e:
                  print(f"    Warning: Could not get signature for {class_name}.__init__: {e}", file=sys.stderr)
                  current_class_lines.append("    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None: ...")
                  class_body_present = True

        # Process async methods -> sync stubs
        for member_name, member in inspect.getmembers(cls):
            if inspect.iscoroutinefunction(member) and member_name.startswith("async_"):
                sync_func_name = member_name[len("async_"):]
                if not sync_func_name: continue # Skip if name was just "async_"

                try:
                    sig = inspect.signature(member)
                    params_str = []
                    for pname, param in sig.parameters.items():
                         ptype = get_param_type(param.annotation, module.__name__)
                         param_str = f"{pname}: {ptype}"
                         if param.default is not inspect.Parameter.empty:
                              param_str += f" = {repr(param.default)}"
                         params_str.append(param_str)

                    return_type = get_sync_return_type(sig.return_annotation, module.__name__)
                    current_class_lines.append(f"    def {sync_func_name}({', '.join(params_str)}) -> {return_type}: ...")
                    class_body_present = True
                except (ValueError, TypeError) as e:
                    print(f"    Warning: Could not get signature for {class_name}.{member_name}: {e}", file=sys.stderr)
                    current_class_lines.append(f"    def {sync_func_name}(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any: ...")
                    class_body_present = True

            # Optional: Add stubs for existing non-async methods if needed
            # elif inspect.isfunction(member) and not member_name.startswith('_') and member_name in cls.__dict__:
            #    # ... generate stub for regular sync method ...
            #    pass

        # If class body is empty, add '...'
        if not class_body_present:
             current_class_lines.append("    ...")

        class_stubs[class_name].extend(current_class_lines)

    # --- Determine stub file path ---
    if STUBS_IN_SAME_DIR:
        pyi_path = module_path.with_suffix(".pyi")
    else:
        # Example: place in <PROJECT_ROOT>/stubs/your_library/module.pyi
        stub_dir = PROJECT_ROOT / "stubs"
        pyi_path = stub_dir / relative_path.with_suffix(".pyi")

    # --- Write the .pyi file ---
    if not class_stubs: # Only write file if we found classes (or adjust logic if needed)
        # If an old stub exists, maybe remove it? Or leave it?
        # For simplicity, we only write if new content is generated.
        # print(f"  No stubs generated for {module_dot_path}. Skipping file write.")
        # If a .pyi file exists but no classes were found, should we delete it?
        if pyi_path.exists():
             print(f"  Note: No classes found in {module_path}, but stub file {pyi_path} exists. It was not modified.")
        return

    print(f"  Generating stub file: {pyi_path}")
    pyi_path.parent.mkdir(parents=True, exist_ok=True)
    with open(pyi_path, "w", encoding="utf-8") as f:
        # Write necessary imports first (simple version, only typing for now)
        # A more robust solution would analyze all types used in signatures.
        f.write("import typing\n")
        # Add imports based on types found in annotations if needed
        # e.g., if 'datetime.datetime' was found -> f.write("import datetime\n")
        f.write("\n")

        # Write class stubs
        first_class = True
        for class_name in sorted(class_stubs.keys()):
            if not first_class:
                f.write("\n\n") # Add blank lines between classes
            f.write("\n".join(class_stubs[class_name]))
            f.write("\n")
            first_class = False


def find_target_files(roots: list[Path], specific_files: list[str], specific_folders: list[str]) -> list[tuple[Path, Path]]:
    """Finds all .py files to process based on configuration."""
    target_files = set()
    for root in roots:
         root_abs = PROJECT_ROOT / root
         if not root_abs.is_dir():
              print(f"Warning: Source directory not found: {root_abs}", file=sys.stderr)
              continue

         # Add specific files relative to this root
         for sf in specific_files:
              f_path = (root_abs / sf).resolve()
              if f_path.is_file():
                   target_files.add((f_path, root_abs))
              else:
                   # Check if specified relative to project root instead
                   f_path_proj = (PROJECT_ROOT / sf).resolve()
                   if f_path_proj.is_file():
                        target_files.add((f_path_proj, root_abs)) # Associate with first root? Need clear logic.
                        # Let's assume specific files are relative to *a* source dir.
                   else:
                        print(f"Warning: Specific file not found relative to {root_abs}: {sf}", file=sys.stderr)

         # Add files in specific folders relative to this root
         for folder in specific_folders:
              folder_path = (root_abs / folder).resolve()
              if folder_path.is_dir():
                   for item in folder_path.glob("*.py"):
                        if item.is_file():
                             target_files.add((item, root_abs))
              else:
                   print(f"Warning: Specific folder not found relative to {root_abs}: {folder}", file=sys.stderr)

    return sorted(list(target_files), key=lambda x: x[0]) # Sort by file path


if __name__ == "__main__":
    print("Starting stub generation...")
    source_roots = [PROJECT_ROOT / src_dir for src_dir in SOURCE_DIRS_RELATIVE]
    files_to_process = find_target_files(source_roots, SPECIFIC_FILES_RELATIVE, SPECIFIC_FOLDERS_RELATIVE)

    if not files_to_process:
        print("No target Python files found based on configuration.")
        sys.exit(0)

    processed_count = 0
    for py_file, source_root in files_to_process:
        print(f"Processing file: {py_file.relative_to(PROJECT_ROOT)}")
        try:
            generate_stubs_for_module(py_file, source_root)
            processed_count += 1
        except Exception as e:
            print(f"  FATAL Error processing {py_file}: {e}", file=sys.stderr)
            # Optionally raise the exception or exit: raise e / sys.exit(1)

    print(f"\nStub generation finished. Processed {processed_count} files.")