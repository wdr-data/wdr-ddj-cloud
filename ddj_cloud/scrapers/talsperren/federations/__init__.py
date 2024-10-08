import importlib
from pathlib import Path

current_dir = Path(__file__).parent

# Get a list of all Python files in the current directory
module_files = current_dir.glob("*.py")

# Import each submodule dynamically to ensure that the federation
# subclasses are loaded
for module_file in module_files:
    if module_file.name.startswith("__"):
        continue  # Skip special files, including self

    module_name = module_file.name[:-3]  # Remove the ".py" extension
    importlib.import_module("." + module_name, package=__name__)
