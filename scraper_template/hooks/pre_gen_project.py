from calendar import month_name
import re
import sys


MODULE_REGEX = r'^[_a-zA-Z][_a-zA-Z0-9]+$'

module_name = '{{ cookiecutter.scraper_name }}'

if not re.match(MODULE_REGEX, module_name):
    print(f'ERROR: The scraper name must be a valid Python module name. ')
    if any(char in module_name for char in ["-", " "]):
        print("Tip: Use _ instead of - or space")

    #Exit to cancel project
    sys.exit(1)
