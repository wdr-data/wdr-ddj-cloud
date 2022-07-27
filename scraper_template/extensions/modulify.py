import re

from jinja2.ext import Extension
from slugify import slugify


def modulify(value):
    """Generate a valid Python module name from any text"""
    # Start off with a slug
    slugified = slugify(value, separator="_")
    # Remove leading numbers and underscores
    modulified = re.sub(r"^[0-9_]*", "", slugified)

    return modulified


class ModulifyExtension(Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["modulify"] = modulify
