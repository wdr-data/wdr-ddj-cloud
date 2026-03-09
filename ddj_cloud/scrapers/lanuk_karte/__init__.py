import logging
from os import getenv

if getenv("STAGE") == "testing":
    logging.basicConfig(level=logging.DEBUG)
