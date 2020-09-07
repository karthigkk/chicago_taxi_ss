import logging

from regression_model.config import config
from regression_model.config import logging_config
from zipfile import ZipFile
import pathlib

VERSION_PATH = config.VERSION

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging_config.get_console_handler())
logger.propagate = False

zip_file = ZipFile(pathlib.Path("regression_model.zip/regression_model/config/config.py").resolve().parent.parent.parent)

# with open(VERSION_PATH, 'r') as version:
with zip_file.open('regression_model/VERSION') as version:
    __version__ = version.read().strip()
