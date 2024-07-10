import logging

from rich.logging import RichHandler

global_logger = logging.getLogger(__name__)
global_logger.setLevel(logging.DEBUG)
global_logger.addHandler(RichHandler())
