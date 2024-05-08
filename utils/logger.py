import logging
from datetime import datetime


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
