import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime


def setup_logging():
    logs_dir = "logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename_with_timestamp = f"crawler_{current_time}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        filename=os.path.join(logs_dir, filename_with_timestamp),
        when="midnight",
        interval=1,
        backupCount=30
    )

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s:%(filename)s-%(lineno)d', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
