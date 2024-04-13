import os
import logging
import logging.handlers
import time
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

# Creating the logs folder if not present
if not os.path.exists("./logs"):
    os.mkdir("./logs")

# Setting up the logger
logger = logging.getLogger(__name__)
file_handler = logging.handlers.RotatingFileHandler(
    "./logs/webserver.log",
    maxBytes=1048576,
    backupCount=10,
    encoding="utf-8"
)
logger_format = logging.Formatter(
    "[%(asctime)s %(levelname)s] %(threadName)s.%(funcName)s(): %(message)s"
)
logger_format.converter = time.gmtime
file_handler.setFormatter(logger_format)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

logger.info("****************************** Starting server ******************************")

# Creating the results folder if not present
if not os.path.exists("./results"):
    os.mkdir("./results")

# Checking the number of threads to create
if 'TP_NUM_OF_THREADS' in os.environ:
    num_of_threads = int(os.environ.get("TP_NUM_OF_THREADS"))
else:
    num_of_threads = os.cpu_count()

webserver = Flask(__name__)

webserver.logger = logger

logger.info("Importing CSV data")
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

logger.info("Initializing thread pool")
webserver.tasks_runner = ThreadPool(num_of_threads, webserver.data_ingestor, logger)

webserver.job_counter = 1

from app import routes
