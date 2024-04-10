from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import os

# Creating the results folder if not present
if not os.path.exists("./results"):
    os.mkdir("./results")

# Checking the number of threads to create
if 'TP_NUM_OF_THREADS' in os.environ:
    num_of_threads = os.environ.get("TP_NUM_OF_THREADS")
else:
    num_of_threads = os.cpu_count()

webserver = Flask(__name__)
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.tasks_runner = ThreadPool(num_of_threads, webserver.data_ingestor.table)

webserver.job_counter = 1

from app import routes
