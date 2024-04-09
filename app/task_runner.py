from queue import Queue
from threading import Thread, Event
import time
import os

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task

        # Creating the results folder if not present
        if not os.path.exists("./results"):
            os.mkdir("./results")

        # Creating an empty job queue
        self.job_queue = Queue()

        # Checking the number of threads to create
        if 'TP_NUM_OF_THREADS' in os.environ:
            self.num_of_threads = os.environ.get("TP_NUM_OF_THREADS")
        else:
            self.num_of_threads = os.cpu_count()

        # Creating and starting the threads
        for _ in range(self.num_of_threads):
            worker = TaskRunner(self.job_queue)
            worker.start()


class TaskRunner(Thread):
    def __init__(self, job_queue):
        # TODO: init necessary data structures
        Thread.__init__(self)
        self.job_queue = job_queue

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            pass
