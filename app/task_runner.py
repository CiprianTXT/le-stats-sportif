from queue import Queue
from threading import Thread, Condition
import time
import pandas
import json

class ThreadPool:
    def __init__(self, num_of_threads, table):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task

        # Creating an empty job queue
        self.job_queue = Queue()

        # Creating an empty job status dictionary
        self.job_status = {}

        # Flag for graceful shutdown
        self.shutdown_notification = []

        # Creating a Condition object
        self.condition = Condition()

        # Creating and starting the threads
        for _ in range(num_of_threads):
            worker = TaskRunner(self.job_queue, self.job_status, self.shutdown_notification, self.condition, table)
            worker.start()

    def is_running(self):
        return not self.shutdown_notification

    def shutdown(self):
        self.shutdown_notification.append(True)

class TaskRunner(Thread):
    def __init__(self, job_queue, job_status, shutdown_notification, condition, table):
        # Initializing necessary data structures
        Thread.__init__(self)
        self.job_queue = job_queue
        self.job_status = job_status
        self.shutdown_notification = shutdown_notification
        self.condition = condition
        self.table = table

    def exec_states_mean(self, question, job_id):
        states_avg = []

        # Filter table by value in question column and group by state afterwards
        filtered_table = self.table.loc[self.table["Question"] == question]
        for state, table in filtered_table.groupby("LocationDesc"):
            states_avg.append((state, table["Data_Value"].mean()))

        # Sort data by value
        states_avg = dict(sorted(states_avg, key=lambda state: state[1]))

        # Save the result on disk
        result = json.dumps(states_avg, sort_keys=False)
        with open(f"./results/job_id_{job_id}.json", "w") as output_file:
            output_file.write(result)
        self.job_status[job_id] = "done"

    def run(self):
        # Repeat until graceful_shutdown and empty queue
        with self.condition:
            while not (self.shutdown_notification and self.job_queue.empty()):
                # Put all workers on hold as long as there are no jobs
                if self.job_queue.empty():
                    self.condition.wait()
                else:
                    # Get pending job
                    job = self.job_queue.get()

                    # Execute the job and save the result to disk
                    request = job[0]
                    if request == "states_mean":
                        self.exec_states_mean(job[1], job[2])

        print(f"{self.name} shut down")
