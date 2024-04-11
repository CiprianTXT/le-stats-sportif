from queue import Queue
from threading import Thread, Condition
import time
import json


class ThreadPool:
    def __init__(self, num_of_threads, data_ingestor):
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
            worker = TaskRunner(
                self.job_queue,
                self.job_status,
                self.shutdown_notification,
                self.condition,
                data_ingestor)
            worker.start()

    def is_running(self):
        return not self.shutdown_notification

    def shutdown(self):
        self.shutdown_notification.append(True)


class TaskRunner(Thread):
    def __init__(self, job_queue, job_status, shutdown_notification, condition, data_ingestor):
        # Initializing necessary data structures
        Thread.__init__(self)
        self.job_queue = job_queue
        self.job_status = job_status
        self.shutdown_notification = shutdown_notification
        self.condition = condition
        self.table = data_ingestor.table
        self.questions_best_is_min = data_ingestor.questions_best_is_min
        self.questions_best_is_max = data_ingestor.questions_best_is_max

    def exec_states_mean(self, question, job_id):
        states_avg = []

        # Filter table by Question column values and group by state afterwards
        filtered_table = self.table.loc[self.table["Question"] == question]
        for state, table in filtered_table.groupby("LocationDesc"):
            states_avg.append((state, table["Data_Value"].mean()))

        # Sort data by value
        states_avg = dict(sorted(states_avg, key=lambda state: state[1]))

        # Save the result on disk
        result = json.dumps(states_avg, sort_keys=False)
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_state_mean(self, question, state, job_id):
        # Filter table by Question and LocationDesc columns values
        filtered_table = self.table.loc[
            (self.table["Question"] == question) &
            (self.table["LocationDesc"] == state)
        ]

        # Save the result on disk
        result = json.dumps({
            state: filtered_table["Data_Value"].mean()
        })
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_best5(self, question, job_id):
        states_best5 = []

        # Filter table by Question column values and group by state afterwards
        filtered_table = self.table.loc[self.table["Question"] == question]
        for state, table in filtered_table.groupby("LocationDesc"):
            states_best5.append((state, table["Data_Value"].mean()))

        # Sort data by value depending on the question
        if question in self.questions_best_is_min:
            states_best5 = dict(sorted(states_best5, key=lambda state: state[1])[:5])
        else:
            states_best5 = dict(sorted(states_best5, key=lambda state: state[1], reverse=True)[:5])

        # Save the result on disk
        result = json.dumps(states_best5, sort_keys=False)
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_worst5(self, question, job_id):
        states_worst5 = []

        # Filter table by Question column values and group by state afterwards
        filtered_table = self.table.loc[self.table["Question"] == question]
        for state, table in filtered_table.groupby("LocationDesc"):
            states_worst5.append((state, table["Data_Value"].mean()))

        # Sort data by value depending on the question
        if question in self.questions_best_is_min:
            states_worst5 = dict(sorted(states_worst5, key=lambda state: state[1], reverse=True)[:5])
        else:
            states_worst5 = dict(sorted(states_worst5, key=lambda state: state[1])[:5])

        # Save the result on disk
        result = json.dumps(states_worst5, sort_keys=False)
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_global_mean(self, question, job_id):
        # Filter table by Question column values
        filtered_table = self.table.loc[self.table["Question"] == question]

        # Save the result on disk
        result = json.dumps({
            "global_mean": filtered_table["Data_Value"].mean()
        })
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_diff_from_mean(self, question, job_id):
        # Filter table by Question column values
        filtered_table = self.table.loc[self.table["Question"] == question]
        global_avg = filtered_table["Data_Value"].mean()

        # Group filtered table by LocationDesc column values
        diff_states_avg = []
        for state, table in filtered_table.groupby("LocationDesc"):
            diff_states_avg.append((state, global_avg - table["Data_Value"].mean()))

        # Sort data by value
        diff_states_avg = dict(sorted(diff_states_avg, key=lambda state: state[1], reverse=True))

        # Save the result on disk
        result = json.dumps(diff_states_avg, sort_keys=False)
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_state_diff_from_mean(self, question, state, job_id):
        # Filter table by Question column values to calculate global mean
        filtered_table = self.table.loc[self.table["Question"] == question]
        global_avg = filtered_table["Data_Value"].mean()

        # Further filter the table by LocationDesc column values
        filtered_table = filtered_table.loc[filtered_table["LocationDesc"] == state]

        # Save the result on disk
        result = json.dumps({
            state: global_avg - filtered_table["Data_Value"].mean()
        })
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_mean_by_category(self, question, job_id):
        category_mean = {}

        # Filter table by Question column values, then group by
        filtered_table = self.table.loc[self.table["Question"] == question]
        for category, table in filtered_table.groupby(["LocationDesc", "StratificationCategory1", "Stratification1"]):
            category_mean[str(category)] = table["Data_Value"].mean()

        # Save the result on disk
        result = json.dumps(category_mean)
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
        self.job_status[job_id] = "done"

    def exec_state_mean_by_category(self, question, state, job_id):
        category_mean = {}

        # Filter table by Question and LocationDesc columns values, then group by
        filtered_table = self.table.loc[
            (self.table["Question"] == question) &
            (self.table["LocationDesc"] == state)
        ]
        for category, table in filtered_table.groupby(["StratificationCategory1", "Stratification1"]):
            category_mean[str(category)] = table["Data_Value"].mean()

        # Save the result on disk
        result = json.dumps({
            state: category_mean
        })
        with open(f"./results/job_id_{job_id}.json", "w", encoding="UTF-8") as output_file:
            output_file.write(result)

        # Mark job as done
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
                    elif request == "state_mean":
                        self.exec_state_mean(job[1], job[2], job[3])
                    elif request == "best5":
                        self.exec_best5(job[1], job[2])
                    elif request == "worst5":
                        self.exec_worst5(job[1], job[2])
                    elif request == "global_mean":
                        self.exec_global_mean(job[1], job[2])
                    elif request == "diff_from_mean":
                        self.exec_diff_from_mean(job[1], job[2])
                    elif request == "state_diff_from_mean":
                        self.exec_state_diff_from_mean(job[1], job[2], job[3])
                    elif request == "mean_by_category":
                        self.exec_mean_by_category(job[1], job[2])
                    elif request == "state_mean_by_category":
                        self.exec_state_mean_by_category(job[1], job[2], job[3])

        print(f"{self.name} shut down")
