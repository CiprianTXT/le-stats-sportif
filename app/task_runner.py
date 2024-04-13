import json
from queue import Queue
from threading import Thread, Condition


class ThreadPool:
    """
    A thread pool for managing multiple TaskRunner instances.

    The ThreadPool creates and manages a pool of worker threads (TaskRunner instances)
    for processing jobs.

    Parameters:
        num_of_threads (int): The number of worker threads to create.
        data_ingestor (DataIngestor): An object providing access to the data for processing.
        logger (Logger): An object providing access to the logger.

    Attributes:
        job_queue (Queue): A queue containing the jobs to be processed.
        job_status (dict): A dictionary to store the status of each job.
        shutdown_notification (list): A flag indicating whether the task runner should shut down.
        condition (Condition): A threading condition for synchronization.
    """

    def __init__(self, num_of_threads, data_ingestor, logger):
        # Initializing job queue
        self.job_queue = Queue()

        # Initializing job status dictionary
        self.job_status = {}

        # Flag for graceful shutdown
        self.shutdown_notification = []

        # Initializing Condition object
        self.condition = Condition()

        # Creating and starting the threads
        for _ in range(num_of_threads):
            worker = TaskRunner(
                self.job_queue,
                self.job_status,
                self.shutdown_notification,
                self.condition,
                data_ingestor,
                logger
            )
            logger.info("Starting %s", worker.name)
            worker.start()

    def is_running(self):
        """
        Checks if the ThreadPool is running.

        Returns:
            bool: True if the ThreadPool is running, False otherwise.
        """
        return not self.shutdown_notification

    def shutdown(self):
        """
        Signals the ThreadPool to shut down gracefully.

        This method appends a True value to the `shutdown_notification` list,
        indicating to the worker threads to finish processing pending jobs
        and then shut down.

        Returns:
            None
        """
        self.shutdown_notification.append(True)


class TaskRunner(Thread):
    """
    A class representing a task runner for processing jobs in a threaded environment.

    Parameters:
        Thread (class): The Thread class from the threading module.

    Attributes:
        job_queue (Queue): A queue containing the jobs to be processed.
        job_status (dict): A dictionary to store the status of each job.
        shutdown_notification (list): A flag indicating whether the task runner should shut down.
        condition (Condition): A threading condition for synchronization.
        table (DataFrame): The data table for processing jobs.
        questions_best_is_min (list): A list of questions where lower values are considered 'best'.
        questions_best_is_max (list): A list of questions where higher values are considered 'best'.
        logger (Logger): An object providing access to the logger.
    """

    def __init__(self, job_queue, job_status, shutdown_notification, condition, data_ingestor, logger):
        Thread.__init__(self)
        self.job_queue = job_queue
        self.job_status = job_status
        self.shutdown_notification = shutdown_notification
        self.condition = condition
        self.table = data_ingestor.table
        self.questions_best_is_min = data_ingestor.questions_best_is_min
        self.questions_best_is_max = data_ingestor.questions_best_is_max
        self.logger = logger

    def save_job_to_disk(self, result, job_id):
        """
        Saves the given result to a JSON file on disk with the "job_id_{job_id}.json" format.

        Parameters:
            result (dict): The result to be saved.
            job_id (int): The ID of the job used for naming the resulting JSON file.

        Returns:
            None
        """
        with open(f"./results/job_id_{job_id}.json", "w", encoding="utf-8") as output_file:
            json.dump(result, output_file, sort_keys=False)

    def exec_states_mean(self, question, job_id):
        """
        Executes the job to calculate the mean values for states based on a given question.

        Parameters:
            question (str): The question for which to calculate the means.
            job_id (int): The ID of the job.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, input: '%s'", job_id, question)

        states_mean = []

        # Filter table by Question column values and group by state afterwards
        filtered_table = self.table.loc[self.table["Question"] == question]
        for state, table in filtered_table.groupby("LocationDesc"):
            states_mean.append((state, table["Data_Value"].mean()))

        # Sort data by value
        states_mean = dict(sorted(states_mean, key=lambda state: state[1]))

        # Save the result on disk
        self.save_job_to_disk(states_mean, job_id)

        self.logger.info("Result %s saved on disk", states_mean)

    def exec_state_mean(self, question, state, job_id):
        """
        Executes the job to calculate the mean value for a specific state and question.

        Parameters:
            question (str): The question for which to calculate the mean.
            state (str): The state for which to calculate the mean.
            job_id (int): The ID of the job.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, input: '%s', '%s'", job_id, question, state)

        # Filter table by Question and LocationDesc columns values
        filtered_table = self.table.loc[
            (self.table["Question"] == question) &
            (self.table["LocationDesc"] == state)
        ]

        # Save the result on disk
        state_mean = {state: filtered_table["Data_Value"].mean()}
        self.save_job_to_disk(state_mean, job_id)

        self.logger.info("Result %s saved on disk", state_mean)

    def exec_top5(self, question, job_id, best=True):
        """
        Executes the job to calculate the top 5 best or worst states based on a given question.

        Parameters:
            question (str): The question for which to calculate the top 5 states.
            job_id (int): The ID of the job.
            best (bool): Flag indicating whether to calculate the top 5 best (True) or worst (False) states.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, %s case, input: '%s'", job_id, 'best' if best is True else 'worst', question)

        states_top5 = []

        # Filter table by Question column values and group by state afterwards
        filtered_table = self.table.loc[self.table["Question"] == question]
        for state, table in filtered_table.groupby("LocationDesc"):
            states_top5.append((state, table["Data_Value"].mean()))

        # Sort data by value depending on the question and best/worst case
        if question in self.questions_best_is_min:
            states_top5 = dict(sorted(states_top5, key=lambda state: state[1], reverse=not best)[:5])
        else:
            states_top5 = dict(sorted(states_top5, key=lambda state: state[1], reverse=best)[:5])

        # Save the result on disk
        self.save_job_to_disk(states_top5, job_id)

        self.logger.info("Result %s saved on disk", states_top5)

    def exec_global_mean(self, question, job_id):
        """
        Executes the job to calculate the global mean value for a given question.

        Parameters:
            question (str): The question for which to calculate the global mean.
            job_id (int): The ID of the job.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, input: '%s'", job_id, question)

        # Filter table by Question column values
        filtered_table = self.table.loc[self.table["Question"] == question]

        # Save the result on disk
        global_mean = {"global_mean": filtered_table["Data_Value"].mean()}
        self.save_job_to_disk(global_mean, job_id)

        self.logger.info("Result %s saved on disk", global_mean)

    def exec_diff_from_mean(self, question, job_id):
        """
        Executes the job to calculate the difference of each state's mean value from the global mean.

        Parameters:
            question (str): The question for which to calculate the differences.
            job_id (int): The ID of the job.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, input: '%s'", job_id, question)

        # Filter table by Question column values
        filtered_table = self.table.loc[self.table["Question"] == question]
        global_mean = filtered_table["Data_Value"].mean()

        # Group filtered table by LocationDesc column values
        diff_states_mean = []
        for state, table in filtered_table.groupby("LocationDesc"):
            diff_states_mean.append((state, global_mean - table["Data_Value"].mean()))

        # Sort data by value
        diff_states_mean = dict(sorted(diff_states_mean, key=lambda state: state[1], reverse=True))

        # Save the result on disk
        self.save_job_to_disk(diff_states_mean, job_id)

        self.logger.info("Result %s saved on disk", diff_states_mean)

    def exec_state_diff_from_mean(self, question, state, job_id):
        """
        Executes the job to calculate the difference of a specific state's mean value and the global mean.

        Parameters:
            question (str): The question for which to calculate the difference.
            state (str): The state for which to calculate the difference.
            job_id (int): The ID of the job.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, input: '%s', '%s'", job_id, question, state)

        # Filter table by Question column values to calculate global mean
        filtered_table = self.table.loc[self.table["Question"] == question]
        global_mean = filtered_table["Data_Value"].mean()

        # Further filter the table by LocationDesc column values
        filtered_table = filtered_table.loc[filtered_table["LocationDesc"] == state]

        # Save the result on disk
        state_diff_states_mean = {state: global_mean - filtered_table["Data_Value"].mean()}
        self.save_job_to_disk(state_diff_states_mean, job_id)

        self.logger.info("Result %s saved on disk", state_diff_states_mean)

    def exec_mean_by_category(self, question, job_id):
        """
        Executes the job to calculate the mean values by category for a given question.

        Parameters:
            question (str): The question for which to calculate the means.
            job_id (int): The ID of the job.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, input: '%s'", job_id, question)

        category_mean = {}

        # Filter table by Question column values, then group by categories
        filtered_table = self.table.loc[self.table["Question"] == question]
        for category, table in filtered_table.groupby(["LocationDesc", "StratificationCategory1", "Stratification1"]):
            category_mean[str(category)] = table["Data_Value"].mean()

        # Save the result on disk
        self.save_job_to_disk(category_mean, job_id)

        self.logger.info("Result %s saved on disk", category_mean)

    def exec_state_mean_by_category(self, question, state, job_id):
        """
        Executes the job to calculate the mean values by category for a specific state and question.

        Parameters:
            question (str): The question for which to calculate the means.
            state (str): The state for which to calculate the means.
            job_id (int): The ID of the job.

        Returns:
            None
        """
        self.logger.info("Executing job with id %s, input: '%s', '%s'", job_id, question, state)

        state_category_mean = {}

        # Filter table by Question and LocationDesc columns values, then group by categories
        filtered_table = self.table.loc[
            (self.table["Question"] == question) & (self.table["LocationDesc"] == state)
        ]
        for category, table in filtered_table.groupby(["StratificationCategory1", "Stratification1"]):
            state_category_mean[str(category)] = table["Data_Value"].mean()

        # Save the result on disk
        self.save_job_to_disk({state: state_category_mean}, job_id)

        self.logger.info("Result %s saved on disk", state_category_mean)


    def run(self):
        self.logger.info("Started successfully")

        # Repeat until graceful_shutdown and empty queue
        with self.condition:
            while not (self.shutdown_notification and self.job_queue.empty()):
                # Put all workers on hold as long as there are no jobs
                if self.job_queue.empty():
                    self.logger.info("No pending jobs, waiting")
                    self.condition.wait()
                    self.logger.info("Received wake up notification from dispatcher")
                else:
                    # Get pending job
                    job = self.job_queue.get()
                    request = job[0]
                    data = job[1]
                    job_id = job[2]
                    self.logger.info("Got job '%s', %s with id %s", request, data, job_id)

                    # Execute the job and save the result to disk
                    if request == "states_mean":
                        self.exec_states_mean(data[0], job_id)
                    elif request == "state_mean":
                        self.exec_state_mean(data[0], data[1], job_id)
                    elif request == "best5":
                        self.exec_top5(data[0], job_id)
                    elif request == "worst5":
                        self.exec_top5(data[0], job_id, best=False)
                    elif request == "global_mean":
                        self.exec_global_mean(data[0], job_id)
                    elif request == "diff_from_mean":
                        self.exec_diff_from_mean(data[0], job_id)
                    elif request == "state_diff_from_mean":
                        self.exec_state_diff_from_mean(data[0], data[1], job_id)
                    elif request == "mean_by_category":
                        self.exec_mean_by_category(data[0], job_id)
                    elif request == "state_mean_by_category":
                        self.exec_state_mean_by_category(data[0], data[1], job_id)

                    # Mark job as done
                    self.job_status[job[-1]] = "done"
                    self.logger.info("Finished job with id %s", job_id)
            self.logger.info("Shutting down")
