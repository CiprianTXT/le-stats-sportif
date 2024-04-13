import json
from functools import wraps
from flask import request, jsonify
from app import webserver


def request_handler(request_name):
    """
    Decorator for handling requests.

    This decorator adds request handling functionality to the decorated function.
    It checks if the thread pool is running, and if so, registers the request as a job in the pool's queue.

    Args:
        request_name (str): The name of the request.

    Returns:
        wrapper: The decorated function.
    """
    def decorator(handler):
        @wraps(handler)
        def wrapper():
            if webserver.tasks_runner.is_running():
                # Get request data
                data = request.json
                webserver.logger.info("Request '%s' received, data: %s", request_name, data)

                # Register job. Don't wait for task to finish
                job_id = webserver.job_counter
                job = [request_name, list(data.values()), job_id]
                webserver.tasks_runner.job_queue.put(job)
                webserver.tasks_runner.job_status[job_id] = "running"

                # Notify workers about incoming job
                webserver.logger.info("Queued the job with id %s, notifying available worker", job_id)
                with webserver.tasks_runner.condition:
                    webserver.tasks_runner.condition.notify()

                # Increment job_id counter
                webserver.job_counter += 1

                # Return associated job_id
                result = {
                    "status": "queued",
                    "job_id": job_id
                }
                webserver.logger.info("Returning %s to client", result)
                return jsonify(result)

            result = {"status": "Shutting down"}
            webserver.logger.info("Request received, but the thread pool was shut down, returning %s to client", result)
            return jsonify(result)
        return wrapper
    return decorator


@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs_request():
    """
    Function that returns the number of jobs currently running.

    Returns:
        JSON response:
            - "status": The response status ("done").
            - "data": The number of jobs currently running.
    """
    webserver.logger.info("Request received")

    job_status_values = list(webserver.tasks_runner.job_status.values())
    result = {
        "status": "done",
        "data": len(list(filter(lambda value: value == "running", job_status_values)))
    }

    webserver.logger.info("Returning %s to client", result)
    return jsonify(result)


@webserver.route('/api/jobs', methods=['GET'])
def jobs_request():
    """
    Function that returns the status of all jobs.

    Returns:
        JSON response:
            - "status": The response status ("done").
            - "data": The list of jobs and their status.
    """
    webserver.logger.info("Request received")

    jobs = []
    for current_id in range(1, webserver.job_counter):
        current_status = webserver.tasks_runner.job_status[current_id]
        jobs.append({f"job_id_{current_id}": current_status})
    result = {
        "status": "done",
        "data": jobs
    }

    webserver.logger.info("Returning %s to client", result)
    return jsonify(result)


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_results_request(job_id):
    """
    Function that returns the result of a specified job.

    Args:
        job_id (str): The ID of the job for which the result is requested.

    Returns:
        JSON response:
            - "status": The response status ("done" or "running").
            - "data": The result of the job if done.
            - "reason" (if status is "error"): The reason for the error.
    """
    webserver.logger.info("Request received, requesting status of job with id %s", job_id)

    job_id = int(job_id)

    # Check if job_id is valid
    if job_id not in range(1, webserver.job_counter):
        result = {
            "status": "error",
            "reason": "Invalid job_id"
        }
        webserver.logger.info("Returning %s to client", result)
        return jsonify(result)

    # Check if job_id is done and return the data
    if webserver.tasks_runner.job_status[job_id] == "done":
        with open(f"./results/job_id_{job_id}.json", encoding="utf-8") as file:
            data = json.load(file)
        result = {
            "status": "done",
            "data": data
        }
        webserver.logger.info("Returning %s to client", result)
        return jsonify(result)

    # If not, return running status
    result = {"status": "running"}
    webserver.logger.info("Returning %s to client", result)
    return jsonify(result)


@webserver.route('/api/states_mean', methods=['POST'])
@request_handler("states_mean")
def states_mean_request():
    """
    Function that adds a 'states_mean' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/state_mean', methods=['POST'])
@request_handler("state_mean")
def state_mean_request():
    """
    Function that adds a 'state_mean' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1",
            "state": "State1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/best5', methods=['POST'])
@request_handler("best5")
def best5_request():
    """
    Function that adds a 'best5' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/worst5', methods=['POST'])
@request_handler("worst5")
def worst5_request():
    """
    Function that adds a 'worst5' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/global_mean', methods=['POST'])
@request_handler("global_mean")
def global_mean_request():
    """
    Function that adds a 'global_mean' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/diff_from_mean', methods=['POST'])
@request_handler("diff_from_mean")
def diff_from_mean_request():
    """
    Function that adds a 'diff_from_mean' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/state_diff_from_mean', methods=['POST'])
@request_handler("state_diff_from_mean")
def state_diff_from_mean_request():
    """
    Function that adds a 'state_diff_from_mean' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1",
            "state": "State1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/mean_by_category', methods=['POST'])
@request_handler("mean_by_category")
def mean_by_category_request():
    """
    Function that adds a 'mean_by_category' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/state_mean_by_category', methods=['POST'])
@request_handler("state_mean_by_category")
def state_mean_by_category_request():
    """
    Function that adds a 'state_mean_by_category' job to the queue for execution.

    Request JSON:
        {
            "question": "Question1",
            "state": "State1"
        }

    Returns:
        JSON response:
            - "status": The response status ("queued" or "Shutting down").
            - "job_id": The ID of the job added to the queue.
    """


@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown_request():
    """
    Function that initiates a graceful shutdown of the thread pool.

    Returns:
        JSON response:
            - "status": The response status ("Shutting down").
    """
    webserver.logger.info("Request received")

    if webserver.tasks_runner.is_running():
        webserver.tasks_runner.shutdown()

        # Notify workers about shutdown event
        webserver.logger.info("Notifying all workers about shutdown event")
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify_all()

    result = {"status": "Shutting down"}
    webserver.logger.info("Thread pool is shutting down, returning %s to client", result)
    return jsonify(result)

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg


def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
