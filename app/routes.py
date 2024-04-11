import json
from flask import request, jsonify
from app import webserver


@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs_request():
    job_status_values = list(webserver.tasks_runner.job_status.values())
    return jsonify({
        "status": "done",
        "data": len(list(filter(lambda value: value == "running", job_status_values)))
    })


@webserver.route('/api/jobs', methods=['GET'])
def jobs_request():
    jobs = []
    for current_id in range(1, webserver.job_counter):
        current_status = webserver.tasks_runner.job_status[current_id]
        jobs.append({f"job_id_{current_id}": current_status})

    return jsonify({
        "status": "done",
        "data": jobs
    })


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_results_request(job_id):
    job_id = int(job_id)
    print(f"JobID is {job_id}")

    # Check if job_id is valid
    if job_id not in range(1, webserver.job_counter):
        return jsonify({
            "status": "error",
            "reason": "Invalid job_id"
        })

    # Check if job_id is done and return the result
    if webserver.tasks_runner.job_status[job_id] == "done":
        with open(f"./results/job_id_{job_id}.json", encoding="UTF-8") as file:
            result = json.load(file)
        return jsonify({
            "status": "done",
            "data": result
        })

    # If not, return running status
    return jsonify({"status": "running"})


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["states_mean", data["question"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["state_mean", data["question"], data["state"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["best5", data["question"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["worst5", data["question"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["global_mean", data["question"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["diff_from_mean", data["question"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["state_diff_from_mean", data["question"], data["state"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["mean_by_category", data["question"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    if webserver.tasks_runner.is_running():
        # Get request data
        data = request.json
        print(f"Got request {data}")

        # Register job. Don't wait for task to finish
        job_id = webserver.job_counter
        job = ["state_mean_by_category", data["question"], data["state"], job_id]
        webserver.tasks_runner.job_queue.put(job)
        webserver.tasks_runner.job_status[job_id] = "running"

        # Notify workers about incoming job
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify()

        # Increment job_id counter
        webserver.job_counter += 1

        # Return associated job_id
        return jsonify({
            "status": "queued",
            "job_id": job_id
        })

    return jsonify({"status": "Shutting down"})


@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown_request():
    if webserver.tasks_runner.is_running():
        webserver.tasks_runner.shutdown()
        # Notify workers about shutdown event
        with webserver.tasks_runner.condition:
            webserver.tasks_runner.condition.notify_all()

    return jsonify({"status": "Shutting down"})

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
