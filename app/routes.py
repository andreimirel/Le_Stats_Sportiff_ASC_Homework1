from threading import Lock
import logging
import json
import traceback
from flask import request, jsonify, abort, Response
from app import webserver

# Configure logging to write to both file and console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s',
    handlers=[
        logging.FileHandler("webserver.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("routes")

# Thread-safe job counter implementation
class JobCounter:
    def __init__(self):
        self._counter = 0
        self._lock = Lock()
    
    def get_next_id(self):
        with self._lock:
            self._counter += 1
            return str(self._counter)

job_counter = JobCounter()

def submit_job_request(task_callable):
    """
    Process job requests:
    - Extract JSON data from request
    - Check for shutdown signal
    - Generate thread-safe unique job ID
    - Add job to queue
    - Return JSON response with job_id
    
    Args:
        task_callable: Processing function to call with payload
    
    Returns:
        Response: JSON response with job ID or error
    """
    try:
        # Validate JSON presence
        if not request.is_json:
            logger.warning("Request without JSON payload received")
            return jsonify({'status': 'error', 'reason': 'JSON payload required'}), 400
            
        payload = request.json
        logger.debug("Received job request: %s", payload)

        # Check shutdown status
        if webserver.tasks_runner.shutdown_event.is_set():
            logger.info("Shutdown active, rejecting job submission")
            return jsonify({'job_id': -1, 'status': 'error', 'reason': 'shutting down'}), 503

        # Generate thread-safe job ID
        job_id_str = job_counter.get_next_id()

        # Add job to queue
        webserver.tasks_runner.add_job(task_callable, payload, job_id_str)
        logger.debug("Job submitted with id: %s", job_id_str)
        
        return jsonify({'job_id': job_id_str, 'status': 'submitted'})
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request")
        return jsonify({'status': 'error', 'reason': 'Invalid JSON format'}), 400
    except Exception as e:
        logger.error("Error submitting job: %s\n%s", str(e), traceback.format_exc())
        return jsonify({'status': 'error', 'reason': 'Internal server error'}), 500


# ------------------ Job endpoints ------------------

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """Calculate mean values for all states"""
    return submit_job_request(webserver.data_ingestor.states_mean)

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """Calculate mean for a specific state"""
    return submit_job_request(webserver.data_ingestor.state_mean)

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """Get top 5 performing states"""
    return submit_job_request(webserver.data_ingestor.best5)

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """Get bottom 5 performing states"""
    return submit_job_request(webserver.data_ingestor.worst5)

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """Calculate global mean"""
    return submit_job_request(webserver.data_ingestor.global_mean)

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """Calculate difference from mean for all states"""
    return submit_job_request(webserver.data_ingestor.diff_from_mean)

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """Calculate difference from mean for a state"""
    return submit_job_request(webserver.data_ingestor.state_diff_from_mean)

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """Calculate category-based means for all states"""
    return submit_job_request(webserver.data_ingestor.mean_by_category)

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """Calculate category-based mean for a state"""
    return submit_job_request(webserver.data_ingestor.state_mean_by_category)


# ------------------ Other endpoints ------------------

@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    """
    Endpoint that receives JSON data and returns confirmation
    
    Returns:
        Response: JSON response with received data or error
    """
    if request.method == 'POST':
        try:
            if not request.is_json:
                return jsonify({"error": "Request must be JSON"}), 400
                
            data = request.json
            logger.debug("Received data in post_endpoint: %s", data)
            response = {"message": "Received data successfully", "data": data}
            return jsonify(response)
        except Exception as e:
            logger.error("Error in post_endpoint: %s", str(e))
            return jsonify({"error": "Server error processing request"}), 500
    
    return jsonify({"error": "Method not allowed"}), 405


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    """
    Return results for a specific job
    
    Returns:
        Response: JSON response with job status and data
    """
    try:
        logger.debug("Fetching results for job_id: %s", job_id)
        tasks_runner = webserver.tasks_runner
        
        current_status = tasks_runner.job_status.get(job_id)
        
        if current_status is None:
            logger.warning("Invalid job_id requested: %s", job_id)
            return jsonify({'status': 'error', 'reason': 'Invalid job_id'}), 404
        
        if current_status == 'done':
            output = tasks_runner.job_results.get(job_id)
            return jsonify({'status': 'done', 'data': output})
        elif current_status == 'error':
            return jsonify({
                'status': 'error',
                'reason': 'Job processing failed',
                'data': tasks_runner.job_results.get(job_id, {})
            }), 500
        
        return jsonify({'status': current_status})
    
    except Exception as e:
        logger.error("Error retrieving job results: %s", str(e))
        return jsonify({'status': 'error', 'reason': 'Server error'}), 500


@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    """
    Initiate graceful shutdown
    
    Returns:
        Response: JSON confirmation of shutdown initiation
    """
    try:
        logger.info("Initiating graceful shutdown")
        webserver.tasks_runner.shutdown()
        return jsonify({'status': 'shutdown initiated'})
    except Exception as e:
        logger.error("Error during shutdown: %s", str(e))
        return jsonify({'status': 'error', 'reason': str(e)}), 500


@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    """
    Return current number of jobs in queue
    
    Returns:
        Response: JSON with job count
    """
    try:
        job_count = webserver.tasks_runner.job_queue.qsize()
        result = 0 if webserver.tasks_runner.shutdown_event.is_set() and job_count == 0 else job_count
        return jsonify({'jobs': result})
    except Exception as e:
        logger.error("Error getting job count: %s", str(e))
        return jsonify({'status': 'error', 'reason': str(e)}), 500


@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    """
    Return complete job results dictionary
    
    Returns:
        Response: JSON with all job results
    """
    try:
        return jsonify({
            'status': 'done', 
            'data': webserver.tasks_runner.job_results,
            'job_count': len(webserver.tasks_runner.job_results)
        })
    except Exception as e:
        logger.error("Error retrieving jobs: %s", str(e))
        return jsonify({'status': 'error', 'reason': str(e)}), 500


@webserver.route('/api/job_status', methods=['GET'])
def job_status():
    """
    Return status of all jobs
    
    Returns:
        Response: JSON with job statuses
    """
    try:
        return jsonify({
            'status': 'done',
            'data': webserver.tasks_runner.job_status
        })
    except Exception as e:
        logger.error("Error retrieving job status: %s", str(e))
        return jsonify({'status': 'error', 'reason': str(e)}), 500


@webserver.route('/')
@webserver.route('/index')
def index():
    """
    Main page showing available routes
    
    Returns:
        str: HTML message with route list
    """
    try:
        route_list = get_defined_routes()
        message = "<h1>Nutrition and Obesity Data API</h1>"
        message += "<p>Available endpoints:</p>"
        html_list = "<ul>"
        html_list += "".join(f"<li>{route}</li>" for route in route_list)
        html_list += "</ul>"
        return message + html_list
    except Exception as e:
        logger.error("Error generating index page: %s", str(e))
        return "Server error", 500


def get_defined_routes():
    """
    Get list of defined API endpoints
    
    Returns:
        list: Formatted route strings
    """
    routes = []
    for rule in webserver.url_map.iter_rules():
        if rule.endpoint != 'static':  # Exclude static endpoint
            methods = ', '.join(sorted(rule.methods - {'HEAD', 'OPTIONS'}))
            routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return sorted(routes)

