from queue import Queue, Empty
from threading import Thread, Event, Lock
import os
import json

class ThreadPool:
    """
    Thread pool implementation for parallel job processing.
    Manages a queue of jobs that are processed by a configurable number of worker threads.
    """
    def __init__(self):
        # Fundamental data structures
        self.job_queue = Queue()
        self.status_lock = Lock()  # For thread-safe access to dictionaries
        self.job_status = {}
        self.job_results = {}
        self.shutdown_event = Event()
        
        # Initial configuration
        self._setup_worker_infrastructure()
        self._initialize_workers()

    def _setup_worker_infrastructure(self):
        """Configure basic execution parameters"""
        # Determine the number of threads
        env_value = os.getenv("TP_NUM_OF_THREADS")
        system_cores = os.cpu_count() or 1
        self.num_threads = self._calculate_optimal_thread_count(env_value, system_cores)
        
        # Initialize worker containers
        self.workers = []

    def _calculate_optimal_thread_count(self, env_var, core_count):
        """Calculate the maximum allowed number of threads"""
        if env_var:
            try:
                return max(1, min(int(env_var), core_count))
            except ValueError:
                return core_count
        return core_count

    def _initialize_workers(self):
        """Launch and space task executors"""
        for _ in range(self.num_threads):
            self._spawn_and_register_worker()

    def _spawn_and_register_worker(self):
        """Create a new worker and add it to the pool"""
        task_executor = self._create_task_executor()
        task_executor.start()
        self.workers.append(task_executor)

    def _create_task_executor(self):
        """Create a properly configured task executor"""
        return TaskRunner(
            task_queue=self.job_queue,
            status_registry=self.job_status,
            result_store=self.job_results,
            shutdown_flag=self.shutdown_event,
            status_lock=self.status_lock
        )

    def add_job(self, task_func, payload, job_id):
        """Add a job to the processing system
        
        Args:
            task_func: The function to execute
            payload: The data to pass to the function
            job_id: Unique identifier for the job
            
        Raises:
            ValueError: If job_id is already in use
        """
        job_id_str = str(job_id)
        self._validate_job_identifier(job_id_str)
        self._register_new_job(task_func, payload, job_id_str)
        return job_id_str

    def _validate_job_identifier(self, job_id):
        """Ensure job ID uniqueness"""
        with self.status_lock:
            if job_id in self.job_status:
                raise ValueError(f"Duplicate job ID: {job_id}")

    def _register_new_job(self, func, data, job_id):
        """Register a job in the system"""
        self.job_queue.put((func, data, job_id))
        with self.status_lock:
            self.job_status[job_id] = "running"
        self._log_job_submission(job_id)

    def _log_job_submission(self, job_id):
        """Dedicated logger for job addition events"""
        print(f"[System] Job {job_id} adăugat în coadă")

    def get_job_status(self, job_id):
        """Get the status of a job
        
        Args:
            job_id: The job identifier
            
        Returns:
            str: The job status or None if job not found
        """
        with self.status_lock:
            return self.job_status.get(str(job_id))
            
    def get_job_result(self, job_id):
        """Get the result of a completed job
        
        Args:
            job_id: The job identifier
            
        Returns:
            The job result or None if job not found or not completed
        """
        status = self.get_job_status(job_id)
        if status == "done":
            with self.status_lock:
                return self.job_results.get(str(job_id))
        return None

    def shutdown(self):
        """Initiate the orderly shutdown process"""
        if self.shutdown_event.is_set():
            print("[Warning] Shutdown already in progress")
            return
            
        self._activate_shutdown_sequence()
        self._await_pending_operations()
        self._terminate_workers()

    def _activate_shutdown_sequence(self):
        """Transition the system to shutdown mode"""
        self.shutdown_event.set()
        print("[System] Inițiat proces de shutdown")

    def _await_pending_operations(self):
        """Wait for remaining tasks to complete"""
        try:
            self.job_queue.join()
            print("[System] Toate task-urile au fost finalizate")
        except Exception as e:
            print(f"[Error] Error waiting for pending operations: {str(e)}")

    def _terminate_workers(self):
        """Stop all executors"""
        for worker in self.workers:
            try:
                worker.join(timeout=5.0)  # Add timeout to prevent hanging
            except Exception:
                pass
        print("[System] Toate workerii au fost opriți")


class TaskRunner(Thread):
    """
    Task executor that processes jobs from a queue.
    Each instance runs in its own thread and processes jobs until shutdown.
    """
    def __init__(self, task_queue, status_registry, result_store, shutdown_flag, status_lock):
        super().__init__(daemon=True)
        self.task_source = task_queue
        self.status_tracker = status_registry
        self.result_archive = result_store
        self.shutdown_indicator = shutdown_flag
        self.status_lock = status_lock

    def run(self):
        """Main execution cycle"""
        while not self._should_terminate():
            try:
                self._process_next_task()
            except Exception:
                pass

    def _should_terminate(self):
        """Check termination conditions"""
        return self.shutdown_indicator.is_set() and self.task_source.empty()

    def _process_next_task(self):
        """Extract and execute a task from the queue"""
        task_id = None
        try:
            # Add timeout to allow checking shutdown condition periodically
            try:
                task_func, input_data, task_id = self.task_source.get(timeout=1.0)
                self._execute_task_safely(task_func, input_data, task_id)
            except Empty:
                return
        except Exception:
            pass
        finally:
            # Make sure we always call task_done() if we called get()
            if task_id is not None:
                self._finalize_task_processing()

    def _execute_task_safely(self, func, data, task_id):
        """Execute a task with exception handling"""
        try:
            result = func(data)
            self._handle_successful_execution(task_id, result)
        except Exception as e:
            self._handle_execution_failure(task_id, e)

    def _handle_successful_execution(self, task_id, result):
        """Process successful results"""
        with self.status_lock:
            self.result_archive[task_id] = result
            self.status_tracker[task_id] = "done"
            
        self._persist_execution_results(task_id, result)

    def _handle_execution_failure(self, task_id, error):
        """Process execution errors"""
        with self.status_lock:
            self.status_tracker[task_id] = "error"
            self.result_archive[task_id] = {"error": str(error)}
            
        print(f"[Eroare] Task {task_id} a eșuat: {str(error)}")
        # Also persist error results
        self._persist_execution_results(task_id, {"error": str(error), "type": type(error).__name__})

    def _persist_execution_results(self, task_id, data):
        """Save results to disk"""
        try:
            os.makedirs("results", exist_ok=True)
            with open(f"results/job_{task_id}.json", "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[Error] Failed to persist results for job {task_id}: {str(e)}")

    def _finalize_task_processing(self):
        """Finalize task processing"""
        self.task_source.task_done()