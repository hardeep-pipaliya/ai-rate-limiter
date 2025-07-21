import os
import sys
import json
import time
import traceback
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

# Add project root to PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../.."))
sys.path.insert(0, project_root)

import pika
from dotenv import load_dotenv
from loguru import logger as _root_logger
from app import create_app
from app.workers.workspace_worker import WorkspaceWorker
from app.config.logger import LoggerSetup
from app.connection.orm_postgres_connection import db
from app.models.workers import Worker as WorkerModel

load_dotenv()
    
class Worker:
    def __init__(self, workspace_id, slug_id):
        self.workspace_id = workspace_id
        self.slug_id = slug_id
        self.pid = os.getpid()
        self.app = create_app()  # Create Flask app instance
        self.worker = None
        self.setup_logger()

    def setup_logger(self):
        _root_logger.remove()
        try:
            self.logger = LoggerSetup().setup_worker_logger(self.pid)
            self.logger.info(f"Worker logger initialized (PID {self.pid})")
        except Exception as e:
            print(f"Logger setup failed: {e}")
            _root_logger.add(sys.stdout, level="INFO")
            self.logger = _root_logger
            self.logger.error(f"Logger setup failed: {e}")    
    def start_consuming(self):
        with self.app.app_context():  # Establish application context
            try:
                # Initialize worker with app context
                self.worker = WorkspaceWorker(self.workspace_id, app=self.app)
                  # First verify we can create log files
                test_log = os.path.join(LoggerSetup().log_dir, f"worker_{self.pid}_test.log")
                try:
                    with open(test_log, 'w') as f:
                        f.write(f"Test log file created at {datetime.now()}")
                    os.unlink(test_log)  # Clean up test file
                    self.logger.info("Log directory is writable")
                except Exception as log_e:
                    self.logger.error(f"Log directory is not writable: {log_e}")
                    raise
                
                # Update worker record with retries
                max_attempts = 3
                for attempt in range(max_attempts):
                    try:
                        self._update_worker_record()
                        self.logger.info("Worker record updated successfully")
                        break
                    except Exception as e:
                        self.logger.error(f"Attempt {attempt + 1} to update worker record failed: {e}")
                        if attempt == max_attempts - 1:
                            raise
                        time.sleep(1)
                
                # Start processing
                self.worker.start()
                
                # Keep main thread alive with monitoring
                while True:
                    time.sleep(1)
                    # Verify worker is still properly registered
                    if not self.worker.is_running or not self.worker.is_connected():
                        self.logger.error("Worker lost connection or stopped running")
                        break
                    
            except KeyboardInterrupt:
                self.logger.info("Keyboard interrupt - shutting down")
            except Exception as e:
                self.logger.error(f"Worker error: {e}")
                self.logger.error(traceback.format_exc())
            finally:
                if self.worker:
                    try:
                        self.worker.stop()
                        self.logger.info("Worker stopped gracefully")
                    except Exception as stop_e:
                        self.logger.error(f"Error stopping worker: {stop_e}")
                self.logger.info("Worker exiting")

    def _update_worker_record(self):
        """Update worker record in database"""
        max_attempts = 3
        attempt = 0
        
        while attempt < max_attempts:
            try:
                db.session.rollback()  # Ensure fresh session
                worker = WorkerModel.query.filter_by(slug_id=self.slug_id).first()
                if worker:
                    worker.pid = self.pid
                    worker.status = 'running'
                    # Normalize path separators
                    log_path = os.path.join(LoggerSetup().log_dir, f"worker_{self.pid}.log")
                    worker.log_file = log_path.replace('\\', '/')
                    worker.last_heartbeat = datetime.now(timezone.utc)
                    worker.started_at = datetime.now(timezone.utc)
                    db.session.commit()
                    self.logger.info(f"Updated worker record in database (PID: {self.pid})")
                    return
            except Exception as e:
                attempt += 1
                self.logger.error(f"Attempt {attempt}: Failed to update worker record: {e}")
                db.session.rollback()
                if attempt < max_attempts:
                    time.sleep(1)
        
        self.logger.error("Max attempts reached - failed to update worker record")

def main(workspace_id: str, slug_id: str):
    worker = Worker(workspace_id, slug_id)
    worker.start_consuming()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python worker.py <workspace_id> <slug_id>", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])