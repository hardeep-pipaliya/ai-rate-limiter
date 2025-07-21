import signal
import os
import sys
import time
import uuid
import subprocess
from datetime import datetime

import psutil
import requests
from dotenv import load_dotenv
from flask import Blueprint, request, jsonify, current_app, send_from_directory, abort
from loguru import logger

from app.config.logger import LoggerSetup
from app.connection.orm_postgres_connection import db
from app.connection.rabbitmq_connection import RabbitMQConnection
from app.models.workers import Worker as WorkerModel
from app.models.workspaces import WorkSpace

load_dotenv()

logger = LoggerSetup().setup_logger()
workers_bp = Blueprint('workers', __name__, url_prefix='/workers')

def read_last_n_lines(file_path, n=1000):
    """Read last n lines from a file"""
    try:
        with open(file_path, 'r') as file:
            # Read all lines and get the last n lines
            lines = file.readlines()
            return ''.join(lines[-n:])
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None

@workers_bp.route('/logs/<worker_id>')
def get_worker_logs(worker_id):
    """Get the log content for a worker"""
    try:
        # Get lines parameter with default of 1000
        lines = request.args.get('lines', default=1000, type=int)
        
        # Get worker from database
        worker = WorkerModel.query.filter_by(slug_id=worker_id).first()
        if not worker:
            return jsonify({'error': 'Worker not found'}), 404
            
        if not worker.log_file:
            return jsonify({'error': 'No log file available'}), 404
            
        # Normalize path
        log_path = worker.log_file.replace('\\', '/')
        
        # Check if file exists
        if not os.path.exists(log_path):
            return jsonify({'error': 'Log file not found'}), 404
            
        # Read the log content
        content = read_last_n_lines(log_path, lines)
        if content is None:
            return jsonify({'error': 'Error reading log file'}), 500
            
        return jsonify({
            'worker_id': worker_id,
            'log_content': content,
            'lines_returned': len(content.splitlines())
        })
        
    except Exception as e:
        logger.error(f"Error getting logs for worker {worker_id}: {e}")
        return jsonify({'error': str(e)}), 500

def is_process_running(pid: int) -> bool:
    """Return True if a process with this PID is alive."""
    return psutil.pid_exists(pid)

class WorkerRoutes:
    @staticmethod
    def start_worker(workspace_id):
        ws = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
        if not ws:
            raise ValueError(f"Workspace {workspace_id} not found")

        worker_script = os.path.abspath('app/workers/worker.py')
        if not os.path.exists(worker_script):
            logger.error(f"Worker script not found at {worker_script}")
            raise FileNotFoundError(worker_script)

        logger.info(f"Launching worker for workspace {workspace_id}")

        # Generate a slug_id upfront
        slug_id = str(uuid.uuid4())
        
        # Create the subprocess first to get the PID
        if os.name != 'nt' and os.access(worker_script, os.X_OK):
            cmd = [worker_script, workspace_id, slug_id]
        else:
            cmd = [sys.executable, worker_script, workspace_id, slug_id]

        # Use shell=False and ensure proper process creation
        proc = subprocess.Popen(
            cmd,
            env=os.environ.copy(),
            cwd=os.getcwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False
        )
        initial_pid = proc.pid
        logger.info(f"Worker launched with initial PID {initial_pid}")

        # Create worker record after process creation
        worker = WorkerModel(
            slug_id=slug_id,
            workspace_id=ws.id,
            pid=initial_pid,
            status='starting',
            worker_name='default',
            log_file=None,
            started_at=datetime.utcnow()
        )

        try:
            db.session.add(worker)
            db.session.commit()
            logger.info(f"Worker record created with PID {initial_pid}")
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to create worker record: {e}")
            # Try to kill the process if DB commit failed
            try:
                proc.terminate()
                time.sleep(1)
                if proc.poll() is None:
                    proc.kill()
            except Exception as kill_error:
                logger.error(f"Failed to kill process: {kill_error}")
            raise RuntimeError(f"Failed to create worker record: {e}")

        return worker

    @staticmethod
    def stop_worker(slug_id):
        """Terminate a worker process and mark it stopped."""
        w = WorkerModel.query.filter_by(slug_id=slug_id).first()
        if not w:
            raise ValueError(f"Worker {slug_id} not found")
        
        pid = w.pid
        if pid and is_process_running(pid):
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.5)
                if is_process_running(pid):
                    os.kill(pid, signal.SIGKILL)
            except Exception as e:
                logger.error(f"Error stopping worker {slug_id} (PID {pid}): {e}")
        
        w.status = 'stopped'
        w.updated_date = datetime.utcnow()
        db.session.commit()
        logger.info(f"Stopped worker {slug_id} (PID {pid})")
        return True

    @workers_bp.route('/scale/<workspace_id>', methods=['POST'])
    def scale_workers(workspace_id):
        data = request.get_json() or {}
        try:
            target = int(data.get('count', -1))
        except:
            return jsonify({'error': 'Invalid count'}), 400
        if target < 0:
            return jsonify({'error': 'Count must be ≥0'}), 400

        ws = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
        if not ws:
            return jsonify({'error': 'Workspace not found'}), 404

        # Start fresh session
        db.session.rollback()
        
        # Get current workers and verify their status
        current_workers = WorkerModel.query.filter_by(workspace_id=ws.id).all()
        live_workers = []
        
        for w in current_workers:
            if w.status in ['running', 'starting']:
                if w.pid and is_process_running(w.pid):
                    live_workers.append(w)
                else:
                    w.status = 'stopped'
                    db.session.commit()

        current_count = len(live_workers)
        to_add = max(0, target - current_count)
        to_remove = max(0, current_count - target)

        added = removed = 0
        errors = []
        new_workers = []  # Track newly created workers

        # Scale down first
        for w in live_workers[:to_remove]:
            try:
                WorkerRoutes.stop_worker(w.slug_id)
                removed += 1
            except Exception as e:
                errors.append(str(e))

        # Scale up - track created workers
        for _ in range(to_add):
            try:
                new_worker = WorkerRoutes.start_worker(workspace_id)
                new_workers.append(new_worker.slug_id)
                added += 1
            except Exception as e:
                errors.append(str(e))
                break

        # Give workers time to start and register (max 10 seconds)
        start_time = time.time()
        ready_workers = []
        while time.time() - start_time < 10 and len(ready_workers) < added:
            db.session.rollback()
            ready_workers = WorkerModel.query.filter(
                WorkerModel.slug_id.in_(new_workers),
                WorkerModel.workspace_id == ws.id,
                WorkerModel.status == 'running',
                WorkerModel.pid.isnot(None)
            ).all()
            
            # Verify PIDs are actually running
            ready_workers = [w for w in ready_workers if is_process_running(w.pid)]
            
            if len(ready_workers) < added:
                time.sleep(0.5)

        # Get final worker list
        db.session.rollback()
        updated_workers = WorkerModel.query.filter_by(workspace_id=ws.id).all()
        
        # Verify each worker's status
        worker_info = []
        for w in updated_workers:
            if w.status in ['running', 'starting']:
                if w.pid and is_process_running(w.pid):
                    worker_info.append({
                        'slug_id': w.slug_id,
                        'pid': w.pid,
                        'status': w.status,
                        'started_at': w.started_at.isoformat() if w.started_at else None
                    })
                else:
                    # Mark as stopped if not actually running
                    w.status = 'stopped'
                    db.session.commit()

        return jsonify({
            'workspace_id': workspace_id,
            'previous_count': current_count,
            'workers_added': added,
            'workers_removed': removed,
            'current_count': len(worker_info),
            'target_count': target,
            'success': not errors,
            'errors': errors if errors else None,
            'workers': worker_info
        })

    @workers_bp.route('/<workspace_id>', methods=['GET'])
    def list_workers(workspace_id):
        # Get 'status' from query parameters
        status_filter = request.args.get('status')
        include_rabbitmq = request.args.get('include_rabbitmq', 'true').lower() == 'true'

        ws = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
        if not ws:
            return jsonify({'error': 'Workspace not found'}), 404

        workers = WorkerModel.query.filter_by(workspace_id=ws.id).all()
        out = []
        
        for w in workers:
            if w.status == 'running' and (not w.pid or not is_process_running(w.pid)):
                # Mark dead running workers as stopped
                w.status = 'stopped'
                db.session.commit()

            # Normalize log file path and create URL
            log_file = None
            if w.log_file:
                # Use the new logs endpoint
                log_file = f"/workers/logs/{w.slug_id}"

            # Now apply the status filter
            if status_filter:
                if w.status == status_filter:
                    out.append({
                        'worker_id': w.slug_id,
                        'pid': w.pid,
                        'status': w.status,
                        'started_at': w.started_at.isoformat() if w.started_at else None,
                        'last_heartbeat': w.last_heartbeat.isoformat() if w.last_heartbeat else None,
                        'log_file': log_file
                    })
            else:
                # If no filter is given, return all workers
                out.append({
                    'worker_id': w.slug_id,
                    'pid': w.pid,
                    'status': w.status,
                    'started_at': w.started_at.isoformat() if w.started_at else None,
                    'last_heartbeat': w.last_heartbeat.isoformat() if w.last_heartbeat else None,
                    'log_file': log_file
                })

        response_data = {
            'workspace_id': ws.workerspace_id, 
            'workers': out
        }
        # Include RabbitMQ queue information if requested
        if include_rabbitmq:
            try:
                # Get RabbitMQ queue statistics for this workspace
                rabbitmq_conn = RabbitMQConnection.get_instance()
                queue_stats = rabbitmq_conn.get_queue_stats(workspace_id)
                
                # Add queue information to the response
                response_data['rabbitmq'] = queue_stats
                
            except Exception as e:
                logger.error(f"Error getting RabbitMQ queue information: {str(e)}")
                response_data['rabbitmq'] = {"error": str(e)}

        return jsonify(response_data)

def cleanup_worker_process(pid):
    try:
        if pid and is_process_running(pid):
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            if is_process_running(pid):
                os.kill(pid, signal.SIGKILL)
        logger.info(f"Cleaned up worker {pid}")
    except Exception as e:
        logger.error(f"Cleanup error for {pid}: {e}")

