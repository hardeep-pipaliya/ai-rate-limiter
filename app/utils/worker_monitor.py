import time
import psutil
import threading
from datetime import datetime, timezone, timedelta
from app.config.logger import LoggerSetup
from app.connection.orm_postgres_connection import db
from app.connection.rabbitmq_connection import RabbitMQConnection
from app.models.workers import Worker as WorkerModel
from app.models.workspaces import WorkSpace

log_setup = LoggerSetup()
logger = log_setup.setup_logger()

class WorkerMonitor:
    """Monitor and manage worker health and performance"""
    
    def __init__(self, check_interval=30):
        self.check_interval = check_interval
        self.running = False
        self.monitor_thread = None
        self.rabbitmq = None
        
    def start(self):
        """Start the worker monitor"""
        if self.running:
            logger.warning("Worker monitor is already running")
            return
            
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Worker monitor started")
    
    def stop(self):
        """Stop the worker monitor"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        logger.info("Worker monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                self._check_worker_health()
                self._check_queue_consumers()
                self._cleanup_stale_workers()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in worker monitor loop: {e}")
                time.sleep(self.check_interval)
    
    def _check_worker_health(self):
        """Check health of all registered workers"""
        try:
            # Get all workers from database
            workers = WorkerModel.query.filter_by(status='running').all()
            
            for worker in workers:
                try:
                    # Check if process is still running
                    if not psutil.pid_exists(worker.pid):
                        logger.warning(f"Worker {worker.id} (PID {worker.pid}) process not found")
                        worker.status = 'stopped'
                        db.session.commit()
                        continue
                    
                    # Check if worker is responsive (heartbeat check)
                    if worker.last_heartbeat:
                        time_since_heartbeat = datetime.now(timezone.utc) - worker.last_heartbeat
                        if time_since_heartbeat > timedelta(minutes=2):  # 2 minute threshold
                            logger.warning(f"Worker {worker.id} hasn't sent heartbeat for {time_since_heartbeat}")
                            # Don't automatically kill, just log warning
                    
                    # Check memory usage
                    try:
                        process = psutil.Process(worker.pid)
                        memory_mb = process.memory_info().rss / 1024 / 1024
                        
                        if memory_mb > 500:  # 500MB threshold
                            logger.warning(f"Worker {worker.id} using {memory_mb:.1f}MB memory")
                            
                    except psutil.NoSuchProcess:
                        logger.warning(f"Worker {worker.id} process disappeared")
                        worker.status = 'stopped'
                        db.session.commit()
                        
                except Exception as worker_error:
                    logger.error(f"Error checking worker {worker.id}: {worker_error}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error checking worker health: {e}")
    
    def _check_queue_consumers(self):
        """Check queue consumer counts and alert if zero"""
        try:
            if not self.rabbitmq:
                self.rabbitmq = RabbitMQConnection.get_instance()
            
            # Get all active workspaces
            workspaces = WorkSpace.query.all()
            
            for workspace in workspaces:
                try:
                    queue_name = str(workspace.workerspace_id)
                    queue_info = self.rabbitmq.get_queue_info(queue_name)
                    
                    if isinstance(queue_info, dict) and 'consumers' in queue_info:
                        consumer_count = queue_info['consumers']
                        message_count = queue_info.get('messages', 0)
                        
                        if consumer_count == 0 and message_count > 0:
                            logger.warning(f"Queue {queue_name} has {message_count} messages but 0 consumers!")
                            
                            # Check if there should be workers for this workspace
                            running_workers = WorkerModel.query.filter_by(
                                workspace_id=workspace.id,
                                status='running'
                            ).count()
                            
                            if running_workers == 0:
                                logger.error(f"No running workers found for workspace {workspace.id} with pending messages")
                            
                        elif consumer_count > 0:
                            logger.debug(f"Queue {queue_name}: {consumer_count} consumers, {message_count} messages")
                            
                except Exception as queue_error:
                    logger.error(f"Error checking queue for workspace {workspace.id}: {queue_error}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error checking queue consumers: {e}")
    
    def _cleanup_stale_workers(self):
        """Clean up stale worker records"""
        try:
            # Find workers that have been stopped for more than 1 hour
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
            
            stale_workers = WorkerModel.query.filter(
                WorkerModel.status == 'stopped',
                WorkerModel.last_heartbeat < cutoff_time
            ).all()
            
            for worker in stale_workers:
                # Double-check the process isn't running
                if not psutil.pid_exists(worker.pid):
                    logger.info(f"Cleaning up stale worker record {worker.id}")
                    db.session.delete(worker)
            
            db.session.commit()
            
        except Exception as e:
            logger.error(f"Error cleaning up stale workers: {e}")
    
    def get_worker_stats(self):
        """Get current worker statistics"""
        try:
            stats = {
                'total_workers': WorkerModel.query.count(),
                'running_workers': WorkerModel.query.filter_by(status='running').count(),
                'stopped_workers': WorkerModel.query.filter_by(status='stopped').count(),
                'workers_by_workspace': {},
                'queue_stats': {}
            }
            
            # Get workers by workspace
            workspaces = WorkSpace.query.all()
            for workspace in workspaces:
                running_count = WorkerModel.query.filter_by(
                    workspace_id=workspace.id,
                    status='running'
                ).count()
                
                stats['workers_by_workspace'][str(workspace.workerspace_id)] = {
                    'running': running_count,
                    'total': WorkerModel.query.filter_by(workspace_id=workspace.id).count()
                }
            
            # Get queue stats
            if not self.rabbitmq:
                self.rabbitmq = RabbitMQConnection.get_instance()
            
            queue_stats = self.rabbitmq.get_queue_stats()
            if 'queues' in queue_stats:
                for queue in queue_stats['queues']:
                    stats['queue_stats'][queue['name']] = {
                        'messages': queue.get('messages', 0),
                        'consumers': queue.get('consumers', 0),
                        'messages_ready': queue.get('messages_ready', 0)
                    }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting worker stats: {e}")
            return {'error': str(e)}

# Singleton instance
worker_monitor = WorkerMonitor() 