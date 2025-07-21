import time
import uuid
import json
import threading
import os
import socket
import sys
import pika
import random
import platform
import signal
import requests
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import or_, func
from datetime import datetime, timezone, timedelta
from app.config.config import ARTICLE_QUEUE_SERIVCE_BASE_URL, RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, RABBITMQ_VHOST
from app.config.logger import LoggerSetup
from app.models.provider_keys import ProviderKey
from app.models.messages import Message, MessageStatus
from app.models.workers import Worker as WorkerModel
from app.models.workspaces import WorkSpace
from app.providers.factory import ProviderFactory
from app.workers.rate_limiter import ApiKeyManager
from app.connection.rabbitmq_connection import RabbitMQConnection
from app.connection.orm_postgres_connection import db

log_setup = LoggerSetup()
logger = log_setup.setup_logger()

def mask_api_key(api_key):
    """Mask API key for logging purposes"""
    if not api_key or len(api_key) < 8:
        return "[MASKED]"
    return f"{api_key[:2]}...{api_key[-2:]}"

class WorkspaceWorker:
    def __init__(self, workspace_id, app):
        """
        Initialize a worker for a specific workspace
        
        Args:
            workspace_id: Can be either:
                - Integer ID (workspaces.id)
                - UUID string (workspaces.slug_id)
                - workerspace_id string (workspaces.workerspace_id)
            app: Flask application instance
        """
        if app is None:
            raise ValueError("Flask app instance is required")
            
        self.app = app
        self.raw_workspace_id = workspace_id.rsplit(":keys", 1)[0] if workspace_id and workspace_id.endswith(":keys") else workspace_id
        
        # Resolve workspace identifiers
        self.workspace_info = self._resolve_workspace_info(self.raw_workspace_id)
        self.workerspace_id = self.workspace_info['workerspace_id']  # For queue names
        self.workspace_db_id = self.workspace_info['id']  # For database foreign key
        
        self.queue_name = str(self.workerspace_id)
        self.api_key_manager = ApiKeyManager()
        self.active = False
        self.is_running = False
        self.lock = threading.Lock()
        self.last_token_report = 0
        self.token_report_interval = 30
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        self.last_heartbeat = time.time()
        self.health_check_interval = 10
        
        # Initialize connection attributes
        self._connection = None
        self._channel = None
        
        # Add RabbitMQ connection parameters from config
        self.rabbitmq_params = {
            'host': RABBITMQ_HOST,
            'port': int(RABBITMQ_PORT),
            'virtual_host': RABBITMQ_VHOST,
            'credentials': {
                'username': RABBITMQ_USERNAME,
                'password': RABBITMQ_PASSWORD
            }
        }
        
        # Add connection retry settings
        self.connection_retry_delay = 5
        self.max_connection_retries = 5
        self.rabbitmq = None  # Will be initialized in start()
        
        # Initialize worker number and platform-specific settings
        with self.app.app_context():
            self.worker_number = self._get_next_worker_number()
            self._update_health_status()
        
        # Platform-specific initializations
        self.is_windows = platform.system().lower() == 'windows'
        self.connection_check_interval = 5  # seconds
        
        # Use appropriate interrupt handling based on platform
        if not self.is_windows:
            signal.signal(signal.SIGTERM, self._handle_sigterm)
            signal.signal(signal.SIGINT, self._handle_sigterm)
        else:
            # Windows-specific: Add event for graceful shutdown
            self.stop_event = threading.Event()

        # Add output queue URL configuration
        # self.article_queue_service_base_url = ARTICLE_QUEUE_SERIVCE_BASE_URL
        
        # Add rate limit tracking
        self.provider_rate_limits = {}  # Track rate limits by provider
        self.rate_limit_lock = threading.Lock()

    def _handle_sigterm(self, signum, frame):
        """Handle termination signal gracefully"""
        logger.info("Received termination signal, stopping worker...")
        self.stop()

    def _resolve_workspace_info(self, workspace_id):
        """
        Returns dict with:
        - 'id': integer primary key
        - 'workerspace_id': string identifier
        - 'slug_id': UUID
        """
        if not workspace_id:
            raise ValueError("Workspace ID cannot be None")
            
        with self.app.app_context():
            workspace = None
            
            # Try by integer ID first
            if isinstance(workspace_id, int) or str(workspace_id).isdigit():
                workspace = WorkSpace.query.filter_by(id=int(workspace_id)).first()
            
            # Try by UUID slug_id if not found
            if not workspace and '-' in str(workspace_id):
                workspace = WorkSpace.query.filter_by(slug_id=str(workspace_id)).first()
            
            # Try by workerspace_id if still not found
            if not workspace:
                workspace = WorkSpace.query.filter_by(workerspace_id=str(workspace_id)).first()
            
            if not workspace:
                raise ValueError(f"No workspace found with identifier: {workspace_id}")
            
            return {
                'id': workspace.id,
                'workerspace_id': workspace.workerspace_id,
                'slug_id': workspace.slug_id
            }

    def _get_next_worker_number(self):
        """Get next worker number for this workspace"""
        try:
            max_num = db.session.query(
                func.max(WorkerModel.worker_number)
            ).filter_by(workspace_id=self.workspace_db_id).scalar()
            
            return (max_num or 0) + 1
        except Exception as e:
            logger.error(f"Error getting next worker number: {e}")
            return 1  # fallback to 1 if there's an error

    def is_connected(self):
        """Check if we have a valid connection and channel"""
        return (hasattr(self, '_connection') and self._connection and self._connection.is_open and
                hasattr(self, '_channel') and self._channel and self._channel.is_open)

    def _update_health_status(self):
        """Update health status in database with improved error handling"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                # Ensure we have a fresh session
                if attempt > 0:
                    db.session.rollback()
                
                worker = WorkerModel.query.filter_by(
                    workspace_id=self.workspace_db_id,
                    pid=os.getpid()
                ).first()
                
                if worker:
                    worker.last_heartbeat = datetime.now(timezone.utc)
                    worker.status = 'running' if self.is_running else 'stopped'
                    db.session.commit()
                    logger.debug(f"Health status updated for worker {worker.id}")
                    return
                else:
                    logger.warning(f"Worker not found in database for PID {os.getpid()}")
                    return
            except Exception as e:
                logger.error(f"Health update attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(1)
                    continue
                
                # Final attempt failed
                logger.error(f"Failed to update health status after {max_attempts} attempts")
                try:
                    db.session.rollback()
                except:
                    pass
                return

    def start(self):
        """Start the worker processing loop"""
        if self.is_running:
            logger.info(f"Worker {self.worker_number} is already running")
            return
            
        logger.info(f"Starting worker {self.worker_number} for workspace {self.workerspace_id}")
        
        try:
            # Initialize RabbitMQ connection first
            logger.info("Initializing RabbitMQ connection...")
            self.rabbitmq = RabbitMQConnection.get_instance()  # Get the singleton instance
            
            # Store the connection and channel references
            self._connection = self.rabbitmq._connection
            self._channel = self.rabbitmq._channel
            
            # Verify connection is established
            if not self._connection or not self._connection.is_open:
                raise Exception("Failed to establish RabbitMQ connection")
            
            logger.info("RabbitMQ connection established successfully")
            
            with self.app.app_context():
                # Verify queue exists and is accessible
                try:
                    self._declare_queue_with_retry()
                    logger.info(f"Queue {self.queue_name} verified")
                except Exception as e:
                    logger.error(f"Queue verification failed: {e}")
                    raise
                
                # Setup consumer before starting processing loop
                if not self._setup_consumer():
                    raise Exception("Failed to setup consumer")
                
                # Start the processing loop
                self.active = True
                self.is_running = True
                
                # Start monitoring thread
                self._start_monitoring_thread()
                
                # Verify consumer is active
                verify_timeout = time.time() + 30  # 30 seconds timeout
                consumer_verified = False
                while time.time() < verify_timeout:
                    try:
                        result = self._channel.queue_declare(
                            queue=self.queue_name,
                            passive=True
                        )
                        consumer_count = result.method.consumer_count
                        logger.info(f"Current consumer count: {consumer_count}")
                        
                        if consumer_count > 0:
                            logger.info("Consumer verified active")
                            consumer_verified = True
                            break
                        
                        time.sleep(2)  # Wait before next check
                    except Exception as e:
                        logger.warning(f"Consumer verification error: {e}")
                        time.sleep(2)
                        continue
                
                if not consumer_verified:
                    logger.error("Failed to verify consumer is active")
                    raise Exception("Consumer verification timeout")
                
                self._processing_loop()
                
        except Exception as e:
            logger.error(f"Error starting worker: {e}")
            # Clean up connection on error
            self._cleanup_connection()
            raise

    def _start_monitoring_thread(self):
        """Start a monitoring thread to check worker health"""
        def monitor_worker():
            last_check = time.time()
            consecutive_failures = 0
            last_activity_time = time.time()
            
            while self.active and self.is_running:
                try:
                    current_time = time.time()
                    
                    # Update last activity time if we've processed a message recently
                    if hasattr(self, '_last_message_time'):
                        if self._last_message_time > last_activity_time:
                            last_activity_time = self._last_message_time
                            consecutive_failures = 0  # Reset failures on activity
                            logger.debug("Activity detected, resetting failure count")
                    
                    # Check every 30 seconds
                    if current_time - last_check >= 30:
                        # Skip health check if we're actively processing messages
                        if current_time - last_activity_time < 60:  # Active in last minute
                            logger.debug("Skipping health check - active message processing")
                            last_check = current_time
                            consecutive_failures = 0
                            continue
                        
                        # Only do full health check if no activity for a while
                        if current_time - last_activity_time >= 7200:  # Increased to 2 hours
                            logger.debug("Performing full health check after inactivity")
                            
                            try:
                                # Check queue status first
                                result = self._channel.queue_declare(
                                    queue=self.queue_name,
                                    passive=True
                                )
                                message_count = result.method.message_count
                                consumer_count = result.method.consumer_count
                                
                                # Log queue status
                                logger.info(f"Queue status: {message_count} messages, {consumer_count} consumers")
                                
                                # Only increment failures for critical issues
                                if message_count > 0 and consumer_count == 0:
                                    logger.warning(f"No consumers with {message_count} pending messages")
                                    consecutive_failures += 1
                                else:
                                    consecutive_failures = 0
                                    
                            except pika.exceptions.ChannelClosedByBroker as e:
                                logger.error(f"Channel closed by broker during health check: {e}")
                                consecutive_failures += 1
                            except pika.exceptions.ConnectionClosedByBroker as e:
                                logger.error(f"Connection closed by broker during health check: {e}")
                                consecutive_failures += 1
                            except pika.exceptions.AMQPConnectionError as e:
                                logger.error(f"AMQP connection error during health check: {e}")
                                consecutive_failures += 1
                            except Exception as e:
                                logger.error(f"Unexpected error during health check: {e}")
                                consecutive_failures += 1
                            
                            # Try to reconnect if we have multiple critical failures
                            if consecutive_failures >= 3:  # Reduced from 10 to 3
                                logger.error(f"Multiple critical health check failures ({consecutive_failures}) - attempting reconnect")
                                try:
                                    # Try to get a new channel first
                                    try:
                                        if self._connection and self._connection.is_open:
                                            self._channel = self._connection.channel()
                                            logger.info("Successfully created new channel")
                                            consecutive_failures = 0
                                            continue
                                    except Exception as channel_error:
                                        logger.warning(f"Failed to create new channel, trying full reconnect: {channel_error}")
                                    
                                    # Try full reconnect if new channel failed
                                    self._cleanup_consumer()
                                    self._cleanup_connection()
                                    
                                    # Get new connection from singleton
                                    self.rabbitmq = RabbitMQConnection.get_instance()
                                    self._connection = self.rabbitmq._connection
                                    self._channel = self.rabbitmq._channel
                                    
                                    if self._setup_consumer():
                                        logger.info("Successfully reconnected")
                                        consecutive_failures = 0
                                        last_activity_time = current_time
                                    else:
                                        logger.error("Failed to reconnect - will keep retrying")
                                except Exception as e:
                                    logger.error(f"Reconnection error: {e}")
                                    # Don't stop worker, just keep trying
                        
                        last_check = current_time
                    
                    # Heartbeat log with activity status
                    if current_time - last_activity_time < 60:
                        logger.debug("Worker active and processing messages")
                    elif current_time - last_activity_time < 7200:  # 2 hours
                        logger.info(f"Worker idle for {(current_time - last_activity_time):.0f}s")
                    
                    time.sleep(10)  # Check every 10 seconds
                    
                except Exception as e:
                    logger.error(f"Monitor error: {e}")
                    time.sleep(15)
            
            logger.info("Worker monitoring thread stopped")
        
        # Start the monitoring thread
        self.monitoring_thread = threading.Thread(target=monitor_worker, daemon=True)
        self.monitoring_thread.start()
        logger.info("Worker monitoring thread started")

    def register_worker(self):
        """Register this worker in the database with improved reliability"""
        max_attempts = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_attempts):
            try:
                # Check if worker already exists
                existing_worker = WorkerModel.query.filter_by(
                    workspace_id=self.workspace_db_id,
                    pid=os.getpid()
                ).first()
                
                if existing_worker:
                    # Update existing worker
                    existing_worker.status = 'running'
                    existing_worker.last_heartbeat = datetime.now(timezone.utc)
                    existing_worker.worker_number = self.worker_number
                    db.session.commit()
                    logger.info(f"Updated existing worker record for PID {os.getpid()}")
                    return
                
                # Normalize path separators for log file
                log_path = os.path.join(LoggerSetup().log_dir, f"worker_{os.getpid()}.log")
                normalized_log_path = log_path.replace('\\', '/')
                
                # Verify log directory exists and is writable
                log_dir = os.path.dirname(normalized_log_path)
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
                
                # Test log file creation
                try:
                    with open(normalized_log_path, 'a') as f:
                        f.write(f"\nWorker registration test - {datetime.now(timezone.utc)}\n")
                except Exception as log_error:
                    logger.error(f"Failed to write to log file: {log_error}")
                    raise
                
                # Create new worker record
                worker = WorkerModel(
                    workspace_id=self.workspace_db_id,
                    pid=os.getpid(),
                    worker_number=self.worker_number,
                    worker_name=f"worker_{self.worker_number}",
                    status='running',
                    log_file=normalized_log_path,
                    last_heartbeat=datetime.now(timezone.utc),
                    started_at=datetime.now(timezone.utc),
                    config={
                        'hostname': socket.gethostname(),
                        'command': ' '.join(sys.argv),
                        'parent_pid': os.getppid()
                    }
                )
                
                db.session.add(worker)
                db.session.commit()
                logger.info(f"Successfully registered worker {self.worker_number} with PID {os.getpid()}")
                return
                
            except Exception as e:
                logger.error(f"Worker registration attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(retry_delay)
                    continue
                raise Exception(f"Failed to register worker after {max_attempts} attempts")

    def _initialize_rabbitmq(self):
        """Initialize RabbitMQ connection with direct Pika management"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                # Create credentials
                credentials = pika.PlainCredentials(
                    username=RABBITMQ_USERNAME,
                    password=RABBITMQ_PASSWORD
                )

                # Set connection parameters with improved settings
                parameters = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=int(RABBITMQ_PORT),
                    virtual_host=RABBITMQ_VHOST,
                    credentials=credentials,
                    heartbeat=60,  # Increased from 30 to reduce heartbeat frequency
                    blocked_connection_timeout=300,
                    connection_attempts=3,
                    retry_delay=5,
                    socket_timeout=30,  # Reduced from default
                    stack_timeout=30,   # Reduced from default
                    tcp_options={
                        'TCP_KEEPIDLE': 60,
                        'TCP_KEEPINTVL': 10,
                        'TCP_KEEPCNT': 3
                    }
                )

                # Close existing connection if open
                if hasattr(self, '_connection') and self._connection and self._connection.is_open:
                    try:
                        self._connection.close()
                    except:
                        pass

                self._connection = pika.BlockingConnection(parameters)
                logger.info("Created new direct RabbitMQ connection")

                # Close existing channel if open
                if hasattr(self, '_channel') and self._channel and self._channel.is_open:
                    try:
                        self._channel.close()
                    except:
                        pass

                self._channel = self._connection.channel()
                self._channel.basic_qos(
                    prefetch_count=1,
                    global_qos=False
                )
                logger.info("Created new RabbitMQ channel with QoS settings")

                # Verify queue exists
                try:
                    self._channel.queue_declare(
                        queue=self.queue_name,
                        passive=True
                    )
                    logger.info(f"Queue {self.queue_name} verified to exist")
                except pika.exceptions.ChannelClosedByBroker:
                    logger.error(f"Queue {self.queue_name} does not exist")
                    raise Exception(f"Queue {self.queue_name} does not exist")

                return True

            except Exception as e:
                logger.error(f"RabbitMQ initialization attempt {attempt + 1} failed: {e}")
                self._cleanup_connection()

                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    logger.error(f"Failed to initialize RabbitMQ after {max_retries} attempts")
                    return False

        return False
    
    def _declare_queue(self):
        """Check if queue exists"""
        try:
            # Only check if queue exists, don't try to create or modify it
            result = self._channel.queue_declare(
                queue=self.queue_name,
                passive=True  # passive=True means only check if it exists
            )
            
            # Log queue status
            message_count = result.method.message_count
            consumer_count = result.method.consumer_count
            logger.info(f"Queue {self.queue_name} exists - Messages: {message_count}, Consumers: {consumer_count}")
            
        except Exception as e:
            logger.error(f"Failed to check queue: {e}")
            raise Exception(f"Queue {self.queue_name} does not exist or is not accessible")

    def _cleanup_connection(self):
        """Clean up RabbitMQ connection safely"""
        try:
            # Clean up consumer and channel first
            self._cleanup_consumer()

            # Close direct connection if exists
            if hasattr(self, '_connection') and self._connection:
                try:
                    if self._connection.is_open:
                        self._connection.close()
                        logger.info("Closed direct RabbitMQ connection")
                except Exception as e:
                    logger.warning(f"Error closing direct connection: {e}")
                finally:
                    self._connection = None

            # Clean up RabbitMQ singleton reference
            if hasattr(self, 'rabbitmq') and self.rabbitmq:
                try:
                    if hasattr(self.rabbitmq, '_connection') and self.rabbitmq._connection:
                        if self.rabbitmq._connection.is_open:
                            self.rabbitmq._connection.close()
                            logger.info("Closed RabbitMQ singleton connection")
                except Exception as e:
                    logger.warning(f"Error closing RabbitMQ singleton connection: {e}")
                finally:
                    self.rabbitmq = None

        except Exception as e:
            logger.error(f"Error in connection cleanup: {e}")

    def _setup_consumer(self):
        """Setup consumer with improved reliability and failsafe mechanisms"""
        max_attempts = 3

        for attempt in range(max_attempts):
            try:
                logger.info(f"Setting up consumer (attempt {attempt + 1}/{max_attempts})")

                if not self._initialize_rabbitmq():
                    logger.error(f"Failed to initialize RabbitMQ on attempt {attempt + 1}")
                    if attempt < max_attempts - 1:
                        time.sleep(2)
                        continue
                    return False

                # Generate unique consumer tag
                if not hasattr(self, 'current_consumer_tag') or not self.current_consumer_tag:
                    hostname = socket.gethostname()
                    process_id = os.getpid()
                    timestamp = int(time.time())
                    self.current_consumer_tag = f"worker_{self.worker_number}_{hostname}_{process_id}_{timestamp}"

                # Setup consumer with auto_ack=False for manual acknowledgment
                try:
                    self._channel.basic_consume(
                        queue=self.queue_name,
                        on_message_callback=self._handle_message,
                        auto_ack=False,
                        consumer_tag=self.current_consumer_tag
                    )
                    logger.info(f"Consumer {self.current_consumer_tag} setup successfully")

                    # Give consumer time to register before verification
                    time.sleep(1.0)

                    # Verify consumer was set up
                    verification_successful = False
                    for verify_attempt in range(3):
                        try:
                            result = self._channel.queue_declare(
                                queue=self.queue_name,
                                passive=True
                            )
                            consumer_count = result.method.consumer_count
                            logger.info(f"Consumer verification: {consumer_count} consumers on queue")

                            if consumer_count > 0:
                                verification_successful = True
                                break
                            else:
                                logger.warning("No consumers found, waiting before retry")
                                time.sleep(1.0)

                        except Exception as verify_error:
                            logger.warning(f"Verification attempt {verify_attempt + 1} failed: {verify_error}")
                            if verify_attempt < 2:
                                time.sleep(1.0)
                            continue

                    if verification_successful:
                        # Final connectivity test
                        try:
                            logger.info("Testing consumer connectivity...")
                            test_passed = True  # You can add actual test logic here
                            logger.info("Consumer setup completed successfully")
                            return True

                        except Exception as test_error:
                            logger.error(f"Consumer connectivity test failed: {test_error}")
                            # Continue to next attempt

                    # If we get here, verification failed
                    logger.warning("Consumer verification failed, trying next attempt")
                    self._cleanup_consumer()
                    time.sleep(2)

                except Exception as consumer_error:
                    logger.error(f"Consumer setup failed on attempt {attempt + 1}: {consumer_error}")
                    self._cleanup_consumer()
                    if attempt < max_attempts - 1:
                        time.sleep(2)
                        continue
                    return False

            except Exception as e:
                logger.error(f"Consumer setup attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(2)
                    continue
                return False

        logger.error("All consumer setup attempts failed")
        return False

    def _cleanup_consumer(self):
        """Clean up consumer and channel safely"""
        try:
            # Cancel consumer if exists and channel is open
            if hasattr(self, 'current_consumer_tag') and self.current_consumer_tag:
                if hasattr(self, '_channel') and self._channel and self._channel.is_open:
                    try:
                        self._channel.basic_cancel(self.current_consumer_tag)
                        logger.info(f"Cancelled consumer {self.current_consumer_tag}")
                    except Exception as e:
                        logger.warning(f"Error cancelling consumer: {e}")
                self.current_consumer_tag = None
                
            # Close channel if open
            if hasattr(self, '_channel') and self._channel and self._channel.is_open:
                try:
                    self._channel.close()
                    logger.info("Closed RabbitMQ channel")
                except Exception as e:
                    logger.warning(f"Error closing channel: {e}")
                self._channel = None
                
        except Exception as e:
            logger.error(f"Error in consumer cleanup: {e}")

    def _processing_loop(self):
        """Main processing loop with improved connection resilience"""
        retry_count = 0
        max_retries = 20  # Increased from 10 to 20
        base_delay = 2
        last_health_check = time.time()
        consecutive_failures = 0
        last_heartbeat = time.time()
        last_connection_check = time.time()
        connection_check_interval = 30  # Check connection every 30 seconds
        last_success = time.time()  # Track last successful operation

        logger.info(f"Starting processing loop for worker {self.worker_number}")

        while self.active and (not self.is_windows or not self.stop_event.is_set()):
            try:
                current_time = time.time()

                # Reset retry count if we've had recent success
                if current_time - last_success < 300:  # 5 minutes
                    retry_count = max(0, retry_count - 1)  # Gradually reduce retry count

                # Send heartbeat during idle periods
                if current_time - last_heartbeat >= 15:
                    try:
                        if self._connection and self._connection.is_open:
                            self._connection.process_data_events(0)  # Non-blocking check
                            last_success = current_time  # Count successful heartbeat as success
                        last_heartbeat = current_time
                    except Exception as heartbeat_error:
                        logger.debug(f"Heartbeat error: {heartbeat_error}")
                        # Let the connection check handle reconnection if needed

                # Periodically check if connection is alive
                if current_time - last_connection_check >= connection_check_interval:
                    try:
                        if not self.is_connected():
                            logger.warning("Connection lost or consumer not setup, attempting to reconnect...")
                            self._cleanup_consumer()
                            self._cleanup_connection()

                            # Get new connection from singleton
                            self.rabbitmq = RabbitMQConnection.get_instance()
                            self._connection = self.rabbitmq._connection
                            self._channel = self.rabbitmq._channel

                            logger.info("Setting up consumer after connection issues")
                            if not self._setup_consumer():
                                raise Exception("Failed to setup consumer")

                            logger.info("Consumer setup successful")
                            consecutive_failures = 0
                            retry_count = max(0, retry_count - 2)  # Reduce retry count on successful reconnect
                            last_success = current_time
                        else:
                            last_success = current_time  # Connection is good
                    except Exception as conn_error:
                        logger.error(f"Connection check error: {conn_error}")
                        consecutive_failures += 1
                        time.sleep(base_delay)
                        
                    last_connection_check = current_time

                # Process messages with short timeout
                try:
                    if self._connection and self._connection.is_open:
                        self._connection.process_data_events(time_limit=1.0)
                        last_success = current_time  # Successful processing
                    time.sleep(0.2)
                except pika.exceptions.ConnectionClosed:
                    logger.warning("Connection closed, will attempt to reconnect on next check")
                    consecutive_failures += 1
                    time.sleep(base_delay)
                    continue
                except pika.exceptions.ChannelClosed:
                    logger.warning("Channel closed, will attempt to reconnect on next check")
                    consecutive_failures += 1
                    time.sleep(base_delay)
                    continue
                except AttributeError as attr_error:
                    logger.warning(f"Connection attribute error: {attr_error}")
                    last_connection_check = current_time - connection_check_interval  # Force connection check
                    time.sleep(base_delay)
                    continue
                except Exception as process_error:
                    logger.error(f"Error processing data events: {process_error}")
                    consecutive_failures += 1
                    time.sleep(base_delay)
                    continue

                # Reset consecutive failures on successful iteration
                consecutive_failures = 0

            except Exception as e:
                logger.error(f"Processing loop error: {e}")
                retry_count += 1
                consecutive_failures += 1

                # Use exponential backoff for retries
                delay = min(base_delay * (2 ** consecutive_failures), 60)  # Cap at 60 seconds
                logger.info(f"Retrying in {delay} seconds (retry {retry_count}/{max_retries})")
                time.sleep(delay)

                if retry_count >= max_retries:
                    logger.error(f"Max retries ({max_retries}) reached, stopping worker")
                    self.active = False
                    break

        logger.info("Processing loop stopped")

    def _declare_queue_with_retry(self, max_retries=3):
        """Check if queue exists with retry mechanism"""
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                # Only do passive declare to check if queue exists
                result = self.rabbitmq._channel.queue_declare(
                    queue=self.queue_name,
                    passive=True
                )
                logger.info(f"Queue {self.queue_name} exists with {result.method.consumer_count} consumers")
                return True                
            except pika.exceptions.ChannelClosedByBroker as e:
                # Queue doesn't exist
                logger.error(f"Queue {self.queue_name} does not exist")
                raise Exception(f"Queue {self.queue_name} does not exist - it must be created via the API first")
                
            except Exception as e:
                retry_count += 1
                last_error = e
                logger.warning(f"Queue check attempt {retry_count} failed: {e}")
                
                # On channel closed error, try to get a new channel
                if isinstance(e, pika.exceptions.ChannelWrongStateError):
                    try:
                        self.rabbitmq._channel = self.rabbitmq._connection.channel()
                    except Exception as channel_error:
                        logger.error(f"Failed to create new channel: {channel_error}")
                
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                    
                logger.error(f"Queue check failed after {max_retries} attempts: {last_error}")
                raise Exception(f"Failed to check queue {self.queue_name} - please verify RabbitMQ connection")

    
    def _handle_message(self, channel, method, properties, body):
        """Wrapper for message processing with enhanced error handling"""
        message_id = None

        try:
            # Extract message ID early for error tracking
            try:
                message_data = json.loads(body.decode())
                message_id = message_data.get("slug_id")
            except Exception as parse_error:
                logger.error(f"Failed to parse message for ID extraction: {parse_error}")

            # Track message processing activity
            self._last_message_time = time.time()

            # Process the message
            process_result = self.process_message(body)

            if process_result is True:
                # Message processed successfully
                try:
                    channel.basic_ack(method.delivery_tag)
                    logger.info(f"Message {message_id or 'unknown'} processed and acknowledged")
                except Exception as ack_error:
                    logger.error(f"Failed to acknowledge message: {ack_error}")
                    # Don't raise here - message was processed successfully

            elif process_result is False:
                # Message needs to be requeued (e.g. rate limit)
                try:
                    channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
                    logger.info(f"Message {message_id or 'unknown'} rejected and requeued for later processing")
                except Exception as reject_error:
                    logger.error(f"Failed to reject/requeue message: {reject_error}")
                    # Don't raise here - we want to continue processing

                # Add delay before processing next message to prevent tight loop
                time.sleep(1)

            else:
                # Unexpected return value
                logger.error(f"Unexpected process_message result: {process_result}")
                try:
                    channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                    logger.info(f"Message {message_id or 'unknown'} rejected due to unexpected result")
                except Exception as reject_error:
                    logger.error(f"Failed to reject message: {reject_error}")

        except Exception as e:
            logger.error(f"Critical error in message handler: {e}")
            logger.error(f"Error type: {type(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

            # Try to reject the message without requeueing to prevent infinite loops
            try:
                if hasattr(method, 'delivery_tag'):
                    channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                    logger.info(f"Message {message_id or 'unknown'} rejected due to critical error")
            except Exception as final_error:
                logger.error(f"Failed to reject message after critical error: {final_error}")
                # At this point, we can't do much more — the message will be redelivered
                # after the channel is closed and reopened

            # Don't raise the exception here — we want to continue processing other messages

    def _check_provider_rate_limit(self, provider_name):
        """Check if provider is currently rate limited"""
        current_time = time.time()
        with self.rate_limit_lock:
            if provider_name in self.provider_rate_limits:
                reset_time = self.provider_rate_limits[provider_name]
                if current_time < reset_time:
                    wait_time = reset_time - current_time
                    logger.warning(f"""
RATE LIMIT ACTIVE - Pausing processing
Provider: {provider_name}
Rate limit active for: {wait_time:.1f} more seconds
Rate limit resets at: {datetime.fromtimestamp(reset_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC
Status: Pausing message processing until rate limit resets
                """.strip())
                    # Sleep for the remaining time or a maximum of 60 seconds
                    sleep_time = min(wait_time, 60)
                    time.sleep(sleep_time)
                    return wait_time
                else:
                    # Rate limit expired, remove it
                    del self.provider_rate_limits[provider_name]
                    logger.info(f"""
RATE LIMIT RESET
Provider: {provider_name}
Status: Rate limit has reset, resuming message processing
                """.strip())
        return 0

    def _set_provider_rate_limit(self, provider_name, reset_seconds):
        """Set rate limit for provider"""
        reset_time = time.time() + reset_seconds
        with self.rate_limit_lock:
            self.provider_rate_limits[provider_name] = reset_time
            logger.warning(f"""
RATE LIMIT ACTIVATED
Provider: {provider_name}
Rate limit period: {reset_seconds} seconds
Rate limit will reset at: {datetime.fromtimestamp(reset_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC
Status: No messages will be processed until rate limit resets
        """.strip())

    def process_message(self, message_body):
        """Process a single message from the queue with improved error handling"""
        message_id = None
        
        try:
            # Parse message data
            message_data = json.loads(message_body.decode())
            message_id = message_data.get("slug_id")
            provider_name = message_data.get("provider")

            if not message_id:
                error_msg = "Message missing required slug_id field"
                logger.error(error_msg)
                return True  # Don't requeue invalid messages

            logger.info(f"Processing message {message_id} from {provider_name}")

            # Check if this provider is currently rate limited
            wait_time = self._check_provider_rate_limit(provider_name)
            if wait_time > 0:
                # Update message status to reflect rate limit
                self._safe_update_message_status(
                    message_id, 
                    "pending", 
                    f"Rate limit active. Will retry after {wait_time:.1f} seconds"
                )
                return False  # Requeue the message

            # Process message with database context and retry logic
            max_db_attempts = 3
            for db_attempt in range(max_db_attempts):
                try:
                    with self.app.app_context():
                        # Find message by slug_id
                        message = Message.query.filter_by(slug_id=message_id).first()
                        
                        if not message:
                            error_msg = f"Message {message_id} not found in database - skipping"
                            logger.error(error_msg)
                            return True  # Don't requeue if message doesn't exist

                        # Get provider key
                        provider_key = ProviderKey.query.filter_by(
                            workspace_id=self.workspace_db_id,
                            name=provider_name
                        ).first()

                        if not provider_key:
                            error_msg = f"No API key found for provider {provider_name}"
                            logger.error(error_msg)
                            self._store_message_results(message_id, {
                                "success": False,
                                "error": error_msg,
                                "error_type": "PROVIDER_KEY_NOT_FOUND"
                            })
                            self._safe_update_message_status(message_id, "failed", error_msg)
                            return True  # Don't requeue if no API key

                        # Process the message
                        return self._process_message_with_provider(message, provider_key, message_data, message_id)
                        
                except Exception as db_error:
                    logger.error(f"Database error on attempt {db_attempt + 1}: {db_error}")
                    if db_attempt < max_db_attempts - 1:
                        time.sleep(1)
                        try:
                            db.session.rollback()
                        except:
                            pass
                        continue
                    else:
                        # Final attempt failed
                        logger.error(f"Database operations failed after {max_db_attempts} attempts")
                        if message_id:
                            self._safe_update_message_status(message_id, "failed", f"Database error: {db_error}")
                        return True  # Don't requeue on persistent DB errors

        except json.JSONDecodeError as json_error:
            logger.error(f"Invalid JSON in message: {json_error}")
            return True  # Don't requeue invalid JSON
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            if message_id:
                self._safe_update_message_status(message_id, "failed", f"Parse error: {e}")
            return True  # Don't requeue on parse error

    def _process_message_with_provider(self, message, provider_key, message_data, message_id):
        """Process message with provider with improved error handling"""
        try:
            # Calculate token count
            system_prompt = message_data.get("system_prompt", "")
            prompt = message_data.get("prompt", "")
            content = message_data.get("content", "")
            total_text = f"{system_prompt}\n{prompt}\n{content}"
            estimated_tokens = len(total_text.split())

            # Check rate limit using the API key manager
            allowed, current_usage, limit, reset_time = self.api_key_manager.rate_limiter.check_rate_limit(
                self.workerspace_id,
                provider_key.name,
                provider_key.id,
                estimated_tokens
            )

            logger.info(f"Rate limit check: allowed={allowed}, usage={current_usage}, limit={limit}")

            if not allowed:
                logger.warning(f"Rate limit exceeded for provider {provider_key.name}")
                self._set_provider_rate_limit(provider_key.name, reset_time)
                self._safe_update_message_status(
                    message_id,
                    MessageStatus.PENDING.value,
                    f"Rate limit exceeded. Current usage: {current_usage}/{limit} tokens. Will retry in {reset_time} seconds"
                )
                return False  # Requeue this message
            
            # Mark message as processing BEFORE making API call
            message.status = MessageStatus.PROCESSING.value
            message.token_count = estimated_tokens
            db.session.commit()

            # Process with provider
            result = self._process_with_provider(provider_key, message_data, message_id)

            if result:
                # Get actual token usage
                actual_tokens = self._extract_token_count(result, estimated_tokens)
                
                # Record actual token usage
                self.api_key_manager.record_usage(
                    provider_key_id=provider_key.id,
                    token_count=actual_tokens
                )

                # Update token count with actual usage
                message.token_count = actual_tokens
                db.session.commit()

                # Update status to SUCCESS first
                self._safe_update_message_status(message_id, MessageStatus.SUCCESS.value)
                
                # Store results
                self._store_message_results(message_id, {
                    "success": True,
                    "result": result,
                    "token_count": actual_tokens,
                    "rate_limit_info": {
                        "current_usage": current_usage + actual_tokens,
                        "limit": limit,
                        "reset_in": reset_time
                    }
                })
                
                logger.info(f"Message {message_id} processed successfully with {actual_tokens} tokens")
                return True
            else:
                error_msg = "No result from provider"
                logger.error(f"Provider returned no result for message {message_id}")
                self._store_message_results(message_id, {
                    "success": False,
                    "error": error_msg,
                    "error_type": "NO_RESULT"
                })
                self._safe_update_message_status(message_id, "failed", error_msg)
                return True

        except Exception as e:
            error_msg = f"Error in provider processing: {e}"
            logger.error(error_msg)
            self._safe_update_message_status(message_id, "failed", str(e))
            return True

    def _extract_token_count(self, result, estimated_tokens):
        """Extract token count from API result"""
        try:
            if result and isinstance(result, dict):
                tokens_info = result.get('tokens', {})
                if isinstance(tokens_info, dict):
                    return tokens_info.get('total_tokens', estimated_tokens)
            return estimated_tokens
        except Exception as e:
            logger.warning(f"Error extracting token count: {e}")
            return estimated_tokens

    def _safe_update_message_status(self, message_id, status, error=None):
        """Safely update message status with retry logic"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                with self.app.app_context():
                    message = Message.query.filter_by(slug_id=message_id).first()
                    
                    if not message:
                        logger.error(f"Message with slug_id {message_id} not found")
                        return
                    
                    if error:
                        message.status = MessageStatus.FAILED.value
                        message.status_code = 500
                        message.result = {
                            "success": False,
                            "error": error,
                            "error_type": "PROCESSING_ERROR",
                            "status_code": 500
                        }
                    else:
                        # Set appropriate status codes
                        if status == "processing":
                            message.status = MessageStatus.PROCESSING.value
                            message.status_code = 102
                        elif status == "completed":
                            message.status = MessageStatus.SUCCESS.value
                            message.status_code = 200
                        elif status == "failed":
                            message.status = MessageStatus.FAILED.value
                            message.status_code = 500
                        elif status == "pending":
                            message.status = MessageStatus.PENDING.value
                            message.status_code = 100
                        else:
                            message.status = status
                            message.status_code = 200
                    
                    db.session.commit()
                    return

            except Exception as e:
                logger.error(f"Status update attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(0.5)
                    try:
                        db.session.rollback()
                    except:
                        pass
                    continue
                else:
                    logger.error(f"Failed to update message status after {max_attempts} attempts")
                    try:
                        db.session.rollback()
                    except:
                        pass

    def _process_with_provider(self, provider_key, message_data, message_id):
        """Execute message processing with provider"""
        try:
            use_simulation = message_data.get("use_simulation", False)
            provider = ProviderFactory.create_provider(
                provider_key.name.split(':')[0],
                provider_key.api_key,
                provider_key.config or {},
                use_simulation=use_simulation
            )

            self._safe_update_message_status(message_id, "processing")

            logger.info(f"Sending request to {provider_key.name} API...")
            
            result = provider.process_message(
                message_data.get("system_prompt", ""),
                message_data.get("prompt", ""),
                message_data.get("content", ""),
                message_data.get("response_format"),
                message_data.get("model", "gpt-3.5-turbo")
            )

            # Add response logging
            logger.info(f"Received response from {provider_key.name} API")
            logger.debug(f"Raw response: {result}")

            if not result:
                raise Exception("Empty response from provider")

            return result

        except Exception as e:
            logger.error(f"API call failed: {str(e)}")
            logger.exception("Full API error traceback:")
            raise

    def _store_message_results(self, message_id, result):
        """Store message results with error handling and non-blocking API calls"""
        try:
            with self.app.app_context():
                message = Message.query.filter_by(slug_id=message_id).first()
                if not message:
                    logger.error(f"Message with slug_id {message_id} not found")
                    return

                message.result = result
            
                if result.get("success", False):
                    if "error" in result:
                        message.status = MessageStatus.FAILED.value
                        message.status_code = 500
                        message.result = {
                            "success": False,
                            "error": result.get("error"),
                            "error_type": "PROCESSING_ERROR",
                            "status_code": 500
                        }
                    else:
                        message.status = MessageStatus.SUCCESS.value
                        message.status_code = 200
                        if "result" in result and isinstance(result["result"], dict):
                            tokens_info = result["result"].get("tokens")
                            if tokens_info and isinstance(tokens_info, dict):
                                message.token_count = tokens_info.get("total_tokens", 1)
                else:
                    # If not successful, ensure we set a failure status code
                    message.status = MessageStatus.FAILED.value
                    message.status_code = result.get("status_code", 500)  # Default to 500 if no status code

                # Commit changes to database first
                db.session.commit()
                logger.info(f"Stored results for message {message_id}: status_code={message.status_code}")
                if message.message_field_type != "primary_keyword_generated_by_ai":
                    # Get processed message count for this article
                    article_message_count = Message.query.filter(
                        Message.article_id == message.article_id,
                        Message.message_field_type == message.message_field_type,
                        ~Message.status.in_([MessageStatus.PENDING.value, MessageStatus.PROCESSING.value])
                    ).count()
                    logger.info(f"Processed message count for article {message.article_id}: {article_message_count}")
                    # Send result to output queue regardless of success/failure
                    try:
                        output_payload = {
                            "message": {
                                "message_id": str(message_id),
                                "workspace_id": str(message.workspace_id),
                                "request": message.request,
                                "sequence_index": message.sequence_index,
                                "prompt": message.prompt,
                                "html_tag": message.html_tag,
                                "ai_response": message.result,
                                "ai_response_status": message.status.value if hasattr(message.status, 'value') else str(message.status),
                                "article_id": str(message.article_id),
                                "article_message_count": article_message_count,
                                "message_field_type": message.message_field_type,
                                "message_priority": message.message_priority,
                                "article_message_total_count": message.article_message_total_count,
                                "status_code": message.status_code
                            }
                        }
                        send_response_url = f"{ARTICLE_QUEUE_SERIVCE_BASE_URL}/queue/publish/article-content-response-queue"
                        logger.debug(f"Sending response to output queue: {send_response_url}")
                        # Set a 60 second timeout for the API call
                        response = requests.post(
                            send_response_url,
                            json=output_payload,
                            headers={"Content-Type": "application/json"},
                            timeout=60
                        )
                        logger.debug(response,'response')
                        
                        # Log the actual response for debugging
                        logger.debug(f"Raw response status: {response.status_code}")
                        logger.debug(f"Raw response content: {response.text}")
                        
                        # Only try to parse JSON if we got a valid response
                        if response.text:
                            response_data = response.json()
                        else:
                            logger.error("Received empty response from API")
                            response_data = {}
                        
                        logger.info(f"Response from output queue: {response_data}")

                        if response.status_code != 200:
                            error_data = response.json() if response.text else {}
                            if isinstance(error_data, dict) and "No workers" in str(error_data.get("error", "")):
                                logger.warning("No worker available, attempting to scale up...")
                                # Try to scale up worker using the workspace ID
                                scale_result = self.scale_worker(str(self.workspace_db_id))
                                
                                if scale_result:
                                    logger.info("Successfully initiated worker scale up, retrying send...")
                                    # Retry the request after scaling
                                    retry_response = requests.post(
                                        send_response_url,
                                        json=output_payload,
                                        headers={"Content-Type": "application/json"},
                                        timeout=60
                                    )
                                    if retry_response.status_code != 200:
                                        logger.warning("Retry failed after scaling")
                                    else:
                                        logger.info("Successfully sent result after scaling")
                                else:
                                    logger.warning("Failed to scale worker")
                            else:
                                logger.warning(f"Failed to send result to output queue: {response.status_code}")
                        else:
                            logger.info(f"Successfully sent result to output queue for message {message_id}")
                    
                    except Exception as api_error:
                        logger.warning(f"Error sending to output queue: {api_error}")
                
        except Exception as e:
            logger.error(f"Failed to store results: {e}")
            try:
                db.session.rollback()
            except:
                pass



    def report_token_usage(self):
        """Report current token usage metrics"""
        try:
            with self.app.app_context():
                metrics = self.api_key_manager.get_usage_metrics(self.workspace_db_id)
                total_used = metrics["total_used"]
                
                if total_used > 0:
                    logger.info(
                        f"Token usage: {total_used}/{metrics['total_capacity']} tokens "
                        f"({metrics['usage_percent']:.1f}% used)"
                    )
        except Exception as e:
            logger.error(f"Error reporting token usage: {e}")

    def _health_check_loop(self):
        """Health monitoring loop"""
        while self.active:
            try:
                with self.app.app_context():
                    self._update_health_status()
            except Exception as e:
                logger.error(f"Health check error: {e}")
            time.sleep(self.health_check_interval)

    def stop(self):
        """Stop the worker processing loop with proper cleanup"""
        if not self.is_running:
            logger.info("Worker is already stopped")
            return
            
        logger.info(f"Stopping worker {self.worker_number}")
        self.active = False
        
        # Set stop event for Windows
        if self.is_windows and hasattr(self, 'stop_event'):
            self.stop_event.set()
        
        try:
            # Clean up consumer first
            self._cleanup_consumer()
            
            # Clean up direct connection
            self._cleanup_connection()
            
            # Update worker status in database with retry
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    with self.app.app_context():
                        worker = WorkerModel.query.filter_by(
                            workspace_id=self.workspace_db_id,
                            pid=os.getpid()
                        ).first()
                        
                        if worker:
                            worker.status = 'stopped'
                            worker.last_heartbeat = datetime.now(timezone.utc)
                            db.session.commit()
                            logger.info(f"Worker {worker.id} status updated to stopped")
                            break
                except Exception as db_error:
                    logger.error(f"Database update attempt {attempt + 1} failed: {db_error}")
                    if attempt < max_attempts - 1:
                        time.sleep(1)
                        try:
                            db.session.rollback()
                        except:
                            pass
                        continue
                    else:
                        logger.error("Failed to update worker status in database")
            
            # Shutdown thread pool
            if hasattr(self, 'thread_pool'):
                try:
                    self.thread_pool.shutdown(wait=True, timeout=5)
                except Exception as pool_error:
                    logger.error(f"Error shutting down thread pool: {pool_error}")
            
            # Wait for monitor thread to finish
            if hasattr(self, 'health_thread'):
                try:
                    self.health_thread.join(timeout=5)
                except Exception as thread_error:
                    logger.error(f"Error joining health thread: {thread_error}")
            
            # Wait for monitoring thread to finish
            if hasattr(self, 'monitoring_thread'):
                try:
                    self.monitoring_thread.join(timeout=5)
                    logger.info("Monitoring thread stopped")
                except Exception as thread_error:
                    logger.error(f"Error joining monitoring thread: {thread_error}")
            
        except Exception as e:
            logger.error(f"Error during worker shutdown: {e}")
        finally:
            self.is_running = False
            logger.info(f"Worker {self.worker_number} stopped successfully")

    def scale_worker(self, worker_id, count=1):
        """Scale up worker instances with timeout and retries"""
        max_retries = 3  # Increased from 1
        base_timeout = 30

        for attempt in range(max_retries):
            try:
                scale_url = f"{ARTICLE_QUEUE_SERIVCE_BASE_URL}/worker/scale/article-content-response-queue"
                logger.info(f"scale_url: {scale_url}")

                # Get worker name from current instance
                worker_name = "url_rewriter_content_response_worker"

                scale_data = {
                    "count": count,
                    "worker_name": worker_name
                }
                headers = {'Content-Type': 'application/json'}

                # Increase timeout with each retry
                timeout = base_timeout * (attempt + 1)
                logger.info(f"Attempting to scale worker {worker_name} (attempt {attempt + 1}/{max_retries}, timeout: {timeout}s)")

                response = requests.post(scale_url, json=scale_data, headers=headers, timeout=timeout)

                if response.status_code == 200:
                    logger.info(f"Successfully scaled worker {worker_name} to {count} instances")

                    # Verify consumer setup after scaling
                    verify_timeout = time.time() + 30  # 30 seconds timeout
                    while time.time() < verify_timeout:
                        try:
                            result = self._channel.queue_declare(
                                queue=self.queue_name,
                                passive=True
                            )
                            consumer_count = result.method.consumer_count
                            logger.info(f"Current consumer count: {consumer_count}")

                            if consumer_count >= count:
                                logger.info(f"Verified {consumer_count} consumers are active")
                                return True

                            time.sleep(2)  # Wait before next check

                        except Exception as e:
                            logger.warning(f"Consumer verification error: {e}")
                            time.sleep(2)
                            continue

                    logger.warning(f"Timed out waiting for consumers to become active")
                    return False

                else:
                    logger.error(f"Failed to scale worker: {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))  # Exponential backoff
                        continue
                    return False

            except requests.exceptions.Timeout:
                logger.error(f"Timeout scaling worker (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    continue
                return False

            except Exception as e:
                logger.error(f"Error scaling worker: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return False

        return False


    def __del__(self):
        """Destructor to ensure cleanup on garbage collection"""
        try:
            if hasattr(self, 'is_running') and self.is_running:
                self.stop()
        except Exception as e:
            # Don't raise exceptions in destructor
            pass