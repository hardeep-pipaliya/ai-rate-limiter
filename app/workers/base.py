from abc import ABC, abstractmethod
import threading
import os
import pika
import time
from datetime import datetime
from app.config.logger import LoggerSetup

class BaseWorker(ABC):
    def __init__(self, channel, queue_name):
        # Determine worker name
        self.worker_name = getattr(self.__class__, 'WORKER_NAME',
                                 self.__class__.__name__.lower())
        self.queue_name = queue_name
        self.pid = os.getpid()
        self.worker_id = f"{self.pid}_{int(time.time())}"
        self.is_running = True
        self.reconnect_delay = 5    # seconds between reconnect attempts
        self.max_retries = 5
        self.processing_lock = threading.Lock()

        # Initialize logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_worker_logger(self.pid)
        self.logger = self.logger.bind(
            worker_name=self.worker_name,
            queue=self.queue_name,
            worker_id=self.worker_id,
            pid=self.pid
        )

        # RabbitMQ connection parameters
        self.connection_params = pika.ConnectionParameters(
            host=os.getenv('RABBITMQ_HOST', 'localhost'),
            credentials=pika.PlainCredentials(
                os.getenv('RABBITMQ_USERNAME', 'guest'),
                os.getenv('RABBITMQ_PASSWORD', 'guest')
            ),
            heartbeat=300,
            blocked_connection_timeout=300,
            connection_attempts=3,
            retry_delay=5,
            socket_timeout=300,
            virtual_host=os.getenv('RABBITMQ_VHOST', '/')
        )

        # Placeholders
        self.connection = None
        self.channel = None

        # Try initial connect
        if not self.connect():
            self.logger.error("Initial connection failed; will retry on start()")

    def connect(self):
        """Try to connect with retry."""
        for attempt in range(1, self.max_retries + 1):
            try:
                # Close existing connections if any
                if self.channel and self.channel.is_open:
                    try:
                        self.channel.close()
                    except:
                        pass
                if self.connection and self.connection.is_open:
                    try:
                        self.connection.close()
                    except:
                        pass

                # Create new connection with explicit port
                self.logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt})")
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    host=os.getenv('RABBITMQ_HOST', 'localhost'),
                    port=int(os.getenv('RABBITMQ_PORT', '5672')),  # Explicit port
                    credentials=pika.PlainCredentials(
                        os.getenv('RABBITMQ_USERNAME', 'guest'),
                        os.getenv('RABBITMQ_PASSWORD', 'guest')
                    ),
                    heartbeat=300,
                    blocked_connection_timeout=300,
                    connection_attempts=3,
                    retry_delay=5,
                    socket_timeout=300,
                    virtual_host=os.getenv('RABBITMQ_VHOST', '/')
                ))
                
                ch = conn.channel()
                
                # Declare queue with arguments
                self.logger.info(f"Declaring queue: {self.queue_name}")
                self.channel.queue_declare(
                    queue=self.queue_name,
                    durable=True,
                    arguments={
                        'x-message-ttl': 86400000,  # 24 hours in milliseconds
                        'x-max-length': 10000       # Maximum number of messages
                    }
                )
                
                # Set QoS
                ch.basic_qos(prefetch_count=1)
                
                self.connection, self.channel = conn, ch
                self.logger.info(f"Connected (attempt {attempt}) to '{self.queue_name}' with {result.method.consumer_count} consumers")
                
                # Verify queue exists
                try:
                    queue_info = ch.queue_declare(queue=self.queue_name, passive=True)
                    self.logger.info(f"Queue verification: {queue_info.method.message_count} messages, {queue_info.method.consumer_count} consumers")
                except Exception as e:
                    self.logger.error(f"Queue verification failed: {e}")
                    raise
                
                return True
                
            except Exception as e:
                self.logger.error(f"Connect attempt {attempt} failed: {e}")
                time.sleep(self.reconnect_delay)
        return False

    def _consume_wrapper(self):
        """Main consume loop, auto-reconnect + re-register."""
        consumer_registered = False
        while self.is_running:
            # Reconnect if broken
            if not (self.connection and self.connection.is_open
                    and self.channel and self.channel.is_open):
                self.logger.warning("Connection lost; reconnecting...")
                try:
                    if self.channel and self.channel.is_open:
                        self.channel.close()
                    if self.connection and self.connection.is_open:
                        self.connection.close()
                except:
                    pass

                if not self.connect():
                    self.logger.error("Reconnect failed; will retry soon")
                    time.sleep(self.reconnect_delay)
                    continue
                consumer_registered = False

            # Register consumer once per good connection
            if not consumer_registered:
                try:
                    self.channel.basic_consume(
                        queue=self.queue_name,
                        on_message_callback=self._handle_callback,
                        auto_ack=False
                    )
                    self.logger.info(f"Consumer registered on '{self.queue_name}'")
                    consumer_registered = True
                except Exception as e:
                    self.logger.error(f"Consumer registration error: {e}")
                    time.sleep(self.reconnect_delay)
                    continue

            # Start consuming (this blocks until connection/channel closes)
            try:
                self.logger.info(f"Starting consumption on '{self.queue_name}'")
                self.channel.start_consuming()
            except (pika.exceptions.ConnectionClosed,
                    pika.exceptions.ChannelClosedByBroker,
                    pika.exceptions.ChannelWrongStateError) as e:
                self.logger.warning(f"Consume error: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected consumption error: {e}")
            finally:
                # Cleanup & prepare to reconnect
                consumer_registered = False
                try:
                    if self.channel and self.channel.is_open:
                        self.channel.close()
                    if self.connection and self.connection.is_open:
                        self.connection.close()
                except Exception as cleanup_err:
                    self.logger.error(f"Cleanup error: {cleanup_err}")
                time.sleep(self.reconnect_delay)

    def start(self):
        """Launch the consumer thread and its internal monitor thread."""
        # Start consumer thread
        self.thread = threading.Thread(
            target=self._consume_wrapper,
            daemon=False,
            name=f"consumer-{self.worker_name}-{self.queue_name}"
        )
        self.thread.start()

        # Internal monitor to detect thread death
        def _self_monitor():
            while True:
                time.sleep(1)
                if not self.thread.is_alive():
                    self.logger.error("Consumer thread died; shutting down worker")
                    # Gracefully stop
                    self.is_running = False
                    try:
                        if self.channel and self.channel.is_open:
                            self.channel.close()
                        if self.connection and self.connection.is_open:
                            self.connection.close()
                    except:
                        pass
                    break

        threading.Thread(target=_self_monitor, daemon=True).start()

        # Wait for consumer to register
        max_wait = 10  # seconds
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if self.connection and self.connection.is_open and self.channel and self.channel.is_open:
                self.logger.info(f"Worker '{self.worker_name}' now consuming '{self.queue_name}'")
                return self
            time.sleep(0.5)
            
        if not self.thread.is_alive():
            raise RuntimeError("Failed to start consumer thread")
            
        return self

    def stop(self):
        """Stop consuming and close connections."""
        self.is_running = False
        if hasattr(self, 'thread') and self.thread.is_alive():
            self.thread.join(timeout=5)
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            self.logger.error(f"Error closing connection: {e}")

    def is_alive(self):
        return hasattr(self, 'thread') and self.thread.is_alive()

    def _handle_callback(self, ch, method, properties, body):
        """ACK/NACK wrapper around process_message."""
        try:
            self.logger.info("Message received; processing")
            success = self.process_message(ch, method, properties, body)
            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.logger.info("Message ACKed")
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                self.logger.info("Message NACKed (requeued)")
        except Exception as e:
            self.logger.error(f"Error in message callback: {e}")
            if ch.is_open:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    @abstractmethod
    def process_message(self, ch, method, properties, body):
        """
        Implement message processing.
        Return True (ACK) or False (NACK & requeue).
        """
        pass
