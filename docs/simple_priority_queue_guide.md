# Quick Guide: Priority Queue Implementation

## 1. Creating a Priority Queue

RabbitMQ priority queue setup with real code example:

```python
# From app/workers/workspace_worker.py
def _initialize_rabbitmq(self):
    # Create a new channel
    self._channel = self._connection.channel()
    self._channel.basic_qos(
        prefetch_count=1,  # Process one message at a time
        global_qos=False   # Apply per-consumer
    )

    # Declare main queue with priority support
    self._channel.queue_declare(
        queue=self.queue_name,
        durable=True,
        arguments={
            'x-max-priority': 10  # Enable priority queue with 10 levels
        }
    )
```

Key points:

- Priority levels: 1-10 (1 is highest priority)
- Queue is durable (survives broker restarts)
- Uses APISIX for rate limiting

## 2. Publishing Messages to Priority Queue

Real code example for publishing messages with priority:

```python
# From app/workers/rate_limiter.py
def publish_message(self, queue_name, message, priority=5):
    """
    Publish a message to RabbitMQ with priority
    Args:
        queue_name: Name of the queue
        message: Message content (will be JSON serialized)
        priority: Message priority (1-10, default 5)
    """
    try:
        channel = self.get_connection().channel()

        # Publish message with priority
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                priority=priority # Set message priority
            )
        )

    except Exception as e:
        logger.error(f"Error publishing message: {e}")
        raise

# Example usage:
message = {
    "content": "Your message content",
    "workspace_id": workspace_id,
    "provider": "openai",
    "estimated_tokens": token_count
}
publish_message(queue_name="your_queue", message=message, priority=1)  # High priority
```

## 3. Scaling Workers

Real code example for worker implementation and scaling:

```python
# From app/workers/base.py
class BaseWorker(ABC):
    def __init__(self, channel, queue_name):
        self.queue_name = queue_name
        self.pid = os.getpid()
        self.worker_id = f"{self.pid}_{int(time.time())}"
        self.is_running = True
        self.reconnect_delay = 5
        self.max_retries = 5
        self.processing_lock = threading.Lock()

        # RabbitMQ connection parameters
        self.connection_params = pika.ConnectionParameters(
            host=os.getenv('RABBITMQ_HOST', 'localhost'),
            credentials=pika.PlainCredentials(
                os.getenv('RABBITMQ_USERNAME', 'guest'),
                os.getenv('RABBITMQ_PASSWORD', 'guest')
            ),
            heartbeat=300,
            blocked_connection_timeout=300
        )

    def connect(self):
        """Connect with retry logic"""
        for attempt in range(1, self.max_retries + 1):
            try:
                conn = pika.BlockingConnection(self.connection_params)
                ch = conn.channel()

                # Set QoS for better load distribution
                ch.basic_qos(prefetch_count=1)

                self.connection, self.channel = conn, ch
                return True

            except Exception as e:
                logger.error(f"Connect attempt {attempt} failed: {e}")
                time.sleep(self.reconnect_delay)
        return False

# Example of running multiple workers:
def start_workers(num_workers=3):
    """Start multiple worker processes"""
    workers = []
    for i in range(num_workers):
        worker = BaseWorker(channel=None, queue_name='your_queue')
        process = multiprocessing.Process(
            target=worker.start,
            name=f'worker_{i}'
        )
        process.start()
        workers.append(process)
    return workers

# Start workers
if __name__ == '__main__':
    NUM_WORKERS = 3  # Adjust based on your needs
    workers = start_workers(NUM_WORKERS)
```

### Scaling Tips:

1. **Number of Workers**:

   - Start with `NUM_WORKERS = CPU_CORES`
   - Monitor queue length and adjust workers accordingly
   - Each worker processes one message at a time (prefetch_count=1)

2. **Worker Health**:

   - Workers automatically reconnect on failure
   - Each worker has unique ID for tracking
   - Use processing_lock for thread safety

3. **Load Balancing**:
   - RabbitMQ handles worker distribution
   - Priority messages get processed first
   - Use basic_qos to prevent worker overload
