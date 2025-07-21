import time
import threading
import json
import pika
from datetime import datetime, timedelta
from sqlalchemy import func
from tenacity import retry, stop_after_attempt, wait_exponential, stop_after_delay, retry_if_result
from app.config.logger import LoggerSetup
from app.connection.orm_postgres_connection import db
from app.models.provider_keys import ProviderKey
from app.models.workspaces import WorkSpace
from app.utils.rate_limiter import rate_limiter as global_rate_limiter

# Initialize logger setup
log_setup = LoggerSetup()
logger = log_setup.setup_logger()

# Retry configuration for database operations
db_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)

# Retry configuration for RabbitMQ operations
mq_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    reraise=True
)

def mask_api_key(api_key):
    """Mask API key for logging purposes"""
    if not api_key or len(api_key) < 8:
        return "[MASKED]"
    return f"{api_key[:2]}...{api_key[-2:]}"


class RateLimiter:
    """Hybrid rate limiter using both PostgreSQL"""
    
    def __init__(self):
        self.lock = threading.Lock()
    
    @db_retry
    def check_rate_limit(self, workspace_id, provider, key_id, tokens=1):
        """
        Check rate limit using the global rate limiter
        Returns: (allowed, current_usage, limit, reset_time)
        """
        # Use the global  rate limiter
        allowed, current_usage, limit, reset_time = global_rate_limiter.check_rate_limit(
            workspace_id, provider, key_id, tokens
        )
        return allowed, current_usage, limit, reset_time
    
    @db_retry
    def get_rate_limit_status(self, workspace_id, provider, key_id):
        """
        Get rate limit status using the global rate limiter
        Returns dict with usage details
        """
        return global_rate_limiter.get_rate_limit_status(workspace_id, provider, key_id)
    
    @db_retry
    def record_usage(self, provider_key_id, token_count=1):
        """
        Record usage in both systems:
        1. PostgreSQL for persistent tracking
        """
        with self.lock:
            # Get the provider key to identify it in 
            provider_key = ProviderKey.query.get(provider_key_id)
            if not provider_key:
                logger.error(f"Provider key {provider_key_id} not found")
                return
            
            # Get workspace
            workspace = WorkSpace.query.get(provider_key.workspace_id)
            if not workspace:
                logger.error(f"Workspace {provider_key.workspace_id} not found")
                return
            
            # Record in global rate limiter
            global_rate_limiter.record_usage(provider_key_id, token_count)
            
            logger.info(f"""
Token Usage Updated:
Provider: {provider_key.name}
Key ID: {provider_key_id}
Added Tokens: {token_count}
            """.strip())

class ApiKeyManager:
    """Manages API keys with hybrid rate limiting"""
    
    def __init__(self):
        self.rate_limiter = RateLimiter()
        self.rabbitmq_connection = None
    
    @mq_retry
    def get_connection(self):
        """Get RabbitMQ connection with retries"""
        if not self.rabbitmq_connection or self.rabbitmq_connection.is_closed:
            self.rabbitmq_connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
        return self.rabbitmq_connection
    
    @db_retry
    def get_available_key(self, workspace_id, required_tokens=1):
        """
        Get an available API key using hybrid rate limiting
        """
        workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
        if not workspace:
            logger.error(f"Workspace {workspace_id} not found")
            return None
        
        keys = ProviderKey.query.filter_by(workspace_id=workspace.id).all()
        for key in keys:
            # Use the hybrid rate limiter
            allowed, _, _, _ = self.rate_limiter.check_rate_limit(
                workspace_id,
                key.name,
                key.key_id,
                required_tokens
            )
            if allowed:
                logger.info(f"Using key {key.id} ({mask_api_key(key.api_key)})")
                return key
        
        logger.warning(f"No API keys available for {required_tokens} tokens")
        return None
    
    @retry(stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=1, max=5),
           retry=retry_if_result(lambda result: result is None))
    def wait_for_key(self, workspace_id, required_tokens=1, timeout=60):
        """Wait for an available key using Tenacity"""
        return self.get_available_key(workspace_id, required_tokens)
    
    def record_usage(self, provider_key_id, token_count=1):
        """Record usage in both systems"""
        self.rate_limiter.record_usage(provider_key_id, token_count)
    
    @db_retry
    def get_usage_metrics(self, workspace_id):
        """
        Get combined usage metrics from both systems
        """
        workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
        if not workspace:
            return {
                'error': f"Workspace {workspace_id} not found",
                'total_used': 0,
                'total_capacity': 0,
                'usage_percent': 0,
                'keys': []
            }
        
        keys = ProviderKey.query.filter_by(workspace_id=workspace.id).all()
        metrics = {
            'total_used': 0,
            'total_capacity': 0,
            'keys': []
        }
        
        for key in keys:
            status = self.rate_limiter.get_rate_limit_status(
                workspace_id,
                key.name,
                key.key_id
            )
            
            metrics['total_used'] += status.get('usage', 0)
            metrics['total_capacity'] += status.get('limit', 0)
            
            metrics['keys'].append({
                'id': key.id,
                'name': key.name,
                'used': status.get('usage', 0),
                'limit': status.get('limit', 0),
                'period': status.get('period', 60),
                'remaining': status.get('remaining', 0),
                'reset_in': status.get('reset_in', 0)
            })
        
        if metrics['total_capacity'] > 0:
            metrics['usage_percent'] = (
                metrics['total_used'] / metrics['total_capacity'] * 100
            )
        else:
            metrics['usage_percent'] = 0
            
        return metrics
    
    @mq_retry
    def publish_message(self, queue_name, message):
        """Publish a message to RabbitMQ with retries"""
        try:
            connection = self.get_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            
            # Publish message to queue
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2  # make message persistent
                )
            )
            
            logger.info(f"Published message to {queue_name}")
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            raise

# Singleton instance
api_key_manager = ApiKeyManager()