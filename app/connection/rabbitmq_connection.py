# app/utils/rabbitmq_connection.py

import traceback
import pika
import os
import time
import requests
import json
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from loguru import logger
from app.config.config import RABBITMQ_API_PORT, RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_USERNAME, RABBITMQ_PORT, RABBITMQ_VHOST
import threading


class RabbitMQConnection:
    _instance = None
    _connection = None
    _channel = None
    _lock = threading.Lock()

    def __init__(self, connection_params=None):
        logger.info("Initializing RabbitMQ Connection")
        if connection_params is None:
            connection_params = self._get_connection_params()
        self._connect(connection_params)

    def _get_connection_params(self):
        return pika.ConnectionParameters(
            host=os.getenv('RABBITMQ_HOST', 'localhost'),
            port=int(os.getenv('RABBITMQ_PORT', 5672)),
            virtual_host=os.getenv('RABBITMQ_VHOST', '/'),
            credentials=pika.PlainCredentials(
                username=os.getenv('RABBITMQ_USERNAME', 'guest'),
                password=os.getenv('RABBITMQ_PASSWORD', 'guest')
            ),
            connection_attempts=5,
            retry_delay=3,
            heartbeat=60,
            blocked_connection_timeout=300,
            socket_timeout=10
        )

    def _connect(self, connection_params):
        max_retries = 5
        retry_delay = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{max_retries})")
                
                # Close existing connection if any
                if self._connection and self._connection.is_open:
                    try:
                        self._connection.close()
                    except:
                        pass
                
                self._connection = pika.BlockingConnection(connection_params)
                self._channel = self._connection.channel()
                logger.info("RabbitMQ connection established successfully")
                return
                
            except Exception as e:
                logger.error(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)  # Exponential backoff
                else:
                    logger.error("All RabbitMQ connection attempts failed")
                    raise Exception(f"Failed to connect to RabbitMQ after {max_retries} attempts")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new RabbitMQ instance")
                    try:
                        cls._instance = cls()
                    except Exception as e:
                        logger.error(f"Failed to create RabbitMQ instance: {e}")
                        raise
        else:
            # Check if existing instance is still healthy
            if not cls._instance.is_healthy():
                logger.warning("Existing RabbitMQ instance unhealthy, recreating...")
                with cls._lock:
                    try:
                        cls._instance = cls()
                    except Exception as e:
                        logger.error(f"Failed to recreate RabbitMQ instance: {e}")
                        raise
        
        return cls._instance

    def is_healthy(self):
        """Check if connection is healthy"""
        try:
            return (self._connection and 
                   self._connection.is_open and 
                   self._channel and 
                   self._channel.is_open)
        except:
            return False

    def get_channel(self):
        """Ensure the channel is open and return it with improved error handling."""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                if self._channel is None or self._channel.is_closed:
                    logger.warning(f"Channel is closed or None, creating new channel (attempt {attempt + 1})")
                    
                    if self._connection is None or not self._connection.is_open:
                        logger.warning("Connection is closed or None, reconnecting...")
                        self._connect(self._get_connection_params())
                    
                    self._channel = self._connection.channel()
                    logger.info("New channel created successfully")
                
                return self._channel
                
            except Exception as e:
                logger.error(f"Channel creation attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(1)
                    try:
                        self._connect(self._get_connection_params())
                    except:
                        pass
                else:
                    logger.error("Failed to create channel after all attempts")
                    raise

    def purge_queue(self, queue_name: str):
        try:
            channel = self.get_channel()
            channel.queue_purge(queue=queue_name)
            logger.info(f"Queue {queue_name} purged successfully")
        except Exception as e:
            logger.error(f"Error purging queue {queue_name}: {e}")
            raise

    def create_queue(self, queue_name):
        try:
            logger.info(f"Attempting to create queue: {queue_name}")
            channel = self.get_channel()
            
            # Create main queue with priority support
            channel.queue_declare(
                queue=queue_name, 
                durable=True,
                arguments={
                    'x-max-priority': 10  # Enable priority queue support
                }
            )
            logger.info(f"Queue {queue_name} created successfully with priority support")

        except pika.exceptions.ChannelClosedByBroker as e:
            if "inequivalent arg 'x-max-priority'" in str(e):
                logger.error(f"Queue {queue_name} already exists with different priority settings")
                raise Exception(f"Queue {queue_name} exists but with different priority settings. Delete and recreate to enable priorities.")
            raise
        except Exception as e:
            logger.error(f"Queue creation error: {str(e)}")
            raise

    def get_queue_info(self, queue_name: Optional[str] = None) -> Dict[str, Any]:
        try:
            import urllib.parse
            encoded_vhost = urllib.parse.quote(RABBITMQ_VHOST, safe='')
            base_url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api"
            url = f"{base_url}/queues/{encoded_vhost}/{queue_name}" if queue_name else f"{base_url}/queues/{encoded_vhost}"
            response = requests.get(url, auth=(RABBITMQ_USERNAME, RABBITMQ_PASSWORD), timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get queue info: {response.status_code} - {response.text}")
                return {"error": f"Failed to get queue info: {response.status_code}"}
        except Exception as e:
            logger.error(f"Error getting queue information: {str(e)}")
            return {"error": str(e)}

    def get_queue_stats(self, workspace_id: str = None) -> Dict[str, Any]:
        try:
            queue_info = self.get_queue_info()
            if isinstance(queue_info, dict) and "error" in queue_info:
                return queue_info

            filtered_queues = [q for q in queue_info if workspace_id in q.get('name', '')] if workspace_id else queue_info

            result = {
                "total_queues": len(filtered_queues),
                "total_messages": sum(q.get('messages', 0) for q in filtered_queues),
                "total_consumers": sum(q.get('consumers', 0) for q in filtered_queues),
                "queues": []
            }

            for queue in filtered_queues:
                result["queues"].append({
                    "name": queue.get('name'),
                    "messages": queue.get('messages', 0),
                    "messages_ready": queue.get('messages_ready', 0),
                    "messages_unacknowledged": queue.get('messages_unacknowledged', 0),
                    "consumers": queue.get('consumers', 0),
                    "state": queue.get('state', 'unknown'),
                    "message_stats": queue.get('message_stats', {})
                })

            return result
        except Exception as e:
            logger.error(f"Error getting queue statistics: {str(e)}")
            return {"error": str(e)}

    def close_connection(self):
        if RabbitMQConnection._connection and RabbitMQConnection._connection.is_open:
            RabbitMQConnection._connection.close()
            print("Connection closed.")
