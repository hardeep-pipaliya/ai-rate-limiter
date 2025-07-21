from flask import Blueprint, request, jsonify
import time
import json
import uuid
import pika
import psutil
import requests
from app.config.logger import LoggerSetup
from app.models.workspaces import WorkSpace
from app.models.messages import Message, MessageStatus
from app.utils.model_utils import get_provider_for_model
from app.connection.orm_postgres_connection import db
from app.config.config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_API_PORT
from datetime import datetime
RABBITMQ_AUTH = (RABBITMQ_USERNAME, RABBITMQ_PASSWORD)


logger = LoggerSetup().setup_worker_logger()

# Blueprint for message-related routes
message_bp = Blueprint('message', __name__, url_prefix='/message')

class MessageRoutes:
    @message_bp.route('/clean/', methods=['DELETE'])
    def clean_all_messages():
        """Delete all messages in the database"""
        try:
            message_count = Message.query.count()
            if message_count == 0:
                return jsonify({
                    "success": False,
                    "message": "No messages found in the database."
                }), 404
            try:
                Message.query.delete()
                db.session.commit()
                logger.info(f"Successfully deleted all ({message_count}) messages from the database.")
                return jsonify({
                    "success": True,
                    "message": f"Successfully deleted all ({message_count}) messages from the database.",
                    "deleted_count": message_count
                }), 200
            except Exception as db_error:
                db.session.rollback()
                logger.error(f"Database error while deleting all messages: {str(db_error)}")
                return jsonify({
                    "success": False,
                    "message": f"Database error while deleting all messages: {str(db_error)}"
                }), 500
        except Exception as e:
            logger.error(f"Error in clean_all_messages: {str(e)}")
            return jsonify({
                "success": False,
                "message": "An error occurred while deleting all messages",
                "error": str(e)
            }), 500
    @message_bp.route('/publish', methods=['POST'])
    def publish_message():
        """Submit a message for processing"""
        try:
            data = request.get_json()
            prompt = data.get('prompt')
            content = data.get('content', '')
            workspace_id = data.get('workspace_id')
            response_format = data.get('response_format')
            use_simulation = data.get('use_simulation', False)
            message_priority = data.get('message_priority', 0)  # Default priority is 0, RabbitMQ supports 0-10
            message_field_type = data.get('message_field_type', '')
            prompt= data.get('prompt', '')
            sequence_index = data.get('sequence_index', 0)
            model = data.get('model', '')
            system_prompt = data.get('system_prompt', '')
            sequence_index = data.get('sequence_index',0)
            html_tag = data.get('html_tag','')
            
            
            # Ensure message_priority is an integer and normalize to RabbitMQ limits (0-10)
            try:
                message_priority = int(message_priority)
                # Normalize any priority value to 0-10 range (higher input = higher priority)
                if message_priority < 0:
                    message_priority = 0
                elif message_priority <= 10:
                    # Keep values 0-10 as-is
                    pass
                else:
                    # For values > 10, use logarithmic scaling to ensure higher numbers get higher priority
                    import math
                    # Use log base 2 and scale to 1-10 range
                    log_priority = math.log2(message_priority)
                    # Map log values to 1-10 range
                    message_priority = min(10, max(1, int(log_priority)))
            except (TypeError, ValueError):
                logger.error(f"Invalid message_priority value: {message_priority}")
                return jsonify({
                    "success": False,
                    "message": "message_priority must be a valid integer"
                }), 400
            
            # Get article related data
            article_id = data.get('article_id')
            article_message_total_count = data.get('article_message_total_count', 0)
          
            
            if not prompt  or not workspace_id:
                logger.error("Missing required fields: prompt, content, and workspace_id are required")
                return jsonify({
                    "success": False, 
                    "message": "Missing required fields: prompt, content, and workspace_id are required"
                }), 400

            # Convert article_id to UUID if provided
            if article_id:
                try:
                    article_id = uuid.UUID(article_id)
                except ValueError:
                    logger.error(f"Invalid article_id format: {article_id}")
                    return jsonify({
                        "success": False,
                        "message": "Invalid article_id format. Must be a valid UUID."
                    }), 400

            workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
            if not workspace:
                return jsonify({
                    "success": False,
                    "message": f"Workspace {workspace_id} is not registered. Please register the workspace first.",
                    "registration_required": True
                }), 404

            # Generate a new slug_id as message_id
            if not data.get('message_id'):
                message_id = str(uuid.uuid4())
            else:
                message_id = data.get('message_id')
                
            # Convert message_id to UUID before querying
            try:
                message_uuid = uuid.UUID(message_id)
            except ValueError:
                logger.error(f"Invalid message_id format: {message_id}")
                return jsonify({
                    "success": False,
                    "message": "Invalid message_id format. Must be a valid UUID."
                }), 400
                
            # Check if message already exists in database
            existing_message = Message.query.filter_by(slug_id=message_uuid).first()
            is_retry = existing_message is not None

            try:
                provider, error = get_provider_for_model(model, workspace_id, db.session)
                if error:
                    logger.error(f"Error getting provider for model {model}: {error}")
                    return jsonify({"success": False, "message": error}), 400
            except Exception as e:
                logger.error(f"Error getting provider for model: {str(e)}")
                return jsonify({"success": False, "message": f"Error getting provider: {str(e)}"}), 500

            message_data = {
                "slug_id": message_id,
                "sequence_index": sequence_index,
                "workspace_id": workspace_id,
                "model": model,
                "provider": provider.name,
                "system_prompt": system_prompt,
                "prompt": prompt,
                "content": content,
                "html_tag": html_tag,
                "timestamp": time.time(),
                "response_format": response_format,
                "use_simulation": use_simulation,
                "is_retry": is_retry,
                "article_id": str(article_id) if article_id else None,
                "article_message_total_count": article_message_total_count,
                "message_field_type": message_field_type,
                "message_priority": message_priority  # Add message priority to message data
            }

            # Check if workers are available for the queue
            try:
                consumers_url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
                logger.info(f"Checking RabbitMQ consumers at: {consumers_url}")
                consumers_response = requests.get(consumers_url, auth=RABBITMQ_AUTH, timeout=30)
            
                if consumers_response.status_code != 200:
                    error_msg = f"Failed to fetch workers from RabbitMQ: {consumers_response.status_code}"
                    logger.error(error_msg)
                    return jsonify({"success": False, "message": error_msg}), consumers_response.status_code
            
                consumers = consumers_response.json()
                logger.info(f"RabbitMQ Consumers for workspace {workspace_id}: {consumers}")
            
                queue_workers = [consumer for consumer in consumers if consumer.get("queue", {}).get("name") == workspace_id]

                # Additional check for database workers
                from app.models.workers import Worker
                active_workers = Worker.query.filter_by(
                    workspace_id=workspace.id,
                    status='running'
                ).all()
            
                logger.info(f"Active workers in database for workspace {workspace_id}: {len(active_workers)}")
            
                # Verify if workers are actually running
                import psutil
                running_workers = [
                    worker for worker in active_workers 
                    if worker.pid and psutil.pid_exists(worker.pid)
                ]
            
                logger.info(f"Actually running workers for workspace {workspace_id}: {len(running_workers)}")
            
                # If no running workers found in either RabbitMQ or database, return error
                if not queue_workers and not running_workers:
                    return jsonify({
                        "success": False,
                        "message": f"No worker available for workspace {workspace_id}. Please scale a worker first.",
                        "worker_required": True
                    }), 404
            except Exception as e:
                logger.error(f"Error checking workers: {str(e)}")
                # Continue with message processing even if worker check fails
                logger.info("Continuing with message processing despite worker check failure")

            # Persist initial message
            try:
                if is_retry and existing_message:
                    # Update existing message for retry
                    existing_message.status = MessageStatus.PENDING.value
                    existing_message.request = json.dumps(message_data)
                    existing_message.result = json.dumps({})
                    existing_message.status_code = 100  # Set status code for pending
                    existing_message.updated_date = datetime.utcnow()
                    existing_message.article_id = article_id
                    existing_message.message_field_type = message_field_type
                    existing_message.message_priority = message_priority
                    existing_message.article_message_total_count = article_message_total_count
                    existing_message.sequence_index = sequence_index
                    existing_message.prompt = prompt
                    existing_message.html_tag = html_tag
                    db.session.commit()
                    logger.info(f"Message {message_id} updated for retry in database")
                else:
                    # Create new message
                    new_message = Message(
                        workspace_id=workspace.id,
                        provider_key_id=provider.id,
                        status=MessageStatus.PENDING.value,
                        message=content,
                        request=json.dumps(message_data),
                        result=json.dumps({}),
                        status_code=100,  # Set status code for pending
                        token_count=1,  # Default token count, can be updated later
                        slug_id=message_id,
                        article_id=article_id,
                        article_message_total_count=article_message_total_count,
                        message_field_type=message_field_type,
                        message_priority=message_priority,  # Set message priority in database
                        sequence_index=sequence_index,
                        prompt=prompt,
                        html_tag=html_tag
                    )
                    db.session.add(new_message)
                    db.session.commit()
                    logger.info(f"New message {message_id} persisted to database")
            except Exception as e:
                db.session.rollback()
                logger.error(f"Error persisting message to database: {str(e)}")
                return jsonify({"success": False, "message": f"Database error: {str(e)}"}), 500

            # Publish to RabbitMQ
            try:
                logger.info(f"Publishing message {message_id} to RabbitMQ queue {workspace_id}")
            
                # Use imported config values
                logger.info(f"RabbitMQ connection parameters: host={RABBITMQ_HOST}, port={RABBITMQ_PORT}")
            
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD),
                    connection_attempts=3,
                    retry_delay=2
                ))
                channel = connection.channel()

                queue_name = workspace_id
                
                # Check if queue exists - don't create if missing
                try:
                    # Passive declare only checks existence
                    channel.queue_declare(queue=queue_name, passive=True)
                    logger.info(f"Queue {queue_name} exists, proceeding with message publish")
                except pika.exceptions.ChannelClosedByBroker as e:
                    logger.error(f"Queue {queue_name} does not exist")
                    return jsonify({
                        "success": False,
                        "message": f"Queue {queue_name} does not exist. Please create the queue first.",
                        "queue_required": True
                    }), 404
                
                # Publish message to queue with priority
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=json.dumps(message_data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                        priority=message_priority  # Set message priority (0-10, validated above)
                    )
                )
                
                logger.info(f"Message {message_id} published")
            except pika.exceptions.ChannelClosedByBroker as e:
                import traceback
                logger.error(f"RabbitMQ channel error: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                
                error_msg = str(e)
                if "inequivalent arg 'x-max-priority'" in error_msg:
                    logger.error(f"Queue {queue_name} exists but doesn't support priorities: {error_msg}")
                    return jsonify({
                        "success": False, 
                        "message": "Queue exists but doesn't support priorities. Contact administrator to reconfigure the queue.",
                        "error_type": "QUEUE_CONFIG_ERROR"
                    }), 400
                elif "NOT_FOUND" in error_msg:
                    return jsonify({
                        "success": False,
                        "message": f"Queue {queue_name} not found. Please create the queue first.",
                        "queue_required": True
                    }), 404
                elif "ACCESS_REFUSED" in error_msg:
                    return jsonify({
                        "success": False,
                        "message": "Permission denied to access queue. Check your RabbitMQ credentials.",
                        "error_type": "PERMISSION_ERROR"
                    }), 403
                logger.error(f"Queue error: {error_msg}")
                return jsonify({
                    "success": False, 
                    "message": f"Queue error: {error_msg}",
                    "error_type": "QUEUE_ERROR"
                }), 400
                
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"RabbitMQ connection error: {str(e)}")
                return jsonify({
                    "success": False,
                    "message": "Failed to connect to message queue service. Please try again later.",
                    "error_type": "CONNECTION_ERROR"
                }), 503
                
            except Exception as e:
                import traceback
                logger.error(f"Error publishing message to RabbitMQ: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                return jsonify({
                    "success": False, 
                    "message": f"Unexpected error while publishing message: {str(e)}",
                    "error_type": "INTERNAL_ERROR"
                }), 500

            return jsonify({
                "success": True,
                "message": "Message queued for processing",
                "message_id": message_id,
                "workspace_id": workspace_id,
                "sequence_index": sequence_index,
                "provider": provider.name
            }), 201

        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Error in publish_message: {str(e)}")
            logger.error(f"Traceback: {error_trace}")
            return jsonify({"success": False, "message": f"Unexpected error: {str(e)}"}), 500

    @message_bp.route('/<message_id>', methods=['GET'])
    def get_message(message_id):
        """Fetch the stored message by its slug_id and return its details"""
        try:
            # Validate UUID format first
            try:
                # Remove any extra whitespace
                message_id = message_id.strip()
                uuid_obj = uuid.UUID(message_id)
            except ValueError:
                return jsonify({
                    "success": False, 
                    "error": "Invalid message ID format",
                }), 400

            msg = Message.query.filter_by(slug_id=str(uuid_obj)).first()
            if not msg:
                return jsonify({
                    "success": False, 
                    "message": f"Message with ID {message_id} was not found"
                }), 404

            # Convert status enum to its string value
            status_value = msg.status.value if isinstance(msg.status, MessageStatus) else msg.status

            # JSON columns already Python dicts
            request_payload = msg.request
            
            article_message_count = Message.query.filter(
                Message.article_id == msg.article_id,
                Message.message_field_type == msg.message_field_type,
                ~Message.status.in_([MessageStatus.PENDING.value, MessageStatus.PROCESSING.value])
            ).count()

            return jsonify({
                "success": True,
                "message_id": str(msg.slug_id),
                "workspace_id": msg.workspace_id,
                "provider_key_id": msg.provider_key_id,
                "ai_response_status": status_value,
                "request": request_payload,
                "ai_response": msg.result,
                "status_code": msg.status_code,
                "message_field_type": msg.message_field_type,
                "message_priority": msg.message_priority,
                "article_id": msg.article_id,
                "article_message_total_count": msg.article_message_total_count,
                "article_message_count": article_message_count,
                "sequence_index": msg.sequence_index,
                "prompt": msg.prompt,
                "html_tag": msg.html_tag,
                "created_date": msg.created_date.isoformat(),
                "updated_date": msg.updated_date.isoformat()
            }), 200

        except Exception as e:
            logger.error(f"Error in get_message: {e}")
            return jsonify({
                "success": False,
                "message": "An error occurred while retrieving the message",
                "error": str(e)
            }), 500

    @message_bp.route('/delete/<article_id>', methods=['DELETE'])
    def delete_article_messages(article_id):
        """Delete all messages associated with an article ID"""
        try:
            # Validate UUID format
            try:
                article_id = article_id.strip()
                uuid_obj = uuid.UUID(article_id)
            except ValueError:
                return jsonify({
                    "success": False,
                    "error": "Invalid article ID format. Must be a valid UUID."
                }), 400

            # Find all messages for this article_id
            messages = Message.query.filter_by(article_id=str(uuid_obj)).all()
            
            if not messages:
                return jsonify({
                    "success": False,
                    "message": f"No messages found for article ID {article_id}"
                }), 404

            # Count messages before deletion
            message_count = len(messages)

            try:
                # Delete all messages for this article_id
                Message.query.filter_by(article_id=str(uuid_obj)).delete()
                db.session.commit()
                
                logger.info(f"Successfully deleted {message_count} messages for article ID {article_id}")
                
                return jsonify({
                    "success": True,
                    "message": f"Successfully deleted {message_count} messages for article ID {article_id}",
                    "deleted_count": message_count
                }), 200

            except Exception as db_error:
                db.session.rollback()
                logger.error(f"Database error while deleting messages: {str(db_error)}")
                return jsonify({
                    "success": False,
                    "message": f"Database error while deleting messages: {str(db_error)}"
                }), 500

        except Exception as e:
            logger.error(f"Error in delete_article_messages: {str(e)}")
            return jsonify({
                "success": False,
                "message": "An error occurred while deleting messages",
                "error": str(e)
            }), 500

# To register in your app factory or main:
# from app.routes.message_router import message_bp
# app.register_blueprint(message_bp)
