# workspace_router.py

import os
import signal
import time
import traceback
from flask import Blueprint, request, jsonify
import psutil
from app.config.logger import LoggerSetup
from app.connection.orm_postgres_connection import db
from app.connection.rabbitmq_connection import RabbitMQConnection
from app.models.workspaces import WorkSpace
from app.models.provider_keys import ProviderKey
from sqlalchemy.exc import SQLAlchemyError
import uuid
from datetime import datetime
from app.models.workers import Worker as WorkerModel


logger = LoggerSetup().setup_logger()

# Two separate blueprints
workspace_bp = Blueprint('queue', __name__, url_prefix='/queue')
workspaces_bp = Blueprint('queues', __name__, url_prefix='/queues')


class WorkspaceRoutes:

    @workspace_bp.route('/register', methods=['POST'])
    def register_workspace():
        logger.info("Workspace registration request received")
        try:
            data = request.get_json()
            logger.info(f"Registration data: {data}")
            
            workspace_id = data.get('workspace_id')
            if not workspace_id:
                logger.error("Missing workspace_id")
                return jsonify({
                    "success": False,
                    "message": "Missing workspace_id"
                }), 400

            # Detailed RabbitMQ connection logging
            try:
                logger.info(f"Attempting RabbitMQ connection for workspace: {workspace_id}")
                logger.info(f"RabbitMQ Host: {os.getenv('RABBITMQ_HOST')}")
                logger.info(f"RabbitMQ Port: {os.getenv('RABBITMQ_PORT')}")
                
                rabbitmq_conn = RabbitMQConnection.get_instance()
                rabbitmq_conn.create_queue(workspace_id)
                
                logger.info("RabbitMQ queue created successfully")
            
            except Exception as e:
                logger.error(f"RabbitMQ Queue Creation Error: {str(e)}")
                return jsonify({
                    "success": False,
                    "message": f"RabbitMQ error: {str(e)}"
                }), 500

            providers = data.get('providers', [])

            if workspace_id.endswith(":keys"):
                workspace_id = workspace_id.split(":keys")[0]
                logger.info(f"Modified workspace_id: {workspace_id}")

            workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
            if not workspace:
                logger.info(f"Creating new workspace: {workspace_id}")
                workspace = WorkSpace(
                    slug_id=uuid.uuid4(),
                    workerspace_id=workspace_id
                )
                db.session.add(workspace)
                db.session.flush()
                logger.info(f"New workspace created with ID: {workspace.id}")

            provider_key_ids = []
            provider_errors = []

            for provider in providers:
                try:
                    logger.info(f"Adding provider key for: {provider.get('name')}")
                    provider_key = ProviderKey(
                        slug_id=uuid.uuid4(),
                        workspace_id=workspace.id,
                        name=provider['name'],
                        api_key=provider['api_key'],
                        rate_limit=provider.get('rate_limit', 1000),
                        rate_limit_period=provider.get('rate_limit_period', 'hour'),
                        rate_limit_period_value=provider.get('rate_limit_period_value', 1),
                        config=provider.get('config', {}),
                        created_date=datetime.utcnow(),
                        updated_date=datetime.utcnow()
                    )
                    db.session.add(provider_key)
                    db.session.flush()
                    provider_key_ids.append({
                        "key_id": str(provider_key.slug_id),
                        "provider": provider_key.name
                    })
                    logger.info(f"Provider key added successfully for: {provider['name']}")
                except Exception as e:
                    logger.error(f"Failed to add provider key: {str(e)}")
                    provider_errors.append({
                        "provider": provider.get('name'),
                        "error": str(e)
                    })

            db.session.commit()
            logger.info("Workspace registration completed successfully")

            response = {
                "success": True,
                "workspace_id": workspace_id,
                "message": f"Workspace {workspace_id} created",
                "provider_key_ids": provider_key_ids,
            }

            if provider_errors:
                response["provider_errors"] = provider_errors

            return jsonify(response), 201

        except SQLAlchemyError as e:
            logger.error(f"Database error during workspace registration: {str(e)}")
            db.session.rollback()
            return jsonify({
                "success": False,
                "message": f"Database error: {str(e)}"
            }), 500
        except Exception as e:
            logger.error(f"Unexpected error during workspace registration: {str(e)}")
            db.session.rollback()
            return jsonify({
                "success": False,
                "message": str(e)
            }), 500

        
    @workspaces_bp.route('/', methods=['GET'])
    def list_workspaces():
        """List all workspaces or a specific workspace if workspace_id is provided"""
        try:
            workspace_id = request.args.get('workspace_id')  # <-- Get the workspace_id query param

            if workspace_id:
                # If workspace_id is provided, filter by it
                workspaces = WorkSpace.query.filter_by(workerspace_id=workspace_id).all()
            else:
                # Fetch all workspaces
                workspaces = WorkSpace.query.all()

            workspace_list = []

            for workspace in workspaces:
                # Step 1: Check if the RabbitMQ queue exists for this workspace
                if not check_workspace_in_rabbitmq(workspace.workerspace_id):
                    try:
                        # If the queue is missing, create it in RabbitMQ
                        rabbitmq = RabbitMQConnection.get_instance()
                        rabbitmq.create_queue(queue_name=workspace.workerspace_id)
                        print(f"Queue for workspace {workspace.workerspace_id} created in RabbitMQ.")
                    except Exception as e:
                        # Log or handle error if queue creation fails
                        print(f"Error creating queue for workspace {workspace.workerspace_id}: {str(e)}")
                        return jsonify({
                            "success": False,
                            "message": f"Error creating RabbitMQ queue for workspace {workspace.workerspace_id}: {str(e)}"
                        }), 500

                # Step 2: Fetch provider keys associated with this workspace
                provider_keys = ProviderKey.query.filter_by(workspace_id=workspace.id).all()
                api_keys = []

                for key in provider_keys:
                    api_keys.append({
                        "provider": key.name,
                        "key_id": str(key.slug_id),
                        "masked_key": mask_api_key(key.api_key),
                        "rate_limit": key.rate_limit,
                        "rate_limit_period": key.rate_limit_period,
                        "model_info": key.config
                    })

                # Step 3: Add workspace to response list
                workspace_list.append({
                    "workspace_id": workspace.workerspace_id,
                    "created_at": workspace.created_date.strftime('%Y-%m-%d %H:%M:%S') if workspace.created_date else '',
                    "api_keys": api_keys
                })

            return jsonify({
                "success": True,
                "data": workspace_list
            })

        except SQLAlchemyError as e:
            return jsonify({
                "success": False,
                "message": f"Database error: {str(e)}"
            }), 500
        except Exception as e:
            return jsonify({
                "success": False,
                "message": str(e)
            }), 500

    @workspace_bp.route('/delete/<workspace_id>', methods=['DELETE'])
    def delete_workspace(workspace_id):
        """Delete a workspace, its provider keys, and its RabbitMQ queue"""
        try:
            # Normalize workspace_id if needed
            if workspace_id.endswith(":keys"):
                workspace_id = workspace_id.split(":keys")[0]

            # Find the workspace
            workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()

            if not workspace:
                return jsonify({
                    "success": False,
                    "message": f"Workspace {workspace_id} not found"
                }), 404

            # --- First, delete the RabbitMQ queue ---
            try:
                rabbitmq = RabbitMQConnection.get_instance()
                # Delete the queue by passive declare + delete
                rabbitmq._channel.queue_delete(queue=workspace_id)
            except Exception as e:
                # Not critical — just log it
                print(f"Warning: Failed to delete RabbitMQ queue for workspace {workspace_id}: {str(e)}")

            # Delete all provider keys linked to this workspace
            ProviderKey.query.filter_by(workspace_id=workspace.id).delete()

            # Delete the workspace itself
            db.session.delete(workspace)
            db.session.commit()

            return jsonify({
                "success": True,
                "message": f"Workspace {workspace_id}, provider keys, and RabbitMQ queue deleted successfully"
            }), 200

        except SQLAlchemyError as e:
            db.session.rollback()
            return jsonify({
                "success": False,
                "message": f"Database error: {str(e)}"
            }), 500
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "success": False,
                "message": str(e)
            }), 500


    @workspace_bp.route('/clear/<workspace_id>', methods=['POST'])
    def clear_workspace(workspace_id):
        """Clear all messages and stop workers for a workspace"""
        try:
            workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
            if not workspace:
                return jsonify({
                    "success": False,
                    "message": f"Workspace {workspace_id} not found"
                }), 404

            workers = WorkerModel.query.filter_by(
                workspace_id=workspace.id,
                status='running'
            ).all()

            for worker in workers:
                if worker.pid and psutil.pid_exists(worker.pid):
                    try:
                        os.kill(worker.pid, signal.SIGTERM)
                        time.sleep(0.5)
                        if psutil.pid_exists(worker.pid):
                            os.kill(worker.pid, signal.SIGKILL)
                    except Exception as e:
                        logger.error(f"Error stopping worker {worker.slug_id}: {e}")
                worker.status = 'stopped'
                worker.updated_date = datetime.utcnow()

            try:
                rabbitmq = RabbitMQConnection.get_instance()
                rabbitmq.purge_queue(workspace_id)
            except Exception as e:
                logger.error(f"Error clearing queue: {e}")
                return jsonify({
                    "success": False,
                    "message": f"Error clearing queue: {str(e)}"
                }), 500

            db.session.commit()

            return jsonify({
                "success": True,
                "message": f"Workspace {workspace_id} cleared: stopped all workers and removed queued messages",
                "workers_stopped": len(workers)
            }), 200

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error clearing workspace: {e}")
            return jsonify({
                "success": False,
                "message": str(e)
            }), 500

    
# Helper
def mask_api_key(api_key):
    if not api_key:
        return ""
    if len(api_key) <= 8:
        return '*' * len(api_key)
    return api_key[:4] + '*' * (len(api_key) - 8) + api_key[-4:]


# Helper to check workspace ID in RabbitMQ queue
def check_workspace_in_rabbitmq(workspace_id):
    try:
        rabbitmq = RabbitMQConnection.get_instance()
        channel = rabbitmq._channel  # Assuming you have a RabbitMQ connection in place
        
        # Check if the queue exists (without modifying it)
        channel.queue_declare(queue=workspace_id, passive=True)
        return True  # Queue exists

    except Exception as e:
        # If the exception is raised, the queue does not exist
        print(f"Error checking RabbitMQ for workspace {workspace_id}: {str(e)}")
        return False

