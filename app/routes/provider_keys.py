from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from app.config.logger import LoggerSetup
from app.connection.orm_postgres_connection import db
from app.models.provider_keys import ProviderKey
from sqlalchemy.exc import SQLAlchemyError
import uuid

from app.models.workspaces import WorkSpace
logger = LoggerSetup().setup_logger()

provider_keys_bp = Blueprint('provider_keys', __name__, url_prefix='')

class ProviderKeysRoutes:

    @provider_keys_bp.route('/provider-keys/', methods=['GET'])
    def list_provider_keys():
        """List provider keys for a specific workspace"""
        try:
            workspace_uuid = request.args.get('workspace_id')
            if not workspace_uuid:
                return jsonify({
                    "success": False,
                    "message": "workspace_id query parameter is required"
                }), 400

            workspace = WorkSpace.query.filter_by(workerspace_id=workspace_uuid).first()
            if not workspace:
                return jsonify({
                    "success": False,
                    "message": f"Workspace {workspace_uuid} not found"
                }), 404

            keys = ProviderKey.query.filter_by(workspace_id=workspace.id).all()

            key_list = [{
                "key_id": str(key.slug_id),
                "workspace_id": key.workspace_id,
                "name": key.name,
                "api_key": key.api_key,
                "rate_limit": key.rate_limit,
                "rate_limit_period": key.rate_limit_period,
                "config": key.config,
                "created_date": key.created_date,
                "updated_date": key.updated_date
            } for key in keys]

            return jsonify({
                "success": True,
                "keys": key_list
            }), 200

        except Exception as e:
            return jsonify({
                "success": False,
                "message": str(e)
            }), 500

    @provider_keys_bp.route('/provider-key/add', methods=['POST'])
    def add_provider_key():
        """Add a provider key"""
        try:
            data = request.get_json()
            if not data or 'workspace_id' not in data or 'name' not in data or 'api_key' not in data:
                return jsonify({
                    "success": False,
                    "message": "Missing required fields: workspace_id, name, api_key"
                }), 400

            # Validate provider name
            supported_providers = ['openai', 'anthropic', 'azure', 'novita']
            provider_name = data['name'].lower()
            if provider_name not in supported_providers:
                return jsonify({
                    "success": False,
                    "message": f"Unsupported provider: {data['name']}. Supported providers are: {', '.join(supported_providers)}"
                }), 400

            # Fetch workspace by UUID first 
            workspace = WorkSpace.query.filter_by(workerspace_id=data['workspace_id']).first()
            if not workspace:
                return jsonify({
                    "success": False,
                    "message": f"Workspace {data['workspace_id']} not found"
                }), 404

            new_key = ProviderKey(
                slug_id=uuid.uuid4(),
                workspace_id=workspace.id, 
                name=data['name'],
                api_key=data['api_key'],
                rate_limit=data.get('rate_limit', 1000),
                rate_limit_period=data.get('rate_limit_period', 'hour'),
                rate_limit_period_value=data.get('rate_limit_period_value', 1),
                config=data.get('config', {
                    'model': 'gpt-3.5-turbo',
                    'api_version': 'v1',
                    'endpoint': 'https://api.openai.com/v1/chat/completions'
                })
            )

            db.session.add(new_key)
            db.session.commit()

            return jsonify({
                "success": True,
                "key_id": str(new_key.slug_id),
                "message": "Provider key added successfully"
            }), 201

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

    @provider_keys_bp.route('/provider-key/update/<key_id>', methods=['PUT'])
    def update_provider_key(key_id):
        """Update a provider key"""
        try:
            data = request.get_json()
            if not data:
                return jsonify({
                    "success": False,
                    "message": "No data provided"
                }), 400

            key = ProviderKey.query.filter_by(slug_id=key_id).first()
            if not key:
                return jsonify({
                    "success": False,
                    "message": f"Provider key {key_id} not found"
                }), 404

            if 'api_key' in data:
                key.api_key = data['api_key']
            if 'rate_limit' in data:
                key.rate_limit = data['rate_limit']
            if 'rate_limit_period' in data:
                key.rate_limit_period = data['rate_limit_period']
            if 'rate_limit_period_value' in data:
                key.rate_limit_period_value = data['rate_limit_period_value']
            if 'config' in data:
                key.config = data['config']
            if 'name' in data:
                key.name = data['name']

            key.updated_date = datetime.now(timezone.utc)
            db.session.commit()

            return jsonify({
                "success": True,
                "message": f"Provider key {key_id} updated successfully"
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

    @provider_keys_bp.route('/provider-key/delete/<key_id>', methods=['DELETE'])
    def delete_provider_key(key_id):
        """Delete a provider key by key_id"""
        try:
            key = ProviderKey.query.filter_by(slug_id=key_id).first()
            if not key:
                return jsonify({
                    "success": False,
                    "message": f"Provider key {key_id} not found"
                }), 404

            db.session.delete(key)
            db.session.commit()

            return jsonify({
                "success": True,
                "message": f"Provider key {key_id} deleted successfully"
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
