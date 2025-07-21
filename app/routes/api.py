from flask import Blueprint, jsonify
from app.utils.middleware import rate_limit_middleware
from app.utils.helpers import format_response

bp = Blueprint('api', __name__)

@bp.route('/health', methods=['GET'])
@rate_limit_middleware
def health_check():
    return format_response(message="Service is healthy")

@bp.route('/test', methods=['GET'])
@rate_limit_middleware
def test_rate_limit():
    """Test endpoint to demonstrate rate limiting"""
    return format_response(
        message="Request successful",
        data={"test": "This is a rate-limited endpoint"}
    )