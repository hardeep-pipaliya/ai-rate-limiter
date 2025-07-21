from functools import wraps
from flask import request, jsonify
from app.providers.rate_limit_provider import RateLimitProvider
from app.utils.helpers import format_response

def rate_limit_middleware(f):
    rate_limiter = RateLimitProvider()
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        client_ip = request.remote_addr
        is_allowed, limit_info = rate_limiter.check_rate_limit(f"rate_limit:{client_ip}")
        
        if not is_allowed:
            response = format_response(
                data=limit_info,
                message="Rate limit exceeded",
                status=False
            )
            return jsonify(response), 429
            
        response = f(*args, **kwargs)
        if isinstance(response, tuple):
            resp, code = response
        else:
            resp, code = response, 200
            
        # Add rate limit headers
        headers = {
            'X-RateLimit-Limit': str(limit_info['total']),
            'X-RateLimit-Remaining': str(limit_info['remaining']),
            'X-RateLimit-Reset': str(limit_info['reset'])
        }
        
        if isinstance(resp, dict):
            return jsonify(resp), code, headers
        return resp, code, headers
        
    return decorated_function