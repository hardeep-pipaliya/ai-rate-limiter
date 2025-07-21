from typing import Tuple, Dict
import time

class RateLimitProvider:
    _instance = None
    _storage: Dict[str, Dict] = {}

    def __init__(self, rate_limit: int = 60, time_window: int = 60):
        """
        Initialize rate limiter with in-memory storage
        :param rate_limit: Maximum number of requests allowed in time window
        :param time_window: Time window in seconds
        """
        self.rate_limit = rate_limit
        self.time_window = time_window

    @classmethod
    def get_instance(cls) -> 'RateLimitProvider':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _cleanup_old_requests(self, key: str, current_time: int) -> None:
        """Remove requests older than the time window"""
        if key not in self._storage:
            self._storage[key] = []
        
        self._storage[key] = [
            timestamp for timestamp in self._storage[key]
            if timestamp > current_time - self.time_window
        ]

    def check_rate_limit(self, key: str) -> Tuple[bool, dict]:
        """
        Check if request should be rate limited
        :param key: Unique identifier (e.g., IP address or API key)
        :return: Tuple of (is_allowed, limit_info)
        """
        current = int(time.time())
        
        self._cleanup_old_requests(key, current)
        request_count = len(self._storage[key])
        
        if request_count < self.rate_limit:
            self._storage[key].append(current)
            
        is_allowed = request_count < self.rate_limit
        remaining = max(0, self.rate_limit - request_count)
        
        return is_allowed, {
            "remaining": remaining,
            "total": self.rate_limit,
            "reset": current + self.time_window
        }