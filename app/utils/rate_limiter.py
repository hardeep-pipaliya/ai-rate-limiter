import time
import threading
from datetime import datetime, timedelta
from sqlalchemy import func
from tenacity import retry, stop_after_attempt, wait_exponential
from app.connection.orm_postgres_connection import db
from app.models.provider_keys import ProviderKey
from app.models.messages import Message, MessageStatus
from app.models.workspaces import WorkSpace
from app.config.logger import LoggerSetup

# Initialize logger setup
log_setup = LoggerSetup()
logger = log_setup.setup_logger()

# Retry configuration for database operations
db_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)

class RateLimiter:
    """PostgreSQL-based rate limiter using existing tables"""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.cache = {}  # Simple in-memory cache for rate limit status
        self.cache_timeout = 5  # Seconds to cache rate limit status
    
    def check_rate_limit(self, workspace_id, provider, key_id, requested_tokens):
        """
        Check if processing would exceed rate limit
        Returns: (allowed, current_usage, limit, reset_time)
        """
        with self.lock:
            # Get provider key
            provider_key = self._get_provider_key(workspace_id, provider, key_id)
            if not provider_key:
                return False, 0, 0, 0
                
            # Get current window
            window = self._get_current_window(provider_key)
            
            # Use a cache key that's more stable for rolling windows
            # Round to nearest minute for cache stability
            cache_time = int(datetime.now().timestamp() // 60) * 60
            cache_key = f"{workspace_id}:{provider}:{key_id}:{cache_time}"
            
            # Check if cache is still valid (within cache timeout)
            current_usage = 0
            if cache_key in self.cache:
                cache_entry = self.cache[cache_key]
                # Check if cache entry is still valid (within 10 seconds)
                if isinstance(cache_entry, dict) and cache_entry.get('timestamp', 0) > time.time() - 10:
                    current_usage = cache_entry.get('usage', 0)
                    logger.info(f"Using cached usage: {current_usage}")
                else:
                    # Cache expired, remove it
                    del self.cache[cache_key]
                    current_usage = 0
            
            if current_usage == 0:
                # Get current usage in window from database
                current_usage = self._get_used_tokens(key_id, window)
                # Cache the result with timestamp
                self.cache[cache_key] = {
                    'usage': current_usage,
                    'timestamp': time.time()
                }
                logger.info(f"Cached new usage: {current_usage}")
            
            # Check if would exceed limit
            would_exceed = current_usage + requested_tokens > provider_key.rate_limit
            
            # Calculate reset time (for rolling window, this is when oldest usage expires)
            reset_time = int(provider_key.rate_limit_period_value * 60)  # Convert to seconds
            if provider_key.rate_limit_period == 'hour':
                reset_time = int(provider_key.rate_limit_period_value * 3600)
            elif provider_key.rate_limit_period == 'day':
                reset_time = int(provider_key.rate_limit_period_value * 86400)
            
            return (
                not would_exceed,  # allowed
                current_usage,     # current usage
                provider_key.rate_limit,  # limit
                reset_time        # seconds until reset
            )
            
    def _get_current_window(self, provider_key):
        """Get start and end time for current window - uses rolling window that looks back from current time"""
        now = datetime.now()
        
        # Use rolling window that looks back from current time
        if provider_key.rate_limit_period == 'minute':
            # Look back N minutes from now
            start = now - timedelta(minutes=provider_key.rate_limit_period_value)
            end = now
        elif provider_key.rate_limit_period == 'hour':
            # Look back N hours from now
            start = now - timedelta(hours=provider_key.rate_limit_period_value)
            end = now
        elif provider_key.rate_limit_period == 'day':
            # Look back N days from now
            start = now - timedelta(days=provider_key.rate_limit_period_value)
            end = now
        else:
            # Default to looking back 1 hour from now
            logger.warning(f"Invalid rate_limit_period '{provider_key.rate_limit_period}', defaulting to 1 hour rolling window")
            start = now - timedelta(hours=1)
            end = now
        
        logger.info(f"Rolling window: {start} to {end} (looking back {provider_key.rate_limit_period_value} {provider_key.rate_limit_period}(s))")
        return (start, end)
        
    def _get_used_tokens(self, provider_key_id, window):
        """
        Get tokens used in current window by querying messages table
        Uses updated_date instead of created_date to track when messages were completed
        """
        logger.info(f"Querying tokens for provider_key_id={provider_key_id}, window={window[0]} to {window[1]}")
        
        # Include messages that are either SUCCESS or PROCESSING (consuming tokens)
        # Use updated_date to track when message was completed, not when it was created
        result = db.session.query(
            func.sum(Message.token_count).label('total')
        ).filter(
            Message.provider_key_id == provider_key_id,
            Message.updated_date >= window[0],  # Use updated_date instead of created_date
            Message.updated_date < window[1],   # Use updated_date instead of created_date
            Message.status.in_([MessageStatus.SUCCESS.value, MessageStatus.PROCESSING.value]),  # Count both success and processing
            Message.token_count.isnot(None)  # Ensure token count is not null
        ).first()
        
        # Debug: show what messages are being counted
        counted_messages = db.session.query(Message).filter(
            Message.provider_key_id == provider_key_id,
            Message.updated_date >= window[0],  # Use updated_date instead of created_date
            Message.updated_date < window[1],   # Use updated_date instead of created_date
            Message.status.in_([MessageStatus.SUCCESS.value, MessageStatus.PROCESSING.value]),
            Message.token_count.isnot(None)
        ).all()
        
        logger.info(f"Found {len(counted_messages)} messages being counted (SUCCESS or PROCESSING) with total tokens: {result.total or 0}")
        if len(counted_messages) > 0:
            logger.info("Messages found:")
            for msg in counted_messages:
                logger.info(f"  - Message {msg.slug_id}: updated_date={msg.updated_date}, status={msg.status}, token_count={msg.token_count}")
        
        return result.total or 0 if result else 0

    def record_usage(self, provider_key_id, token_count=1):
        """Record actual token usage for a provider key"""
        with self.lock:
            # Get the provider key
            provider_key = ProviderKey.query.get(provider_key_id)
            if not provider_key:
                logger.error(f"Provider key {provider_key_id} not found")
                return
            
            # Get workspace
            workspace = WorkSpace.query.get(provider_key.workspace_id)
            if not workspace:
                logger.error(f"Workspace {provider_key.workspace_id} not found")
                return

            # Get current window
            window = self._get_current_window(provider_key)
            
            # Update cache using the same key strategy as check_rate_limit
            cache_time = int(datetime.now().timestamp() // 60) * 60
            cache_key = f"{workspace.workerspace_id}:{provider_key.name}:{provider_key_id}:{cache_time}"
            
            if cache_key in self.cache:
                cache_entry = self.cache[cache_key]
                if isinstance(cache_entry, dict):
                    # Update existing cache entry
                    cache_entry['usage'] += token_count
                    cache_entry['timestamp'] = time.time()
                    new_total = cache_entry['usage']
                else:
                    # Old cache format, convert to new format
                    new_total = cache_entry + token_count
                    self.cache[cache_key] = {
                        'usage': new_total,
                        'timestamp': time.time()
                    }
            else:
                # Get current usage and add new tokens
                current_usage = self._get_used_tokens(provider_key_id, window)
                new_total = current_usage + token_count
                self.cache[cache_key] = {
                    'usage': new_total,
                    'timestamp': time.time()
                }

            # Log the update
            logger.info(f"""
Token Usage Updated:
Provider: {provider_key.name}
Key ID: {provider_key_id}
Window: {window[0]} to {window[1]}
Added Tokens: {token_count}
New Total: {new_total}
            """.strip())

    def _clear_expired_cache(self):
        """Clear expired cache entries"""
        now = datetime.now()
        to_delete = []
        for key in self.cache:
            try:
                # Key format: workspace_id:provider:key_id:timestamp
                window_start = float(key.split(":")[-1])
                if datetime.fromtimestamp(window_start) < now:
                    to_delete.append(key)
            except (ValueError, IndexError):
                to_delete.append(key)
        
        for key in to_delete:
            del self.cache[key]
    
    @db_retry
    def _get_provider_key(self, workspace_id, provider, key_id):
        """Helper to get provider key with consistent behavior"""
        if workspace_id.endswith(":keys"):
            workspace_id = workspace_id.rsplit(":keys", 1)[0]
        
        workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
        if not workspace:
            return None
        
        # First try exact match
        key = ProviderKey.query.filter_by(
            workspace_id=workspace.id,
            name=provider,
            id=key_id  # Use id instead of key_id
        ).first()
        
        if not key:
            # Try provider prefix match
            key = ProviderKey.query.filter(
                ProviderKey.workspace_id == workspace.id,
                ProviderKey.name.startswith(f"{provider}:"),
                ProviderKey.id == key_id  # Use id instead of key_id
            ).first()
        
        return key
    
    def _get_time_window(self, period, period_value=1):
        """Get start and end of current rate limit window"""
        now = datetime.utcnow()
        
        if period == 'second':
            start = now.replace(microsecond=0)
            end = start + timedelta(seconds=period_value)
        elif period == 'minute':
            start = now.replace(second=0, microsecond=0)
            end = start + timedelta(minutes=period_value)
        elif period == 'hour':
            start = now.replace(minute=0, second=0, microsecond=0)
            end = start + timedelta(hours=period_value)
        elif period == 'day':
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=period_value)
        else:
            # Default to minute
            start = now.replace(second=0, microsecond=0)
            end = start + timedelta(minutes=period_value)
            
        return (start, end)

# Global instance
rate_limiter = RateLimiter()