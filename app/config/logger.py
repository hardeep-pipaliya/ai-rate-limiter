from loguru import logger
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# First try to load .env, then .env.docker if .env is missing
load_dotenv()
class LoggerSetup:
    def __init__(self):
        # Get log directory from environment variable with fallback
        self.base_log_dir = os.getenv("LOG_DIR", "app/static/logs/workers")
        
        # Normalize path to use forward slashes
        self.base_log_dir = self.base_log_dir.replace('\\', '/')
        
        # Make sure the base directory exists with proper permissions
        try:
            os.makedirs(self.base_log_dir, exist_ok=True)
            # Set proper permissions on Linux/Unix systems
            if os.name != 'nt':  # Not Windows
                import stat
                mode = stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH  # 0o775
                os.chmod(self.base_log_dir, mode)
                print(f"Set permissions 0o775 on {self.base_log_dir}")
        except Exception as e:
            print(f"Error setting up base log directory {self.base_log_dir}: {e}")
        
        # No longer need daily log dir since we're using base_log_dir directly
        self.log_dir = self.base_log_dir

    def setup_logger(self):
        """Setup the main logger with console and file handlers."""
        # Remove default logger
        logger.remove()
        
        # Add console logger
        logger.add(sys.stdout, level="INFO")
        
        # Add file logger for all logs directly in the base folder
        log_file = f"{self.log_dir}/app.log"
        logger.add(
            log_file,
            rotation="1 day",
            retention="1 week",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | PID:{process} | {message}",
            level="INFO",
            enqueue=True,
            delay=False,
            mode="a",
            backtrace=True,
            diagnose=True,
            catch=True
        )
        
        print(f"Main logger setup with file: {log_file}")
        return logger

    def setup_worker_logger(self, pid=None):
        """Setup worker-specific logger."""
        # Remove default logger
        logger.remove()
        
        # Add console logger first
        logger.add(sys.stdout, level="INFO")
        
        # Create log filename based on optional PID
        log_filename = f"worker_{pid}.log" if pid else "worker.log"
        log_file = f"{self.log_dir}/{log_filename}".replace('\\', '/')  # Ensure forward slashes
        
        # Debug print
        print(f"Setting up worker logger at: {log_file}")
        
        # Make sure the directory exists
        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
        except Exception as e:
            print(f"Error ensuring log directory exists: {e}")
        
        # Configure logger
        try:
            logger.add(
                log_file,
                rotation="1 day",
                retention="1 week",
                format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
                level="INFO",
                enqueue=True,
                delay=False,
                mode="a",
                backtrace=True,
                diagnose=True,
                catch=True
            )
            
            # Test write to verify logger is working
            logger.info(f"Worker logger initialized with PID={pid}")
            
            print(f"Successfully set up worker logger at: {log_file}")
            return logger
            
        except Exception as e:
            print(f"Failed to set up worker logger: {e}")
            # Return a console-only logger as fallback
            return logger