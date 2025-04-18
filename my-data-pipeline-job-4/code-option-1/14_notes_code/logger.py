import logging
import sys
from typing import Dict, Any

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Dictionary to store loggers
loggers: Dict[str, logging.Logger] = {}


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name."""
    if name in loggers:
        return loggers[name]
    
    logger = logging.getLogger(name)
    loggers[name] = logger
    
    return logger


def configure_logger(log_level: str = "INFO", log_file: str = None) -> None:
    """Configure the logger with the specified log level and file."""
    # Set the log level
    level = getattr(logging, log_level.upper())
    logging.getLogger().setLevel(level)
    
    # Add a file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
    
    # Update all existing loggers
    for logger in loggers.values():
        logger.setLevel(level)


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds context information to log messages."""
    
    def __init__(self, logger: logging.Logger, extra: Dict[str, Any] = None):
        super().__init__(logger, extra or {})
    
    def process(self, msg, kwargs):
        """Process the log message by adding context information."""
        extra = kwargs.get('extra', {})
        extra.update(self.extra)
        kwargs['extra'] = extra
        
        # Add context information to the message
        context_str = ' '.join(f'{k}={v}' for k, v in self.extra.items())
        if context_str:
            msg = f"{msg} [{context_str}]"
        
        return msg, kwargs


def get_adapter(name: str, extra: Dict[str, Any] = None) -> LoggerAdapter:
    """Get a logger adapter with the specified name and extra context."""
    logger = get_logger(name)
    return LoggerAdapter(logger, extra)