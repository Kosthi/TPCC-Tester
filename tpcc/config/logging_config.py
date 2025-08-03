"""
Logging configuration for TPC-C benchmark.
Provides centralized logging setup for both console and file output.
"""

import logging
import logging.handlers
import sys
from pathlib import Path


def setup_logging(
    log_level: str = "INFO",
    log_file: str = None,
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """
    Configure logging to output to both console and file.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file. If None, uses default location
        max_file_size: Maximum size of log file before rotation (bytes)
        backup_count: Number of backup log files to keep
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Create logs directory
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)

    if log_file is None:
        log_file = log_dir / "tpcc_benchmark.log"
    else:
        log_file = Path(log_file)

    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )

    simple_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(simple_formatter)

    # Rotating file handler for automatic log rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_file_size, backupCount=backup_count, encoding="utf-8"
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(detailed_formatter)

    # Add handlers to root logger
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Log the configuration
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured: level={log_level}, file={log_file}")


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name."""
    return logging.getLogger(name)


# Default configuration when module is imported
if __name__ != "__main__":
    # Only set up logging if not already configured
    if not logging.getLogger().handlers:
        setup_logging()
