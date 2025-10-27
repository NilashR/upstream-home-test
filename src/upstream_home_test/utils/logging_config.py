"""Structured JSON logging configuration for the data pipeline."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def get_project_root() -> Path:
    """Get the project root directory (where pyproject.toml is located)."""
    current_file = Path(__file__).resolve()
    # Go up 4 levels: utils -> upstream_home_test -> src -> project_root
    return current_file.parent.parent.parent.parent


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            "step": getattr(record, "step", "unknown"),
            "metrics": getattr(record, "metrics", {}),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging(log_level: str = "INFO", clear_log_file: bool = True) -> logging.Logger:
    """Set up structured JSON logging for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        clear_log_file: Whether to clear the existing log file (default: True)
        
    Returns:
        Configured logger instance
    """
    # Get project root directory (where pyproject.toml is located)
    project_root = get_project_root()
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Configure root logger
    logger = logging.getLogger("upstream_home_test")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JSONFormatter())
    logger.addHandler(console_handler)
    
    # File handler - optionally clear log file
    log_file_path = logs_dir / "pipeline.log"
    if clear_log_file and log_file_path.exists():
        log_file_path.unlink()  # Remove existing log file
    
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)
    
    return logger


def log_pipeline_step(
    logger: logging.Logger | None,
    step: str,
    event: str,
    metrics: Dict[str, Any] | None = None,
    level: str = "INFO",
) -> None:
    """Log a pipeline step with structured data.
    
    Args:
        logger: Logger instance (can be None for testing)
        step: Pipeline step name
        event: Event description
        metrics: Optional metrics dictionary
        level: Log level
    """
    if logger is None:
        # Skip logging if no logger provided (useful for testing)
        return
        
    extra = {
        "step": step,
        "metrics": metrics or {},
    }
    
    log_level = getattr(logging, level.upper())
    logger.log(log_level, event, extra=extra)
