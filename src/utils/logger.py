"""
Logger utility for FlowForge ETL Platform
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from src.utils.config import LOG_FORMAT, LOG_FILE, LOGS_DIR

class FlowForgeLogger:
    def __init__(self, name: str = "flowforge"):
        self.logger = logging.getLogger(name)
        self.setup_logger()
    
    def setup_logger(self):
        """Configure logger with file and console handlers"""
        if self.logger.handlers:
            return  # Logger already configured
        
        self.logger.setLevel(logging.INFO)
        
        # Create formatters
        formatter = logging.Formatter(LOG_FORMAT)
        
        # File handler
        file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.WARNING)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def log_etl_step(self, step: str, message: str, level: str = "info"):
        """Log ETL step with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{step.upper()}] {message}"
        
        if level.lower() == "info":
            self.logger.info(log_message)
        elif level.lower() == "warning":
            self.logger.warning(log_message)
        elif level.lower() == "error":
            self.logger.error(log_message)
        elif level.lower() == "debug":
            self.logger.debug(log_message)
    
    def log_data_info(self, operation: str, rows: int, cols: int, file_name: str = ""):
        """Log data transformation information"""
        message = f"{operation} - Rows: {rows}, Columns: {cols}"
        if file_name:
            message += f", File: {file_name}"
        self.log_etl_step("TRANSFORM", message)
    
    def log_user_action(self, action: str, details: str = ""):
        """Log user actions for reproducibility"""
        message = f"User Action: {action}"
        if details:
            message += f" - {details}"
        self.log_etl_step("USER", message)

# Global logger instance
logger = FlowForgeLogger()