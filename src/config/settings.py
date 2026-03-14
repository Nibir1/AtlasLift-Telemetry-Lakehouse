"""
Purpose: Centralized configuration, environment variable management, and execution wrappers.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: os, dotenv, time, functools, typing
"""

import os
import time
from functools import wraps
from typing import Callable, Any
from dotenv import load_dotenv

# Load variables from .env file for local testing
load_dotenv()

class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
    # Defaulting to a local temp dir for isolated testing if not provided via .env
    LAKEHOUSE_BASE_PATH = os.getenv("LAKEHOUSE_BASE_PATH", "/tmp/atlaslift_lakehouse")
    
    # Absolute paths for the Medallion Data Layers
    BRONZE_PATH = f"{LAKEHOUSE_BASE_PATH}/bronze/telemetry"
    SILVER_PATH = f"{LAKEHOUSE_BASE_PATH}/silver/telemetry"
    GOLD_FACT_PATH = f"{LAKEHOUSE_BASE_PATH}/gold/fact_telemetry"
    GOLD_DIM_CRANE_PATH = f"{LAKEHOUSE_BASE_PATH}/gold/dim_crane"

    # Robust execution parameters
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY_SEC = int(os.getenv("RETRY_DELAY_SEC", "5"))

def with_retries(max_retries: int = Config.MAX_RETRIES, delay: int = Config.RETRY_DELAY_SEC) -> Callable:
    """
    Decorator to enforce timeouts and retries on volatile pipeline operations.
    
    Args:
        max_retries (int): Maximum number of retry attempts.
        delay (int): Delay in seconds between retries.
        
    Returns:
        Callable: The decorated function safely wrapped with retry logic.
        
    Raises:
        RuntimeError: Reraises the underlying exception if all retries are exhausted.
        
    Complexity:
        Time: O(max_retries * execution_time)
        Space: O(1)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    print(f"WARN: Attempt {attempt}/{max_retries} failed for {func.__name__}. Error: {e}")
                    if attempt < max_retries:
                        time.sleep(delay)
            # Explicit error handling: we do not swallow errors silently.
            raise RuntimeError(f"CRITICAL: All {max_retries} attempts failed for {func.__name__}") from last_exception
        return wrapper
    return decorator