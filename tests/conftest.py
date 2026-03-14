"""
Purpose: PySpark and Delta Lake local testing harness configuration.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: pytest, pyspark, delta-spark, tempfile, logging

This module initializes an isolated local Spark environment. It prevents tests 
from interacting with production data or requiring active cloud compute.
"""

import logging
import shutil
import tempfile
from typing import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Configure explicit logging for the test harness
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Initializes an ephemeral, local PySpark session with Delta Lake capabilities.
    
    Args:
        None
        
    Returns:
        Generator[SparkSession, None, None]: An active SparkSession object configured for Delta Lake.
        
    Raises:
        RuntimeError: If the SparkSession cannot be initialized due to missing dependencies 
                      or environment conflicts.
        
    Complexity:
        Time: O(1) for initialization.
        Space: O(1) in memory allocation, plus temporary disk space for the warehouse.
    """
    # Create a temporary directory to act as our local Spark warehouse
    temp_dir = tempfile.mkdtemp(prefix="atlaslift_spark_warehouse_")
    logger.info(f"Initialized temporary directory for Spark warehouse: {temp_dir}")
    
    try:
        # Build base Spark session with strict local resource constraints and Delta configurations
        builder = SparkSession.builder \
            .appName("AtlasLift-Local-Test-Harness") \
            .master("local[2]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", temp_dir) \
            .config("spark.driver.memory", "2g") \
            .config("spark.default.parallelism", "2") \
            .config("spark.sql.shuffle.partitions", "2") # Keep shuffles small for local tests
        
        # Apply Delta pip configuration to fetch required JARs
        spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Suppress verbose Spark logging in the console for cleaner test outputs
        spark_session.sparkContext.setLogLevel("WARN")
        logger.info("Successfully created local PySpark Delta session.")
        
        yield spark_session
        
    except Exception as e:
        # Explicit error handling: we do not swallow initialization errors.
        logger.error(f"Failed to initialize PySpark session: {str(e)}")
        raise RuntimeError(f"Spark initialization failed: {str(e)}") from e
        
    finally:
        # Teardown logic: Ensure resources are released after all tests finish
        try:
            if 'spark_session' in locals():
                spark_session.stop()
                logger.info("Spark session successfully stopped.")
        except Exception as stop_err:
            logger.warning(f"Error encountered while stopping Spark session: {str(stop_err)}")
            
        try:
            # Force deletion of the temporary warehouse directory
            shutil.rmtree(temp_dir)
            logger.info(f"Successfully cleaned up temporary directory: {temp_dir}")
        except Exception as clean_err:
            logger.warning(f"Failed to clean up temporary directory {temp_dir}. Manual deletion may be required. Error: {str(clean_err)}")