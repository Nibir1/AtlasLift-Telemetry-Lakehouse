"""
Purpose: Master orchestrator for the AtlasLift Medallion Data Pipeline.
Author: Lead Cloud Data Architect
Date: 2026-03-14
Dependencies: pyspark, config, pipelines
"""

import logging
from pyspark.sql import SparkSession

# Note: In Python execution, ensuring modules are accessible requires proper project scoping.
# Assuming execution from the src/ directory or via proper module packaging (-m).
from config.settings import Config, with_retries
from pipelines.bronze_ingestion import ingest_bronze
from pipelines.silver_cleansing import cleanse_silver
from pipelines.gold_aggregation import build_gold

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@with_retries(max_retries=Config.MAX_RETRIES, delay=Config.RETRY_DELAY_SEC)
def run_pipeline(spark: SparkSession, raw_data_path: str) -> None:
    """
    Executes the full Bronze -> Silver -> Gold pipeline sequentially.
    Wrapped in a retry decorator to handle transient cluster or storage faults.
    """
    logger.info("Starting AtlasLift-Telemetry-Lakehouse Medallion Pipeline...")
    
    logger.info("--- Executing Phase: BRONZE INGESTION ---")
    ingest_bronze(spark, raw_data_path, Config.BRONZE_PATH)
    
    logger.info("--- Executing Phase: SILVER CLEANSING ---")
    cleanse_silver(spark, Config.BRONZE_PATH, Config.SILVER_PATH)
    
    logger.info("--- Executing Phase: GOLD AGGREGATION ---")
    build_gold(spark, Config.SILVER_PATH, Config.GOLD_FACT_PATH, Config.GOLD_DIM_CRANE_PATH)
    
    logger.info("Pipeline executed successfully from end to end.")

if __name__ == "__main__":
    try:
        # Databricks native environments provide 'spark' globally. 
        # This builder acts as a fallback for local execution.
        spark = SparkSession.builder \
            .appName("AtlasLift-Medallion-Pipeline") \
            .getOrCreate()
        
        # Determine landing zone path
        RAW_DATA_SOURCE = f"{Config.LAKEHOUSE_BASE_PATH}/landing_zone"
        
        run_pipeline(spark, RAW_DATA_SOURCE)
        
    except Exception as e:
        logger.critical(f"Pipeline failed critically and could not recover: {str(e)}")
        raise
    finally:
        # Ensure we release JVM memory locally
        if 'spark' in locals():
            spark.stop()