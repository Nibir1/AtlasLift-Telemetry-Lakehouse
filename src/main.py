"""
Purpose: Master orchestrator for the AtlasLift Medallion Data Pipeline.
Author: Lead Cloud Data Architect
Date: 2026-03-15
Dependencies: pyspark, config, pipelines
"""

import logging
from pyspark.sql import SparkSession

from config.settings import Config, with_retries
from pipelines.bronze_ingestion import ingest_bronze
from pipelines.silver_cleansing import cleanse_silver
from pipelines.gold_aggregation import build_gold

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def seed_mock_data(spark: SparkSession, path: str) -> None:
    """
    Seeds the landing zone with mock JSON data if it is empty.
    This ensures we have physical data in Azure for Power BI to query.
    """
    try:
        # Check if data already exists to prevent duplicate seeding
        spark.read.format("json").load(path)
        logger.info("Mock telemetry data already exists in landing zone.")
    except Exception:
        logger.info("Landing zone empty. Generating mock telemetry data...")
        mock_data = [
            {"message_id": "msg_001", "crane_id": "CRN-FI-01", "timestamp": "2026-03-15T10:00:00Z", "hoist_load_kg": 25000.5, "vibration_mm_s": 12.4, "crane_model": "SMARTON", "operational_region": "Nordics"},
            {"message_id": "msg_002", "crane_id": "CRN-FI-01", "timestamp": "2026-03-15T10:05:00Z", "hoist_load_kg": 45000.0, "vibration_mm_s": 45.1, "crane_model": "SMARTON", "operational_region": "Nordics"},
            {"message_id": "msg_003", "crane_id": "CRN-DE-05", "timestamp": "2026-03-15T10:00:00Z", "hoist_load_kg": 10500.0, "vibration_mm_s": 8.2, "crane_model": "CXT", "operational_region": "Central Europe"},
            {"message_id": "msg_004", "crane_id": "CRN-FI-01", "timestamp": "2026-03-15T10:10:00Z", "hoist_load_kg": 85000.0, "vibration_mm_s": 95.0, "crane_model": "SMARTON", "operational_region": "Nordics"},
            {"message_id": "msg_005", "crane_id": "CRN-UK-02", "timestamp": "2026-03-15T10:15:00Z", "hoist_load_kg": 5000.0, "vibration_mm_s": -5.0, "crane_model": "CXT", "operational_region": "UK"}
        ]
        df = spark.createDataFrame(mock_data)
        df.write.format("json").mode("overwrite").save(path)
        logger.info("Mock data successfully written to landing zone.")

@with_retries(max_retries=Config.MAX_RETRIES, delay=Config.RETRY_DELAY_SEC)
def run_pipeline(spark: SparkSession, raw_data_path: str) -> None:
    """Executes the full Bronze -> Silver -> Gold pipeline sequentially."""
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
        spark = SparkSession.builder.appName("AtlasLift-Medallion-Pipeline").getOrCreate()
        RAW_DATA_SOURCE = f"{Config.LAKEHOUSE_BASE_PATH}/landing_zone"
        
        # 1. Ensure we have data
        seed_mock_data(spark, RAW_DATA_SOURCE)
        # 2. Run the Medallion architecture
        run_pipeline(spark, RAW_DATA_SOURCE)
        
    except Exception as e:
        logger.critical(f"Pipeline failed critically: {str(e)}")
        raise
    
    # CRITICAL FIX: Removed the spark.stop() block. Databricks manages the lifecycle.