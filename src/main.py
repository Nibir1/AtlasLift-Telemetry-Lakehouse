"""
Purpose: Master orchestrator for the AtlasLift Medallion Data Pipeline.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: pyspark, config, pipelines
"""

import logging
import os
import json
from pyspark.sql import SparkSession

from config.settings import Config, with_retries
from pipelines.bronze_ingestion import ingest_bronze
from pipelines.silver_cleansing import cleanse_silver
from pipelines.gold_aggregation import build_gold

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def seed_cloud_mock_data(landing_zone_path: str) -> None:
    """Generates mock telemetry payloads on the cluster if the directory is empty."""
    os.makedirs(landing_zone_path, exist_ok=True)
    file_path = os.path.join(landing_zone_path, "telemetry_batch_1.json")
    
    if not os.path.exists(file_path):
        logger.info(f"No raw data found. Seeding mock telemetry at {file_path}")
        mock_data = [
            {"message_id": "msg_001", "crane_id": "CRN-FI-01", "timestamp": "2026-03-14T10:00:00Z", "hoist_load_kg": 25000.5, "vibration_mm_s": 12.4, "crane_model": "SMARTON", "operational_region": "Nordics"},
            {"message_id": "msg_002", "crane_id": "CRN-FI-01", "timestamp": "2026-03-14T10:05:00Z", "hoist_load_kg": 45000.0, "vibration_mm_s": 45.1, "crane_model": "SMARTON", "operational_region": "Nordics"},
            {"message_id": "msg_003", "crane_id": "CRN-DE-05", "timestamp": "2026-03-14T10:00:00Z", "hoist_load_kg": 10500.0, "vibration_mm_s": 8.2, "crane_model": "CXT", "operational_region": "Central Europe"}
        ]
        with open(file_path, "w") as f:
            for record in mock_data:
                f.write(json.dumps(record) + "\n")

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
        spark = SparkSession.builder \
            .appName("AtlasLift-Medallion-Pipeline") \
            .getOrCreate()
        
        RAW_DATA_SOURCE = f"{Config.LAKEHOUSE_BASE_PATH}/landing_zone"
        
        # Self-healing: Ensure data exists before pipeline runs
        seed_cloud_mock_data(RAW_DATA_SOURCE)
        
        run_pipeline(spark, RAW_DATA_SOURCE)
        
    except Exception as e:
        logger.critical(f"Pipeline failed critically and could not recover: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()