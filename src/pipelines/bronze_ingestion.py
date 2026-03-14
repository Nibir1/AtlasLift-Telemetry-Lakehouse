"""
Purpose: Ingest raw telemetry JSON into the Bronze Delta layer idempotently.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: pyspark, delta
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

def ingest_bronze(spark: SparkSession, raw_data_path: str, target_path: str) -> None:
    """
    Reads raw JSON payloads, appends metadata, and merges into the Bronze Delta table.
    
    Args:
        spark (SparkSession): The active Spark session.
        raw_data_path (str): The URI/path to the landing zone containing raw JSON.
        target_path (str): The URI/path to the target Bronze Delta table.
        
    Returns:
        None
        
    Raises:
        RuntimeError: If schema reading fails or the Delta merge transaction aborts.
        
    Complexity:
        Time: O(N) where N is the volume of raw JSON messages.
        Space: Distributed across Spark executors.
    """
    try:
        # We use standard JSON reading to ensure local cross-platform compatibility.
        # In a pure Databricks environment, this would utilize Autoloader format("cloudFiles").
        raw_df = spark.read.format("json").load(raw_data_path)
        
        # We append lineage and auditing metadata immediately upon ingestion.
        bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                          .withColumn("source_system", lit("crane_iot_sensors"))

        # Idempotent load: If the table exists, update changed records and insert new ones.
        if DeltaTable.isDeltaTable(spark, target_path):
            target_table = DeltaTable.forPath(spark, target_path)
            target_table.alias("target").merge(
                bronze_df.alias("source"),
                "target.message_id = source.message_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info(f"Bronze MERGE successful into {target_path}")
        else:
            # Bootstrapping the initial table creation
            bronze_df.write.format("delta").mode("overwrite").save(target_path)
            logger.info(f"Bronze INITIAL LOAD successful into {target_path}")
            
    except Exception as e:
        logger.error(f"Bronze ingestion pipeline failed: {str(e)}")
        raise RuntimeError(f"Bronze Phase Exception: {str(e)}") from e