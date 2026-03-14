"""
Purpose: Transform cleansed telemetry into a Power BI-optimized Star Schema.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: pyspark, delta
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

def build_gold(spark: SparkSession, source_path: str, fact_path: str, dim_path: str) -> None:
    """
    Extracts dimensions and formats fact data for DirectQuery consumption.
    
    Args:
        spark: Active Spark session.
        source_path: Path to the Silver table.
        fact_path: Path to the Gold Fact table.
        dim_path: Path to the Gold Dimension table.
    """
    try:
        silver_df = spark.read.format("delta").load(source_path)

        # 1. Dim Crane Generation (Type 1 Slowly Changing Dimension concept)
        # We extract unique crane attributes. In a real environment, this might be joined with an ERP DB.
        dim_crane_df = silver_df.select(
            "crane_id", 
            "crane_model", 
            "operational_region"
        ).distinct()

        if DeltaTable.isDeltaTable(spark, dim_path):
            dim_table = DeltaTable.forPath(spark, dim_path)
            dim_table.alias("target").merge(
                dim_crane_df.alias("source"),
                "target.crane_id = source.crane_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            dim_crane_df.write.format("delta").mode("overwrite").save(dim_path)

        # 2. Fact Telemetry Formatting
        # We cast the string timestamp to a native PySpark TimestampType. 
        # This is mandatory for Power BI's native Time Intelligence DAX functions to work.
        fact_df = silver_df.select(
            col("message_id"),
            col("crane_id"),
            to_timestamp(col("timestamp")).alias("event_time"),
            col("hoist_load_kg"),
            col("vibration_mm_s")
        )

        if DeltaTable.isDeltaTable(spark, fact_path):
            fact_table = DeltaTable.forPath(spark, fact_path)
            fact_table.alias("target").merge(
                fact_df.alias("source"),
                "target.message_id = source.message_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            fact_df.write.format("delta").mode("overwrite").save(fact_path)
            
        logger.info("Gold Star Schema generation completed successfully.")

    except Exception as e:
        logger.error(f"Gold aggregation failed to execute: {str(e)}")
        raise RuntimeError(f"Gold Phase Exception: {str(e)}") from e