"""
Purpose: Cleanse telemetry and enforce Pandera data contracts for the Silver layer.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: pyspark, delta, pandera
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel
import pyspark.sql.types as T

logger = logging.getLogger(__name__)

class TelemetryContract(DataFrameModel):
    """
    Pandera schema defining the absolute truth of valid Silver-layer telemetry.
    If data violates this contract, the pipeline will fail immediately (Circuit Breaker).
    """
    message_id: T.StringType() = pa.Field(nullable=False)
    crane_id: T.StringType() = pa.Field(nullable=False)
    timestamp: T.StringType() = pa.Field(nullable=False)
    # Konecranes industrial business rules: 50,000 kg max safe working load limit.
    hoist_load_kg: T.DoubleType() = pa.Field(ge=0.0, le=50000.0) 
    vibration_mm_s: T.DoubleType() = pa.Field(ge=0.0, le=100.0)

def cleanse_silver(spark: SparkSession, source_path: str, target_path: str) -> None:
    """
    Filters bad records and strictly asserts the TelemetryContract.
    
    Args:
        spark: Active Spark session.
        source_path: Path to the Bronze table.
        target_path: Path to the Silver table.
    """
    try:
        bronze_df = spark.read.format("delta").load(source_path)

        # 1. Cleansing: We drop records violating physical reality to prevent downstream skew.
        cleansed_df = bronze_df.filter(
            col("message_id").isNotNull() &
            col("crane_id").isNotNull() &
            (col("hoist_load_kg") >= 0.0) &
            (col("hoist_load_kg") <= 50000.0) &
            (col("vibration_mm_s") >= 0.0) &
            (col("vibration_mm_s") <= 100.0)
        )

        # 2. Contract Enforcement: Pandera validates the cleansed payload.
        # This acts as a circuit breaker. If our PySpark filters above missed a schema drift
        # (e.g., a new column data type changes), Pandera throws a fatal error here.
        validated_df = TelemetryContract.validate(cleansed_df)

        # 3. Idempotent Target Load
        if DeltaTable.isDeltaTable(spark, target_path):
            target_table = DeltaTable.forPath(spark, target_path)
            target_table.alias("target").merge(
                validated_df.alias("source"),
                "target.message_id = source.message_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info(f"Silver MERGE successful into {target_path}")
        else:
            validated_df.write.format("delta").mode("overwrite").save(target_path)
            logger.info(f"Silver INITIAL LOAD successful into {target_path}")

    except Exception as e:
        logger.error(f"Silver cleansing failed to execute: {str(e)}")
        raise RuntimeError(f"Silver Phase Exception: {str(e)}") from e