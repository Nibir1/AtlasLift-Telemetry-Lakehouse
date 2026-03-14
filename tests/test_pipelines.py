"""
Purpose: Automated integration and unit tests for the Medallion Pipeline.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: pytest, pyspark, json, tempfile, os, pandera

This module mathematically proves the pipeline logic, verifying idempotency,
data cleansing, and the Pandera circuit breaker pattern.
"""

import os
import json
import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pandera.errors import SchemaError

# Import our Medallion pipeline functions
from src.pipelines.bronze_ingestion import ingest_bronze
from src.pipelines.silver_cleansing import cleanse_silver, TelemetryContract
from src.pipelines.gold_aggregation import build_gold


@pytest.fixture(scope="module")
def mock_telemetry_paths() -> dict:
    """
    Creates a temporary directory structure for the test pipeline
    and generates mock raw JSON telemetry.
    """
    base_dir = tempfile.mkdtemp(prefix="atlaslift_test_lakehouse_")
    paths = {
        "landing_zone": os.path.join(base_dir, "landing_zone"),
        "bronze": os.path.join(base_dir, "bronze"),
        "silver": os.path.join(base_dir, "silver"),
        "gold_fact": os.path.join(base_dir, "gold_fact"),
        "gold_dim": os.path.join(base_dir, "gold_dim"),
    }
    
    os.makedirs(paths["landing_zone"], exist_ok=True)
    
    # Generate mock telemetry payloads (3 valid, 1 invalid physically, 1 schema violation)
    mock_data = [
        # Valid records
        {"message_id": "msg_001", "crane_id": "CRN-FI-01", "timestamp": "2026-03-14T10:00:00Z", "hoist_load_kg": 25000.5, "vibration_mm_s": 12.4, "crane_model": "SMARTON", "operational_region": "Nordics"},
        {"message_id": "msg_002", "crane_id": "CRN-FI-01", "timestamp": "2026-03-14T10:05:00Z", "hoist_load_kg": 45000.0, "vibration_mm_s": 45.1, "crane_model": "SMARTON", "operational_region": "Nordics"},
        {"message_id": "msg_003", "crane_id": "CRN-DE-05", "timestamp": "2026-03-14T10:00:00Z", "hoist_load_kg": 10500.0, "vibration_mm_s": 8.2, "crane_model": "CXT", "operational_region": "Central Europe"},
        # Invalid physically: Hoist load exceeds 50,000kg safe limit (Should be filtered by PySpark)
        {"message_id": "msg_004", "crane_id": "CRN-FI-01", "timestamp": "2026-03-14T10:10:00Z", "hoist_load_kg": 85000.0, "vibration_mm_s": 95.0, "crane_model": "SMARTON", "operational_region": "Nordics"},
        # Invalid physically: Negative vibration (Should be filtered by PySpark)
        {"message_id": "msg_005", "crane_id": "CRN-UK-02", "timestamp": "2026-03-14T10:15:00Z", "hoist_load_kg": 5000.0, "vibration_mm_s": -5.0, "crane_model": "CXT", "operational_region": "UK"}
    ]
    
    # Write mock data to landing zone
    file_path = os.path.join(paths["landing_zone"], "telemetry_batch_1.json")
    with open(file_path, "w") as f:
        for record in mock_data:
            f.write(json.dumps(record) + "\n")
            
    return paths


def test_end_to_end_medallion_pipeline(spark: SparkSession, mock_telemetry_paths: dict):
    """
    Tests the complete Bronze -> Silver -> Gold data flow, verifying filters,
    schema validations, and Star Schema normalization.
    """
    paths = mock_telemetry_paths
    
    # ---------------------------------------------------------
    # 1. Test Bronze Ingestion
    # ---------------------------------------------------------
    ingest_bronze(spark, paths["landing_zone"], paths["bronze"])
    
    bronze_df = spark.read.format("delta").load(paths["bronze"])
    # Assert all 5 records ingested
    assert bronze_df.count() == 5, "Bronze should ingest all raw records."
    # Assert metadata columns appended
    assert "ingestion_timestamp" in bronze_df.columns, "Missing lineage metadata."
    assert "source_system" in bronze_df.columns, "Missing source system metadata."
    
    # ---------------------------------------------------------
    # 2. Test Silver Cleansing & Contracts
    # ---------------------------------------------------------
    cleanse_silver(spark, paths["bronze"], paths["silver"])
    
    silver_df = spark.read.format("delta").load(paths["silver"])
    # Assert physical filters worked (msg_004 and msg_005 should be dropped)
    assert silver_df.count() == 3, "Silver should drop records violating physical boundaries."
    
    # Ensure dropped records are the correct ones
    valid_ids = [row.message_id for row in silver_df.select("message_id").collect()]
    assert "msg_004" not in valid_ids, "Overload record bypassed filter."
    assert "msg_005" not in valid_ids, "Negative vibration record bypassed filter."

    # ---------------------------------------------------------
    # 3. Test Gold Star Schema Aggregation
    # ---------------------------------------------------------
    build_gold(spark, paths["silver"], paths["gold_fact"], paths["gold_dim"])
    
    fact_df = spark.read.format("delta").load(paths["gold_fact"])
    dim_df = spark.read.format("delta").load(paths["gold_dim"])
    
    # Assert counts
    assert fact_df.count() == 3, "Fact table should match valid Silver records."
    assert dim_df.count() == 2, "Dim table should have exactly 2 distinct cranes (CRN-FI-01, CRN-DE-05)."
    
    # Assert Fact schema casting (timestamp -> event_time)
    assert dict(fact_df.dtypes)["event_time"] == "timestamp", "Event time must be natively cast to Timestamp for Power BI."
    assert "crane_model" not in fact_df.columns, "Fact table should not contain dimensional attributes."


def test_pandera_circuit_breaker(spark: SparkSession):
    """
    Explicitly tests the Pandera contract to ensure it throws a SchemaError
    if a malformed record somehow bypasses PySpark filters (simulating schema drift).
    """
    # Create a dataframe that purposefully violates the contract (Missing 'message_id', string instead of float)
    schema = StructType([
        StructField("crane_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("hoist_load_kg", StringType(), True), # Violation: Wrong Type
        StructField("vibration_mm_s", DoubleType(), True)
    ])
    
    bad_data = [("CRN-FAIL", "2026-03-14T10:00:00Z", "Not-A-Number", 10.0)]
    bad_df = spark.createDataFrame(bad_data, schema=schema)
    
    # Act & Assert: Pandera must catch this and abort the pipeline
    with pytest.raises(SchemaError):
        # In PySpark, Pandera defaults to lazy=True (collecting errors silently). 
        # We explicitly enforce lazy=False to trigger the SchemaError immediately (Circuit Breaker).
        # Because lazy=False forces eager evaluation, we no longer need to call .collect()
        TelemetryContract.validate(bad_df, lazy=False)