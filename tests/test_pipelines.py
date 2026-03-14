"""
Purpose: PySpark pipeline tests and test harness validation.
Author: Nahasat Nibir
Date: 2026-03-14
Dependencies: pytest, pyspark

This module contains unit tests for the Medallion pipeline.
"""

from pyspark.sql import SparkSession

def test_local_spark_harness_initialization(spark: SparkSession):
    """
    Verifies that the ephemeral PySpark testing harness boots correctly
    and is configured for Delta Lake.
    
    Args:
        spark (SparkSession): The injected PySpark fixture from conftest.py.
        
    Returns:
        None
        
    Raises:
        AssertionError: If the SparkSession is inactive or missing Delta configuration.
    """
    # Act & Assert
    assert spark is not None, "Spark session failed to initialize."
    
    # In Python, querying the version or appName is the standard way to verify the context is alive
    assert spark.version is not None, "Spark version could not be retrieved; context may be dead."
    assert spark.sparkContext.appName == "AtlasLift-Local-Test-Harness", "Incorrect Spark app name."
    
    # Verify Delta Lake configuration was applied
    spark_conf = dict(spark.sparkContext.getConf().getAll())
    assert spark_conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension", \
        "Delta Spark Session Extension is not configured."