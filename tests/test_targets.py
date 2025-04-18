# nlp_etl/tests/test_targets.py

"""Tests for target module."""
import pytest
from pyspark.sql import SparkSession
from ..app.targets import TargetFactory
from ..app.config import TargetConfig

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()

def test_hive_target(spark_session):
    config = TargetConfig("hive", {"database": "test"}, "test_table")
    target = TargetFactory.create(config)
    df = spark_session.createDataFrame([(1, "text")], ["id", "text"])
    # Just test that write_data runs without error (mocked)
    target.write_data(df)