# nlp_etl/tests/test_datasource.py
import pytest
from pyspark.sql import SparkSession
from nlp_etl.datasources.hive import HiveDataSource

def test_hive_datasource(spark_session):
    config = DataSourceConfig("hive", "HR", {"database": "test"}, ["test_table"], text_column="text")
    source = HiveDataSource(config)
    # Mock Spark table read
    df = source.read_data(spark_session)
    assert "text" in df.columns