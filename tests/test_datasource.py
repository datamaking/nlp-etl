# nlp_etl/tests/test_datasource.py
"""Tests for data source module."""
import pytest

from pyspark.sql import SparkSession
from app.datasources.hive import HiveDataSource
from app.config import DataSourceConfig

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()

def test_hive_datasource(spark_session):
    config = DataSourceConfig("hive", "HR", {"database": "test"}, ["test_table"], text_column="content")
    source = HiveDataSource(config)
    df = source.read_data(spark_session)
    assert "content" in df.columns
    assert df.count() > 0