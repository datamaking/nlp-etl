import pytest
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.config.pipeline_config import PipelineConfig, SourceConfig
from src.core.context import PipelineContext
from src.source.hive_source import HiveSource


@pytest.fixture
def spark_session():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder
        .appName("test_hive_source")
        .master("local[1]")
        .getOrCreate()
    )


@pytest.fixture
def pipeline_context(spark_session):
    """Create a pipeline context for testing."""
    source_config = SourceConfig(
        type="hive",
        name="test_source",
        database="test_db",
        table="test_table"
    )
    
    config = PipelineConfig(
        department="HR",
        pipeline_name="test_pipeline",
        source_config=source_config,
        preprocessing_config={},
        chunking_config={},
        embedding_config={},
        target_config={}
    )
    
    return PipelineContext(
        config=config,
        spark=spark_session
    )


def test_extract_data_single_table(spark_session, pipeline_context):
    """Test extracting data from a single Hive table."""
    # Create a mock DataFrame
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("employee_feedback", StringType(), True)
    ])
    
    mock_data = [
        (1, "John Doe", "Great work environment"),
        (2, "Jane Smith", "Excellent team collaboration")
    ]
    
    mock_df = spark_session.createDataFrame(mock_data, schema)
    
    # Mock the spark.sql method to return our mock DataFrame
    spark_session.sql = MagicMock(return_value=mock_df)
    
    # Create the HiveSource
    hive_source = HiveSource("test_hive_source")
    
    # Extract data
    result = hive_source.extract_data(pipeline_context)
    
    # Verify the result
    assert result is not None
    assert result.count() == 2
    assert len(result.columns) == 3
    
    # Verify that spark.sql was called with the expected query
    spark_session.sql.assert_called_once_with("SELECT * FROM test_db.test_table")


def test_extract_data_multiple_tables(spark_session, pipeline_context):
    """Test extracting data from multiple Hive tables with join."""
    # Update the source config to use multiple tables
    pipeline_context.config.source_config.table = None
    pipeline_context.config.source_config.tables = ["employees", "departments"]
    pipeline_context.config.source_config.join_conditions = ["employees.dept_id = departments.id"]
    
    # Create mock DataFrames
    employees_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("dept_id", IntegerType(), False)
    ])
    
    departments_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False)
    ])
    
    employees_data = [
        (1, "John Doe", 101),
        (2, "Jane Smith", 102)
    ]
    
    departments_data = [
        (101, "HR"),
        (102, "Engineering")
    ]
    
    employees_df = spark_session.createDataFrame(employees_data, employees_schema)
    departments_df = spark_session.createDataFrame(departments_data, departments_schema)
    
    # Mock the spark.sql method to return our mock DataFrames
    spark_session.sql = MagicMock(side_effect=[employees_df, departments_df, employees_df])
    
    # Create the HiveSource
    hive_source = HiveSource("test_hive_source")
    
    # Extract data
    result = hive_source.extract_data(pipeline_context)
    
    # Verify the result
    assert result is not None
    
    # Verify that spark.sql was called with the expected queries
    assert spark_session.sql.call_count == 3
    spark_session.sql.assert_any_call("SELECT * FROM test_db.employees")
    spark_session.sql.assert_any_call("SELECT * FROM test_db.departments")