import pytest
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.config.pipeline_config import PipelineConfig, PreprocessingConfig
from src.core.context import PipelineContext
from src.preprocessing.cleaner import TextCleaner


@pytest.fixture
def spark_session():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder
        .appName("test_cleaner")
        .master("local[1]")
        .getOrCreate()
    )


@pytest.fixture
def pipeline_context(spark_session):
    """Create a pipeline context for testing."""
    preprocessing_config = PreprocessingConfig(
        steps=["cleaning"],
        options={}
    )
    
    config = PipelineConfig(
        department="HR",
        pipeline_name="test_pipeline",
        source_config={},
        preprocessing_config=preprocessing_config,
        chunking_config={},
        embedding_config={},
        target_config={}
    )
    
    return PipelineContext(
        config=config,
        spark=spark_session
    )


def test_preprocess_with_text_content(spark_session, pipeline_context):
    """Test preprocessing with text_content column."""
    # Create a test DataFrame
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("text_content", StringType(), True)
    ])
    
    data = [
        (1, "This is a test with URL: https://example.com and HTML <b>tags</b>."),
        (2, "Another test with numbers 123 and special chars !@#$.")
    ]
    
    df = spark_session.createDataFrame(data, schema)
    
    # Set the DataFrame in the context
    pipeline_context.set_data(df)
    
    # Create the TextCleaner
    cleaner = TextCleaner("test_cleaner")
    
    # Process the data
    cleaner.process_data(pipeline_context)
    
    # Get the processed data
    result = pipeline_context.get_data()
    
    # Verify the result
    assert result is not None
    assert "cleaned_text" in result.columns
    
    # Collect the results for verification
    rows = result.collect()
    
    # Check that the text has been cleaned
    assert "url" in rows[0]["cleaned_text"]
    assert "tags" in rows[0]["cleaned_text"]
    assert "html" not in rows[0]["cleaned_text"].lower()  # HTML tags should be removed
    assert "https" not in rows[0]["cleaned_text"].lower()  # URLs should be removed
    
    assert "another test with numbers" in rows[1]["cleaned_text"].lower()
    assert "123" not in rows[1]["cleaned_text"]  # Numbers should be removed
    assert "!@#$" not in rows[1]["cleaned_text"]  # Special chars should be removed


def test_preprocess_with_department_specific_column(spark_session, pipeline_context):
    """Test preprocessing with department-specific column."""
    # Create a test DataFrame
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("employee_feedback", StringType(), True)  # HR department column
    ])
    
    data = [
        (1, "This is employee feedback with URL: https://example.com."),
        (2, "Another feedback with numbers 123 and special chars !@#$.")
    ]
    
    df = spark_session.createDataFrame(data, schema)
    
    # Set the DataFrame in the context
    pipeline_context.set_data(df)
    
    # Create the TextCleaner
    cleaner = TextCleaner("test_cleaner")
    
    # Process the data
    cleaner.process_data(pipeline_context)
    
    # Get the processed data
    result = pipeline_context.get_data()
    
    # Verify the result
    assert result is not None
    assert "cleaned_text" in result.columns
    
    # Collect the results for verification
    rows = result.collect()
    
    # Check that the text has been cleaned
    assert "employee feedback" in rows[0]["cleaned_text"].lower()
    assert "https" not in rows[0]["cleaned_text"].lower()  # URLs should be removed
    
    assert "another feedback" in rows[1]["cleaned_text"].lower()
    assert "123" not in rows[1]["cleaned_text"]  # Numbers should be removed
    assert "!@#$" not in rows[1]["cleaned_text"]  # Special chars should be removed