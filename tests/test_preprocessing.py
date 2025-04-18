# nlp_etl/tests/test_preprocessing.py

"""Tests for preprocessing module."""
import pytest
from pyspark.sql import SparkSession
from ..app.preprocessing import PreprocessingFactory
from ..app.config import PreprocessingConfig

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()

def test_preprocessing_chain(spark_session):
    config = PreprocessingConfig(["text_clean", "stopwords"])
    preprocessor = PreprocessingFactory.create(config)
    df = spark_session.createDataFrame([(1, "The quick and brown fox")], ["id", "text"])
    result = preprocessor.process(df, "text")
    assert "text" in result.columns
    text = result.collect()[0]["text"]
    assert "the" not in text.lower()