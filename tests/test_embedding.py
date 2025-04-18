# nlp_etl/tests/test_embedding.py

"""Tests for embedding module."""
import pytest
from pyspark.sql import SparkSession
from ..app.embedding import EmbedderFactory
from ..app.config import EmbeddingConfig

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()

def test_sentence_embedder(spark_session):
    config = EmbeddingConfig(["sentence"])
    embedders = EmbedderFactory.create(config)
    df = spark_session.createDataFrame([(1, ["sentence one", "sentence two"])], ["id", "chunks"])
    result = embedders[0].embed(df)
    embeddings = result.collect()[0]["embeddings"]
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 384  # Mock embedding size