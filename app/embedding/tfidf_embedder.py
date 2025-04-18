# nlp_etl/embedding/tfidf_embedder.py
"""Generates TF-IDF embeddings (mock implementation)."""
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql import DataFrame
from .embedder import Embedder

class TFIDFEmbedder(Embedder):
    """Creates mock TF-IDF embeddings."""
    @staticmethod
    def mock_tfidf(chunks):
        """Generate mock TF-IDF vectors (100-dim)."""
        return [[0.2] * 100 for _ in chunks] if chunks else []

    def embed(self, df: DataFrame) -> DataFrame:
        """Apply mock TF-IDF embeddings to the DataFrame."""
        tfidf_udf = udf(self.mock_tfidf, ArrayType(ArrayType(FloatType())))
        return df.withColumn("tfidf", tfidf_udf(df["chunks"]))