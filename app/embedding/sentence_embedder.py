# nlp_etl/app/embedding/sentence_embedder.py

"""Generates sentence embeddings (mock implementation)."""
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer
from pyspark.sql.functions import udf
import pandas as pd

from app.embedding.embedder import Embedder
from pyspark.sql import DataFrame


class SentenceEmbedder(Embedder):
    def __init__(self, model_name):
        self.model_name = model_name  # Store the model name, not the model itself

    def embed(self, df):
        # Define a Pandas UDF to generate embeddings
        @pandas_udf(ArrayType(FloatType()))
        def embed_udf(text_series: pd.Series) -> pd.Series:
            # Load the model inside the UDF (once per partition)
            print("Debugging")
            print(self.model_name)
            model = SentenceTransformer(self.model_name)
            # Generate embeddings for the entire series
            embeddings = model.encode(text_series.tolist())
            # Return as a pandas Series of lists
            return pd.Series(embeddings.tolist())

        # Apply the UDF to the "chunks" column
        return df.withColumn("embeddings", embed_udf(df["content"]))

"""
class SentenceEmbedder(Embedder):
    def __init__(self):
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        # Define the UDF here
        self.embed_udf = udf(lambda text: self.model.encode(text).tolist(), ArrayType(FloatType()))

    '''@pandas_udf(ArrayType(ArrayType(FloatType())))
    def generate_embeddings(self, chunks_series):
        embeddings = self.model.encode(chunks_series.to_list(), batch_size=32)
        return pd.Series([e.tolist() for e in embeddings])'''

    def embed(self, df: DataFrame) -> DataFrame:
        return df.withColumn("embeddings", self.embed_udf(df["content"]))"""

# Factory
class EmbedderFactory:
    @staticmethod
    def create(config):
        embedders = []
        '''if "sentence" in config.methods:
            embedders.append(SentenceEmbedder())'''
        if "sentence" in config.methods:
            embedders.append(SentenceEmbedder("all-MiniLM-L6-v2"))
        '''if "tfidf" in config.methods:
            embedders.append(TFIDFEmbedder())'''
        return embedders