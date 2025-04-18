# nlp_etl/app/chunking/sentence_chunker.py

"""Splits text into sentences."""
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import DataFrame
import nltk
nltk.download("punkt", quiet=True)

from .chunker import Chunker

class SentenceChunker(Chunker):
    """Chunks text into sentences using NLTK."""
    def __init__(self, config):
        self.config = config

    @staticmethod
    def split_sentences(text):
        """Split text into sentences."""
        return nltk.sent_tokenize(text) if text else []

    def chunk(self, df: DataFrame, text_column: str) -> DataFrame:
        """Apply sentence chunking to the DataFrame."""
        chunk_udf = udf(self.split_sentences, ArrayType(StringType()))
        return df.withColumn("chunks", chunk_udf(df[text_column]))

# Factory
'''class ChunkerFactory:
    @staticmethod
    def create(config):
        if config.strategy == "sentence":
            return SentenceChunker(config)
        elif config.strategy == "fixed_size":
            return FixedSizeChunker(config)
        raise ValueError("Unknown chunking strategy")'''