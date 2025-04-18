# nlp_etl/chunking/fixed_size_chunker.py

"""Splits text into fixed-size chunks."""
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import DataFrame

from .chunker import Chunker

class FixedSizeChunker(Chunker):
    """Chunks text into fixed-size word segments."""
    def __init__(self, config):
        self.config = config

    def split_fixed_size(self, text):
        """Split text into chunks of fixed size."""
        if not text or not self.config.chunk_size:
            return [text] if text else []
        words = text.split()
        return [
            " ".join(words[i:i + self.config.chunk_size])
            for i in range(0, len(words), self.config.chunk_size)
        ]

    def chunk(self, df: DataFrame, text_column: str) -> DataFrame:
        """Apply fixed-size chunking to the DataFrame."""
        chunk_udf = udf(self.split_fixed_size, ArrayType(StringType()))
        return df.withColumn("chunks", chunk_udf(df[text_column]))