# nlp_etl/app/chunking/chunker.py

"""Abstract base class for chunking strategies."""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class Chunker(ABC):
    """Abstract class defining the interface for chunkers."""
    @abstractmethod
    def chunk(self, df: DataFrame, text_column: str) -> DataFrame:
        """Split text into chunks."""
        pass