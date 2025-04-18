# nlp_etl/embedding/embedder.py

"""Abstract base class for embedding methods."""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class Embedder(ABC):
    """Abstract class defining the interface for embedders."""
    @abstractmethod
    def embed(self, df: DataFrame) -> DataFrame:
        """Generate embeddings for text chunks."""
        pass