from pyspark.sql import DataFrame

# nlp_etl/chunking/chunker.py
from abc import ABC, abstractmethod
class Chunker(ABC):
    @abstractmethod
    def chunk(self, df: DataFrame, text_column: str) -> DataFrame:
        pass