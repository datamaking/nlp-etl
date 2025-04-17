# nlp_etl/embedding/embedder.py
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class Embedder(ABC):
    @abstractmethod
    def embed(self, df: DataFrame) -> DataFrame:
        pass