# nlp_etl/app/datasources/datasource.py

"""Abstract base class for data sources."""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class DataSource(ABC):
    """Abstract class defining the interface for data sources."""
    @abstractmethod
    def read_data(self, spark) -> DataFrame:
        """Read data and return a PySpark DataFrame."""
        pass