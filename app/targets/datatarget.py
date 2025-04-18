# nlp_etl/app/targets/datatarget.py

"""Abstract base class for data targets."""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class DataTarget(ABC):
    """Abstract class defining the interface for data targets."""
    @abstractmethod
    def write_data(self, df: DataFrame):
        """Write data to the target."""
        pass