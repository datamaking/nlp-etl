# nlp_etl/targets/datatarget.py
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class DataTarget(ABC):
    @abstractmethod
    def write_data(self, df: DataFrame):
        pass