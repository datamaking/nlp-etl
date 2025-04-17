# nlp_etl/datasources/datasource.py
from abc import ABC, abstractmethod
class DataSource(ABC):
    @abstractmethod
    def read_data(self, spark):
        pass