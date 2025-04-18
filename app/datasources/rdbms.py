# nlp_etl/app/datasources/rdbms.py

"""RDBMS data source implementation."""
from pyspark.sql import DataFrame
from .datasource import DataSource

class RDBMSDataSource(DataSource):
    """Reads data from an RDBMS."""
    def __init__(self, config):
        self.config = config

    def read_data(self, spark) -> DataFrame:
        # Mock implementation; replace with actual JDBC read in production
        return spark.createDataFrame(
            [(1, "Sample text from RDBMS")],
            ["id", self.config.text_column]
        )