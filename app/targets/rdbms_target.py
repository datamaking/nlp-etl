# nlp_etl/app/targets/rdbms_target.py

"""RDBMS target implementation."""
from .datatarget import DataTarget
from pyspark.sql import DataFrame

class RDBMSTarget(DataTarget):
    """Writes data to an RDBMS table."""
    def __init__(self, config):
        self.config = config

    def write_data(self, df: DataFrame):
        # Mock implementation; replace with actual JDBC write
        print(f"Writing to RDBMS table {self.config.table_name}")