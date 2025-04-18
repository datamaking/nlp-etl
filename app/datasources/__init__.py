# nlp_etl/app/datasources/__init__.py
"""Factory for creating data source instances."""
from .hive import HiveDataSource
from .file import FileDataSource
from .rdbms import RDBMSDataSource

class DataSourceFactory:
    """Creates appropriate data source based on configuration."""
    @staticmethod
    def create(config):
        if config.source_type == "hive":
            return HiveDataSource(config)
        elif config.source_type == "file":
            return FileDataSource(config)
        elif config.source_type == "rdbms":
            return RDBMSDataSource(config)
        raise ValueError(f"Unknown source type: {config.source_type}")