# nlp_etl/targets/__init__.py

"""Factory for creating target instances."""
from .hive_target import HiveTarget
from .file_target import FileTarget
from .rdbms_target import RDBMSTarget
from .vector_db_target import VectorDBTarget

class TargetFactory:
    """Creates appropriate target based on configuration."""
    @staticmethod
    def create(config):
        if config.target_type == "hive":
            return HiveTarget(config)
        elif config.target_type == "file":
            return FileTarget(config)
        elif config.target_type == "rdbms":
            return RDBMSTarget(config)
        elif config.target_type == "vector_db":
            return VectorDBTarget(config)
        raise ValueError(f"Unknown target type: {config.target_type}")