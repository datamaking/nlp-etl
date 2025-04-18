from typing import Dict, Any

from src.core.context import PipelineContext
from src.source.base_source import BaseSource
from src.source.hive_source import HiveSource
from src.source.hdfs_source import HDFSSource
from src.source.file_source import FileSource
from src.source.rdbms_source import RDBMSSource
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SourceFactory:
    """Factory for creating data source instances."""
    
    @staticmethod
    def create_source(context: PipelineContext) -> BaseSource:
        """Create a source instance based on the configuration."""
        source_config = context.config.source_config
        source_type = source_config.type.lower()
        
        logger.info(f"Creating source of type: {source_type}")
        
        if source_type == "hive":
            return HiveSource(source_config.name)
        elif source_type == "hdfs":
            return HDFSSource(source_config.name)
        elif source_type == "file":
            return FileSource(source_config.name)
        elif source_type == "rdbms":
            return RDBMSSource(source_config.name)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")