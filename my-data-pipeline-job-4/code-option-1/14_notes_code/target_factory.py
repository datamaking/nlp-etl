from typing import Dict, Any

from src.core.context import PipelineContext
from src.target.base_target import BaseTarget
from src.target.hive_target import HiveTarget
from src.target.hdfs_target import HDFSTarget
from src.target.file_target import FileTarget
from src.target.rdbms_target import RDBMSTarget
from src.target.vector_db_target import VectorDBTarget
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TargetFactory:
    """Factory for creating data target instances."""
    
    @staticmethod
    def create_target(context: PipelineContext) -> BaseTarget:
        """Create a target instance based on the configuration."""
        target_config = context.config.target_config
        target_type = target_config.type.lower()
        
        logger.info(f"Creating target of type: {target_type}")
        
        if target_type == "hive":
            return HiveTarget(target_config.name)
        elif target_type == "hdfs":
            return HDFSTarget(target_config.name)
        elif target_type == "file":
            return FileTarget(target_config.name)
        elif target_type == "rdbms":
            return RDBMSTarget(target_config.name)
        elif target_type == "vector_db":
            return VectorDBTarget(target_config.name)
        else:
            raise ValueError(f"Unsupported target type: {target_type}")