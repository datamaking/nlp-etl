from typing import List, Dict, Any

from pyspark.sql import DataFrame, SparkSession

from src.core.context import PipelineContext
from src.source.base_source import BaseSource
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HDFSSource(BaseSource):
    """Source implementation for HDFS files."""
    
    def extract_data(self, context: PipelineContext) -> DataFrame:
        """Extract data from HDFS files."""
        source_config = context.config.source_config
        spark = context.spark
        
        # Get the path and format
        path = source_config.path
        format_type = source_config.format.lower()
        options = source_config.options or {}
        
        logger.info(f"Extracting data from HDFS path: {path} with format: {format_type}")
        
        # Read the data based on the format
        if format_type == "parquet":
            return spark.read.options(**options).parquet(path)
        elif format_type == "csv":
            return spark.read.options(**options).csv(path)
        elif format_type == "json":
            return spark.read.options(**options).json(path)
        elif format_type == "text":
            return spark.read.options(**options).text(path)
        elif format_type == "orc":
            return spark.read.options(**options).orc(path)
        elif format_type == "avro":
            return spark.read.format("avro").options(**options).load(path)
        else:
            raise ValueError(f"Unsupported format type for HDFS source: {format_type}")