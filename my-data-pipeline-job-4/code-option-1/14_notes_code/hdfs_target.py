from typing import Dict, Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp

from src.core.context import PipelineContext
from src.target.base_target import BaseTarget
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HDFSTarget(BaseTarget):
    """Target implementation for HDFS files."""
    
    def write_full_load(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to HDFS using full load strategy."""
        target_config = context.config.target_config
        
        # Get target path and format
        path = target_config.path
        format_type = target_config.format.lower()
        options = target_config.options or {}
        
        logger.info(f"Writing data to HDFS path {path} with format {format_type} using full load strategy")
        
        # Write data based on the format
        if format_type == "parquet":
            data.write.options(**options).mode("overwrite").parquet(path)
        elif format_type == "csv":
            data.write.options(**options).mode("overwrite").csv(path)
        elif format_type == "json":
            data.write.options(**options).mode("overwrite").json(path)
        elif format_type == "orc":
            data.write.options(**options).mode("overwrite").orc(path)
        elif format_type == "avro":
            data.write.format("avro").options(**options).mode("overwrite").save(path)
        else:
            raise ValueError(f"Unsupported format type for HDFS target: {format_type}")
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to HDFS path {path}")
    
    def write_incremental_scd2(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to HDFS using incremental load with SCD Type 2 strategy."""
        target_config = context.config.target_config
        
        # Get target path and format
        path = target_config.path
        format_type = target_config.format.lower()
        options = target_config.options or {}
        
        logger.info(f"Writing data to HDFS path {path} with format {format_type} using SCD Type 2 strategy")
        
        # For HDFS, we'll implement a simplified SCD Type 2 approach
        # We'll create a new directory with the current timestamp and write the full dataset there
        
        # Generate a timestamp for the new directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_path = f"{path}/{timestamp}"
        
        # Add SCD Type 2 tracking columns
        data_with_tracking = data.withColumn("effective_start_date", current_timestamp()) \
                                 .withColumn("effective_end_date", lit(None).cast("timestamp")) \
                                 .withColumn("is_current", lit(True))
        
        # Write data to the new directory
        if format_type == "parquet":
            data_with_tracking.write.options(**options).parquet(new_path)
        elif format_type == "csv":
            data_with_tracking.write.options(**options).csv(new_path)
        elif format_type == "json":
            data_with_tracking.write.options(**options).json(new_path)
        elif format_type == "orc":
            data_with_tracking.write.options(**options).orc(new_path)
        elif format_type == "avro":
            data_with_tracking.write.format("avro").options(**options).save(new_path)
        else:
            raise ValueError(f"Unsupported format type for HDFS target: {format_type}")
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to HDFS path {new_path}")
        
        # Create a _SUCCESS file to indicate the write is complete
        context.spark.createDataFrame([("success",)], ["status"]).write.mode("overwrite").csv(f"{new_path}/_SUCCESS")
    
    def write_incremental_cdc(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to HDFS using incremental load with CDC strategy."""
        target_config = context.config.target_config
        
        # Get target path and format
        path = target_config.path
        format_type = target_config.format.lower()
        options = target_config.options or {}
        
        logger.info(f"Writing data to HDFS path {path} with format {format_type} using CDC strategy")
        
        # For HDFS, we'll implement a simplified CDC approach
        # We'll create a new directory with the current timestamp and write the changes there
        
        # Generate a timestamp for the new directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        changes_path = f"{path}/changes/{timestamp}"
        
        # Add CDC tracking columns
        data_with_tracking = data.withColumn("cdc_operation", lit("UPSERT")) \
                                 .withColumn("cdc_timestamp", current_timestamp())
        
        # Write data to the changes directory
        if format_type == "parquet":
            data_with_tracking.write.options(**options).parquet(changes_path)
        elif format_type == "csv":
            data_with_tracking.write.options(**options).csv(changes_path)
        elif format_type == "json":
            data_with_tracking.write.options(**options).json(changes_path)
        elif format_type == "orc":
            data_with_tracking.write.options(**options).orc(changes_path)
        elif format_type == "avro":
            data_with_tracking.write.format("avro").options(**options).save(changes_path)
        else:
            raise ValueError(f"Unsupported format type for HDFS target: {format_type}")
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to HDFS changes path {changes_path}")
        
        # Create a _SUCCESS file to indicate the write is complete
        context.spark.createDataFrame([("success",)], ["status"]).write.mode("overwrite").csv(f"{changes_path}/_SUCCESS")