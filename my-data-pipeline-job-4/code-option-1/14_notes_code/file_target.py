from typing import Dict, Any
from datetime import datetime
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp

from src.core.context import PipelineContext
from src.target.base_target import BaseTarget
from src.utils.logger import get_logger

logger = get_logger(__name__)


class FileTarget(BaseTarget):
    """Target implementation for local files."""
    
    def write_full_load(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to local files using full load strategy."""
        target_config = context.config.target_config
        
        # Get target path and format
        path = target_config.path
        format_type = target_config.format.lower()
        options = target_config.options or {}
        
        logger.info(f"Writing data to local file path {path} with format {format_type} using full load strategy")
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Write data based on the format
        if format_type == "parquet":
            data.write.options(**options).mode("overwrite").parquet(path)
        elif format_type == "csv":
            data.write.options(**options).mode("overwrite").csv(path)
        elif format_type == "json":
            data.write.options(**options).mode("overwrite").json(path)
        elif format_type == "text":
            # For text files, we need to ensure we have a single column
            if len(data.columns) > 1:
                # If we have multiple columns, convert to a single column
                from pyspark.sql.functions import concat_ws
                columns = data.columns
                data = data.select(concat_ws(",", *columns).alias("text"))
            
            data.write.options(**options).mode("overwrite").text(path)
        else:
            raise ValueError(f"Unsupported format type for file target: {format_type}")
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to local file path {path}")
    
    def write_incremental_scd2(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to local files using incremental load with SCD Type 2 strategy."""
        target_config = context.config.target_config
        
        # Get target path and format
        path = target_config.path
        format_type = target_config.format.lower()
        options = target_config.options or {}
        
        logger.info(f"Writing data to local file path {path} with format {format_type} using SCD Type 2 strategy")
        
        # For local files, we'll implement a simplified SCD Type 2 approach
        # We'll create a new directory with the current timestamp and write the full dataset there
        
        # Generate a timestamp for the new directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_path = f"{os.path.dirname(path)}/{timestamp}"
        
        # Ensure the directory exists
        os.makedirs(new_path, exist_ok=True)
        
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
        elif format_type == "text":
            # For text files, we need to ensure we have a single column
            if len(data_with_tracking.columns) > 1:
                # If we have multiple columns, convert to a single column
                from pyspark.sql.functions import concat_ws
                columns = data_with_tracking.columns
                data_with_tracking = data_with_tracking.select(concat_ws(",", *columns).alias("text"))
            
            data_with_tracking.write.options(**options).text(new_path)
        else:
            raise ValueError(f"Unsupported format type for file target: {format_type}")
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to local file path {new_path}")
        
        # Create a _SUCCESS file to indicate the write is complete
        with open(f"{new_path}/_SUCCESS", "w") as f:
            f.write("success")
    
    def write_incremental_cdc(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to local files using incremental load with CDC strategy."""
        target_config = context.config.target_config
        
        # Get target path and format
        path = target_config.path
        format_type = target_config.format.lower()
        options = target_config.options or {}
        
        logger.info(f"Writing data to local file path {path} with format {format_type} using CDC strategy")
        
        # For local files, we'll implement a simplified CDC approach
        # We'll create a new directory with the current timestamp and write the changes there
        
        # Generate a timestamp for the new directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        changes_path = f"{os.path.dirname(path)}/changes/{timestamp}"
        
        # Ensure the directory exists
        os.makedirs(changes_path, exist_ok=True)
        
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
        elif format_type == "text":
            # For text files, we need to ensure we have a single column
            if len(data_with_tracking.columns) > 1:
                # If we have multiple columns, convert to a single column
                from pyspark.sql.functions import concat_ws
                columns = data_with_tracking.columns
                data_with_tracking = data_with_tracking.select(concat_ws(",", *columns).alias("text"))
            
            data_with_tracking.write.options(**options).text(changes_path)
        else:
            raise ValueError(f"Unsupported format type for file target: {format_type}")
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to local file changes path {changes_path}")
        
        # Create a _SUCCESS file to indicate the write is complete
        with open(f"{changes_path}/_SUCCESS", "w") as f:
            f.write("success")