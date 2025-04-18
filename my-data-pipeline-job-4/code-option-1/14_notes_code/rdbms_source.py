from typing import List, Dict, Any

from pyspark.sql import DataFrame, SparkSession

from src.core.context import PipelineContext
from src.source.base_source import JoinableSource
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RDBMSSource(JoinableSource):
    """Source implementation for RDBMS databases."""
    
    def extract_individual_datasets(self, context: PipelineContext) -> List[DataFrame]:
        """Extract data from one or more RDBMS tables or queries."""
        source_config = context.config.source_config
        spark = context.spark
        
        # Get JDBC connection properties
        jdbc_url = source_config.jdbc_url
        jdbc_driver = source_config.jdbc_driver
        credentials = source_config.credentials or {}
        
        # Create connection properties
        connection_properties = {
            "driver": jdbc_driver,
            **credentials
        }
        
        datasets = []
        
        # Check if we're using a query
        if hasattr(source_config, 'query') and source_config.query:
            # Single query
            logger.info(f"Extracting data from RDBMS using query")
            
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({source_config.query}) AS query_table",
                properties=connection_properties
            )
            datasets.append(df)
        
        # Check if we're using tables
        elif hasattr(source_config, 'tables') and source_config.tables:
            # Multiple tables
            logger.info(f"Extracting data from RDBMS tables: {', '.join(source_config.tables)}")
            
            for table in source_config.tables:
                df = spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    properties=connection_properties
                )
                datasets.append(df)
        
        else:
            raise ValueError("No query or tables specified in RDBMS source configuration")
        
        return datasets
    
    def join_datasets(self, context: PipelineContext, datasets: List[DataFrame]) -> DataFrame:
        """Join multiple RDBMS tables into a single DataFrame."""
        source_config = context.config.source_config
        
        if not hasattr(source_config, 'join_conditions') or not source_config.join_conditions:
            raise ValueError("Join conditions must be specified when joining multiple RDBMS tables")
        
        if len(datasets) != len(source_config.tables):
            raise ValueError(f"Number of datasets ({len(datasets)}) does not match number of tables ({len(source_config.tables)})")
        
        # Start with the first dataset
        result = datasets[0]
        
        # Join with the remaining datasets
        for i in range(1, len(datasets)):
            join_condition = source_config.join_conditions[i-1]
            
            # Parse the join condition to get the join columns
            # This is a simplified implementation
            # In a real-world scenario, you would parse the condition more carefully
            join_parts = join_condition.split('=')
            left_col = join_parts[0].strip()
            right_col = join_parts[1].strip()
            
            # Perform the join
            result = result.join(datasets[i], result[left_col] == datasets[i][right_col])
        
        return result