from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.core.context import PipelineContext
from src.source.base_source import JoinableSource
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HiveSource(JoinableSource):
    """Source implementation for Hive tables."""
    
    def extract_individual_datasets(self, context: PipelineContext) -> List[DataFrame]:
        """Extract data from one or more Hive tables."""
        source_config = context.config.source_config
        spark = context.spark
        
        # Enable Hive support
        spark.sql("SET spark.sql.catalogImplementation=hive")
        
        # Check if we're using a table or tables
        if hasattr(source_config, 'table') and source_config.table:
            # Single table
            logger.info(f"Extracting data from Hive table: {source_config.database}.{source_config.table}")
            
            # Build the query
            query = f"SELECT * FROM {source_config.database}.{source_config.table}"
            
            # Execute the query
            df = spark.sql(query)
            return [df]
        
        elif hasattr(source_config, 'tables') and source_config.tables:
            # Multiple tables
            logger.info(f"Extracting data from multiple Hive tables: {', '.join(source_config.tables)}")
            
            # Extract each table
            datasets = []
            for table in source_config.tables:
                query = f"SELECT * FROM {source_config.database}.{table}"
                df = spark.sql(query)
                datasets.append(df)
            
            return datasets
        
        else:
            raise ValueError("No table or tables specified in Hive source configuration")
    
    def join_datasets(self, context: PipelineContext, datasets: List[DataFrame]) -> DataFrame:
        """Join multiple Hive tables into a single DataFrame."""
        source_config = context.config.source_config
        
        if not hasattr(source_config, 'join_conditions') or not source_config.join_conditions:
            raise ValueError("Join conditions must be specified when joining multiple Hive tables")
        
        if len(datasets) != len(source_config.tables):
            raise ValueError(f"Number of datasets ({len(datasets)}) does not match number of tables ({len(source_config.tables)})")
        
        # Register temporary views for each dataset
        for i, df in enumerate(datasets):
            view_name = f"temp_view_{i}"
            df.createOrReplaceTempView(view_name)
        
        # Build the join query
        join_query = self._build_join_query(source_config.tables, source_config.join_conditions)
        
        # Execute the join query
        logger.info(f"Executing join query: {join_query}")
        result = context.spark.sql(join_query)
        
        return result
    
    def _build_join_query(self, tables: List[str], join_conditions: List[str]) -> str:
        """Build a SQL query to join multiple tables."""
        # This is a simplified implementation
        # In a real-world scenario, you would build a more sophisticated query
        
        # Start with the first table
        query = f"SELECT * FROM temp_view_0"
        
        # Add joins for the remaining tables
        for i in range(1, len(tables)):
            query += f" JOIN temp_view_{i} ON {join_conditions[i-1]}"
        
        return query