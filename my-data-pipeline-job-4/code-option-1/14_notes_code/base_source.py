from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.core.context import PipelineContext
from src.core.pipeline import TemplateStage
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseSource(TemplateStage, ABC):
    """Base class for all data source implementations."""
    
    def __init__(self, name: str):
        super().__init__(name)
    
    def process_data(self, context: PipelineContext) -> None:
        """Process the data by extracting it from the source."""
        logger.info(f"Extracting data using source: {self.name}")
        
        # Extract data based on configuration
        data = self.extract_data(context)
        
        # Set the extracted data in the context
        context.set_data(data)
        
        # Log data statistics
        self._log_data_statistics(data)
    
    @abstractmethod
    def extract_data(self, context: PipelineContext) -> DataFrame:
        """Extract data from the source and return as a DataFrame."""
        pass
    
    def should_save_data(self, context: PipelineContext) -> bool:
        """Determine if extracted data should be saved to an intermediate location."""
        return context.config.source_config.intermediate_path is not None
    
    def save_data(self, context: PipelineContext) -> None:
        """Save extracted data to an intermediate location."""
        if not self.should_save_data(context):
            return
        
        intermediate_path = context.config.source_config.intermediate_path
        logger.info(f"Saving extracted data to intermediate location: {intermediate_path}")
        
        data = context.get_data()
        data.write.mode("overwrite").parquet(intermediate_path)
        
        # Add metadata about the saved data
        context.add_metadata("source_intermediate_path", intermediate_path)
        context.add_metadata("source_intermediate_format", "parquet")
    
    def _log_data_statistics(self, data: DataFrame) -> None:
        """Log statistics about the extracted data."""
        try:
            count = data.count()
            columns = data.columns
            logger.info(f"Extracted {count} rows with {len(columns)} columns")
            logger.debug(f"Columns: {', '.join(columns)}")
        except Exception as e:
            logger.warning(f"Could not log data statistics: {str(e)}")


class JoinableSource(BaseSource, ABC):
    """Base class for sources that support joining multiple tables/queries."""
    
    def extract_data(self, context: PipelineContext) -> DataFrame:
        """Extract data by joining multiple tables or queries."""
        # Extract individual datasets
        datasets = self.extract_individual_datasets(context)
        
        # If only one dataset, return it directly
        if len(datasets) == 1:
            return datasets[0]
        
        # Join multiple datasets
        return self.join_datasets(  == 1:
            return datasets[0]
        
        # Join multiple datasets
        return self.join_datasets(context, datasets)
    
    @abstractmethod
    def extract_individual_datasets(self, context: PipelineContext) -> List[DataFrame]:
        """Extract individual datasets from the source."""
        pass
    
    @abstractmethod
    def join_datasets(self, context: PipelineContext, datasets: List[DataFrame]) -> DataFrame:
        """Join multiple datasets into a single DataFrame."""
        pass