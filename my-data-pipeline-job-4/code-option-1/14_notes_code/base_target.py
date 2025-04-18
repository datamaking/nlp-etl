from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame

from src.core.context import PipelineContext
from src.core.pipeline import TemplateStage
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseTarget(TemplateStage, ABC):
    """Base class for all data target implementations."""
    
    def __init__(self, name: str):
        super().__init__(name)
    
    def process_data(self, context: PipelineContext) -> None:
        """Process the data by writing it to the target."""
        logger.info(f"Writing data to target: {self.name}")
        
        # Get the data from the context
        data = context.get_data()
        
        # Write data based on load type
        load_type = context.config.target_config.load_type.lower()
        
        if load_type == "full":
            self.write_full_load(context, data)
        elif load_type == "incremental_scd2":
            self.write_incremental_scd2(context, data)
        elif load_type == "incremental_cdc":
            self.write_incremental_cdc(context, data)
        else:
            raise ValueError(f"Unsupported load type: {load_type}")
    
    @abstractmethod
    def write_full_load(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data using full load strategy."""
        pass
    
    @abstractmethod
    def write_incremental_scd2(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data using incremental load with SCD Type 2 strategy."""
        pass
    
    @abstractmethod
    def write_incremental_cdc(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data using incremental load with CDC strategy."""
        pass
    
    def should_load_data(self, context: PipelineContext) -> bool:
        """Check if data should be loaded from an intermediate source."""
        # If there's an intermediate path from embedding, we might want to load from there
        if hasattr(context.config.target_config, 'load_from_intermediate') and \
           context.config.target_config.load_from_intermediate:
            return True
        return False
    
    def load_data(self, context: PipelineContext) -> None:
        """Load data from an intermediate source."""
        if not self.should_load_data(context):
            return
        
        # Check if we have metadata about the intermediate path
        if 'embedding_intermediate_path' in context.metadata:
            path = context.metadata['embedding_intermediate_path']
            logger.info(f"Loading data from intermediate path: {path}")
            
            # Load the data
            data = context.spark.read.parquet(path)
            
            # Set the data in the context
            context.set_data(data)