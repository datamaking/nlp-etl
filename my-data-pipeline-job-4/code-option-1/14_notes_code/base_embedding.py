from abc import ABC, abstractmethod
from typing import Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, FloatType

from src.core.context import PipelineContext
from src.core.pipeline import TemplateStage
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseEmbedding(TemplateStage, ABC):
    """Base class for all embedding implementations."""
    
    def __init__(self, name: str):
        super().__init__(name)
    
    def process_data(self, context: PipelineContext) -> None:
        """Process the data by generating embeddings."""
        logger.info(f"Generating embeddings using: {self.name}")
        
        # Get the data from the context
        data = context.get_data()
        
        # Generate embeddings
        data_with_embeddings = self.generate_embeddings(context, data)
        
        # Update the data in the context
        context.set_data(data_with_embeddings)
    
    @abstractmethod
    def generate_embeddings(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Generate embeddings for the data and return the updated DataFrame."""
        pass
    
    def should_load_data(self, context: PipelineContext) -> bool:
        """Check if data should be loaded from an intermediate source."""
        # If there's an intermediate path from chunking, we might want to load from there
        if hasattr(context.config.embedding_config, 'load_from_intermediate') and \
           context.config.embedding_config.load_from_intermediate:
            return True
        return False
    
    def load_data(self, context: PipelineContext) -> None:
        """Load data from an intermediate source."""
        if not self.should_load_data(context):
            return
        
        # Check if we have metadata about the intermediate path
        if 'chunking_intermediate_path' in context.metadata:
            path = context.metadata['chunking_intermediate_path']
            logger.info(f"Loading data from intermediate path: {path}")
            
            # Load the data
            data = context.spark.read.parquet(path)
            
            # Set the data in the context
            context.set_data(data)
    
    def should_save_data(self, context: PipelineContext) -> bool:
        """Check if data should be saved to an intermediate target."""
        return context.config.embedding_config.intermediate_path is not None
    
    def save_data(self, context: PipelineContext) -> None:
        """Save data to an intermediate target."""
        if not self.should_save_data(context):
            return
        
        intermediate_path = context.config.embedding_config.intermediate_path
        logger.info(f"Saving embeddings to intermediate location: {intermediate_path}")
        
        data = context.get_data()
        data.write.mode("overwrite").parquet(intermediate_path)
        
        # Add metadata about the saved data
        context.add_metadata("embedding_intermediate_path", intermediate_path)
        context.add_metadata("embedding_intermediate_format", "parquet")