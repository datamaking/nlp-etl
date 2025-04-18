from abc import ABC, abstractmethod
from typing import Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType

from src.core.context import PipelineContext
from src.core.pipeline import TemplateStage
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseChunker(TemplateStage, ABC):
    """Base class for all text chunking implementations."""
    
    def __init__(self, name: str):
        super().__init__(name)
    
    def process_data(self, context: PipelineContext) -> None:
        """Process the data by applying chunking."""
        logger.info(f"Applying chunking strategy: {self.name}")
        
        # Get the data from the context
        data = context.get_data()
        
        # Apply chunking
        chunked_data = self.chunk(context, data)
        
        # Apply smoothing if configured
        if context.config.chunking_config.smooth:
            chunked_data = self.smooth_chunks(context, chunked_data)
        
        # Update the data in the context
        context.set_data(chunked_data)
    
    @abstractmethod
    def chunk(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Apply chunking to the data and return the chunked DataFrame."""
        pass
    
    def smooth_chunks(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Apply smoothing to the chunks."""
        logger.info("Applying chunk smoothing")
        
        # This is a basic implementation of chunk smoothing
        # In a real-world scenario, you might want to implement more sophisticated smoothing
        
        # Check if we have the required columns
        if "chunk_text" not in data.columns or "chunk_id" not in data.columns:
            logger.warning("Required columns for smoothing not found, skipping smoothing")
            return data
        
        # Get smoothing options
        options = context.config.chunking_config.options or {}
        min_chunk_length = options.get("min_chunk_length", 50)
        max_chunk_length = options.get("max_chunk_length", 1000)
        
        # Filter out chunks that are too short or too long
        result = data.filter(
            (col("chunk_text").isNotNull()) &
            (length(col("chunk_text")) >= min_chunk_length) &
            (length(col("chunk_text")) &lt;= max_chunk_length)
        )
        
        # Log statistics
        total_chunks = data.count()
        smoothed_chunks = result.count()
        logger.info(f"Chunk smoothing: {total_chunks} chunks before, {smoothed_chunks} chunks after")
        
        return result
    
    def should_load_data(self, context: PipelineContext) -> bool:
        """Check if data should be loaded from an intermediate source."""
        # If there's an intermediate path from preprocessing, we might want to load from there
        if hasattr(context.config.chunking_config, 'load_from_intermediate') and \
           context.config.chunking_config.load_from_intermediate:
            return True
        return False
    
    def load_data(self, context: PipelineContext) -> None:
        """Load data from an intermediate source."""
        if not self.should_load_data(context):
            return
        
        # Check if we have metadata about the intermediate path
        if 'preprocessing_intermediate_path' in context.metadata:
            path = context.metadata['preprocessing_intermediate_path']
            logger.info(f"Loading data from intermediate path: {path}")
            
            # Load the data
            data = context.spark.read.parquet(path)
            
            # Set the data in the context
            context.set_data(data)
    
    def should_save_data(self, context: PipelineContext) -> bool:.set_data(data)
    
    def should_save_data(self, context: PipelineContext) -> bool:
        """Check if data should be saved to an intermediate target."""
        return context.config.chunking_config.intermediate_path is not None
    
    def save_data(self, context: PipelineContext) -> None:
        """Save data to an intermediate target."""
        if not self.should_save_data(context):
            return
        
        intermediate_path = context.config.chunking_config.intermediate_path
        logger.info(f"Saving chunked data to intermediate location: {intermediate_path}")
        
        data = context.get_data()
        data.write.mode("overwrite").parquet(intermediate_path)
        
        # Add metadata about the saved data
        context.add_metadata("chunking_intermediate_path", intermediate_path)
        context.add_metadata("chunking_intermediate_format", "parquet")