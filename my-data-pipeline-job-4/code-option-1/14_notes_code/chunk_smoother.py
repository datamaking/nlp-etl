from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, lit

from src.core.context import PipelineContext
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ChunkSmoother:
    """Utility class for smoothing chunks."""
    
    @staticmethod
    def smooth_chunks(data: DataFrame, min_length: int = 50, max_length: int = 1000) -> DataFrame:
        """Apply smoothing to chunks based on length constraints."""
        logger.info(f"Smoothing chunks with min_length={min_length}, max_length={max_length}")
        
        # Filter out chunks that are too short or too long
        result = data.filter(
            (col("chunk_text").isNotNull()) &
            (length(col("chunk_text")) >= min_length) &
            (length(col("chunk_text")) &lt;= max_length)
        )
        
        # Log statistics
        total_chunks = data.count()
        smoothed_chunks = result.count()
        logger.info(f"Chunk smoothing: {total_chunks} chunks before, {smoothed_chunks} chunks after")
        
        return result
    
    @staticmethod
    def merge_short_chunks(data: DataFrame, min_length: int = 50) -> DataFrame:
        """Merge short chunks with adjacent chunks."""
        logger.info(f"Merging short chunks with threshold={min_length}")
        
        # This is a simplified implementation
        # In a real-world scenario, you would implement a more sophisticated merging algorithm
        
        # Identify short chunks
        short_chunks = data.filter(length(col("chunk_text")) &lt; min_length)
        
        # If no short chunks, return the original data
        if short_chunks.count() == 0:
            return data
        
        # For demonstration purposes, we'll just mark short chunks
        result = data.withColumn(
            "is_short_chunk",
            length(col("chunk_text")) &lt; min_length
        )
        
        # In a real implementation, you would merge short chunks with adjacent chunks
        # This would require window functions and more complex logic
        
        return result
    
    @staticmethod
    def split_long_chunks(data: DataFrame, max_length: int = 1000, overlap: int = 100) -> DataFrame:
        """Split long chunks into smaller chunks with overlap."""
        logger.info(f"Splitting long chunks with max_length={max_length}, overlap={overlap}")
        
        # This is a simplified implementation
        # In a real-world scenario, you would implement a more sophisticated splitting algorithm
        
        # Identify long chunks
        long_chunks = data.filter(length(col("chunk_text")) > max_length)
        
        # If no long chunks, return the original data
        if long_chunks.count() == 0:
            return data
        
        # For demonstration purposes, we'll just mark long chunks
        result = data.withColumn(
            "is_long_chunk",
            length(col("chunk_text")) > max_length
        )
        
        # In a real implementation, you would split long chunks into smaller chunks
        # This would require UDFs and more complex logic
        
        return result