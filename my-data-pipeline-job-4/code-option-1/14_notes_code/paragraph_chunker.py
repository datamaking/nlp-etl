from typing import Dict, Any, List
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, explode, split, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType

from src.core.context import PipelineContext
from src.chunking.base_chunker import BaseChunker
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ParagraphChunker(BaseChunker):
    """Chunker implementation that splits text into paragraphs."""
    
    def chunk(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Split text into paragraph chunks."""
        logger.info("Chunking text into paragraphs")
        
        # Get the text column to chunk
        text_column = self._get_text_column(context, data)
        
        # Define UDF for paragraph tokenization
        @udf(ArrayType(StringType()))
        def tokenize_paragraphs(text):
            if text is None:
                return []
            
            # Split by double newlines or multiple newlines
            paragraphs = re.split(r'\n\s*\n|\n{2,}', text)
            
            # Filter out empty paragraphs
            paragraphs = [p.strip() for p in paragraphs if p.strip()]
            
            return paragraphs
        
        # Apply the UDF to the text column to get an array of paragraphs
        with_paragraphs = data.withColumn("paragraphs", tokenize_paragraphs(col(text_column)))
        
        # Explode the paragraphs array to get one row per paragraph
        exploded = with_paragraphs.select(
            "*",
            explode("paragraphs").alias("chunk_text")
        )
        
        # Add a chunk ID
        result = exploded.withColumn("chunk_id", monotonically_increasing_id())
        
        # Add metadata about the original document
        result = result.withColumn("source_id", monotonically_increasing_id())
        
        # Select relevant columns
        # We keep the original columns and add the chunk-specific ones
        columns_to_keep = [c for c in data.columns if c != "paragraphs"] + ["chunk_id", "chunk_text", "source_id"]
        result = result.select(*columns_to_keep)
        
        return result
    
    def _get_text_column(self, context: PipelineContext, data: DataFrame) -> str:
        """Determine which column to use for chunking."""
        # Check if we have a specific text column configured
        if hasattr(context.config.chunking_config, 'text_column') and \
           context.config.chunking_config.text_column in data.columns:
            return context.config.chunking_config.text_column
        
        # Check if we have processed text from previous steps
        if "stemmed_text" in data.columns:
            return "stemmed_text"
        if "text_without_stopwords" in data.columns:
            return "text_without_stopwords"
        if "cleaned_text" in data.columns:
            return "cleaned_text"
        if "parsed_text" in data.columns:
            return "parsed_text"
        if "text_content" in data.columns:
            return "text_content"
        
        # Check if we have a department-specific text column
        department = context.config.department
        department_columns = {
            "ADMIN": "admin_notes",
            "HR": "employee_feedback",
            "FINANCE": "transaction_notes",
            "IT_HELPDESK": "ticket_description"
        }
        
        if department in department_columns and department_columns[department] in data.columns:
            return department_columns[department]
        
        # Default to the first string column
        string_columns = [f.name for f in data.schema.fields if isinstance(f.dataType, StringType)]
        if string_columns:
            return string_columns[0]
        
        raise ValueError("No suitable text column found for chunking")