from typing import Dict, Any, List
import re
import nltk
from nltk.tokenize import sent_tokenize

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, explode, split, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType

from src.core.context import PipelineContext
from src.chunking.base_chunker import BaseChunker
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SentenceChunker(BaseChunker):
    """Chunker implementation that splits text into sentences."""
    
    def __init__(self, name: str):
        super().__init__(name)
        # Download NLTK resources if needed
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            logger.info("Downloading NLTK punkt tokenizer")
            nltk.download('punkt', quiet=True)
    
    def chunk(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Split text into sentence chunks."""
        logger.info("Chunking text into sentences")
        
        # Get the text column to chunk
        text_column = self._get_text_column(context, data)
        
        # Define UDF for sentence tokenization
        @udf(ArrayType(StringType()))
        def tokenize_sentences(text):
            if text is None:
                return []
            
            # Use NLTK's sentence tokenizer
            sentences = sent_tokenize(text)
            
            return sentences
        
        # Apply the UDF to the text column to get an array of sentences
        with_sentences = data.withColumn("sentences", tokenize_sentences(col(text_column)))
        
        # Explode the sentences array to get one row per sentence
        exploded = with_sentences.select(
            "*",
            explode("sentences").alias("chunk_text")
        )
        
        # Add a chunk ID
        result = exploded.withColumn("chunk_id", monotonically_increasing_id())
        
        # Add metadata about the original document
        result = result.withColumn("source_id", monotonically_increasing_id())
        
        # Select relevant columns
        # We keep the original columns and add the chunk-specific ones
        columns_to_keep = [c for c in data.columns if c != "sentences"] + ["chunk_id", "chunk_text", "source_id"]
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