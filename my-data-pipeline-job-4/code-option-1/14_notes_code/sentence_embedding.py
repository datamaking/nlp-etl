from typing import Dict, Any, List, Iterator
import numpy as np
from sentence_transformers import SentenceTransformer
import torch

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd

from src.core.context import PipelineContext
from src.embedding.base_embedding import BaseEmbedding
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SentenceEmbedding(BaseEmbedding):
    """Embedding implementation using sentence transformers."""
    
    def __init__(self, name: str, model_name: str = "google/st5-base", batch_size: int = 32):
        super().__init__(name)
        self.model_name = model_name
        self.batch_size = batch_size
        self._model = None
    
    @property
    def model(self):
        """Lazy-load the model."""
        if self._model is None:
            logger.info(f"Loading sentence transformer model: {self.model_name}")
            self._model = SentenceTransformer(self.model_name)
        return self._model
    
    def generate_embeddings(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Generate sentence embeddings for the data."""
        logger.info(f"Generating sentence embeddings using model: {self.model_name}")
        
        # Get the text column to use for embeddings
        text_column = "chunk_text" if "chunk_text" in data.columns else self._get_text_column(context, data)
        
        # Define pandas UDF for batch processing
        @pandas_udf(ArrayType(FloatType()))
        def generate_embeddings_batch(texts: pd.Series) -> pd.Series:
            # Convert to list
            text_list = texts.tolist()
            
            # Generate embeddings in batches
            all_embeddings = []
            for i in range(0, len(text_list), self.batch_size):
                batch = text_list[i:i + self.batch_size]
                # Filter out None values
                batch = [t for t in batch if t is not None]
                if batch:
                    embeddings = self.model.encode(batch, show_progress_bar=False)
                    all_embeddings.extend(embeddings.tolist())
                else:
                    all_embeddings.extend([[0.0] * self.model.get_sentence_embedding_dimension()] * len(batch))
            
            # Handle any None values that were filtered out
            result = []
            j = 0
            for t in text_list:
                if t is not None:
                    result.append(all_embeddings[j])
                    j += 1
                else:
                    result.append(None)
            
            return pd.Series(result)
        
        # Apply the UDF to generate embeddings
        result = data.withColumn("embedding", generate_embeddings_batch(col(text_column)))
        
        return result
    
    def _get_text_column(self, context: PipelineContext, data: DataFrame) -> str:
        """Determine which column to use for embeddings."""
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
        
        raise ValueError("No suitable text column found for embeddings")