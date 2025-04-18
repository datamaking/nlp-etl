from typing import Dict, Any, List
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, collect_list
from pyspark.sql.types import ArrayType, FloatType

from src.core.context import PipelineContext
from src.embedding.base_embedding import BaseEmbedding
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TfidfEmbedding(BaseEmbedding):
    """Embedding implementation using TF-IDF."""
    
    def generate_embeddings(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Generate TF-IDF embeddings for the data."""
        logger.info("Generating TF-IDF embeddings")
        
        # Get the text column to use for embeddings
        text_column = "chunk_text" if "chunk_text" in data.columns else self._get_text_column(context, data)
        
        # Collect all text data to fit the TF-IDF vectorizer
        # Note: This is not scalable for very large datasets
        # In a real-world scenario, you might want to use a more distributed approach
        texts = [row[text_column] for row in data.select(text_column).collect()]
        
        # Create and fit the TF-IDF vectorizer
        vectorizer = TfidfVectorizer(max_features=100)  # Limit to 100 features for demonstration
        vectorizer.fit(texts)
        
        # Define UDF for generating TF-IDF embeddings
        @udf(ArrayType(FloatType()))
        def generate_tfidf(text):
            if text is None:
                return None
            
            # Transform the text to TF-IDF vector
            vector = vectorizer.transform([text]).toarray()[0]
            
            # Convert to list for Spark
            return vector.tolist()
        
        # Apply the UDF to generate embeddings
        result = data.withColumn("embedding", generate_tfidf(col(text_column)))
        
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