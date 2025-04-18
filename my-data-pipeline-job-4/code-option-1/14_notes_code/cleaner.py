import re
from typing import Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from src.core.context import PipelineContext
from src.preprocessing.base_preprocessor import BasePreprocessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TextCleaner(BasePreprocessor):
    """Preprocessor for cleaning text content."""
    
    def preprocess(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Clean text content in the data."""
        logger.info("Cleaning text content")
        
        # Determine which column to clean
        text_column = self._get_text_column(context, data)
        
        # Define UDF for text cleaning
        @udf(StringType())
        def clean_text(text):
            if text is None:
                return None
            
            # Convert to lowercase
            text = text.lower()
            
            # Remove URLs
            text = re.sub(r'https?://\S+|www\.\S+', ' ', text)
            
            # Remove HTML tags
            text = re.sub(r'<.*?>', ' ', text)
            
            # Remove special characters and numbers
            text = re.sub(r'[^a-zA-Z\s]', ' ', text)
            
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text
        
        # Apply the UDF to the text column
        result = data.withColumn("cleaned_text", clean_text(col(text_column)))
        
        return result
    
    def _get_text_column(self, context: PipelineContext, data: DataFrame) -> str:
        """Determine which column to use for text cleaning."""
        # Check if we have parsed text from HTML
        if "parsed_text" in data.columns:
            return "parsed_text"
        
        # Check if we have text content
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
        
        raise ValueError("No suitable text column found for cleaning")