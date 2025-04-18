from typing import Dict, Any
import nltk
from nltk.stem import PorterStemmer

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from src.core.context import PipelineContext
from src.preprocessing.base_preprocessor import BasePreprocessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Stemmer(BasePreprocessor):
    """Preprocessor for stemming text."""
    
    def __init__(self, name: str):
        super().__init__(name)
        self.stemmer = PorterStemmer()
    
    def preprocess(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Apply stemming to text content."""
        logger.info("Applying stemming to text")
        
        # Define UDF for stemming
        @udf(StringType())
        def stem_text(text):
            if text is None:
                return None
            
            # Split text into words
            words = text.split()
            
            # Apply stemming to each word
            stemmed_words = [self.stemmer.stem(word) for word in words]
            
            # Join words back into text
            return ' '.join(stemmed_words)
        
        # Determine which column to use for stemming
        input_column = "text_without_stopwords" if "text_without_stopwords" in data.columns else "cleaned_text"
        
        # Apply the UDF to the input column
        result = data.withColumn("stemmed_text", stem_text(input_column))
        
        return result