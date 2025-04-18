from typing import Dict, Any, List, Set
import nltk
from nltk.corpus import stopwords

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from src.core.context import PipelineContext
from src.preprocessing.base_preprocessor import BasePreprocessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StopwordRemover(BasePreprocessor):
    """Preprocessor for removing stopwords from text."""
    
    def __init__(self, name: str):
        super().__init__(name)
        # Download NLTK resources if needed
        try:
            nltk.data.find('corpora/stopwords')
        except LookupError:
            logger.info("Downloading NLTK stopwords")
            nltk.download('stopwords', quiet=True)
    
    def preprocess(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Remove stopwords from text content."""
        logger.info("Removing stopwords from text")
        
        # Get stopwords
        stop_words = set(stopwords.words('english'))
        
        # Add custom stopwords if specified in options
        options = context.config.preprocessing_config.options or {}
        if 'custom_stopwords' in options:
            custom_stopwords = options['custom_stopwords']
            if isinstance(custom_stopwords, list):
                stop_words.update(custom_stopwords)
        
        # Define UDF for stopword removal
        @udf(StringType())
        def remove_stopwords(text):
            if text is None:
                return None
            
            # Split text into words
            words = text.split()
            
            # Remove stopwords
            filtered_words = [word for word in words if word.lower() not in stop_words]
            
            # Join words back into text
            return ' '.join(filtered_words)
        
        # Apply the UDF to the cleaned text column
        result = data.withColumn("text_without_stopwords", remove_stopwords("cleaned_text"))
        
        return result