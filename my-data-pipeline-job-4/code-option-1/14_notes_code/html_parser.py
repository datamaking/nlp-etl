from typing import Dict, Any
from bs4 import BeautifulSoup

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from src.core.context import PipelineContext
from src.preprocessing.base_preprocessor import BasePreprocessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HTMLParser(BasePreprocessor):
    """Preprocessor for parsing HTML content."""
    
    def preprocess(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Parse HTML content in the data."""
        logger.info("Parsing HTML content")
        
        # Check if the data contains HTML content
        if "html_content" not in data.columns:
            logger.warning("No 'html_content' column found in the data, skipping HTML parsing")
            return data
        
        # Define UDF for HTML parsing
        @udf(StringType())
        def parse_html(html_content):
            if html_content is None:
                return None
            
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                return soup.get_text(separator=' ', strip=True)
            except Exception as e:
                logger.warning(f"Error parsing HTML: {str(e)}")
                return html_content
        
        # Apply the UDF to the HTML content column
        result = data.withColumn("parsed_text", parse_html("html_content"))
        
        return result