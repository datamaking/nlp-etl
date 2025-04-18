from typing import Dict, Any, List
import os
from bs4 import BeautifulSoup

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.core.context import PipelineContext
from src.source.base_source import BaseSource
from src.utils.logger import get_logger

logger = get_logger(__name__)


class FileSource(BaseSource):
    """Source implementation for local files."""
    
    def extract_data(self, context: PipelineContext) -> DataFrame:
        """Extract data from local files."""
        source_config = context.config.source_config
        spark = context.spark
        
        # Get the path and format
        path = source_config.path
        format_type = source_config.format.lower()
        options = source_config.options or {}
        
        logger.info(f"Extracting data from local file path: {path} with format: {format_type}")
        
        # Read the data based on the format
        if format_type == "parquet":
            return spark.read.options(**options).parquet(path)
        elif format_type == "csv":
            return spark.read.options(**options).csv(path)
        elif format_type == "json":
            return spark.read.options(**options).json(path)
        elif format_type == "text":
            return spark.read.options(**options).text(path)
        elif format_type == "html":
            # For HTML files, we need special handling
            return self._read_html_files(context, path)
        else:
            raise ValueError(f"Unsupported format type for file source: {format_type}")
    
    def _read_html_files(self, context: PipelineContext, path: str) -> DataFrame:
        """Read HTML files and convert to a DataFrame."""
        spark = context.spark
        
        # Check if path is a directory or a single file
        if os.path.isdir(path):
            # Get all HTML files in the directory
            html_files = [os.path.join(path, f) for f in os.listdir(path) 
                         if f.endswith('.html') or f.endswith('.htm')]
        else:
            # Single file
            html_files = [path]
        
        if not html_files:
            raise ValueError(f"No HTML files found at path: {path}")
        
        # Read each HTML file and extract content
        data = []
        for file_path in html_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Extract text content using BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                text_content = soup.get_text(separator=' ', strip=True)
                
                # Add to data list
                data.append((file_path, html_content, text_content))
            except Exception as e:
                logger.warning(f"Error reading HTML file {file_path}: {str(e)}")
        
        # Create DataFrame
        schema = StructType([
            StructField("file_path", StringType(), False),
            StructField("html_content", StringType(), False),
            StructField("text_content", StringType(), False)
        ])
        
        return spark.createDataFrame(data, schema)