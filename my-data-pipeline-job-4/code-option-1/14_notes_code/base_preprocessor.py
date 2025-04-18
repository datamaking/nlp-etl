from abc import ABC, abstractmethod
from typing import Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from src.core.context import PipelineContext
from src.core.pipeline import TemplateStage
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BasePreprocessor(TemplateStage, ABC):
    """Base class for all text preprocessing implementations."""
    
    def __init__(self, name: str):
        super().__init__(name)
    
    def process_data(self, context: PipelineContext) -> None:
        """Process the data by applying preprocessing steps."""
        logger.info(f"Applying preprocessing step: {self.name}")
        
        # Get the data from the context
        data = context.get_data()
        
        # Apply the preprocessing
        processed_data = self.preprocess(context, data)
        
        # Update the data in the context
        context.set_data(processed_data)
    
    @abstractmethod
    def preprocess(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Apply preprocessing to the data and return the processed DataFrame."""
        pass
    
    def should_load_data(self, context: PipelineContext) -> bool:
        """Check if data should be loaded from an intermediate source."""
        # If this is the first preprocessing step and there's an intermediate path
        # from the source, we might want to load from there
        if hasattr(context.config.preprocessing_config, 'load_from_intermediate') and \
           context.config.preprocessing_config.load_from_intermediate:
            return True
        return False
    
    def load_data(self, context: PipelineContext) -> None:
        """Load data from an intermediate source."""
        if not self.should_load_data(context):
            return
        
        # Check if we have metadata about the intermediate path
        if 'source_intermediate_path' in context.metadata:
            path = context.metadata['source_intermediate_path']
            logger.info(f"Loading data from intermediate path: {path}")
            
            # Load the data
            data = context.spark.read.parquet(path)
            
            # Set the data in the context
            context.set_data(data)
    
    def should_save_data(self, context: PipelineContext) -> bool:
        """Check if data should be saved to an intermediate target."""
        return context.config.preprocessing_config.intermediate_path is not None
    
    def save_data(self, context: PipelineContext) -> None:
        """Save data to an intermediate target."""
        if not self.should_save_data(context):
            return
        
        intermediate_path = context.config.preprocessing_config.intermediate_path
        logger.info(f"Saving preprocessed data to intermediate location: {intermediate_path}")
        
        data = context.get_data()
        data.write.mode("overwrite").parquet(intermediate_path)
        
        # Add metadata about the saved data
        context.add_metadata("preprocessing_intermediate_path", intermediate_path)
        context.add_metadata("preprocessing_intermediate_format", "parquet")


class PreprocessingChain(BasePreprocessor):
    """Chain of Responsibility pattern implementation for preprocessing steps."""
    
    def __init__(self, name: str):
        super().__init__(name)
        self.preprocessors: List[BasePreprocessor] = []
    
    def add_preprocessor(self, preprocessor: BasePreprocessor) -> 'PreprocessingChain':
        """Add a preprocessor to the chain."""
        self.preprocessors.append(preprocessor)
        return self
    
    def preprocess(self, context: PipelineContext, data: DataFrame) -> DataFrame:
        """Apply all preprocessors in the chain."""
        result = data
        
        for preprocessor in self.preprocessors:
            logger.info(f"Applying preprocessor in chain: {preprocessor.name}")
            result = preprocessor.preprocess(context, result)
        
        return result