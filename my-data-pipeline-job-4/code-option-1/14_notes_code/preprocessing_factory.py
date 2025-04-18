from typing import Dict, Any, List

from src.core.context import PipelineContext
from src.preprocessing.base_preprocessor import BasePreprocessor, PreprocessingChain
from src.preprocessing.html_parser import HTMLParser
from src.preprocessing.cleaner import TextCleaner
from src.preprocessing.stopword_remover import StopwordRemover
from src.preprocessing.stemmer import Stemmer
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PreprocessingFactory:
    """Factory for creating preprocessing instances."""
    
    @staticmethod
    def create_preprocessor(context: PipelineContext) -> BasePreprocessor:
        """Create a preprocessor chain based on the configuration."""
        preprocessing_config = context.config.preprocessing_config
        steps = preprocessing_config.steps
        options = preprocessing_config.options or {}
        
        logger.info(f"Creating preprocessing chain with steps: {', '.join(steps)}")
        
        # Create a chain of preprocessors
        chain = PreprocessingChain("preprocessing_chain")
        
        # Add preprocessors to the chain based on the configured steps
        for step in steps:
            step_lower = step.lower()
            
            if step_lower == "html_parsing":
                chain.add_preprocessor(HTMLParser("html_parser"))
            elif step_lower == "cleaning":
                chain.add_preprocessor(TextCleaner("text_cleaner"))
            elif step_lower == "stopword_removal":
                chain.add_preprocessor(StopwordRemover("stopword_remover"))
            elif step_lower == "stemming":
                chain.add_preprocessor(Stemmer("stemmer"))
            else:
                logger.warning(f"Unknown preprocessing step: {step}")
        
        return chain