from typing import Dict, Any

from src.core.context import PipelineContext
from src.chunking.base_chunker import BaseChunker
from src.chunking.sentence_chunker import SentenceChunker
from src.chunking.paragraph_chunker import ParagraphChunker
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ChunkingFactory:
    """Factory for creating chunking strategy instances."""
    
    @staticmethod
    def create_chunker(context: PipelineContext) -> BaseChunker:
        """Create a chunker instance based on the configuration."""
        chunking_config = context.config.chunking_config
        strategy = chunking_config.strategy.lower()
        
        logger.info(f"Creating chunker with strategy: {strategy}")
        
        if strategy == "sentence":
            return SentenceChunker("sentence_chunker")
        elif strategy == "paragraph":
            return ParagraphChunker("paragraph_chunker")
        else:
            raise ValueError(f"Unsupported chunking strategy: {strategy}")