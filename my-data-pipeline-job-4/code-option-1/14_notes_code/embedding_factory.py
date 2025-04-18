from typing import Dict, Any

from src.core.context import PipelineContext
from src.embedding.base_embedding import BaseEmbedding
from src.embedding.tfidf_embedding import TfidfEmbedding
from src.embedding.sentence_embedding import SentenceEmbedding
from src.utils.logger import get_logger

logger = get_logger(__name__)


class EmbeddingFactory:
    """Factory for creating embedding instances."""
    
    @staticmethod
    def create_embedding(context: PipelineContext) -> BaseEmbedding:
        """Create an embedding instance based on the configuration."""
        embedding_config = context.config.embedding_config
        embedding_type = embedding_config.type.lower()
        
        logger.info(f"Creating embedding of type: {embedding_type}")
        
        if embedding_type == "tfidf":
            return TfidfEmbedding("tfidf_embedding")
        elif embedding_type == "sentence":
            model = embedding_config.model or "google/st5-base"
            batch_size = embedding_config.batch_size or 32
            return SentenceEmbedding("sentence_embedding", model, batch_size)
        else:
            raise ValueError(f"Unsupported embedding type: {embedding_type}")