# nlp_etl/app/chunking/__init__.py

"""Factory for creating chunker instances."""
from .sentence_chunker import SentenceChunker
from .fixed_size_chunker import FixedSizeChunker

class ChunkerFactory:
    """Creates a chunker based on configuration."""
    @staticmethod
    def create(config):
        if config.strategy == "sentence":
            return SentenceChunker(config)
        elif config.strategy == "fixed_size":
            return FixedSizeChunker(config)
        raise ValueError(f"Unknown chunking strategy: {config.strategy}")