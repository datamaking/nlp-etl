# nlp_etl/embedding/__init__.py

"""Factory for creating embedder instances."""
from .sentence_embedder import SentenceEmbedder
from .tfidf_embedder import TFIDFEmbedder

class EmbedderFactory:
    """Creates embedders based on configuration."""
    @staticmethod
    def create(config):
        embedders = []
        if "sentence" in config.methods:
            embedders.append(SentenceEmbedder())
        if "tfidf" in config.methods:
            embedders.append(TFIDFEmbedder())
        return embedders