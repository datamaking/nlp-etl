# nlp_etl/app/preprocessing/__init__.py

"""Factory for creating preprocessing instances."""
from .html_cleaner import HTMLCleaner
from .text_cleaner import TextCleaner
from .stopword_remover import StopwordRemover
from .stemmer import Stemmer
from .preprocessor import Preprocessor

class PreprocessingFactory:
    """Creates a preprocessor chain based on configuration."""
    @staticmethod
    def create(config):
        steps = []
        if "html_clean" in config.steps:
            steps.append(HTMLCleaner())
        if "text_clean" in config.steps:
            steps.append(TextCleaner())
        if "stopwords" in config.steps:
            steps.append(StopwordRemover())
        if "stemming" in config.steps:
            steps.append(Stemmer())
        return Preprocessor(steps)