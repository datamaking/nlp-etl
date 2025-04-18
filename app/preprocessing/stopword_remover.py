# nlp_etl/preprocessing/stopword_remover.py

"""Removes stopwords from text."""
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

class StopwordRemover:
    """Removes common stopwords from text."""
    def __init__(self):
        self.stopwords = set(["the", "is", "and", "a", "to"])  # Example stopwords

    def remove_stopwords(self, text):
        """Remove stopwords from the text."""
        if not text:
            return text
        return " ".join(word for word in text.split() if word.lower() not in self.stopwords)

    def apply(self, df: DataFrame, text_column: str) -> DataFrame:
        """Apply stopword removal to the DataFrame."""
        remove_udf = udf(self.remove_stopwords, StringType())
        return df.withColumn(text_column, remove_udf(df[text_column]))