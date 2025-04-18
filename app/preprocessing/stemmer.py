# nlp_etl/app/preprocessing/stemmer.py

"""Applies stemming to text."""
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from nltk.stem import PorterStemmer
from pyspark.sql import DataFrame

class Stemmer:
    """Reduces words to their root form using Porter Stemming."""
    def __init__(self):
        self.stemmer = PorterStemmer()

    def stem(self, text):
        """Stem each word in the text."""
        if not text:
            return text
        return " ".join(self.stemmer.stem(word) for word in text.split())

    def apply(self, df: DataFrame, text_column: str) -> DataFrame:
        """Apply stemming to the DataFrame."""
        stem_udf = udf(self.stem, StringType())
        return df.withColumn(text_column, stem_udf(df[text_column]))