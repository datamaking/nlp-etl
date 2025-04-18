# nlp_etl/app/preprocessing/text_cleaner.py

"""Basic text cleaning (e.g., remove punctuation)."""
from pyspark.sql.functions import regexp_replace
from pyspark.sql import DataFrame

class TextCleaner:
    """Cleans text by removing punctuation and special characters."""
    def apply(self, df: DataFrame, text_column: str) -> DataFrame:
        """Remove non-alphanumeric characters except spaces."""
        return df.withColumn(
            text_column,
            regexp_replace(df[text_column], "[^a-zA-Z0-9 ]", "")
        )