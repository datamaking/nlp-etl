# nlp_etl/app/preprocessing/preprocessor.py

"""Base class for chaining preprocessing steps."""
from pyspark.sql import DataFrame

class Preprocessor:
    """Chains multiple preprocessing steps."""
    def __init__(self, steps):
        self.steps = steps

    def process(self, df: DataFrame, text_column: str) -> DataFrame:
        """Apply all preprocessing steps sequentially."""
        result_df = df
        for step in self.steps:
            result_df = step.apply(result_df, text_column)
        return result_df