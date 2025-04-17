# nlp_etl/preprocessing/preprocessor.py
from pyspark.sql import DataFrame
class Preprocessor:
    def __init__(self, steps):
        self.steps = steps

    def process(self, df: DataFrame, text_column: str) -> DataFrame:
        result_df = df
        for step in self.steps:
            result_df = step.apply(result_df, text_column)
        return result_df