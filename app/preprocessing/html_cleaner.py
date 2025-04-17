# nlp_etl/preprocessing/html_cleaner.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from bs4 import BeautifulSoup
from app.preprocessing.preprocessor import Preprocessor


class HTMLCleaner:
    @staticmethod
    def clean_html(text):
        return BeautifulSoup(text, "html.parser").get_text() if text else text

    def apply(self, df: DataFrame, text_column: str) -> DataFrame:
        clean_udf = udf(self.clean_html, StringType())
        return df.withColumn("cleaned_text", clean_udf(df[text_column])).drop(text_column).withColumnRenamed("cleaned_text", text_column)

# Factory
class PreprocessingFactory:
    @staticmethod
    def create(config):
        steps = []
        if "html_clean" in config.steps:
            steps.append(HTMLCleaner())
        '''if "text_clean" in config.steps:
            steps.append(TextCleaner())
        if "stopwords" in config.steps:
            steps.append(StopwordRemover())
        if "stemming" in config.steps:
            steps.append(Stemmer())'''
        return Preprocessor(steps)