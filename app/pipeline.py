# nlp_etl/app/pipeline.py

"""Main orchestrator for the NLP ETL pipeline."""
import time
import logging
from pyspark.sql import SparkSession
from app.logging_setup import setup_logging
from app.datasources import DataSourceFactory
from preprocessing import PreprocessingFactory
from chunking import ChunkerFactory
from embedding.sentence_embedder import EmbedderFactory
from targets import TargetFactory

class Pipeline:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.appName("NLP_ETL_Pipeline").enableHiveSupport().getOrCreate()
        setup_logging()
        self.logger = logging.getLogger(__name__)
        self.timing = {}

    def run(self):
        df = None
        text_column = self.config.data_source_config.text_column

        # Data Source Stage
        if self.config.data_source_config:
            start = time.time()
            source = DataSourceFactory.create(self.config.data_source_config)
            df = source.read_data(self.spark)
            df.write.parquet("intermediate/source_output", mode="overwrite")
            self.timing["data_source"] = time.time() - start
            self.logger.info("Data source stage completed")

        # Preprocessing Stage
        if self.config.preprocessing_config:
            start = time.time()
            df = self.spark.read.parquet("intermediate/source_output")
            preprocessor = PreprocessingFactory.create(self.config.preprocessing_config)
            df = preprocessor.process(df, text_column)
            print("Post preprocessing: ====================? ")
            df.show()
            df.write.parquet("intermediate/preprocessing_output", mode="overwrite")
            self.timing["preprocessing"] = time.time() - start
            self.logger.info("Preprocessing stage completed")

        # Chunking Stage
        if self.config.chunking_config:
            start = time.time()
            df = self.spark.read.parquet("intermediate/preprocessing_output")
            chunker = ChunkerFactory.create(self.config.chunking_config)
            df = chunker.chunk(df, text_column)
            print("Post chunking: ====================? ")
            df.show()
            df.write.parquet("intermediate/chunking_output", mode="overwrite")
            self.timing["chunking"] = time.time() - start
            self.logger.info("Chunking stage completed")

        # Embedding Stage
        if self.config.embedding_config:
            start = time.time()
            df = self.spark.read.parquet("intermediate/chunking_output")
            print("Before embedding: ====================? ")
            df.show()
            embedders = EmbedderFactory.create(self.config.embedding_config)
            for embedder in embedders:
                df = embedder.embed(df)

            # to be removed
            print("Post embedding: ============================> ")
            df.show()
            df.write.parquet("intermediate/embedding_output", mode="overwrite")
            self.timing["embedding"] = time.time() - start
            self.logger.info("Embedding stage completed")

        # Target Stage
        if self.config.target_configs:
            start = time.time()
            df = self.spark.read.parquet("intermediate/embedding_output")
            for target_config in self.config.target_configs:
                target = TargetFactory.create(target_config)
                target.write_data(df)
            self.timing["targets"] = time.time() - start
            self.logger.info("Target stage completed")

        # Report
        self.logger.info("Execution Time Report:")
        for stage, duration in self.timing.items():
            self.logger.info(f"{stage}: {duration:.2f} seconds")