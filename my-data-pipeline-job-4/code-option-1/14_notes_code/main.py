import argparse
import os
import sys
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession

from src.config.pipeline_config import PipelineConfig
from src.config.config_loader import ConfigLoader
from src.core.pipeline import Pipeline
from src.source.source_factory import SourceFactory
from src.preprocessing.preprocessing_factory import PreprocessingFactory
from src.chunking.chunking_factory import ChunkingFactory
from src.embedding.embedding_factory import EmbeddingFactory
from src.target.target_factory import TargetFactory
from src.utils.logger import get_logger, configure_logger

logger = get_logger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def build_pipeline(config: PipelineConfig, spark: SparkSession) -> Pipeline:
    """Build the pipeline with all stages."""
    logger.info("Building pipeline")
    
    # Create the pipeline
    pipeline = Pipeline(config, spark)
    
    # Add source stage
    source = SourceFactory.create_source(pipeline.context)
    pipeline.add_stage(source)
    
    # Add preprocessing stage
    preprocessor = PreprocessingFactory.create_preprocessor(pipeline.context)
    pipeline.add_stage(preprocessor)
    
    # Add chunking stage
    chunker = ChunkingFactory.create_chunker(pipeline.context)
    pipeline.add_stage(chunker)
    
    # Add embedding stage
    embedding = EmbeddingFactory.create_embedding(pipeline.context)
    pipeline.add_stage(embedding)
    
    # Add target stage
    target = TargetFactory.create_target(pipeline.context)
    pipeline.add_stage(target)
    
    return pipeline


def run_pipeline(config: PipelineConfig, spark: Optional[SparkSession] = None) -> None:
    """Run the pipeline with the specified configuration."""
    logger.info(f"Running pipeline: {config.pipeline_name}")
    
    # Create Spark session if not provided
    if spark is None:
        spark = create_spark_session(config.pipeline_name)
    
    # Build the pipeline
    pipeline = build_pipeline(config, spark)
    
    # Run the pipeline
    context = pipeline.run()
    
    # Print execution report
    if config.track_execution_time:
        report = context.get_execution_report()
        logger.info(f"Pipeline execution completed. Total time: {report['total_time']:.2f} seconds")
        for stage_name, time in report['stages'].items():
            logger.info(f"  {stage_name}: {time:.2f} seconds")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="NLP ETL Pipeline")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--department", type=str, help="Department (ADMIN, HR, FINANCE, IT_HELPDESK)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--log-file", type=str, help="Path to log file")
    
    return parser.parse_args()


def main():
    """Main entry point."""
    # Parse command line arguments
    args = parse_args()
    
    # Configure logging
    configure_logger(args.log_level, args.log_file)
    
    # Load configuration
    if args.config:
        # Load from config file
        config = ConfigLoader.from_json(args.config)
    elif args.department:
        # Load default config for department
        config = ConfigLoader.get_default_config(args.department)
    else:
        # Try to load from environment variables
        config = ConfigLoader.from_env()
    
    # Run the pipeline
    run_pipeline(config)


if __name__ == "__main__":
    main()