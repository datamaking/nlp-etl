# run_pipeline.py
"""Entry point script to run the NLP ETL pipeline."""
from app.config import PipelineConfig, DataSourceConfig, PreprocessingConfig, ChunkingConfig, EmbeddingConfig, TargetConfig
from app.pipeline import Pipeline

# Example configuration
config = PipelineConfig(
    data_source_config=DataSourceConfig(
        source_type="hive",
        department="HR",
        connection_details={"database": "docs_db"},
        tables=["docs_db.ag_news_tbl"],
        text_column="content"
    ),
    preprocessing_config=PreprocessingConfig(["html_clean", "text_clean", "stopwords"]),
    chunking_config=ChunkingConfig(strategy="sentence"),
    embedding_config=EmbeddingConfig(["sentence"]),
    target_configs=[
        TargetConfig("hive", {"database": "processed_db"}, "processed_feedback")
    ]
)

# Run the pipeline
pipeline = Pipeline(config)
pipeline.run()