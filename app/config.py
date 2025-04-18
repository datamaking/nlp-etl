# nlp_etl/config.py
"""Configuration classes for the NLP ETL pipeline."""

class DataSourceConfig:
    """Configuration for data sources."""
    def __init__(self, source_type, department, connection_details, tables=None, queries=None, join_conditions=None, text_column=None):
        self.source_type = source_type  # "hive", "file", "rdbms"
        self.department = department    # "ADMIN", "HR", "FINANCE", "IT HELPDESK"
        self.connection_details = connection_details
        self.tables = tables or []
        self.queries = queries or []
        self.join_conditions = join_conditions or []
        self.text_column = text_column  # Department-specific text column

class PreprocessingConfig:
    """Configuration for preprocessing steps."""
    def __init__(self, steps):
        self.steps = steps  # List of step names: ["html_clean", "text_clean", "stopwords", "stemming"]

class ChunkingConfig:
    """Configuration for chunking strategies."""
    def __init__(self, strategy, chunk_size=None, overlap=False):
        self.strategy = strategy  # "sentence", "fixed_size"
        self.chunk_size = chunk_size
        self.overlap = overlap    # Smoothing via overlap

class EmbeddingConfig:
    """Configuration for embedding methods."""
    def __init__(self, methods):
        self.methods = methods  # ["sentence", "tfidf"]

class TargetConfig:
    """Configuration for data targets."""
    def __init__(self, target_type, connection_details, table_name=None, mode="full"):
        self.target_type = target_type  # "hive", "file", "rdbms", "vector_db"
        self.connection_details = connection_details
        self.table_name = table_name
        self.mode = mode  # "full", "scd2", "cdc"

class PipelineConfig:
    """Overall pipeline configuration."""
    def __init__(self, data_source_config, preprocessing_config=None, chunking_config=None, embedding_config=None, target_configs=None):
        self.data_source_config = data_source_config
        self.preprocessing_config = preprocessing_config
        self.chunking_config = chunking_config
        self.embedding_config = embedding_config
        self.target_configs = target_configs or []

# Example configuration
example_config = PipelineConfig(
    data_source_config=DataSourceConfig(
        source_type="hive",
        department="HR",
        connection_details={"database": "hr_db"},
        tables=["employee_feedback"],
        text_column="feedback_text"
    ),
    preprocessing_config=PreprocessingConfig(["html_clean", "text_clean", "stopwords"]),
    chunking_config=ChunkingConfig(strategy="sentence"),
    embedding_config=EmbeddingConfig(["sentence"]),
    target_configs=[TargetConfig("hive", {"database": "processed_db"}, "processed_feedback")]
)