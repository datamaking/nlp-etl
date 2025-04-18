from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union


@dataclass
class SourceConfig:
    """Configuration for data source module."""
    type: str  # hive, hdfs, file, rdbms
    # Common parameters
    name: str
    intermediate_path: Optional[str] = None
    
    # Type-specific parameters
    # For hive
    database: Optional[str] = None
    table: Optional[str] = None
    
    # For HDFS/file
    path: Optional[str] = None
    format: Optional[str] = None  # csv, json, parquet, text, html
    options: Dict[str, str] = field(default_factory=dict)
    
    # For RDBMS
    jdbc_url: Optional[str] = None
    jdbc_driver: Optional[str] = None
    query: Optional[str] = None
    tables: List[str] = field(default_factory=list)
    join_conditions: List[str] = field(default_factory=list)
    credentials: Dict[str, str] = field(default_factory=dict)


@dataclass
class PreprocessingConfig:
    """Configuration for preprocessing module."""
    steps: List[str] = field(default_factory=list)  # html_parsing, cleaning, stopword_removal, stemming
    options: Dict[str, Any] = field(default_factory=dict)
    intermediate_path: Optional[str] = None


@dataclass
class ChunkingConfig:
    """Configuration for chunking module."""
    strategy: str  # sentence, paragraph, fixed_size, sliding_window
    text_column: str
    smooth: bool = False
    options: Dict[str, Any] = field(default_factory=dict)
    intermediate_path: Optional[str] = None


@dataclass
class EmbeddingConfig:
    """Configuration for embedding module."""
    type: str  # tfidf, sentence
    model: Optional[str] = None  # For sentence embeddings
    batch_size: int = 32
    options: Dict[str, Any] = field(default_factory=dict)
    intermediate_path: Optional[str] = None


@dataclass
class TargetConfig:
    """Configuration for target module."""
    type: str  # hive, hdfs, file, rdbms, vector_db
    name: str
    load_type: str = "full"  # full, incremental_scd2, incremental_cdc
    
    # Common parameters
    options: Dict[str, Any] = field(default_factory=dict)
    
    # Type-specific parameters
    # For hive
    database: Optional[str] = None
    table: Optional[str] = None
    
    # For HDFS/file
    path: Optional[str] = None
    format: Optional[str] = None  # csv, json, parquet
    
    # For RDBMS
    jdbc_url: Optional[str] = None
    jdbc_driver: Optional[str] = None
    table: Optional[str] = None
    credentials: Dict[str, str] = field(default_factory=dict)
    
    # For vector DB
    db: Optional[str] = None  # chroma, postgres, neo4j
    collection: Optional[str] = None
    vector_column: Optional[str] = None
    metadata_columns: List[str] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Main configuration for the NLP ETL pipeline."""
    department: str  # ADMIN, HR, FINANCE, IT_HELPDESK
    pipeline_name: str = "nlp_etl_pipeline"
    
    # Module configurations
    source_config: Union[SourceConfig, Dict[str, Any]]
    preprocessing_config: Union[PreprocessingConfig, Dict[str, Any]]
    chunking_config: Union[ChunkingConfig, Dict[str, Any]]
    embedding_config: Union[EmbeddingConfig, Dict[str, Any]]
    target_config: Union[TargetConfig, Dict[str, Any]]
    
    # Pipeline options
    track_execution_time: bool = True
    log_level: str = "INFO"
    
    def __post_init__(self):
        """Convert dictionary configs to dataclass instances if needed."""
        if isinstance(self.source_config, dict):
            self.source_config = SourceConfig(**self.source_config)
        
        if isinstance(self.preprocessing_config, dict):
            self.preprocessing_config = PreprocessingConfig(**self.preprocessing_config)
        
        if isinstance(self.chunking_config, dict):
            self.chunking_config = ChunkingConfig(**self.chunking_config)
        
        if isinstance(self.embedding_config, dict):
            self.embedding_config = EmbeddingConfig(**self.embedding_config)
        
        if isinstance(self.target_config, dict):
            self.target_config = TargetConfig(**self.target_config)