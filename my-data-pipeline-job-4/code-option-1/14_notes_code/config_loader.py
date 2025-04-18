import json
import os
from typing import Dict, Any, Optional
from pathlib import Path

from src.config.pipeline_config import PipelineConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ConfigLoader:
    """Loads configuration from various sources."""
    
    @staticmethod
    def from_json(file_path: str) -> PipelineConfig:
        """Load configuration from a JSON file."""
        logger.info(f"Loading configuration from JSON file: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                config_dict = json.load(f)
            
            return PipelineConfig(**config_dict)
        except Exception as e:
            logger.error(f"Error loading configuration from JSON: {str(e)}")
            raise
    
    @staticmethod
    def from_dict(config_dict: Dict[str, Any]) -> PipelineConfig:
        """Create configuration from a dictionary."""
        logger.info("Creating configuration from dictionary")
        return PipelineConfig(**config_dict)
    
    @staticmethod
    def from_env() -> PipelineConfig:
        """Load configuration from environment variables."""
        logger.info("Loading configuration from environment variables")
        
        # This is a simplified example - in a real implementation,
        # you would parse all the required environment variables
        config_dict = {
            "department": os.environ.get("NLP_ETL_DEPARTMENT", ""),
            "pipeline_name": os.environ.get("NLP_ETL_PIPELINE_NAME", "nlp_etl_pipeline"),
            "source_config": {
                "type": os.environ.get("NLP_ETL_SOURCE_TYPE", ""),
                "name": os.environ.get("NLP_ETL_SOURCE_NAME", ""),
                # Add other source config parameters as needed
            },
            # Add other module configurations as needed
        }
        
        return PipelineConfig(**config_dict)
    
    @staticmethod
    def get_default_config(department: str) -> PipelineConfig:
        """Get default configuration for a specific department."""
        logger.info(f"Getting default configuration for department: {department}")
        
        # Define department-specific configurations
        department_configs = {
            "ADMIN": {
                "source_config": {
                    "type": "hive",
                    "name": "admin_source",
                    "database": "admin_db",
                    "table": "admin_data"
                },
                "chunking_config": {
                    "strategy": "sentence",
                    "text_column": "admin_notes"
                }
            },
            "HR": {
                "source_config": {
                    "type": "hive",
                    "name": "hr_source",
                    "database": "hr_db",
                    "table": "employee_data"
                },
                "chunking_config": {
                    "strategy": "paragraph",
                    "text_column": "employee_feedback"
                }
            },
            "FINANCE": {
                "source_config": {
                    "type": "rdbms",
                    "name": "finance_source",
                    "jdbc_url": "jdbc:postgresql://localhost:5432/finance",
                    "tables": ["transactions", "accounts"]
                },
                "chunking_config": {
                    "strategy": "sentence",
                    "text_column": "transaction_notes"
                }
            },
            "IT_HELPDESK": {
                "source_config": {
                    "type": "file",
                    "name": "helpdesk_source",
                    "path": "/data/helpdesk",
                    "format": "json"
                },
                "chunking_config": {
                    "strategy": "paragraph",
                    "text_column": "ticket_description"
                }
            }
        }
        
        # Get department-specific config or raise error if not found
        if department not in department_configs:
            raise ValueError(f"No default configuration available for department: {department}")
        
        dept_config = department_configs[department]
        
        # Create a complete configuration with defaults
        config_dict = {
            "department": department,
            "pipeline_name": f"{department.lower()}_nlp_pipeline",
            "source_config": dept_config["source_config"],
            "preprocessing_config": {
                "steps": ["html_parsing", "cleaning", "stopword_removal", "stemming"],
                "options": {}
            },
            "chunking_config": dept_config["chunking_config"],
            "embedding_config": {
                "type": "sentence",
                "model": "google/st5-base",
                "batch_size": 32
            },
            "target_config": {
                "type": "vector_db",
                "name": f"{department.lower()}_target",
                "db": "chroma",
                "collection": f"{department.lower()}_embeddings",
                "load_type": "full"
            }
        }
        
        return PipelineConfig(**config_dict)