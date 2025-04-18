# NLP ETL Data Pipeline

An enterprise-grade NLP ETL data pipeline built with PySpark that supports multiple data sources, preprocessing steps, chunking strategies, embedding methods, and target systems.

## Project Structure

\`\`\`
nlp-etl-pipeline/
├── README.md
├── requirements.txt
├── setup.py
├── src/
│   ├── config/          # Configuration classes
│   ├── core/            # Core pipeline components
│   ├── source/          # Data source modules
│   ├── preprocessing/   # Text preprocessing modules
│   ├── chunking/        # Text chunking modules
│   ├── embedding/       # Vector embedding modules
│   ├── target/          # Data target modules
│   ├── utils/           # Utility modules
│   └── main.py          # Main entry point
└── tests/               # Unit tests
\`\`\`

## Features

- **Modular Design**: Each component is designed to be interchangeable and configurable
- **Multiple Data Sources**: Support for Hive, HDFS, local files, and RDBMS
- **Preprocessing**: HTML parsing, data cleaning, stopword removal, stemming
- **Chunking**: Multiple chunking strategies with smoothing
- **Embedding**: TF-IDF and sentence embeddings (Google ST5)
- **Multiple Targets**: Hive, HDFS, local files, RDBMS, and vector databases (ChromaDB, PostgreSQL, Neo4j)
- **Load Strategies**: Full load, incremental load with SCD Type 2, and CDC
- **Department-Specific Processing**: Support for ADMIN, HR, FINANCE, and IT HELPDESK data
- **Performance Tracking**: Execution time tracking for each module

## Design Patterns Used

- Pipeline Pattern
- Strategy Pattern
- Factory Pattern
- Adapter Pattern
- Decorator Pattern
- Chain of Responsibility
- Template Method Pattern
- Observer Pattern
- Circuit Breaker Pattern
- Configuration Pattern

## Installation

\`\`\`bash
pip install -r requirements.txt
python setup.py install
\`\`\`

## Usage

### Basic Usage

```python
from src.main import run_pipeline
from src.config.pipeline_config import PipelineConfig

# Create a pipeline configuration
config = PipelineConfig(
    department="HR",
    source_config={
        "type": "hive",
        "table": "hr_data",
    },
    preprocessing_config={
        "steps": ["html_parsing", "cleaning", "stopword_removal", "stemming"]
    },
    chunking_config={
        "strategy": "sentence",
        "smooth": True
    },
    embedding_config={
        "type": "sentence",
        "model": "google/st5-base"
    },
    target_config={
        "type": "vector_db",
        "db": "chroma",
        "load_type": "full"
    }
)

# Run the pipeline
run_pipeline(config)