#!/bin/bash
# This script will create the folder structure for the nlp-etl-pipeline project.
# Run with: ./create_project.sh

# Set base project folder name
PROJECT_DIR="nlp-etl-pipeline"

# Remove the project directory if it already exists
if [ -d "$PROJECT_DIR" ]; then
    echo "Removing existing $PROJECT_DIR folder..."
    rm -rf "$PROJECT_DIR"
fi

# Create the base directory structure
echo "Creating project structure..."
mkdir -p "$PROJECT_DIR"/{src,tests}

# Create files in the root folder
touch "$PROJECT_DIR/README.md"
touch "$PROJECT_DIR/requirements.txt"
touch "$PROJECT_DIR/setup.py"

# Create src folder structure and empty __init__.py files
mkdir -p "$PROJECT_DIR/src"/{config,core,source,preprocessing,chunking,embedding,target,utils}
touch "$PROJECT_DIR/src/__init__.py"
touch "$PROJECT_DIR/src/config/__init__.py"
touch "$PROJECT_DIR/src/core/__init__.py"
touch "$PROJECT_DIR/src/source/__init__.py"
touch "$PROJECT_DIR/src/preprocessing/__init__.py"
touch "$PROJECT_DIR/src/chunking/__init__.py"
touch "$PROJECT_DIR/src/embedding/__init__.py"
touch "$PROJECT_DIR/src/target/__init__.py"
touch "$PROJECT_DIR/src/utils/__init__.py"

# Create files in src/config
cat <<EOL > "$PROJECT_DIR/src/config/pipeline_config.py"
# pipeline_config.py
# This file contains pipeline configuration constants and classes.
EOL
cat <<EOL > "$PROJECT_DIR/src/config/config_loader.py"
# config_loader.py
# This file contains functions to load and parse pipeline configuration.
EOL

# Create files in src/core
cat <<EOL > "$PROJECT_DIR/src/core/pipeline.py"
# pipeline.py
# This file contains the main pipeline execution logic.
EOL
cat <<EOL > "$PROJECT_DIR/src/core/context.py"
# context.py
# This file manages the pipeline context and state.
EOL

# Create files in src/source
cat <<EOL > "$PROJECT_DIR/src/source/source_factory.py"
# source_factory.py
# Factory to create data source objects.
EOL
cat <<EOL > "$PROJECT_DIR/src/source/base_source.py"
# base_source.py
# Abstract base class for data sources.
EOL
cat <<EOL > "$PROJECT_DIR/src/source/hive_source.py"
# hive_source.py
# Implementation for Hive source.
EOL
cat <<EOL > "$PROJECT_DIR/src/source/hdfs_source.py"
# hdfs_source.py
# Implementation for HDFS data source.
EOL
cat <<EOL > "$PROJECT_DIR/src/source/file_source.py"
# file_source.py
# Implementation for local file source.
EOL
cat <<EOL > "$PROJECT_DIR/src/source/rdbms_source.py"
# rdbms_source.py
# Implementation for RDBMS data source.
EOL

# Create files in src/preprocessing
cat <<EOL > "$PROJECT_DIR/src/preprocessing/preprocessing_factory.py"
# preprocessing_factory.py
# Factory to create preprocessor objects.
EOL
cat <<EOL > "$PROJECT_DIR/src/preprocessing/base_preprocessor.py"
# base_preprocessor.py
# Abstract base class for preprocessors.
EOL
cat <<EOL > "$PROJECT_DIR/src/preprocessing/html_parser.py"
# html_parser.py
# HTML parsing functionalities.
EOL
cat <<EOL > "$PROJECT_DIR/src/preprocessing/cleaner.py"
# cleaner.py
# Text cleaning functionalities.
EOL
cat <<EOL > "$PROJECT_DIR/src/preprocessing/stopword_remover.py"
# stopword_remover.py
# Stopword removal functionalities.
EOL
cat <<EOL > "$PROJECT_DIR/src/preprocessing/stemmer.py"
# stemmer.py
# Text stemming functionalities.
EOL

# Create files in src/chunking
cat <<EOL > "$PROJECT_DIR/src/chunking/chunking_factory.py"
# chunking_factory.py
# Factory to create chunker objects.
EOL
cat <<EOL > "$PROJECT_DIR/src/chunking/base_chunker.py"
# base_chunker.py
# Abstract base class for chunkers.
EOL
cat <<EOL > "$PROJECT_DIR/src/chunking/sentence_chunker.py"
# sentence_chunker.py
# Chunker implementation for sentence-level chunking.
EOL
cat <<EOL > "$PROJECT_DIR/src/chunking/paragraph_chunker.py"
# paragraph_chunker.py
# Chunker implementation for paragraph-level chunking.
EOL
cat <<EOL > "$PROJECT_DIR/src/chunking/chunk_smoother.py"
# chunk_smoother.py
# Chunk smoothing logic.
EOL

# Create files in src/embedding
cat <<EOL > "$PROJECT_DIR/src/embedding/embedding_factory.py"
# embedding_factory.py
# Factory to create embedding objects.
EOL
cat <<EOL > "$PROJECT_DIR/src/embedding/base_embedding.py"
# base_embedding.py
# Abstract base class for embedding generators.
EOL
cat <<EOL > "$PROJECT_DIR/src/embedding/tfidf_embedding.py"
# tfidf_embedding.py
# TF-IDF embedding implementation.
EOL
cat <<EOL > "$PROJECT_DIR/src/embedding/sentence_embedding.py"
# sentence_embedding.py
# Sentence embedding implementation.
EOL

# Create files in src/target
cat <<EOL > "$PROJECT_DIR/src/target/target_factory.py"
# target_factory.py
# Factory to create target objects.
EOL
cat <<EOL > "$PROJECT_DIR/src/target/base_target.py"
# base_target.py
# Abstract base class for target adapters.
EOL
cat <<EOL > "$PROJECT_DIR/src/target/hive_target.py"
# hive_target.py
# Hive target adapter implementation.
EOL
cat <<EOL > "$PROJECT_DIR/src/target/hdfs_target.py"
# hdfs_target.py
# HDFS target adapter implementation.
EOL
cat <<EOL > "$PROJECT_DIR/src/target/file_target.py"
# file_target.py
# Local file target adapter implementation.
EOL
cat <<EOL > "$PROJECT_DIR/src/target/rdbms_target.py"
# rdbms_target.py
# RDBMS target adapter implementation.
EOL
cat <<EOL > "$PROJECT_DIR/src/target/vector_db_target.py"
# vector_db_target.py
# Vector database target adapter implementation.
EOL

# Create files in src/utils
cat <<EOL > "$PROJECT_DIR/src/utils/logger.py"
# logger.py
# Logging utilities.
EOL
cat <<EOL > "$PROJECT_DIR/src/utils/exception_handler.py"
# exception_handler.py
# Exception handling logic.
EOL
cat <<EOL > "$PROJECT_DIR/src/utils/timer.py"
# timer.py
# Timer utility functions.
EOL
cat <<EOL > "$PROJECT_DIR/src/utils/circuit_breaker.py"
# circuit_breaker.py
# Circuit breaker implementation.
EOL

# Create src/main.py
cat <<EOL > "$PROJECT_DIR/src/main.py"
#!/usr/bin/env python
# main.py
# Entry point for the NLP ETL pipeline.

def main():
    print("Running NLP ETL Pipeline...")

if __name__ == '__main__':
    main()
EOL
chmod +x "$PROJECT_DIR/src/main.py"

# Create tests folder structure
mkdir -p "$PROJECT_DIR/tests/test_source"
mkdir -p "$PROJECT_DIR/tests/test_preprocessing"
mkdir -p "$PROJECT_DIR/tests/test_chunking"
mkdir -p "$PROJECT_DIR/tests/test_embedding"
mkdir -p "$PROJECT_DIR/tests/test_target"
touch "$PROJECT_DIR/tests/__init__.py"
cat <<EOL > "$PROJECT_DIR/tests/test_pipeline.py"
# test_pipeline.py
# Integration tests for the NLP ETL pipeline.
def test_pipeline():
    assert True  # Replace with actual tests.
EOL

echo "Project structure for $PROJECT_DIR created successfully."
