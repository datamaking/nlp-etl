#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# Create top-level project directory
mkdir -p nlp_etl

# Create top-level files
touch nlp_etl/__init__.py
echo "# Configuration classes" > nlp_etl/config.py
echo "# Logging configuration" > nlp_etl/logging_setup.py
echo "# Custom exceptions" > nlp_etl/exceptions.py
echo "# Main pipeline orchestrator" > nlp_etl/pipeline.py

# Create datasources directory and its files
mkdir -p nlp_etl/datasources
touch nlp_etl/datasources/__init__.py
echo "# Abstract DataSource class" > nlp_etl/datasources/datasource.py
echo "# Hive data source" > nlp_etl/datasources/hive.py
echo "# File-based data source" > nlp_etl/datasources/file.py
echo "# RDBMS data source" > nlp_etl/datasources/rdbms.py

# Create preprocessing directory and its files
mkdir -p nlp_etl/preprocessing
touch nlp_etl/preprocessing/__init__.py
echo "# Preprocessor base and chain" > nlp_etl/preprocessing/preprocessor.py
echo "# HTML parsing" > nlp_etl/preprocessing/html_cleaner.py
echo "# Data cleaning" > nlp_etl/preprocessing/text_cleaner.py
echo "# Stopword removal" > nlp_etl/preprocessing/stopword_remover.py
echo "# Stemming" > nlp_etl/preprocessing/stemmer.py

# Create chunking directory and its files
mkdir -p nlp_etl/chunking
touch nlp_etl/chunking/__init__.py
echo "# Abstract Chunker class" > nlp_etl/chunking/chunker.py
echo "# Sentence-based chunking" > nlp_etl/chunking/sentence_chunker.py
echo "# Fixed-size chunking" > nlp_etl/chunking/fixed_size_chunker.py

# Create embedding directory and its files
mkdir -p nlp_etl/embedding
touch nlp_etl/embedding/__init__.py
echo "# Abstract Embedder class" > nlp_etl/embedding/embedder.py
echo "# Sentence embedding" > nlp_etl/embedding/sentence_embedder.py
echo "# TF-IDF embedding" > nlp_etl/embedding/tfidf_embedder.py

# Create targets directory and its files
mkdir -p nlp_etl/targets
touch nlp_etl/targets/__init__.py
echo "# Abstract DataTarget class" > nlp_etl/targets/datatarget.py
echo "# Hive target" > nlp_etl/targets/hive_target.py
echo "# File target" > nlp_etl/targets/file_target.py
echo "# RDBMS target" > nlp_etl/targets/rdbms_target.py
echo "# Vector database target" > nlp_etl/targets/vector_db_target.py

# Create utils directory and its files
mkdir -p nlp_etl/utils
touch nlp_etl/utils/__init__.py
echo "# Helper functions" > nlp_etl/utils/helpers.py

# Create tests directory and its files
mkdir -p nlp_etl/tests
touch nlp_etl/tests/__init__.py
touch nlp_etl/tests/test_datasource.py
touch nlp_etl/tests/test_preprocessing.py
touch nlp_etl/tests/test_chunking.py
touch nlp_etl/tests/test_embedding.py
touch nlp_etl/tests/test_targets.py

# Create remaining top-level files
echo "# Dependencies" > nlp_etl/requirements.txt
echo "# Documentation" > nlp_etl/README.md
echo "# Entry point script" > nlp_etl/run_pipeline.py

echo "Project folder structure for 'nlp_etl' has been successfully created."
