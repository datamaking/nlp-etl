# nlp_etl/app/utils/helpers.py

"""Utility functions for the pipeline."""
def validate_config(config):
    """Basic validation for pipeline configuration."""
    if not config.data_source_config:
        raise ValueError("Data source configuration is required")
    if not config.data_source_config.text_column:
        raise ValueError("Text column must be specified in data source config")