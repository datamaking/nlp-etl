from setuptools import setup, find_packages

setup(
    name="nlp_etl_pipeline",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "sentence-transformers>=2.2.0",
        "beautifulsoup4>=4.9.0",
        "nltk>=3.5",
        "numpy>=1.19.0",
        "pandas>=1.0.0",
        "pymongo>=4.0.0",
        "psycopg2-binary>=2.8.0",
        "neo4j>=5.0.0",
        "chromadb>=0.4.0",
        "python-dotenv>=1.0.0",
    ],
    python_requires=">=3.8",
    author="NLP ETL Pipeline Team",
    author_email="info@nlpetlpipeline.com",
    description="An enterprise-grade NLP ETL data pipeline built with PySpark",
    keywords="nlp, etl, pyspark, data pipeline",
    url="https://github.com/nlpetlpipeline/nlp-etl-pipeline",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)