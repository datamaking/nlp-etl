from typing import Dict, Any, List
import numpy as np
import json

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_list, struct, to_json

from src.core.context import PipelineContext
from src.target.base_target import BaseTarget
from src.utils.logger import get_logger

logger = get_logger(__name__)


class VectorDBTarget(BaseTarget):
    """Target implementation for vector databases."""
    
    def write_full_load(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to vector database using full load strategy."""
        target_config = context.config.target_config
        
        # Get vector database type
        db_type = target_config.db.lower()
        
        logger.info(f"Writing data to {db_type} vector database using full load strategy")
        
        # Check if we have embeddings
        if "embedding" not in data.columns:
            raise ValueError("No 'embedding' column found in the data")
        
        # Write to the appropriate vector database
        if db_type == "chroma":
            self._write_to_chroma(context, data, "full")
        elif db_type == "postgres":
            self._write_to_postgres(context, data, "full")
        elif db_type == "neo4j":
            self._write_to_neo4j(context, data, "full")
        else:
            raise ValueError(f"Unsupported vector database type: {db_type}")
    
    def write_incremental_scd2(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to vector database using incremental load with SCD Type 2 strategy."""
        target_config = context.config.target_config
        
        # Get vector database type
        db_type = target_config.db.lower()
        
        logger.info(f"Writing data to {db_type} vector database using SCD Type 2 strategy")
        
        # Check if we have embeddings
        if "embedding" not in data.columns:
            raise ValueError("No 'embedding' column found in the data")
        
        # Write to the appropriate vector database
        if db_type == "chroma":
            self._write_to_chroma(context, data, "scd2")
        elif db_type == "postgres":
            self._write_to_postgres(context, data, "scd2")
        elif db_type == "neo4j":
            self._write_to_neo4j(context, data, "scd2")
        else:
            raise ValueError(f"Unsupported vector database type: {db_type}")
    
    def write_incremental_cdc(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to vector database using incremental load with CDC strategy."""
        target_config = context.config.target_config
        
        # Get vector database type
        db_type = target_config.db.lower()
        
        logger.info(f"Writing data to {db_type} vector database using CDC strategy")
        
        # Check if we have embeddings
        if "embedding" not in data.columns:
            raise ValueError("No 'embedding' column found in the data")
        
        # Write to the appropriate vector database
        if db_type == "chroma":
            self._write_to_chroma(context, data, "cdc")
        elif db_type == "postgres":
            self._write_to_postgres(context, data, "cdc")
        elif db_type == "neo4j":
            self._write_to_neo4j(context, data, "cdc")
        else:
            raise ValueError(f"Unsupported vector database type: {db_type}")
    
    def _write_to_chroma(self, context: PipelineContext, data: DataFrame, load_type: str) -> None:
        """Write data to ChromaDB."""
        import chromadb
        from chromadb.config import Settings
        
        target_config = context.config.target_config
        
        # Get ChromaDB configuration
        collection_name = target_config.collection
        
        # Get metadata columns
        metadata_columns = target_config.metadata_columns or []
        if not metadata_columns and "chunk_text" in data.columns:
            metadata_columns = ["chunk_text"]
        
        logger.info(f"Writing to ChromaDB collection: {collection_name}")
        
        # Create ChromaDB client
        client = chromadb.Client(Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory=f"./chroma_data/{collection_name}"
        ))
        
        # Get or create collection
        try:
            collection = client.get_collection(name=collection_name)
        except:
            collection = client.create_collection(name=collection_name)
        
        # Prepare data for ChromaDB
        # We need to collect the data to the driver
        # This is not scalable for very large datasets
        # In a real-world scenario, you might want to use a more distributed approach
        
        # Convert to pandas for easier processing
        pandas_df = data.toPandas()
        
        # Extract IDs, embeddings, and metadata
        ids = pandas_df["chunk_id"].astype(str).tolist()
        embeddings = pandas_df["embedding"].tolist()
        
        # Extract metadata
        metadatas = []
        for _, row in pandas_df.iterrows():
            metadata = {}
            for col in metadata_columns:
                if col in row:
                    metadata[col] = str(row[col])
            metadatas.append(metadata)
        
        # Extract documents (text)
        documents = pandas_df["chunk_text"].tolist() if "chunk_text" in pandas_df.columns else None
        
        # Handle different load types
        if load_type == "full":
            # Delete all existing data
            collection.delete(where={})
            
            # Add new data
            collection.add(
                ids=ids,
                embeddings=embeddings,
                metadatas=metadatas,
                documents=documents
            )
        elif load_type == "scd2" or load_type == "cdc":
            # For both SCD2 and CDC, we'll use upsert
            # ChromaDB doesn't have a direct SCD2 concept
            
            # Upsert data
            collection.upsert(
                ids=ids,
                embeddings=embeddings,
                metadatas=metadatas,
                documents=documents
            )
        
        # Log statistics
        count = len(ids)
        logger.info(f"Wrote {count} records to ChromaDB collection {collection_name}")
    
    def _write_to_postgres(self, context: PipelineContext, data: DataFrame, load_type: str) -> None:
        """Write data to PostgreSQL with vector extension."""
        target_config = context.config.target_config
        
        # Get PostgreSQL configuration
        jdbc_url = target_config.jdbc_url
        jdbc_driver = target_config.jdbc_driver
        table = target_config.table
        credentials = target_config.credentials or {}
        vector_column = target_config.vector_column or "embedding_vector"
        
        logger.info(f"Writing to PostgreSQL table: {table} with vector column: {vector_column}")
        
        # Create connection properties
        connection_properties = {
            "driver": jdbc_driver,
            **credentials
        }
        
        # Prepare data for PostgreSQL
        # We need to convert the embedding array to a PostgreSQL vector format
        # This is a simplified implementation
        # In a real-world scenario, you would use the appropriate vector extension (e.g., pgvector)
        
        # Convert embedding to PostgreSQL vector format
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
        @udf(StringType())
        def array_to_pg_vector(arr):
            if arr is None:
                return None
            return f"[{','.join(map(str, arr))}]"
        
        # Apply the UDF to convert embeddings
        data_with_pg_vector = data.withColumn(vector_column, array_to_pg_vector("embedding"))
        
        # Handle different load types
        if load_type == "full":
            # Full load - overwrite the table
            data_with_pg_vector.write.jdbc(
                url=jdbc_url,
                table=table,
                mode="overwrite",
                properties=connection_properties
            )
        elif load_type == "scd2":
            # SCD Type 2 - similar to RDBMS target
            # This is a simplified implementation
            # In a real-world scenario, you would implement a more sophisticated SCD Type 2 approach
            
            # Check if the table exists
            try:
                current_data = context.spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    properties=connection_properties
                )
                table_exists = True
            except Exception:
                table_exists = False
            
            if not table_exists:
                # Create the table with the initial data
                data_with_pg_vector.write.jdbc(
                    url=jdbc_url,
                    table=table,
                    mode="overwrite",
                    properties=connection_properties
                )
            else:
                # Implement SCD Type 2 logic
                # For simplicity, we'll just append the data
                # In a real implementation, you would handle versioning
                data_with_pg_vector.write.jdbc(
                    url=jdbc_url,
                    table=table,
                    mode="append",
                    properties=connection_properties
                )
        elif load_type == "cdc":
            # CDC - similar to RDBMS target
            # This is a simplified implementation
            # In a real-world scenario, you would implement a more sophisticated CDC approach
            
            # Check if the table exists
            try:
                current_data = context.spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    properties=connection_properties
                )
                table_exists = True
            except Exception:
                table_exists = False
            
            if not table_exists:
                # Create the table with the initial data
                data_with_pg_vector.write.jdbc(
                    url=jdbc_url,
                    table=table,
                    mode="overwrite",
                    properties=connection_properties
                )
            else:
                # Implement CDC logic
                # For simplicity, we'll just append the data
                # In a real implementation, you would handle updates and deletes
                data_with_pg_vector.write.jdbc(
                    url=jdbc_url,
                    table=table,
                    mode="append",
                    properties=connection_properties
                )
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} records to PostgreSQL table {table}")
    
    def _write_to_neo4j(self, context: PipelineContext, data: DataFrame, load_type: str) -> None:
        """Write data to Neo4j graph database."""
        from neo4j import GraphDatabase
        
        target_config = context.config.target_config
        
        # Get Neo4j configuration
        uri = target_config.jdbc_url
        credentials = target_config.credentials or {}
        username = credentials.get("user", "neo4j")
        password = credentials.get("password", "")
        
        logger.info(f"Writing to Neo4j database at: {uri}")
        
        # Create Neo4j driver
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        # Prepare data for Neo4j
        # We need to collect the data to the driver
        # This is not scalable for very large datasets
        # In a real-world scenario, you might want to use a more distributed approach
        
        # Convert to pandas for easier processing
        pandas_df = data.toPandas()
        
        # Handle different load types
        if load_type == "full":
            # Full load - clear existing data and create new nodes
            with driver.session() as session:
                # Clear existing data
                session.run("MATCH (n:Chunk) DETACH DELETE n")
                
                # Create new nodes
                for _, row in pandas_df.iterrows():
                    # Convert row to dictionary
                    properties = row.to_dict()
                    
                    # Convert embedding to list if it's not already
                    if "embedding" in properties and not isinstance(properties["embedding"], list):
                        properties["embedding"] = properties["embedding"].tolist()
                    
                    # Create Cypher query
                    query = """
                    CREATE (c:Chunk {properties})
                    """
                    
                    # Execute query
                    session.run(query, properties=properties)
        
        elif load_type == "scd2" or load_type == "cdc":
            # For both SCD2 and CDC, we'll use merge
            # Neo4j doesn't have a direct SCD2 concept
            
            with driver.session() as session:
                for _, row in pandas_df.iterrows():
                    # Convert row to dictionary
                    properties = row.to_dict()
                    
                    # Convert embedding to list if it's not already
                    if "embedding" in properties and not isinstance(properties["embedding"], list):
                        properties["embedding"] = properties["embedding"].tolist()
                    
                    # Get the chunk ID
                    chunk_id = str(properties.get("chunk_id", ""))
                    
                    # Create Cypher query using MERGE
                    query = """
                    MERGE (c:Chunk {chunk_id: $chunk_id})
                    SET c = $properties
                    """
                    
                    # Execute query
                    session.run(query, chunk_id=chunk_id, properties=properties)
        
        # Close the driver
        driver.close()
        
        # Log statistics
        count = len(pandas_df)
        logger.info(f"Wrote {count} records to Neo4j database")