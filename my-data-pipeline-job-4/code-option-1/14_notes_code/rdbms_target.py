from typing import Dict, Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp

from src.core.context import PipelineContext
from src.target.base_target import BaseTarget
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RDBMSTarget(BaseTarget):
    """Target implementation for RDBMS databases."""
    
    def write_full_load(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to RDBMS table using full load strategy."""
        target_config = context.config.target_config
        
        # Get JDBC connection properties
        jdbc_url = target_config.jdbc_url
        jdbc_driver = target_config.jdbc_driver
        table = target_config.table
        credentials = target_config.credentials or {}
        
        logger.info(f"Writing data to RDBMS table {table} using full load strategy")
        
        # Create connection properties
        connection_properties = {
            "driver": jdbc_driver,
            **credentials
        }
        
        # Write data to RDBMS table
        data.write.jdbc(
            url=jdbc_url,
            table=table,
            mode="overwrite",
            properties=connection_properties
        )
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to RDBMS table {table}")
    
    def write_incremental_scd2(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to RDBMS table using incremental load with SCD Type 2 strategy."""
        target_config = context.config.target_config
        
        # Get JDBC connection properties
        jdbc_url = target_config.jdbc_url
        jdbc_driver = target_config.jdbc_driver
        table = target_config.table
        credentials = target_config.credentials or {}
        
        logger.info(f"Writing data to RDBMS table {table} using SCD Type 2 strategy")
        
        # Create connection properties
        connection_properties = {
            "driver": jdbc_driver,
            **credentials
        }
        
        # Check if the target table exists
        # This is a simplified implementation
        # In a real-world scenario, you would check if the table exists in a more robust way
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
            # If table doesn't exist, create it with the initial data
            logger.info(f"Target table {table} does not exist, creating it")
            
            # Add SCD Type 2 tracking columns
            data_with_tracking = data.withColumn("effective_start_date", current_timestamp()) \
                                     .withColumn("effective_end_date", lit(None).cast("timestamp")) \
                                     .withColumn("is_current", lit(True))
            
            # Write data to RDBMS table
            data_with_tracking.write.jdbc(
                url=jdbc_url,
                table=table,
                mode="overwrite",
                properties=connection_properties
            )
            
            # Log statistics
            count = data.count()
            logger.info(f"Wrote {count} rows to RDBMS table {table}")
            return
        
        # If table exists, implement SCD Type 2 logic
        
        # 1. Read the current data
        current_data = context.spark.read.jdbc(
            url=jdbc_url,
            table=table,
            properties=connection_properties
        )
        
        # 2. Identify the business key columns (simplified example)
        # In a real implementation, this would be configurable
        business_key_cols = ["chunk_id", "source_id"]
        
        # 3. Identify records that have changed
        # This is a simplified implementation
        # In a real-world scenario, you would implement a more sophisticated comparison
        
        # Register temporary views
        current_data.createOrReplaceTempView("current_data")
        data.createOrReplaceTempView("new_data")
        
        # Find changed records
        changed_records_query = f"""
        SELECT n.*
        FROM new_data n
        JOIN current_data c
        ON {' AND '.join([f'n.{col} = c.{col}' for col in business_key_cols])}
        WHERE c.is_current = TRUE
        AND (
            {' OR '.join([f'n.{col} <> c.{col}' for col in data.columns if col not in business_key_cols])}
        )
        """
        
        changed_records = context.spark.sql(changed_records_query)
        
        # Find new records
        new_records_query = f"""
        SELECT n.*
        FROM new_data n
        LEFT JOIN current_data c
        ON {' AND '.join([f'n.{col} = c.{col}' for col in business_key_cols])}
        WHERE c.{business_key_cols[0]} IS NULL
        """
        
        new_records = context.spark.sql(new_records_query)
        
        # 4. Update current records (set end date and is_current flag)
        if changed_records.count() > 0:
            # Create a temporary table for the records to update
            changed_keys = changed_records.select(*business_key_cols)
            changed_keys.createOrReplaceTempView("changed_keys")
            
            # Create a temporary table with the updates
            update_data = context.spark.createDataFrame(
                [(current_timestamp(), False)],
                ["effective_end_date", "is_current"]
            )
            update_data.createOrReplaceTempView("update_data")
            
            # Create a temporary view with the records to update
            update_query = f"""
            SELECT c.*, u.effective_end_date, u.is_current
            FROM current_data c
            JOIN changed_keys k
            ON {' AND '.join([f'c.{col} = k.{col}' for col in business_key_cols])}
            CROSS JOIN update_data u
            WHERE c.is_current = TRUE
            """
            
            records_to_update = context.spark.sql(update_query)
            
            # Write the updated records to a temporary table
            temp_table = f"{table}_updates"
            records_to_update.write.jdbc(
                url=jdbc_url,
                table=temp_table,
                mode="overwrite",
                properties=connection_properties
            )
            
            # Update the main table using SQL
            # This is database-specific and would need to be adapted for different databases
            # This example assumes PostgreSQL
            update_sql = f"""
            UPDATE {table}
            SET effective_end_date = u.effective_end_date,
                is_current = u.is_current
            FROM {temp_table} u
            WHERE {' AND '.join([f'{table}.{col} = u.{col}' for col in business_key_cols])}
            AND {table}.is_current = TRUE
            """
            
            # Execute the update SQL
            # This would typically be done using a JDBC connection
            # For simplicity, we'll just log it
            logger.info(f"Executing SQL: {update_sql}")
        
        #  we'll just log it
            logger.info(f"Executing SQL: {update_sql}")
        
        # 5. Insert new versions of changed records and new records
        if changed_records.count() > 0 or new_records.count() > 0:
            # Combine changed and new records
            records_to_insert = changed_records.union(new_records)
            
            # Add SCD Type 2 tracking columns
            records_to_insert = records_to_insert.withColumn("effective_start_date", current_timestamp()) \
                                               .withColumn("effective_end_date", lit(None).cast("timestamp")) \
                                               .withColumn("is_current", lit(True))
            
            # Insert records
            records_to_insert.write.jdbc(
                url=jdbc_url,
                table=table,
                mode="append",
                properties=connection_properties
            )
            
            # Log statistics
            count = records_to_insert.count()
            logger.info(f"Inserted {count} rows to RDBMS table {table}")
    
    def write_incremental_cdc(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to RDBMS table using incremental load with CDC strategy."""
        target_config = context.config.target_config
        
        # Get JDBC connection properties
        jdbc_url = target_config.jdbc_url
        jdbc_driver = target_config.jdbc_driver
        table = target_config.table
        credentials = target_config.credentials or {}
        
        logger.info(f"Writing data to RDBMS table {table} using CDC strategy")
        
        # Create connection properties
        connection_properties = {
            "driver": jdbc_driver,
            **credentials
        }
        
        # Check if the target table exists
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
            # If table doesn't exist, create it with the initial data
            logger.info(f"Target table {table} does not exist, creating it")
            
            # Add CDC tracking columns
            data_with_tracking = data.withColumn("cdc_operation", lit("INSERT")) \
                                     .withColumn("cdc_timestamp", current_timestamp())
            
            # Write data to RDBMS table
            data_with_tracking.write.jdbc(
                url=jdbc_url,
                table=table,
                mode="overwrite",
                properties=connection_properties
            )
            
            # Log statistics
            count = data.count()
            logger.info(f"Wrote {count} rows to RDBMS table {table}")
            return
        
        # If table exists, implement CDC logic
        
        # 1. Read the current data
        current_data = context.spark.read.jdbc(
            url=jdbc_url,
            table=table,
            properties=connection_properties
        )
        
        # 2. Identify the business key columns (simplified example)
        business_key_cols = ["chunk_id", "source_id"]
        
        # 3. Identify records that have changed, been deleted, or are new
        
        # Register temporary views
        current_data.createOrReplaceTempView("current_data")
        data.createOrReplaceTempView("new_data")
        
        # Find changed records
        changed_records_query = f"""
        SELECT n.*, 'UPDATE' as cdc_operation, current_timestamp() as cdc_timestamp
        FROM new_data n
        JOIN current_data c
        ON {' AND '.join([f'n.{col} = c.{col}' for col in business_key_cols])}
        WHERE {' OR '.join([f'n.{col} <> c.{col}' for col in data.columns if col not in business_key_cols])}
        """
        
        changed_records = context.spark.sql(changed_records_query)
        
        # Find new records
        new_records_query = f"""
        SELECT n.*, 'INSERT' as cdc_operation, current_timestamp() as cdc_timestamp
        FROM new_data n
        LEFT JOIN current_data c
        ON {' AND '.join([f'n.{col} = c.{col}' for col in business_key_cols])}
        WHERE c.{business_key_cols[0]} IS NULL
        """
        
        new_records = context.spark.sql(new_records_query)
        
        # 4. Apply changes to the target table
        
        # For inserts and updates, use JDBC batch operations
        if changed_records.count() > 0 or new_records.count() > 0:
            # Combine all records
            all_changes = changed_records.union(new_records)
            
            # Write to a CDC log table
            cdc_log_table = f"{table}_cdc_log"
            all_changes.write.jdbc(
                url=jdbc_url,
                table=cdc_log_table,
                mode="append",
                properties=connection_properties
            )
            
            # Apply changes to the main table
            # For simplicity, we'll use a direct approach
            
            # For updates
            if changed_records.count() > 0:
                # Create a temporary table for updates
                temp_update_table = f"{table}_updates"
                changed_records.write.jdbc(
                    url=jdbc_url,
                    table=temp_update_table,
                    mode="overwrite",
                    properties=connection_properties
                )
                
                # Execute update SQL
                update_sql = f"""
                UPDATE {table}
                SET {', '.join([f'{col} = u.{col}' for col in data.columns if col not in business_key_cols])},
                    cdc_operation = u.cdc_operation,
                    cdc_timestamp = u.cdc_timestamp
                FROM {temp_update_table} u
                WHERE {' AND '.join([f'{table}.{col} = u.{col}' for col in business_key_cols])}
                """
                
                logger.info(f"Executing SQL: {update_sql}")
            
            # For inserts
            if new_records.count() > 0:
                new_records.write.jdbc(
                    url=jdbc_url,
                    table=table,
                    mode="append",
                    properties=connection_properties
                )
            
            # Log statistics
            logger.info(f"Applied {all_changes.count()} changes to RDBMS table {table}")