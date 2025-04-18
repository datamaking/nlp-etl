from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp

from src.core.context import PipelineContext
from src.target.base_target import BaseTarget
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HiveTarget(BaseTarget):
    """Target implementation for Hive tables."""
    
    def write_full_load(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to Hive table using full load strategy."""
        target_config = context.config.target_config
        
        # Get target table information
        database = target_config.database
        table = target_config.table
        
        logger.info(f"Writing data to Hive table {database}.{table} using full load strategy")
        
        # Enable Hive support
        context.spark.sql("SET spark.sql.catalogImplementation=hive")
        
        # Write data to Hive table
        data.write.mode("overwrite").saveAsTable(f"{database}.{table}")
        
        # Log statistics
        count = data.count()
        logger.info(f"Wrote {count} rows to Hive table {database}.{table}")
    
    def write_incremental_scd2(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to Hive table using incremental load with SCD Type 2 strategy."""
        target_config = context.config.target_config
        
        # Get target table information
        database = target_config.database
        table = target_config.table
        
        logger.info(f"Writing data to Hive table {database}.{table} using SCD Type 2 strategy")
        
        # Enable Hive support
        context.spark.sql("SET spark.sql.catalogImplementation=hive")
        
        # Check if the target table exists
        table_exists = context.spark.sql(f"SHOW TABLES IN {database} LIKE '{table}'").count() > 0
        
        if not table_exists:
            # If table doesn't exist, create it with the initial data
            logger.info(f"Target table {database}.{table} does not exist, creating it")
            
            # Add SCD Type 2 tracking columns
            data_with_tracking = data.withColumn("effective_start_date", current_timestamp()) \
                                     .withColumn("effective_end_date", lit(None).cast("timestamp")) \
                                     .withColumn("is_current", lit(True))
            
            # Write data to Hive table
            data_with_tracking.write.mode("overwrite").saveAsTable(f"{database}.{table}")
            
            # Log statistics
            count = data.count()
            logger.info(f"Wrote {count} rows to Hive table {database}.{table}")
            return
        
        # If table exists, implement SCD Type 2 logic
        
        # 1. Read the current data
        current_data = context.spark.table(f"{database}.{table}")
        
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
            update_current_query = f"""
            UPDATE {database}.{table}
            SET effective_end_date = current_timestamp(),
                is_current = FALSE
            WHERE is_current = TRUE
            AND ({' OR '.join([f'({" AND ".join([f"{col} = '{r[col]}'" for col in business_key_cols])})' for r in changed_records.collect()])})
            """
            
            context.spark.sql(update_current_query)
        
        # 5. Insert new versions of changed records and new records
        if changed_records.count() > 0 or new_records.count() > 0:
            # Combine changed and new records
            records_to_insert = changed_records.union(new_records)
            
            # Add SCD Type 2 tracking columns
            records_to_insert = records_to_insert.withColumn("effective_start_date", current_timestamp()) \
                                               .withColumn("effective_end_date", lit(None).cast("timestamp")) \
                                               .withColumn("is_current", lit(True))
            
            # Insert records
            records_to_insert.write.mode("append").saveAsTable(f"{database}.{table}")
            
            # Log statistics
            count = records_to_insert.count()
            logger.info(f"Inserted {count} rows to Hive table {database}.{table}")
    
    def write_incremental_cdc(self, context: PipelineContext, data: DataFrame) -> None:
        """Write data to Hive table using incremental load with CDC strategy."""
        target_config = context.config.target_config
        
        # Get target table information
        database = target_config.database
        table = target_config.table
        
        logger.info(f"Writing data to Hive table {database}.{table} using CDC strategy")
        
        # Enable Hive support
        context.spark.sql("SET spark.sql.catalogImplementation=hive")
        
        # Check if the target table exists
        table_exists = context.spark.sql(f"SHOW TABLES IN {database} LIKE '{table}'").count() > 0
        
        if not table_exists:
            # If table doesn't exist, create it with the initial data
            logger.info(f"Target table {database}.{table} does not exist, creating it")
            
            # Add CDC tracking columns
            data_with_tracking = data.withColumn("cdc_operation", lit("INSERT")) \
                                     .withColumn("cdc_timestamp", current_timestamp())
            
            # Write data to Hive table
            data_with_tracking.write.mode("overwrite").saveAsTable(f"{database}.{table}")
            
            # Log statistics
            count = data.count()
            logger.info(f"Wrote {count} rows to Hive table {database}.{table}")
            return
        
        # If table exists, implement CDC logic
        
        # 1. Read the current data
        current_data = context.spark.table(f"{database}.{table}")
        
        # 2. Identify the business key columns (simplified example)
        # In a real implementation, this would be configurable
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
        
        # Find deleted records (if applicable)
        # This assumes we have a complete set of data in the new data
        # In a real CDC implementation, you might have a separate process for tracking deletes
        deleted_records_query = f"""
        SELECT c.*, 'DELETE' as cdc_operation, current_timestamp() as cdc_timestamp
        FROM current_data c
        LEFT JOIN new_data n
        ON {' AND '.join([f'n.{col} = c.{col}' for col in business_key_cols])}
        WHERE n.{business_key_cols[0]} IS NULL
        """
        
        deleted_records = context.spark.sql(deleted_records_query)
        
        # 4. Apply changes
        if changed_records.count() > 0 or new_records.count() > 0 or deleted_records.count() > 0:
            # Combine all records
            all_changes = changed_records.union(new_records).union(deleted_records)
            
            # Write changes to the target table
            all_changes.write.mode("append").saveAsTable(f"{database}.{table}_changes")
            
            # Apply changes to the main table
            # This is a simplified implementation
            # In a real-world scenario, you would implement a more sophisticated merge operation
            
            # For inserts and updates
            upserts = changed_records.union(new_records)
            if upserts.count() > 0:
                # Create a temporary view
                upserts.createOrReplaceTempView("upserts")
                
                # Merge into the target table
                merge_query = f"""
                MERGE INTO {database}.{table} t
                USING upserts s
                ON {' AND '.join([f't.{col} = s.{col}' for col in business_key_cols])}
                WHEN MATCHED THEN UPDATE SET
                    {', '.join([f't.{col} = s.{col}' for col in data.columns if col not in business_key_cols])},
                    t.cdc_operation = s.cdc_operation,
                    t.cdc_timestamp = s.cdc_timestamp
                WHEN NOT MATCHED THEN INSERT VALUES
                    ({', '.join([f's.{col}' for col in all_changes.columns])})
                """
                
                context.spark.sql(merge_query)
            
            # For deletes (if applicable)
            if deleted_records.count() > 0:
                delete_query = f"""
                DELETE FROM {database}.{table}
                WHERE ({' OR '.join([f'({" AND ".join([f"{col} = '{r[col]}'" for col in business_key_cols])})' for r in deleted_records.collect()])})
                """
                
                context.spark.sql(delete_query)
            
            # Log statistics
            logger.info(f"Applied {all_changes.count()} changes to Hive table {database}.{table}")