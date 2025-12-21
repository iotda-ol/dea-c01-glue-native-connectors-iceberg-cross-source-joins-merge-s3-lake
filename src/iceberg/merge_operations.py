"""
Iceberg MERGE Operations Module

Implements MERGE (UPSERT) operations for Iceberg tables.
"""

from typing import Any, Dict, List, Optional
import logging


class IcebergMergeOperations:
    """
    Handles MERGE operations for Iceberg tables.
    
    Supports INSERT, UPDATE, and DELETE operations through
    Iceberg's MERGE INTO syntax.
    """
    
    def __init__(self, 
                 catalog_name: str,
                 table_name: str,
                 merge_keys: List[str]):
        """
        Initialize merge operations handler.
        
        Args:
            catalog_name: Glue catalog database name
            table_name: Target Iceberg table name
            merge_keys: List of columns to use for matching records
        """
        self.catalog_name = catalog_name
        self.table_name = table_name
        self.merge_keys = merge_keys
        self.logger = logging.getLogger(__name__)
        self.spark = None
        
    def initialize_spark(self) -> None:
        """Initialize Spark session."""
        from pyspark.sql import SparkSession
        
        if not self.spark:
            self.spark = SparkSession.getActiveSession()
            if not self.spark:
                raise RuntimeError("No active Spark session found")
    
    def merge_data(self, 
                   source_data: Any,
                   update_condition: Optional[str] = None,
                   insert_condition: Optional[str] = None,
                   delete_condition: Optional[str] = None) -> Dict[str, int]:
        """
        Perform MERGE operation on Iceberg table.
        
        Args:
            source_data: Source DataFrame with new/updated records
            update_condition: Optional condition for UPDATE (default: all matches)
            insert_condition: Optional condition for INSERT (default: all non-matches)
            delete_condition: Optional condition for DELETE
            
        Returns:
            Dictionary with operation statistics (rows inserted, updated, deleted)
        """
        if not self.spark:
            self.initialize_spark()
        
        try:
            # Register source data as temp view
            source_data.createOrReplaceTempView("source_data")
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in self.merge_keys
            ])
            
            # Build MERGE statement
            merge_sql = f"""
            MERGE INTO glue_catalog.{self.catalog_name}.{self.table_name} AS target
            USING source_data AS source
            ON {merge_condition}
            """
            
            # Add DELETE clause if specified
            if delete_condition:
                merge_sql += f"\nWHEN MATCHED AND {delete_condition} THEN DELETE"
            
            # Add UPDATE clause
            update_clause = "WHEN MATCHED"
            if update_condition:
                update_clause += f" AND {update_condition}"
            
            # Get all columns for update (excluding merge keys to avoid duplication)
            all_columns = source_data.columns
            update_assignments = ", ".join([
                f"{col} = source.{col}" for col in all_columns
            ])
            
            merge_sql += f"\n{update_clause} THEN UPDATE SET {update_assignments}"
            
            # Add INSERT clause
            insert_clause = "WHEN NOT MATCHED"
            if insert_condition:
                insert_clause += f" AND {insert_condition}"
            
            column_list = ", ".join(all_columns)
            value_list = ", ".join([f"source.{col}" for col in all_columns])
            
            merge_sql += f"\n{insert_clause} THEN INSERT ({column_list}) VALUES ({value_list})"
            
            # Execute MERGE
            self.logger.info(f"Executing MERGE operation")
            self.logger.debug(f"MERGE SQL: {merge_sql}")
            
            self.spark.sql(merge_sql)
            
            # Get operation statistics (would need to query metadata)
            stats = {
                'rows_inserted': 0,  # Placeholder
                'rows_updated': 0,   # Placeholder
                'rows_deleted': 0    # Placeholder
            }
            
            self.logger.info(f"MERGE operation completed successfully")
            return stats
            
        except Exception as e:
            self.logger.error(f"MERGE operation failed: {str(e)}")
            raise
    
    def upsert(self, source_data: Any) -> None:
        """
        Perform simple UPSERT (update if exists, insert if not).
        
        Args:
            source_data: Source DataFrame with records to upsert
        """
        self.merge_data(
            source_data=source_data,
            update_condition=None,
            insert_condition=None
        )
        self.logger.info("UPSERT operation completed")
    
    def delete_by_keys(self, keys_df: Any) -> None:
        """
        Delete records matching specified keys.
        
        Args:
            keys_df: DataFrame with keys of records to delete
        """
        self.merge_data(
            source_data=keys_df,
            delete_condition="1=1",
            update_condition="1=0",  # Don't update
            insert_condition="1=0"   # Don't insert
        )
        self.logger.info("DELETE operation completed")
    
    def incremental_merge(self, 
                         source_data: Any,
                         timestamp_column: str,
                         watermark: str) -> None:
        """
        Perform incremental merge based on timestamp watermark.
        
        Args:
            source_data: Source DataFrame
            timestamp_column: Name of timestamp column
            watermark: Watermark timestamp (process only records after this)
        """
        # Filter source data by watermark
        filtered_source = source_data.filter(
            f"{timestamp_column} > '{watermark}'"
        )
        
        self.upsert(filtered_source)
        self.logger.info(f"Incremental merge completed for watermark: {watermark}")
    
    def merge_with_deduplication(self, 
                                 source_data: Any,
                                 dedup_columns: Optional[List[str]] = None,
                                 order_column: Optional[str] = None) -> None:
        """
        Merge data after deduplicating source.
        
        Args:
            source_data: Source DataFrame (may contain duplicates)
            dedup_columns: Columns to use for deduplication (defaults to merge_keys)
            order_column: Column to use for ordering during dedup (keep latest)
        """
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, desc
        
        dedup_cols = dedup_columns or self.merge_keys
        
        # Deduplicate source data
        if order_column:
            window_spec = Window.partitionBy(*dedup_cols).orderBy(desc(order_column))
            deduped_source = source_data.withColumn("row_num", row_number().over(window_spec)) \
                                       .filter("row_num = 1") \
                                       .drop("row_num")
        else:
            deduped_source = source_data.dropDuplicates(dedup_cols)
        
        self.logger.info(f"Deduplicated source data: {source_data.count()} -> {deduped_source.count()} rows")
        
        # Perform merge
        self.upsert(deduped_source)
