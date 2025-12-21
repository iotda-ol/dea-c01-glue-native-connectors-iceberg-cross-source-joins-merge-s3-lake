"""
Iceberg-specific transformation operations
Handles MERGE, schema evolution, and table maintenance
"""
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional, Dict


class IcebergTransforms:
    """Transformations specific to Apache Iceberg tables"""
    
    def __init__(self, spark: SparkSession, catalog: str = "glue_catalog"):
        """
        Initialize Iceberg transforms
        
        Args:
            spark: Spark session configured for Iceberg
            catalog: Iceberg catalog name
        """
        self.spark = spark
        self.catalog = catalog
    
    def merge_upsert(self,
                    target_table: str,
                    source_df: DataFrame,
                    merge_keys: List[str],
                    update_columns: Optional[List[str]] = None) -> None:
        """
        Perform MERGE (upsert) operation on Iceberg table
        
        Args:
            target_table: Fully qualified table name (db.table)
            source_df: DataFrame with new/updated records
            merge_keys: Columns to match records on
            update_columns: Columns to update (None = all columns)
        """
        # Register source as temp view
        source_df.createOrReplaceTempView("source_updates")
        
        # Build merge conditions
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        
        # Build update set clause
        if update_columns:
            update_set = ", ".join([f"{col} = s.{col}" for col in update_columns])
        else:
            update_set = "*"
        
        # Execute MERGE
        merge_sql = f"""
        MERGE INTO {self.catalog}.{target_table} t
        USING source_updates s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT *
        """
        
        self.spark.sql(merge_sql)
    
    def delete_records(self,
                      table_name: str,
                      filter_condition: str) -> None:
        """
        Delete records from Iceberg table
        
        Args:
            table_name: Fully qualified table name
            filter_condition: WHERE clause condition
        """
        delete_sql = f"""
        DELETE FROM {self.catalog}.{table_name}
        WHERE {filter_condition}
        """
        self.spark.sql(delete_sql)
    
    def evolve_schema_add_columns(self,
                                 table_name: str,
                                 new_columns: Dict[str, str]) -> None:
        """
        Add new columns to Iceberg table (schema evolution)
        
        Args:
            table_name: Fully qualified table name
            new_columns: Dict of {column_name: data_type}
        """
        for col_name, col_type in new_columns.items():
            alter_sql = f"""
            ALTER TABLE {self.catalog}.{table_name}
            ADD COLUMN {col_name} {col_type}
            """
            self.spark.sql(alter_sql)
    
    def compact_files(self,
                     table_name: str,
                     target_file_size_mb: int = 512) -> None:
        """
        Compact small files in Iceberg table
        
        Args:
            table_name: Fully qualified table name
            target_file_size_mb: Target file size in MB
        """
        compact_sql = f"""
        CALL {self.catalog}.system.rewrite_data_files(
            table => '{table_name}',
            options => map('target-file-size-bytes', '{target_file_size_mb * 1024 * 1024}')
        )
        """
        self.spark.sql(compact_sql)
    
    def expire_snapshots(self,
                        table_name: str,
                        older_than_timestamp: str,
                        retain_last: int = 5) -> None:
        """
        Remove old snapshots from Iceberg table
        
        Args:
            table_name: Fully qualified table name
            older_than_timestamp: Timestamp in format 'YYYY-MM-DD HH:MM:SS'
            retain_last: Minimum number of snapshots to keep
        """
        expire_sql = f"""
        CALL {self.catalog}.system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '{older_than_timestamp}',
            retain_last => {retain_last}
        )
        """
        self.spark.sql(expire_sql)
    
    def remove_orphan_files(self, table_name: str) -> None:
        """
        Remove files not referenced by any snapshot
        
        Args:
            table_name: Fully qualified table name
        """
        orphan_sql = f"""
        CALL {self.catalog}.system.remove_orphan_files(
            table => '{table_name}'
        )
        """
        self.spark.sql(orphan_sql)
    
    def time_travel_query(self,
                         table_name: str,
                         as_of_timestamp: str) -> DataFrame:
        """
        Query table as of specific timestamp
        
        Args:
            table_name: Fully qualified table name
            as_of_timestamp: Timestamp in format 'YYYY-MM-DD HH:MM:SS'
        
        Returns:
            DataFrame with historical data
        """
        return self.spark.read \
            .option("as-of-timestamp", as_of_timestamp) \
            .table(f"{self.catalog}.{table_name}")
