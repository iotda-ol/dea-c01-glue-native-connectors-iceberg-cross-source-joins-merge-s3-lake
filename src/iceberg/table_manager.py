"""
Iceberg Table Manager
Manages Apache Iceberg table operations
"""
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Dict, List, Any
import logging


class IcebergTableManager:
    """
    Manager for Apache Iceberg table operations
    Handles table creation, updates, and maintenance
    """
    
    def __init__(self, spark: SparkSession, catalog_name: str = "glue_catalog"):
        """
        Initialize Iceberg table manager
        
        Args:
            spark: Spark session
            catalog_name: Iceberg catalog name
        """
        self.spark = spark
        self.catalog_name = catalog_name
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def create_table(self,
                    database: str,
                    table_name: str,
                    df: DataFrame,
                    partition_cols: Optional[List[str]] = None,
                    table_properties: Optional[Dict[str, str]] = None,
                    location: Optional[str] = None) -> bool:
        """
        Create an Iceberg table
        
        Args:
            database: Database name
            table_name: Table name
            df: DataFrame with schema to use
            partition_cols: List of partition columns
            table_properties: Additional table properties
            location: S3 location for table data
            
        Returns:
            bool: True if successful
        """
        try:
            full_table_name = f"{database}.{table_name}"
            self.logger.info(f"Creating Iceberg table: {full_table_name}")
            
            # Create temporary view from DataFrame
            df.createOrReplaceTempView("temp_table_schema")
            
            # Build CREATE TABLE statement
            create_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} "
            create_sql += "USING iceberg "
            
            if location:
                create_sql += f"LOCATION '{location}' "
            
            # Add partitioning
            if partition_cols:
                partition_spec = ", ".join(partition_cols)
                create_sql += f"PARTITIONED BY ({partition_spec}) "
            
            # Add table properties
            if table_properties:
                props = ", ".join([f"'{k}' = '{v}'" for k, v in table_properties.items()])
                create_sql += f"TBLPROPERTIES ({props}) "
            
            # Add schema from DataFrame
            create_sql += "AS SELECT * FROM temp_table_schema WHERE 1=0"
            
            self.spark.sql(create_sql)
            self.logger.info(f"Successfully created table: {full_table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table: {str(e)}")
            raise
    
    def write_data(self,
                  df: DataFrame,
                  database: str,
                  table_name: str,
                  mode: str = "append") -> bool:
        """
        Write data to Iceberg table
        
        Args:
            df: DataFrame to write
            database: Database name
            table_name: Table name
            mode: Write mode ('append', 'overwrite')
            
        Returns:
            bool: True if successful
        """
        try:
            full_table_name = f"{database}.{table_name}"
            self.logger.info(f"Writing data to {full_table_name} in {mode} mode")
            
            df.writeTo(full_table_name) \
                .using("iceberg") \
                .option("write-format", "parquet") \
                .option("compression-codec", "snappy") \
                .createOrReplace() if mode == "overwrite" else df.writeTo(full_table_name).append()
            
            row_count = df.count()
            self.logger.info(f"Successfully wrote {row_count} rows to {full_table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write data: {str(e)}")
            raise
    
    def merge_data(self,
                  source_df: DataFrame,
                  target_database: str,
                  target_table: str,
                  merge_keys: List[str],
                  update_condition: Optional[str] = None) -> Dict[str, int]:
        """
        Perform MERGE operation on Iceberg table
        
        Args:
            source_df: Source DataFrame
            target_database: Target database name
            target_table: Target table name
            merge_keys: List of columns to match on
            update_condition: Optional condition for updates
            
        Returns:
            Dict: Statistics about merge operation
        """
        try:
            full_table_name = f"{target_database}.{target_table}"
            self.logger.info(f"Performing MERGE into {full_table_name}")
            
            # Create temp view for source data
            source_df.createOrReplaceTempView("merge_source")
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            # Build MERGE statement
            merge_sql = f"""
                MERGE INTO {full_table_name} AS target
                USING merge_source AS source
                ON {merge_condition}
            """
            
            # Add WHEN MATCHED clause
            if update_condition:
                merge_sql += f"""
                    WHEN MATCHED AND {update_condition} THEN UPDATE SET *
                """
            else:
                merge_sql += """
                    WHEN MATCHED THEN UPDATE SET *
                """
            
            # Add WHEN NOT MATCHED clause
            merge_sql += """
                WHEN NOT MATCHED THEN INSERT *
            """
            
            # Execute merge
            self.spark.sql(merge_sql)
            
            self.logger.info(f"Successfully completed MERGE operation")
            
            return {
                "status": "success",
                "source_count": source_df.count()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to merge data: {str(e)}")
            raise
    
    def compact_files(self,
                     database: str,
                     table_name: str,
                     target_file_size_mb: int = 128) -> bool:
        """
        Compact small files in Iceberg table
        
        Args:
            database: Database name
            table_name: Table name
            target_file_size_mb: Target file size in MB
            
        Returns:
            bool: True if successful
        """
        try:
            full_table_name = f"{database}.{table_name}"
            self.logger.info(f"Compacting files in {full_table_name}")
            
            target_size_bytes = target_file_size_mb * 1024 * 1024
            
            compact_sql = f"""
                CALL {self.catalog_name}.system.rewrite_data_files(
                    table => '{full_table_name}',
                    strategy => 'binpack',
                    options => map('target-file-size-bytes', '{target_size_bytes}')
                )
            """
            
            self.spark.sql(compact_sql)
            self.logger.info(f"Successfully compacted files")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to compact files: {str(e)}")
            raise
    
    def expire_snapshots(self,
                        database: str,
                        table_name: str,
                        older_than_days: int = 7,
                        retain_last: int = 5) -> bool:
        """
        Expire old snapshots from Iceberg table
        
        Args:
            database: Database name
            table_name: Table name
            older_than_days: Expire snapshots older than this
            retain_last: Always retain this many snapshots
            
        Returns:
            bool: True if successful
        """
        try:
            full_table_name = f"{database}.{table_name}"
            self.logger.info(f"Expiring snapshots in {full_table_name}")
            
            expire_sql = f"""
                CALL {self.catalog_name}.system.expire_snapshots(
                    table => '{full_table_name}',
                    older_than => TIMESTAMP '{older_than_days} days ago',
                    retain_last => {retain_last}
                )
            """
            
            self.spark.sql(expire_sql)
            self.logger.info(f"Successfully expired snapshots")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to expire snapshots: {str(e)}")
            raise
    
    def get_table_stats(self, database: str, table_name: str) -> Dict[str, Any]:
        """
        Get statistics about an Iceberg table
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Dict: Table statistics
        """
        try:
            full_table_name = f"{database}.{table_name}"
            
            # Get snapshot information
            snapshots_df = self.spark.sql(f"""
                SELECT * FROM {full_table_name}.snapshots
                ORDER BY committed_at DESC
            """)
            
            # Get file information
            files_df = self.spark.sql(f"""
                SELECT * FROM {full_table_name}.files
            """)
            
            stats = {
                "total_snapshots": snapshots_df.count(),
                "total_files": files_df.count(),
                "latest_snapshot": snapshots_df.first().asDict() if snapshots_df.count() > 0 else None
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get table stats: {str(e)}")
            raise
