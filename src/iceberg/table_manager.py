"""
Iceberg Table Manager Module

Manages Apache Iceberg table operations including creation,
schema evolution, and metadata management.
"""

from typing import Any, Dict, List, Optional
Iceberg Table Manager
Manages Apache Iceberg table operations
"""
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Dict, List, Any
import logging


class IcebergTableManager:
    """
    Manages Iceberg table lifecycle and operations.
    
    Handles table creation, schema evolution, partitioning,
    and metadata operations for Iceberg tables in S3.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Iceberg table manager.
        
        Args:
            config: Configuration dictionary with:
                - warehouse_path: S3 path for Iceberg warehouse
                - catalog_name: Glue catalog database name
                - table_name: Name of Iceberg table
        """
        self.config = config
        self.warehouse_path = config.get('warehouse_path')
        self.catalog_name = config.get('catalog_name')
        self.table_name = config.get('table_name')
        self.logger = logging.getLogger(__name__)
        self.spark = None
        
    def initialize_spark(self) -> None:
        """Initialize Spark session with Iceberg configurations."""
        try:
            from pyspark.sql import SparkSession
            
            self.spark = SparkSession.builder \
                .appName("IcebergTableManager") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.glue_catalog.warehouse", self.warehouse_path) \
                .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
                .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
                .getOrCreate()
            
            self.logger.info("Spark session initialized with Iceberg support")
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {str(e)}")
            raise
    
    def create_table(self, 
                     schema: List[Dict[str, str]], 
                     partition_spec: Optional[List[str]] = None,
                     table_properties: Optional[Dict[str, str]] = None) -> None:
        """
        Create a new Iceberg table.
        
        Args:
            schema: List of column definitions (name, type)
            partition_spec: Optional partitioning specification
            table_properties: Optional table properties
        """
        if not self.spark:
            self.initialize_spark()
        
        try:
            # Build CREATE TABLE statement
            columns = ", ".join([f"{col['name']} {col['type']}" for col in schema])
            
            sql = f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{self.catalog_name}.{self.table_name} (
                {columns}
            )
            USING iceberg
            """
            
            # Add partitioning if specified
            if partition_spec:
                partition_clause = ", ".join(partition_spec)
                sql += f"\nPARTITIONED BY ({partition_clause})"
            
            # Add table properties
            if table_properties:
                props = ", ".join([f"'{k}'='{v}'" for k, v in table_properties.items()])
                sql += f"\nTBLPROPERTIES ({props})"
            
            self.spark.sql(sql)
            self.logger.info(f"Created Iceberg table: {self.catalog_name}.{self.table_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to create Iceberg table: {str(e)}")
            raise
    
    def evolve_schema(self, operations: List[Dict[str, Any]]) -> None:
        """
        Evolve table schema (add/drop/rename columns).
        
        Args:
            operations: List of schema evolution operations
                Each operation has 'type' (add/drop/rename) and parameters
        """
        if not self.spark:
            self.initialize_spark()
        
        try:
            for op in operations:
                op_type = op.get('type')
                
                if op_type == 'add':
                    column_name = op.get('column_name')
                    column_type = op.get('column_type')
                    sql = f"ALTER TABLE glue_catalog.{self.catalog_name}.{self.table_name} " \
                          f"ADD COLUMN {column_name} {column_type}"
                    self.spark.sql(sql)
                    self.logger.info(f"Added column {column_name} to table")
                    
                elif op_type == 'drop':
                    column_name = op.get('column_name')
                    sql = f"ALTER TABLE glue_catalog.{self.catalog_name}.{self.table_name} " \
                          f"DROP COLUMN {column_name}"
                    self.spark.sql(sql)
                    self.logger.info(f"Dropped column {column_name} from table")
                    
                elif op_type == 'rename':
                    old_name = op.get('old_name')
                    new_name = op.get('new_name')
                    sql = f"ALTER TABLE glue_catalog.{self.catalog_name}.{self.table_name} " \
                          f"RENAME COLUMN {old_name} TO {new_name}"
                    self.spark.sql(sql)
                    self.logger.info(f"Renamed column {old_name} to {new_name}")
                    
        except Exception as e:
            self.logger.error(f"Schema evolution failed: {str(e)}")
            raise
    
    def get_table_metadata(self) -> Dict[str, Any]:
        """
        Get table metadata and statistics.
        
        Returns:
            Dictionary with table metadata
        """
        if not self.spark:
            self.initialize_spark()
        
        try:
            # Get table description
            describe_df = self.spark.sql(
                f"DESCRIBE TABLE glue_catalog.{self.catalog_name}.{self.table_name}"
            )
            
            # Get table history
            history_df = self.spark.sql(
                f"SELECT * FROM glue_catalog.{self.catalog_name}.{self.table_name}.history"
            )
            
            # Get table snapshots
            snapshots_df = self.spark.sql(
                f"SELECT * FROM glue_catalog.{self.catalog_name}.{self.table_name}.snapshots"
            )
            
            metadata = {
                'schema': describe_df.collect(),
                'history': history_df.collect(),
                'snapshots': snapshots_df.collect()
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Failed to get table metadata: {str(e)}")
            raise
    
    def time_travel_query(self, timestamp: str) -> Any:
        """
        Query table as of a specific timestamp (time travel).
        
        Args:
            timestamp: Timestamp in format 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            DataFrame with historical data
        """
        if not self.spark:
            self.initialize_spark()
        
        try:
            sql = f"""
            SELECT * FROM glue_catalog.{self.catalog_name}.{self.table_name}
            TIMESTAMP AS OF '{timestamp}'
            """
            
            df = self.spark.sql(sql)
            self.logger.info(f"Time travel query executed for timestamp: {timestamp}")
            return df
            
        except Exception as e:
            self.logger.error(f"Time travel query failed: {str(e)}")
            raise
    
    def optimize_table(self) -> None:
        """
        Optimize table (compact small files, remove old metadata).
        """
        if not self.spark:
            self.initialize_spark()
        
        try:
            # Compact data files
            sql = f"CALL glue_catalog.system.rewrite_data_files('{self.catalog_name}.{self.table_name}')"
            self.spark.sql(sql)
            self.logger.info("Completed data file compaction")
            
            # Expire old snapshots (keep last 7 days)
            expire_sql = f"""
            CALL glue_catalog.system.expire_snapshots(
                table => '{self.catalog_name}.{self.table_name}',
                older_than => TIMESTAMP '2023-01-01 00:00:00',
                retain_last => 5
            )
            """
            self.spark.sql(expire_sql)
            self.logger.info("Expired old snapshots")
            
        except Exception as e:
            self.logger.error(f"Table optimization failed: {str(e)}")
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
