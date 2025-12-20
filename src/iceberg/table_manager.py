"""
Iceberg Table Manager Module

Manages Apache Iceberg table operations including creation,
schema evolution, and metadata management.
"""

from typing import Any, Dict, List, Optional
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
            raise
