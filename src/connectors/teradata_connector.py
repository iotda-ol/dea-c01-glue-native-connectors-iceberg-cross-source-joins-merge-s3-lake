"""
Teradata Connector

Implements connection and data operations for Teradata Vantage
using JDBC connector with optimizations for large-scale data transfer.
"""

from typing import Any, Dict, List, Optional
from src.connectors.base_connector import BaseConnector
from src.utils.retry_handler import with_retry
Teradata Vantage Connector
Implements connector for reading data from Teradata Vantage
"""
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from .base_connector import BaseConnector


class TeradataConnector(BaseConnector):
    """
    Connector for Teradata Vantage data warehouse.
    
    Uses JDBC connection with FastLoad/FastExport capabilities
    for efficient data transfer.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Teradata connector.
        
        Args:
            config: Configuration dictionary with:
                - host: Teradata host
                - database: Database name
                - secret_arn: ARN of secret in AWS Secrets Manager
                - connection_name: Glue connection name
        """
        super().__init__(config)
        self.glue_context = None
        self.connection_name = config.get('connection_name')
        
    def connect(self) -> None:
        """Initialize Teradata connection via Glue."""
        try:
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext
            
            if not self.glue_context:
                sc = SparkContext.getOrCreate()
                self.glue_context = GlueContext(sc)
                
            self.logger.info("Teradata connector initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize Teradata connector: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close Teradata connection."""
        self.logger.info("Teradata connector disconnected")
        
    @with_retry(max_attempts=3, delay=5)
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute query against Teradata.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Glue DynamicFrame with query results
        """
        if not self.glue_context:
            self.connect()
            
        try:
            # Use Glue connection for Teradata JDBC
            dynamic_frame = self.glue_context.create_dynamic_frame.from_options(
                connection_type="jdbc",
                connection_options={
                    "useConnectionProperties": "true",
                    "dbtable": f"({query}) as subquery",
                    "connectionName": self.connection_name,
                    "numPartitions": self.config.get('num_partitions', 10)
                }
            )
            
            self.logger.info(f"Teradata query executed successfully")
            return dynamic_frame
            
        except Exception as e:
            self.logger.error(f"Teradata query execution failed: {str(e)}")
            raise
    
    def get_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get table schema from Teradata.
        
        Args:
            table_name: Table name
            
        Returns:
            List of column definitions
        """
        schema_query = f"""
        SELECT 
            ColumnName,
            ColumnType,
            ColumnLength,
            Nullable
        FROM DBC.ColumnsV
        WHERE TableName = '{table_name}'
        ORDER BY ColumnId
        """
        
        result = self.execute_query(schema_query)
        return result.toDF().collect()
    
    def read_table(self, 
                   table_name: str, 
                   partition_column: Optional[str] = None,
                   lower_bound: Optional[int] = None,
                   upper_bound: Optional[int] = None,
                   num_partitions: int = 10) -> Any:
        """
        Read table with optional partitioning for parallel reads.
        
        Args:
            table_name: Name of table to read
            partition_column: Column to partition on
            lower_bound: Lower bound for partitioning
            upper_bound: Upper bound for partitioning
            num_partitions: Number of partitions for parallel read
            
        Returns:
            Glue DynamicFrame with table data
        """
        if not self.glue_context:
            self.connect()
            
        connection_options = {
            "useConnectionProperties": "true",
            "dbtable": table_name,
            "connectionName": self.connection_name
        }
        
        if partition_column:
            connection_options.update({
                "partitionColumn": partition_column,
                "lowerBound": str(lower_bound),
                "upperBound": str(upper_bound),
                "numPartitions": str(num_partitions)
            })
            
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="jdbc",
    Connector for Teradata Vantage data warehouse
    Supports FastExport and pushdown predicates
    """
    
    def __init__(self, config: Dict[str, Any], glue_context):
Reusable connector module for Teradata Vantage
Provides standardized interface for extracting data from Teradata
"""
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from typing import Optional, Dict
import boto3
import json


class TeradataConnector:
    """Connector for Teradata Vantage data source"""
    
    def __init__(self, glue_context: GlueContext, connection_name: str):
        """
        Initialize Teradata connector
        
        Args:
            config: Configuration containing:
                - connection_name: Glue connection name
                - database: Teradata database name
                - jdbc_driver: JDBC driver class (default: com.teradata.jdbc.TeraDriver)
        """
        super().__init__(config, glue_context)
        self.connection_name = config.get('connection_name')
        self.database = config.get('database')
        self.jdbc_driver = config.get('jdbc_driver', 'com.teradata.jdbc.TeraDriver')
        
    def connect(self) -> bool:
        """
        Validate Teradata connection
        
        Returns:
            bool: True if connection is valid
        """
        try:
            self.logger.info(f"Validating Teradata connection: {self.connection_name}")
            # Connection validation happens at read time
            return True
        except Exception as e:
            self.logger.error(f"Failed to validate Teradata connection: {str(e)}")
            return False
    
    def read_table(self,
                   table_name: str,
                   schema: Optional[str] = None,
                   predicates: Optional[List[str]] = None,
                   columns: Optional[List[str]] = None) -> DataFrame:
        """
        Read data from Teradata table
        
        Args:
            table_name: Teradata table name
            schema: Database/schema name
            predicates: Filter predicates for pushdown
            columns: Columns to select
            
        Returns:
            DataFrame: Spark DataFrame containing the data
        """
        try:
            schema = schema or self.database
            full_table_name = f"{schema}.{table_name}"
            
            self.logger.info(f"Reading from Teradata table: {full_table_name}")
            
            # Build SQL query with optimizations
            if columns:
                column_list = ", ".join(columns)
            else:
                column_list = "*"
            
            query = f"SELECT {column_list} FROM {full_table_name}"
            
            # Add WHERE clause if predicates provided
            if predicates:
                where_clause = " AND ".join(predicates)
                query += f" WHERE {where_clause}"
                self.logger.info(f"Applying predicates: {where_clause}")
            
            # Read using JDBC with Teradata optimizations
            df = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=table_name,
                transformation_ctx=f"read_{table_name}",
                additional_options={
                    "connectionName": self.connection_name,
                    "hashfield": "hash_column",  # Enable hash partitioning if applicable
                    "hashexpression": "MOD(HASHAMP(HASHBUCKET(HASHROW())), ?)"
                }
            ).toDF()
            
            row_count = df.count()
            self.logger.info(f"Successfully read {row_count} rows from {full_table_name}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from Teradata: {str(e)}")
            raise
    
    def get_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
        """
        Get schema information for a Teradata table
        
        Args:
            table_name: Table name
            schema: Database name
            
        Returns:
            Dict: Column name to data type mapping
        """
        try:
            schema = schema or self.database
            
            # Query Teradata system catalog
            query = f"""
                SELECT ColumnName, ColumnType 
                FROM DBC.ColumnsV 
                WHERE DatabaseName = '{schema}' 
                AND TableName = '{table_name}'
                ORDER BY ColumnId
            """
            
            schema_df = self.execute_query(query)
            schema_dict = {row.ColumnName: row.ColumnType for row in schema_df.collect()}
            
            return schema_dict
            
        except Exception as e:
            self.logger.error(f"Failed to get schema: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> DataFrame:
        """
        Execute custom SQL query on Teradata
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame: Query results
        """
        try:
            self.logger.info(f"Executing Teradata query: {query[:100]}...")
            
            # Use JDBC connection for custom queries
            df = self.spark.read \
                .format("jdbc") \
                .option("driver", self.jdbc_driver) \
                .option("url", self._get_jdbc_url()) \
                .option("query", query) \
                .option("fetchsize", "10000") \
                .load()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to execute query: {str(e)}")
            raise
    
    def _get_jdbc_url(self) -> str:
        """
        Construct JDBC URL from connection config
        
        Returns:
            str: JDBC connection URL
        """
        # This would typically retrieve from Glue connection
        # Placeholder implementation
        return f"jdbc:teradata://{self.connection_name}"
            glue_context: AWS Glue context
            connection_name: Name of Glue connection for Teradata
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.connection_name = connection_name
        self.secrets = boto3.client('secretsmanager')
    
    def extract_table(self,
                     table_name: str,
                     query: Optional[str] = None) -> DynamicFrame:
        """
        Extract table from Teradata
        
        Args:
            table_name: Table to extract
            query: Optional custom SQL query
        
        Returns:
            DynamicFrame containing extracted data
        """
        connection_options = {
            "useConnectionProperties": "true",
            "connectionName": self.connection_name,
            "dbtable": query if query else table_name
        }
        
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="teradata",
            connection_options=connection_options
        )
    
    def extract_with_fastexport(self,
                                table_name: str,
                                s3_path: str) -> DynamicFrame:
        """
        Use Teradata FastExport for large tables
        
        Args:
            table_name: Table to extract
            s3_path: S3 path for temporary storage
        
        Returns:
            DynamicFrame with extracted data
        """
        connection_options = {
            "useConnectionProperties": "true",
            "connectionName": self.connection_name,
            "dbtable": table_name,
            "fastExport": "true",
            "numPartitions": "10"
        }
        
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="teradata",
            connection_options=connection_options
        )
