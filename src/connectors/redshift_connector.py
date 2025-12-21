"""
Amazon Redshift Connector

Implements connection and data operations for Amazon Redshift using
AWS Glue native connectors for optimal performance.
"""

from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError
from src.connectors.base_connector import BaseConnector
from src.utils.retry_handler import with_retry
Implements connector for reading data from Amazon Redshift
"""
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from .base_connector import BaseConnector


class RedshiftConnector(BaseConnector):
    """
    Connector for Amazon Redshift data warehouse.
    
    Uses AWS Glue native Redshift connector for optimized performance
    with pushdown predicates and parallel reads.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Redshift connector.
        
        Args:
            config: Configuration dictionary with:
                - host: Redshift cluster endpoint
                - port: Port number (default: 5439)
                - database: Database name
                - secret_arn: ARN of secret in AWS Secrets Manager
                - role_arn: IAM role for Glue job
        """
        super().__init__(config)
        self.glue_context = None
        self.spark = None
        self.secret_arn = config.get('secret_arn')
        
    def connect(self) -> None:
        """
        Establish connection to Redshift.
        
        For Glue jobs, this initializes the Glue context.
        """
        try:
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext
            
            if not self.spark:
                sc = SparkContext.getOrCreate()
                self.glue_context = GlueContext(sc)
                self.spark = self.glue_context.spark_session
                
            self.logger.info("Redshift connector initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize Redshift connector: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close Redshift connection."""
        if self.spark:
            # Spark context managed by Glue
            self.logger.info("Redshift connector disconnected")
            
    @with_retry(max_attempts=3, delay=5)
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute query against Redshift using Glue Dynamic Frame.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Glue DynamicFrame with query results
        """
        if not self.glue_context:
            self.connect()
            
        try:
            connection_options = {
                "url": f"jdbc:redshift://{self.config['host']}:{self.config.get('port', 5439)}/{self.config['database']}",
                "dbtable": f"({query}) as subquery",
                "redshiftTmpDir": self.config.get('temp_dir', 's3://glue-temp/redshift/'),
                "aws_iam_role": self.config.get('role_arn')
            }
            
            # Read using Glue native Redshift connector
            dynamic_frame = self.glue_context.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options=connection_options
            )
            
            self.logger.info(f"Query executed successfully, returned {dynamic_frame.count()} rows")
            return dynamic_frame
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get table schema from Redshift.
        
        Args:
            table_name: Table name (can include schema: schema.table)
            
        Returns:
            List of column definitions
        """
        schema_query = f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name.split('.')[-1]}'
        ORDER BY ordinal_position
        """
        
        result = self.execute_query(schema_query)
        return result.toDF().collect()
    
    def read_table(self, table_name: str, predicates: Optional[List[str]] = None) -> Any:
        """
        Read entire table with optional pushdown predicates.
        
        Args:
            table_name: Name of table to read
            predicates: Optional list of WHERE clause predicates for pushdown
            
        Returns:
            Glue DynamicFrame with table data
        """
        if not self.glue_context:
            self.connect()
            
        connection_options = {
            "url": f"jdbc:redshift://{self.config['host']}:{self.config.get('port', 5439)}/{self.config['database']}",
            "dbtable": table_name,
            "redshiftTmpDir": self.config.get('temp_dir', 's3://glue-temp/redshift/'),
            "aws_iam_role": self.config.get('role_arn')
        }
        
        if predicates:
            connection_options["predicates"] = predicates
            
    Connector for Amazon Redshift data warehouse
    Supports native Redshift optimizations and pushdown predicates
    """
    
    def __init__(self, config: Dict[str, Any], glue_context):
Reusable connector module for Amazon Redshift
Provides standardized interface for extracting data from Redshift
"""
import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from typing import Dict, Optional
import boto3
import json


class RedshiftConnector:
    """Connector for Amazon Redshift data source"""
    
    def __init__(self, glue_context: GlueContext, connection_name: str):
        """
        Initialize Redshift connector
        
        Args:
            config: Configuration containing:
                - connection_name: Glue connection name
                - database: Redshift database name
                - temp_dir: S3 temp directory for Redshift unload
        """
        super().__init__(config, glue_context)
        self.connection_name = config.get('connection_name')
        self.database = config.get('database')
        self.temp_dir = config.get('temp_dir', 's3://aws-glue-temp/')
        
    def connect(self) -> bool:
        """
        Validate Redshift connection exists
        
        Returns:
            bool: True if connection exists
        """
        try:
            # Connection is managed by Glue, just validate it exists
            self.logger.info(f"Using Glue connection: {self.connection_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to validate Redshift connection: {str(e)}")
            return False
    
    def read_table(self,
                   table_name: str,
                   schema: Optional[str] = None,
                   predicates: Optional[List[str]] = None,
                   columns: Optional[List[str]] = None) -> DataFrame:
        """
        Read data from Redshift table using Glue DynamicFrame
        
        Args:
            table_name: Redshift table name
            schema: Redshift schema name (default: public)
            predicates: Filter predicates for pushdown optimization
            columns: Columns to select (reduces data transfer)
            
        Returns:
            DataFrame: Spark DataFrame containing the data
        """
        try:
            schema = schema or 'public'
            full_table_name = f"{schema}.{table_name}"
            
            self.logger.info(f"Reading from Redshift table: {full_table_name}")
            
            # Build additional options
            additional_options = {
                "connectionName": self.connection_name,
                "redshiftTmpDir": self.temp_dir
            }
            
            # Add pushdown predicate if provided
            if predicates:
                predicate_string = " AND ".join(predicates)
                additional_options["push_down_predicate"] = predicate_string
                self.logger.info(f"Applying predicate pushdown: {predicate_string}")
            
            # Read using Glue DynamicFrame
            dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=table_name,
                transformation_ctx=f"read_{table_name}",
                additional_options=additional_options
            )
            
            # Convert to DataFrame
            df = dynamic_frame.toDF()
            
            # Apply column selection if specified
            df = self._select_columns(df, columns)
            
            row_count = df.count()
            self.logger.info(f"Successfully read {row_count} rows from {full_table_name}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from Redshift: {str(e)}")
            raise
    
    def get_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
        """
        Get schema information for a Redshift table
        
        Args:
            table_name: Table name
            schema: Schema name (default: public)
            
        Returns:
            Dict: Column name to data type mapping
        """
        try:
            schema = schema or 'public'
            df = self.read_table(table_name, schema)
            return {field.name: field.dataType.simpleString() for field in df.schema.fields}
        except Exception as e:
            self.logger.error(f"Failed to get schema: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> DataFrame:
        """
        Execute custom SQL query on Redshift
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame: Query results
        """
        try:
            self.logger.info(f"Executing Redshift query: {query[:100]}...")
            
            # Use Glue's Redshift query capability
            df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:redshift://{self.connection_name}") \
                .option("query", query) \
                .option("tempdir", self.temp_dir) \
                .load()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to execute query: {str(e)}")
            raise
            glue_context: AWS Glue context
            connection_name: Name of Glue connection for Redshift
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.connection_name = connection_name
        self.secrets = boto3.client('secretsmanager')
    
    def get_connection_details(self) -> Dict:
        """Retrieve connection details from Secrets Manager"""
        try:
            response = self.secrets.get_secret_value(
                SecretId=f"prod/redshift/{self.connection_name}"
            )
            return json.loads(response['SecretString'])
        except Exception as e:
            raise Exception(f"Failed to get connection details: {str(e)}")
    
    def extract_table(self, 
                     database: str, 
                     table_name: str,
                     predicate: Optional[str] = None) -> DynamicFrame:
        """
        Extract table from Redshift
        
        Args:
            database: Database name
            table_name: Table to extract
            predicate: Optional WHERE clause for filtering
        
        Returns:
            DynamicFrame containing extracted data
        """
        connection_options = {
            "useConnectionProperties": "true",
            "connectionName": self.connection_name,
            "dbtable": table_name
        }
        
        if predicate:
            connection_options["dbtable"] = f"(SELECT * FROM {table_name} WHERE {predicate})"
        
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options=connection_options
        )
    
    def extract_incremental(self,
                           database: str,
                           table_name: str,
                           timestamp_column: str,
                           last_timestamp: str) -> DynamicFrame:
        """
        Extract only new/modified records
        
        Args:
            database: Database name
            table_name: Table to extract
            timestamp_column: Column to filter on
            last_timestamp: Last extraction timestamp
        
        Returns:
            DynamicFrame with incremental data
        """
        predicate = f"{timestamp_column} > '{last_timestamp}'"
        return self.extract_table(database, table_name, predicate)
    
    def unload_to_s3(self,
                     query: str,
                     s3_path: str,
                     iam_role: str,
                     format: str = "PARQUET") -> None:
        """
        Unload query results directly to S3 using Redshift UNLOAD
        
        Args:
            query: SQL query to execute
            s3_path: S3 path for output
            iam_role: IAM role ARN for Redshift
            format: Output format (PARQUET, CSV, etc.)
        """
        conn_details = self.get_connection_details()
        
        unload_query = f"""
        UNLOAD ('{query}')
        TO '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS {format}
        ALLOWOVERWRITE
        PARALLEL ON
        """
        
        # Execute using JDBC
        # Implementation depends on your specific requirements
        pass
