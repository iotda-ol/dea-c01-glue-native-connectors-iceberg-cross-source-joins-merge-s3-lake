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
            
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options=connection_options
        )
