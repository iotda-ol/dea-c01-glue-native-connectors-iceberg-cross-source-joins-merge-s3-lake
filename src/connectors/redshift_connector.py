"""
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
