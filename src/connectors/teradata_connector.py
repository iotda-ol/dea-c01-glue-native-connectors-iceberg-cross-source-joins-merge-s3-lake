"""
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
