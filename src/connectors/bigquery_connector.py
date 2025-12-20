"""
Google BigQuery Connector

Implements connection and data operations for Google BigQuery
using the BigQuery connector for AWS Glue.
"""

from typing import Any, Dict, List, Optional
from src.connectors.base_connector import BaseConnector
from src.utils.retry_handler import with_retry


class BigQueryConnector(BaseConnector):
    """
    Connector for Google BigQuery data warehouse.
    
    Uses BigQuery connector with proper authentication and
    billing project configuration.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize BigQuery connector.
        
        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - dataset: BigQuery dataset name
                - credentials_path: Path to GCP service account key
                - temp_gcs_bucket: GCS bucket for temporary files
        """
        super().__init__(config)
        self.glue_context = None
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        
    def connect(self) -> None:
        """Initialize BigQuery connection via Glue."""
        try:
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext
            
            if not self.glue_context:
                sc = SparkContext.getOrCreate()
                self.glue_context = GlueContext(sc)
                
            # Set up BigQuery credentials
            if self.credentials_path:
                self.glue_context.spark_session.conf.set(
                    "credentialsFile", 
                    self.credentials_path
                )
                
            self.logger.info("BigQuery connector initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery connector: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close BigQuery connection."""
        self.logger.info("BigQuery connector disconnected")
        
    @with_retry(max_attempts=3, delay=5)
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute query against BigQuery.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Spark DataFrame with query results
        """
        if not self.glue_context:
            self.connect()
            
        try:
            # Use BigQuery connector for Spark
            df = self.glue_context.spark_session.read \
                .format("bigquery") \
                .option("query", query) \
                .option("project", self.project_id) \
                .option("credentialsFile", self.credentials_path) \
                .load()
            
            self.logger.info(f"BigQuery query executed successfully")
            
            # Convert to DynamicFrame
            return self.glue_context.create_dynamic_frame.from_catalog(
                database=self.config.get('database', 'default'),
                table_name="temp_bigquery_result",
                transformation_ctx="bigquery_read"
            )
            
        except Exception as e:
            self.logger.error(f"BigQuery query execution failed: {str(e)}")
            raise
    
    def get_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get table schema from BigQuery.
        
        Args:
            table_name: Fully qualified table name (project.dataset.table)
            
        Returns:
            List of column definitions
        """
        schema_query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            is_partitioning_column
        FROM `{table_name.split('.')[0]}.{table_name.split('.')[1]}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name.split('.')[-1]}'
        ORDER BY ordinal_position
        """
        
        result = self.execute_query(schema_query)
        return result.toDF().collect()
    
    def read_table(self, 
                   table_name: str, 
                   filter_condition: Optional[str] = None,
                   selected_fields: Optional[List[str]] = None) -> Any:
        """
        Read table from BigQuery with optional filtering and column selection.
        
        Args:
            table_name: Fully qualified table name (project.dataset.table)
            filter_condition: Optional WHERE clause filter
            selected_fields: Optional list of columns to select
            
        Returns:
            Spark DataFrame with table data
        """
        if not self.glue_context:
            self.connect()
            
        spark = self.glue_context.spark_session
        
        read_options = {
            "table": table_name,
            "project": self.project_id,
            "credentialsFile": self.credentials_path
        }
        
        if filter_condition:
            read_options["filter"] = filter_condition
            
        if selected_fields:
            read_options["selectedFields"] = ",".join(selected_fields)
            
        # Read from BigQuery
        df = spark.read.format("bigquery").options(**read_options).load()
        
        # Convert to DynamicFrame
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="bigquery",
            connection_options=read_options
        )
    
    def read_query(self, query: str) -> Any:
        """
        Execute arbitrary SQL query and return results.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Spark DataFrame with query results
        """
        if not self.glue_context:
            self.connect()
            
        return self.glue_context.spark_session.read \
            .format("bigquery") \
            .option("query", query) \
            .option("project", self.project_id) \
            .option("credentialsFile", self.credentials_path) \
            .load()
