"""
Google BigQuery Connector

Implements connection and data operations for Google BigQuery
using the BigQuery connector for AWS Glue.
"""

from typing import Any, Dict, List, Optional
from src.connectors.base_connector import BaseConnector
from src.utils.retry_handler import with_retry
Implements connector for reading data from Google BigQuery
"""
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from .base_connector import BaseConnector


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
    Connector for Google BigQuery
    Uses Spark BigQuery connector for optimized reads
    """
    
    def __init__(self, config: Dict[str, Any], glue_context):
Reusable connector module for Google BigQuery
Provides standardized interface for extracting data from BigQuery
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import boto3
import json


class BigQueryConnector:
    """Connector for Google BigQuery data source"""
    
    def __init__(self, spark: SparkSession, project_id: str, credentials_secret: str):
        """
        Initialize BigQuery connector
        
        Args:
            config: Configuration containing:
                - project_id: GCP project ID
                - credentials_path: Path to service account JSON key
                - temp_bucket: GCS bucket for temporary data
        """
        super().__init__(config, glue_context)
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.temp_bucket = config.get('temp_bucket')
        
        # Configure Spark for BigQuery
        self._configure_spark()
        
    def _configure_spark(self):
        """Configure Spark session with BigQuery connector"""
        self.spark.conf.set("spark.sql.catalog.bigquery", 
                           "com.google.cloud.spark.bigquery.v2.BigQueryCatalog")
        self.spark.conf.set("credentialsFile", self.credentials_path)
        self.spark.conf.set("parentProject", self.project_id)
        
        if self.temp_bucket:
            self.spark.conf.set("temporaryGcsBucket", self.temp_bucket)
    
    def connect(self) -> bool:
        """
        Validate BigQuery connection
        
        Returns:
            bool: True if connection is valid
        """
        try:
            self.logger.info(f"Validating BigQuery connection for project: {self.project_id}")
            # Test connection by listing datasets
            test_query = f"SELECT 1 as test"
            self.spark.read \
                .format("bigquery") \
                .option("query", test_query) \
                .load()
            return True
        except Exception as e:
            self.logger.error(f"Failed to validate BigQuery connection: {str(e)}")
            return False
    
    def read_table(self,
                   table_name: str,
                   schema: Optional[str] = None,
                   predicates: Optional[List[str]] = None,
                   columns: Optional[List[str]] = None) -> DataFrame:
        """
        Read data from BigQuery table
        
        Args:
            table_name: BigQuery table name
            schema: Dataset name (required for BigQuery)
            predicates: Filter predicates for pushdown
            columns: Columns to select
            
        Returns:
            DataFrame: Spark DataFrame containing the data
        """
        try:
            if not schema:
                raise ValueError("Dataset name (schema) is required for BigQuery")
            
            full_table_name = f"{self.project_id}.{schema}.{table_name}"
            self.logger.info(f"Reading from BigQuery table: {full_table_name}")
            
            # Build read options
            read_options = {
                "table": full_table_name,
                "project": self.project_id
            }
            
            # Build query with column selection and filters
            if columns or predicates:
                query = self._build_query(schema, table_name, columns, predicates)
                self.logger.info(f"Using custom query: {query}")
                df = self.spark.read \
                    .format("bigquery") \
                    .option("query", query) \
                    .option("project", self.project_id) \
                    .load()
            else:
                # Direct table read
                df = self.spark.read \
                    .format("bigquery") \
                    .options(**read_options) \
                    .load()
            
            row_count = df.count()
            self.logger.info(f"Successfully read {row_count} rows from {full_table_name}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from BigQuery: {str(e)}")
            raise
    
    def get_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
        """
        Get schema information for a BigQuery table
        
        Args:
            table_name: Table name
            schema: Dataset name
            
        Returns:
            Dict: Column name to data type mapping
        """
        try:
            if not schema:
                raise ValueError("Dataset name is required for BigQuery")
            
            df = self.read_table(table_name, schema)
            return {field.name: field.dataType.simpleString() for field in df.schema.fields}
            
        except Exception as e:
            self.logger.error(f"Failed to get schema: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> DataFrame:
        """
        Execute custom SQL query on BigQuery
        
        Args:
            query: Standard SQL query string
            
        Returns:
            DataFrame: Query results
        """
        try:
            self.logger.info(f"Executing BigQuery query: {query[:100]}...")
            
            df = self.spark.read \
                .format("bigquery") \
                .option("query", query) \
                .option("project", self.project_id) \
                .load()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to execute query: {str(e)}")
            raise
    
    def _build_query(self, 
                     dataset: str, 
                     table: str,
                     columns: Optional[List[str]] = None,
                     predicates: Optional[List[str]] = None) -> str:
        """
        Build optimized SQL query for BigQuery
        
        Args:
            dataset: Dataset name
            table: Table name
            columns: Columns to select
            predicates: Filter predicates
            
        Returns:
            str: SQL query
        """
        column_list = ", ".join(columns) if columns else "*"
        query = f"SELECT {column_list} FROM `{self.project_id}.{dataset}.{table}`"
        
        if predicates:
            where_clause = " AND ".join(predicates)
            query += f" WHERE {where_clause}"
        
        return query
            spark: Spark session
            project_id: GCP project ID
            credentials_secret: AWS Secrets Manager secret name for GCP credentials
        """
        self.spark = spark
        self.project_id = project_id
        self.credentials_secret = credentials_secret
        self.secrets = boto3.client('secretsmanager')
    
    def get_credentials(self) -> Dict:
        """Retrieve BigQuery credentials from Secrets Manager"""
        response = self.secrets.get_secret_value(SecretId=self.credentials_secret)
        return json.loads(response['SecretString'])
    
    def extract_table(self,
                     dataset: str,
                     table_name: str,
                     filter_query: Optional[str] = None) -> DataFrame:
        """
        Extract table from BigQuery
        
        Args:
            dataset: BigQuery dataset name
            table_name: Table to extract
            filter_query: Optional filter SQL
        
        Returns:
            Spark DataFrame with extracted data
        """
        full_table = f"{self.project_id}.{dataset}.{table_name}"
        
        df_builder = self.spark.read \
            .format("bigquery") \
            .option("project", self.project_id) \
            .option("dataset", dataset) \
            .option("table", table_name)
        
        if filter_query:
            df_builder = df_builder.option("filter", filter_query)
        
        return df_builder.load()
    
    def extract_query(self, sql_query: str, temp_dataset: str = "temp") -> DataFrame:
        """
        Execute SQL query and extract results
        
        Args:
            sql_query: SQL query to execute
            temp_dataset: Temporary dataset for materialization
        
        Returns:
            Spark DataFrame with query results
        """
        return self.spark.read \
            .format("bigquery") \
            .option("project", self.project_id) \
            .option("materializationDataset", temp_dataset) \
            .option("query", sql_query) \
            .load()
