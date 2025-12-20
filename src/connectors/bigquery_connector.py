"""
Google BigQuery Connector
Implements connector for reading data from Google BigQuery
"""
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from .base_connector import BaseConnector


class BigQueryConnector(BaseConnector):
    """
    Connector for Google BigQuery
    Uses Spark BigQuery connector for optimized reads
    """
    
    def __init__(self, config: Dict[str, Any], glue_context):
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
