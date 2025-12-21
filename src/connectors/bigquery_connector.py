"""
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
