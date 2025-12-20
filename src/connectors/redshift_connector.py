"""
Amazon Redshift Connector
Implements connector for reading data from Amazon Redshift
"""
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from .base_connector import BaseConnector


class RedshiftConnector(BaseConnector):
    """
    Connector for Amazon Redshift data warehouse
    Supports native Redshift optimizations and pushdown predicates
    """
    
    def __init__(self, config: Dict[str, Any], glue_context):
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
