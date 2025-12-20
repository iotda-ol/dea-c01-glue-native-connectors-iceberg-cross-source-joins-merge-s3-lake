"""
Teradata Connector

Implements connection and data operations for Teradata Vantage
using JDBC connector with optimizations for large-scale data transfer.
"""

from typing import Any, Dict, List, Optional
from src.connectors.base_connector import BaseConnector
from src.utils.retry_handler import with_retry


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
            connection_options=connection_options
        )
