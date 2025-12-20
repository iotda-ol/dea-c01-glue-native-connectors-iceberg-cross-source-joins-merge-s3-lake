"""
Incremental Processor Module

Manages incremental data processing with watermark tracking.
"""

from typing import Any, Dict, Optional
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError


class IncrementalProcessor:
    """
    Manages incremental data processing.
    
    Tracks watermarks (last processed timestamp) to enable
    incremental loads and avoid reprocessing data.
    """
    
    def __init__(self,
                 job_name: str,
                 watermark_table: str,
                 timestamp_column: str,
                 region: str = 'us-east-1'):
        """
        Initialize incremental processor.
        
        Args:
            job_name: Unique name for the job
            watermark_table: DynamoDB table name for storing watermarks
            timestamp_column: Name of timestamp column in source data
            region: AWS region
        """
        self.job_name = job_name
        self.watermark_table = watermark_table
        self.timestamp_column = timestamp_column
        self.logger = logging.getLogger(__name__)
        
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(watermark_table)
    
    def get_last_watermark(self) -> Optional[str]:
        """
        Get the last processed watermark.
        
        Returns:
            Last watermark timestamp or None if no previous run
        """
        try:
            response = self.table.get_item(
                Key={'job_name': self.job_name}
            )
            
            if 'Item' in response:
                watermark = response['Item'].get('watermark')
                self.logger.info(f"Retrieved watermark for {self.job_name}: {watermark}")
                return watermark
            else:
                self.logger.info(f"No previous watermark found for {self.job_name}")
                return None
                
        except ClientError as e:
            self.logger.error(f"Failed to retrieve watermark: {str(e)}")
            raise
    
    def update_watermark(self, watermark: str) -> None:
        """
        Update the watermark after successful processing.
        
        Args:
            watermark: New watermark value (timestamp)
        """
        try:
            self.table.put_item(
                Item={
                    'job_name': self.job_name,
                    'watermark': watermark,
                    'updated_at': datetime.utcnow().isoformat()
                }
            )
            
            self.logger.info(f"Updated watermark for {self.job_name} to {watermark}")
            
        except ClientError as e:
            self.logger.error(f"Failed to update watermark: {str(e)}")
            raise
    
    def filter_incremental_data(self, 
                                data: Any,
                                watermark: Optional[str] = None) -> Any:
        """
        Filter data to include only records after the watermark.
        
        Args:
            data: Input DataFrame or DynamicFrame
            watermark: Optional watermark (uses last watermark if not provided)
            
        Returns:
            Filtered data
        """
        if watermark is None:
            watermark = self.get_last_watermark()
        
        if watermark is None:
            self.logger.info("No watermark found, processing all data")
            return data
        
        # Filter data
        if hasattr(data, 'toDF'):
            # DynamicFrame
            df = data.toDF()
            filtered_df = df.filter(f"{self.timestamp_column} > '{watermark}'")
            
            from awsglue.dynamicframe import DynamicFrame
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext
            
            glue_context = GlueContext(SparkContext.getOrCreate())
            filtered_data = DynamicFrame.fromDF(filtered_df, glue_context, "filtered")
            
        else:
            # DataFrame
            filtered_data = data.filter(f"{self.timestamp_column} > '{watermark}'")
        
        self.logger.info(f"Filtered data using watermark: {watermark}")
        return filtered_data
    
    def get_max_timestamp(self, data: Any) -> Optional[str]:
        """
        Get the maximum timestamp from the dataset.
        
        Args:
            data: Input DataFrame or DynamicFrame
            
        Returns:
            Maximum timestamp value as string
        """
        from pyspark.sql.functions import max as spark_max
        
        df = data.toDF() if hasattr(data, 'toDF') else data
        
        max_ts = df.agg(spark_max(self.timestamp_column)).collect()[0][0]
        
        if max_ts:
            return str(max_ts)
        return None
    
    def process_incrementally(self, 
                            data: Any,
                            auto_update_watermark: bool = True) -> Any:
        """
        Process data incrementally and optionally update watermark.
        
        Args:
            data: Input data
            auto_update_watermark: Whether to automatically update watermark
            
        Returns:
            Filtered incremental data
        """
        # Filter to incremental data
        filtered_data = self.filter_incremental_data(data)
        
        # Update watermark if requested
        if auto_update_watermark:
            max_ts = self.get_max_timestamp(filtered_data)
            if max_ts:
                self.update_watermark(max_ts)
        
        return filtered_data
