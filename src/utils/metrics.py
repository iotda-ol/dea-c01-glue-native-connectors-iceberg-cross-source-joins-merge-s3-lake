"""
Monitoring and metrics utilities
Sends custom metrics to CloudWatch
"""
import boto3
from datetime import datetime
from typing import List, Dict, Optional


class MetricsCollector:
    """Collects and publishes metrics to CloudWatch"""
    
    def __init__(self, namespace: str = "GlueDataPipeline", job_name: Optional[str] = None):
        """
        Initialize metrics collector
        
        Args:
            namespace: CloudWatch namespace
            job_name: Name of the job (used as dimension)
        """
        self.cloudwatch = boto3.client('cloudwatch')
        self.namespace = namespace
        self.job_name = job_name
        self.metrics = []
    
    def record_metric(self,
                     metric_name: str,
                     value: float,
                     unit: str = 'None',
                     dimensions: Optional[Dict[str, str]] = None):
        """
        Record a metric
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Metric unit (Seconds, Count, Bytes, etc.)
            dimensions: Additional dimensions
        """
        metric_data = {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.utcnow()
        }
        
        # Add dimensions
        dims = []
        if self.job_name:
            dims.append({'Name': 'JobName', 'Value': self.job_name})
        
        if dimensions:
            dims.extend([{'Name': k, 'Value': v} for k, v in dimensions.items()])
        
        if dims:
            metric_data['Dimensions'] = dims
        
        self.metrics.append(metric_data)
    
    def record_execution_time(self, duration_seconds: float):
        """Record job execution time"""
        self.record_metric('ExecutionTime', duration_seconds, 'Seconds')
    
    def record_records_processed(self, count: int):
        """Record number of records processed"""
        self.record_metric('RecordsProcessed', count, 'Count')
    
    def record_data_size(self, size_bytes: int):
        """Record data size processed"""
        self.record_metric('DataSizeBytes', size_bytes, 'Bytes')
    
    def record_error(self, error_type: str = 'GenericError'):
        """Record an error occurrence"""
        self.record_metric('ErrorCount', 1, 'Count', {'ErrorType': error_type})
    
    def publish_metrics(self):
        """Publish all collected metrics to CloudWatch"""
        if not self.metrics:
            return
        
        # CloudWatch allows max 20 metrics per request
        batch_size = 20
        for i in range(0, len(self.metrics), batch_size):
            batch = self.metrics[i:i + batch_size]
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=batch
            )
        
        # Clear metrics after publishing
        self.metrics = []
    
    def __enter__(self):
        """Context manager entry"""
        self.start_time = datetime.utcnow()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto publish metrics"""
        if self.start_time:
            duration = (datetime.utcnow() - self.start_time).total_seconds()
            self.record_execution_time(duration)
        
        if exc_type:
            self.record_error(exc_type.__name__)
        
        self.publish_metrics()
