"""
Base ETL Job Template
Provides reusable template for Glue ETL jobs
"""
import sys
from abc import ABC, abstractmethod
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from typing import Dict, Any, Optional
import logging


class BaseETLJob(ABC):
    """
    Abstract base class for ETL jobs
    Provides structure and common functionality
    """
    
    def __init__(self, job_name: str, args: Optional[Dict[str, str]] = None):
        """
        Initialize ETL job
        
        Args:
            job_name: Name of the Glue job
            args: Job arguments (if None, will get from command line)
        """
        self.job_name = job_name
        
        # Get job arguments
        if args is None:
            self.args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        else:
            self.args = args
        
        # Initialize Spark and Glue contexts
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.job_name, self.args)
        
        # Set up logging
        self.logger = self._setup_logger()
        
        # Job metrics
        self.metrics = {}
    
    def run(self):
        """
        Main execution method
        Orchestrates the ETL pipeline
        """
        try:
            self.logger.info(f"Starting job: {self.job_name}")
            self.log_job_parameters()
            
            # Extract
            self.logger.info("Starting extraction phase")
            source_data = self.extract()
            self._log_dataframe_stats(source_data, "source_data")
            
            # Transform
            self.logger.info("Starting transformation phase")
            transformed_data = self.transform(source_data)
            self._log_dataframe_stats(transformed_data, "transformed_data")
            
            # Validate
            self.logger.info("Starting validation phase")
            if self.validate(transformed_data):
                self.logger.info("Validation passed")
            else:
                raise ValueError("Data validation failed")
            
            # Load
            self.logger.info("Starting load phase")
            self.load(transformed_data)
            
            # Post-processing
            self.logger.info("Starting post-processing")
            self.post_process()
            
            self.logger.info(f"Job completed successfully: {self.job_name}")
            self.job.commit()
            
        except Exception as e:
            self.logger.error(f"Job failed: {str(e)}", exc_info=True)
            raise
    
    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from source
        Must be implemented by subclasses
        
        Returns:
            DataFrame: Extracted data
        """
        pass
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform extracted data
        Must be implemented by subclasses
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Transformed data
        """
        pass
    
    @abstractmethod
    def load(self, df: DataFrame):
        """
        Load transformed data to target
        Must be implemented by subclasses
        
        Args:
            df: DataFrame to load
        """
        pass
    
    def validate(self, df: DataFrame) -> bool:
        """
        Validate data quality
        Can be overridden by subclasses
        
        Args:
            df: DataFrame to validate
            
        Returns:
            bool: True if validation passes
        """
        # Default validation: check for data
        return df.count() > 0
    
    def post_process(self):
        """
        Post-processing tasks
        Can be overridden by subclasses
        """
        pass
    
    def _setup_logger(self) -> logging.Logger:
        """
        Set up logger
        
        Returns:
            Logger instance
        """
        logger = logging.getLogger(self.job_name)
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def log_job_parameters(self):
        """Log job parameters"""
        self.logger.info(f"Job parameters: {self.args}")
    
    def _log_dataframe_stats(self, df: DataFrame, name: str):
        """
        Log DataFrame statistics
        
        Args:
            df: DataFrame
            name: Name for logging
        """
        row_count = df.count()
        col_count = len(df.columns)
        
        self.logger.info(
            f"{name} - Rows: {row_count}, Columns: {col_count}"
        )
        
        self.metrics[f"{name}_rows"] = row_count
        self.metrics[f"{name}_columns"] = col_count
    
    def get_arg(self, key: str, default: Any = None) -> Any:
        """
        Get job argument with default
        
        Args:
            key: Argument key
            default: Default value if not found
            
        Returns:
            Argument value
        """
        return self.args.get(key, default)
