"""
Common transformation functions for data processing
Reusable across all data sources and pipelines
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, coalesce
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from typing import List, Dict, Optional


class CommonTransforms:
    """Reusable transformation functions"""
    
    @staticmethod
    def add_audit_columns(df: DataFrame) -> DataFrame:
        """
        Add standard audit columns to DataFrame
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with audit columns added
        """
        return df.withColumn("ingestion_timestamp", current_timestamp()) \
                 .withColumn("last_modified", current_timestamp()) \
                 .withColumn("is_active", lit(True))
    
    @staticmethod
    def add_source_system_column(df: DataFrame, source_system: str) -> DataFrame:
        """
        Add source system identifier column
        
        Args:
            df: Input DataFrame
            source_system: Source system name (redshift, teradata, bigquery)
        
        Returns:
            DataFrame with source_system column
        """
        return df.withColumn("source_system", lit(source_system))
    
    @staticmethod
    def standardize_column_names(df: DataFrame) -> DataFrame:
        """
        Standardize column names to snake_case
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with standardized column names
        """
        import re
        for column in df.columns:
            # Convert to snake_case
            new_name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column)
            new_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', new_name).lower()
            df = df.withColumnRenamed(column, new_name)
        return df
    
    @staticmethod
    def remove_duplicates(df: DataFrame, 
                         key_columns: List[str],
                         order_column: Optional[str] = None) -> DataFrame:
        """
        Remove duplicate records based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: Columns to identify duplicates
            order_column: Column to determine which duplicate to keep (keep latest)
        
        Returns:
            DataFrame with duplicates removed
        """
        if order_column:
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, desc
            
            window_spec = Window.partitionBy(key_columns).orderBy(desc(order_column))
            return df.withColumn("row_num", row_number().over(window_spec)) \
                     .filter(col("row_num") == 1) \
                     .drop("row_num")
        else:
            return df.dropDuplicates(key_columns)
    
    @staticmethod
    def handle_nulls(df: DataFrame, 
                     column_defaults: Dict[str, any]) -> DataFrame:
        """
        Replace null values with defaults
        
        Args:
            df: Input DataFrame
            column_defaults: Dict mapping column names to default values
        
        Returns:
            DataFrame with nulls replaced
        """
        for column, default_value in column_defaults.items():
            if column in df.columns:
                df = df.withColumn(column, coalesce(col(column), lit(default_value)))
        return df
    
    @staticmethod
    def filter_inactive_records(df: DataFrame,
                               active_column: str = "is_active") -> DataFrame:
        """
        Filter out inactive records
        
        Args:
            df: Input DataFrame
            active_column: Name of active flag column
        
        Returns:
            DataFrame with only active records
        """
        return df.filter(col(active_column) == True)
    
    @staticmethod
    def apply_data_masking(df: DataFrame,
                          columns_to_mask: List[str],
                          mask_char: str = "*") -> DataFrame:
        """
        Mask sensitive data columns
        
        Args:
            df: Input DataFrame
            columns_to_mask: List of column names to mask
            mask_char: Character to use for masking
        
        Returns:
            DataFrame with masked columns
        """
        from pyspark.sql.functions import regexp_replace
        
        for column in columns_to_mask:
            if column in df.columns:
                # Mask all but last 4 characters
                df = df.withColumn(
                    column,
                    regexp_replace(col(column), ".(?=.{4})", mask_char)
                )
        return df
