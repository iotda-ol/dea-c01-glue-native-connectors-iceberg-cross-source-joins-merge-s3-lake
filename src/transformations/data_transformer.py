"""
Data Transformation Utilities
Reusable transformation functions for ETL pipelines
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, to_date, trim, upper, lower,
    when, lit, coalesce, concat, regexp_replace
)
from typing import List, Dict, Optional


class DataTransformer:
    """
    Collection of reusable data transformation utilities
    """
    
    @staticmethod
    def standardize_dates(df: DataFrame, 
                         date_columns: List[str],
                         date_format: str = "yyyy-MM-dd") -> DataFrame:
        """
        Standardize date columns to consistent format
        
        Args:
            df: Input DataFrame
            date_columns: List of date column names
            date_format: Target date format
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        for col_name in date_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, to_date(col(col_name), date_format))
        return df
    
    @staticmethod
    def standardize_timestamps(df: DataFrame, 
                              timestamp_columns: List[str],
                              timestamp_format: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
        """
        Standardize timestamp columns to consistent format
        
        Args:
            df: Input DataFrame
            timestamp_columns: List of timestamp column names
            timestamp_format: Target timestamp format
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        for col_name in timestamp_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, to_timestamp(col(col_name), timestamp_format))
        return df
    
    @staticmethod
    def trim_strings(df: DataFrame, string_columns: Optional[List[str]] = None) -> DataFrame:
        """
        Trim whitespace from string columns
        
        Args:
            df: Input DataFrame
            string_columns: List of column names to trim (None = all string columns)
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        if string_columns is None:
            string_columns = [field.name for field in df.schema.fields 
                            if str(field.dataType) == 'StringType']
        
        for col_name in string_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, trim(col(col_name)))
        return df
    
    @staticmethod
    def handle_nulls(df: DataFrame, 
                    null_replacements: Dict[str, any]) -> DataFrame:
        """
        Replace null values with specified defaults
        
        Args:
            df: Input DataFrame
            null_replacements: Dict mapping column names to replacement values
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        for col_name, replacement in null_replacements.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, coalesce(col(col_name), lit(replacement)))
        return df
    
    @staticmethod
    def standardize_column_names(df: DataFrame, 
                                naming_convention: str = "snake_case") -> DataFrame:
        """
        Standardize column names to consistent naming convention
        
        Args:
            df: Input DataFrame
            naming_convention: 'snake_case' or 'camelCase'
            
        Returns:
            DataFrame: Transformed DataFrame with renamed columns
        """
        if naming_convention == "snake_case":
            for col_name in df.columns:
                new_name = col_name.lower().replace(" ", "_").replace("-", "_")
                df = df.withColumnRenamed(col_name, new_name)
        elif naming_convention == "camelCase":
            for col_name in df.columns:
                parts = col_name.lower().replace("-", "_").split("_")
                new_name = parts[0] + "".join(word.capitalize() for word in parts[1:])
                df = df.withColumnRenamed(col_name, new_name)
        
        return df
    
    @staticmethod
    def deduplicate(df: DataFrame, 
                   key_columns: List[str],
                   order_column: Optional[str] = None,
                   keep: str = "first") -> DataFrame:
        """
        Remove duplicate rows based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: Columns to use for identifying duplicates
            order_column: Column to order by when keeping first/last
            keep: 'first' or 'last' (requires order_column)
            
        Returns:
            DataFrame: Deduplicated DataFrame
        """
        if order_column and keep in ["first", "last"]:
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, desc
            
            window_spec = Window.partitionBy(key_columns)
            if keep == "last":
                window_spec = window_spec.orderBy(desc(order_column))
            else:
                window_spec = window_spec.orderBy(order_column)
            
            df = df.withColumn("_row_num", row_number().over(window_spec))
            df = df.filter(col("_row_num") == 1).drop("_row_num")
        else:
            df = df.dropDuplicates(key_columns)
        
        return df
    
    @staticmethod
    def add_audit_columns(df: DataFrame,
                         load_timestamp: Optional[str] = None,
                         source_system: Optional[str] = None) -> DataFrame:
        """
        Add audit columns for data lineage tracking
        
        Args:
            df: Input DataFrame
            load_timestamp: Timestamp column name to add
            source_system: Source system identifier
            
        Returns:
            DataFrame: DataFrame with audit columns
        """
        from pyspark.sql.functions import current_timestamp
        
        if load_timestamp:
            df = df.withColumn(load_timestamp, current_timestamp())
        
        if source_system:
            df = df.withColumn("source_system", lit(source_system))
        
        return df
    
    @staticmethod
    def cast_columns(df: DataFrame, 
                    column_types: Dict[str, str]) -> DataFrame:
        """
        Cast columns to specified data types
        
        Args:
            df: Input DataFrame
            column_types: Dict mapping column names to target types
                         (e.g., {'age': 'int', 'salary': 'decimal(10,2)'})
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        for col_name, data_type in column_types.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(data_type))
        return df
    
    @staticmethod
    def filter_invalid_records(df: DataFrame,
                              validation_rules: Dict[str, str]) -> DataFrame:
        """
        Filter out records that don't meet validation rules
        
        Args:
            df: Input DataFrame
            validation_rules: Dict mapping column names to SQL conditions
                            (e.g., {'age': '> 0', 'email': 'IS NOT NULL'})
            
        Returns:
            DataFrame: Filtered DataFrame
        """
        for col_name, condition in validation_rules.items():
            if col_name in df.columns:
                df = df.filter(f"{col_name} {condition}")
        return df
    
    @staticmethod
    def enrich_with_lookup(df: DataFrame,
                          lookup_df: DataFrame,
                          join_key: str,
                          lookup_columns: List[str]) -> DataFrame:
        """
        Enrich data with lookup table
        
        Args:
            df: Input DataFrame
            lookup_df: Lookup DataFrame
            join_key: Column to join on
            lookup_columns: Columns to bring from lookup
            
        Returns:
            DataFrame: Enriched DataFrame
        """
        lookup_subset = lookup_df.select([join_key] + lookup_columns)
        return df.join(lookup_subset, on=join_key, how="left")
