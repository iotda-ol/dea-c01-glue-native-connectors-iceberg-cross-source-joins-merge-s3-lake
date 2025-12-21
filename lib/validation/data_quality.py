"""
Data Quality Validator
Validates data quality before loading to target
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull
from typing import List, Dict, Optional
import logging


class DataQualityValidator:
    """
    Validates data quality using configurable rules
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.validation_results = []
    
    def validate_not_null(self, 
                         df: DataFrame, 
                         columns: List[str],
                         threshold: float = 0.0) -> bool:
        """
        Validate that columns have no nulls (or below threshold)
        
        Args:
            df: DataFrame to validate
            columns: List of column names to check
            threshold: Maximum acceptable null percentage (0.0 to 1.0)
            
        Returns:
            bool: True if validation passes
        """
        total_rows = df.count()
        passed = True
        
        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = null_count / total_rows if total_rows > 0 else 0
            
            if null_percentage > threshold:
                self.logger.error(
                    f"Column {column} has {null_percentage:.2%} nulls, "
                    f"exceeds threshold of {threshold:.2%}"
                )
                self.validation_results.append({
                    "rule": "not_null",
                    "column": column,
                    "status": "failed",
                    "null_percentage": null_percentage
                })
                passed = False
            else:
                self.validation_results.append({
                    "rule": "not_null",
                    "column": column,
                    "status": "passed",
                    "null_percentage": null_percentage
                })
        
        return passed
    
    def validate_unique(self,
                       df: DataFrame,
                       columns: List[str]) -> bool:
        """
        Validate that column combinations are unique
        
        Args:
            df: DataFrame to validate
            columns: List of columns that should be unique together
            
        Returns:
            bool: True if validation passes
        """
        total_rows = df.count()
        distinct_rows = df.select(columns).distinct().count()
        
        if total_rows != distinct_rows:
            duplicate_count = total_rows - distinct_rows
            self.logger.error(
                f"Found {duplicate_count} duplicate rows for columns {columns}"
            )
            self.validation_results.append({
                "rule": "unique",
                "columns": columns,
                "status": "failed",
                "duplicate_count": duplicate_count
            })
            return False
        
        self.validation_results.append({
            "rule": "unique",
            "columns": columns,
            "status": "passed"
        })
        return True
    
    def validate_value_range(self,
                           df: DataFrame,
                           column: str,
                           min_value: Optional[float] = None,
                           max_value: Optional[float] = None) -> bool:
        """
        Validate that numeric values are within range
        
        Args:
            df: DataFrame to validate
            column: Column name
            min_value: Minimum acceptable value
            max_value: Maximum acceptable value
            
        Returns:
            bool: True if validation passes
        """
        out_of_range = df.filter(
            (col(column) < min_value) if min_value is not None else False |
            (col(column) > max_value) if max_value is not None else False
        ).count()
        
        if out_of_range > 0:
            self.logger.error(
                f"Column {column} has {out_of_range} values out of range "
                f"[{min_value}, {max_value}]"
            )
            self.validation_results.append({
                "rule": "value_range",
                "column": column,
                "status": "failed",
                "out_of_range_count": out_of_range
            })
            return False
        
        self.validation_results.append({
            "rule": "value_range",
            "column": column,
            "status": "passed"
        })
        return True
    
    def validate_format(self,
                       df: DataFrame,
                       column: str,
                       pattern: str) -> bool:
        """
        Validate that string values match a regex pattern
        
        Args:
            df: DataFrame to validate
            column: Column name
            pattern: Regex pattern to match
            
        Returns:
            bool: True if validation passes
        """
        from pyspark.sql.functions import regexp_extract
        
        invalid_count = df.filter(
            regexp_extract(col(column), pattern, 0) == ""
        ).count()
        
        if invalid_count > 0:
            self.logger.error(
                f"Column {column} has {invalid_count} values not matching pattern {pattern}"
            )
            self.validation_results.append({
                "rule": "format",
                "column": column,
                "status": "failed",
                "invalid_count": invalid_count
            })
            return False
        
        self.validation_results.append({
            "rule": "format",
            "column": column,
            "status": "passed"
        })
        return True
    
    def validate_referential_integrity(self,
                                      df: DataFrame,
                                      lookup_df: DataFrame,
                                      foreign_key: str,
                                      primary_key: str) -> bool:
        """
        Validate referential integrity between tables
        
        Args:
            df: DataFrame with foreign keys
            lookup_df: Reference DataFrame with primary keys
            foreign_key: Foreign key column name
            primary_key: Primary key column name in lookup table
            
        Returns:
            bool: True if validation passes
        """
        orphan_count = df.join(
            lookup_df,
            df[foreign_key] == lookup_df[primary_key],
            "left_anti"
        ).count()
        
        if orphan_count > 0:
            self.logger.error(
                f"Found {orphan_count} orphan records with no matching {primary_key}"
            )
            self.validation_results.append({
                "rule": "referential_integrity",
                "foreign_key": foreign_key,
                "status": "failed",
                "orphan_count": orphan_count
            })
            return False
        
        self.validation_results.append({
            "rule": "referential_integrity",
            "foreign_key": foreign_key,
            "status": "passed"
        })
        return True
    
    def get_validation_report(self) -> Dict:
        """
        Get comprehensive validation report
        
        Returns:
            Dict: Validation results summary
        """
        total_validations = len(self.validation_results)
        passed = sum(1 for r in self.validation_results if r["status"] == "passed")
        failed = total_validations - passed
        
        return {
            "total_validations": total_validations,
            "passed": passed,
            "failed": failed,
            "success_rate": passed / total_validations if total_validations > 0 else 0,
            "details": self.validation_results
        }
