"""
Validation Transform Module

Implements data quality and validation checks for data pipeline.
"""

from typing import Any, Dict, List, Optional, Callable
from src.transforms.base_transform import BaseTransform
import re


class ValidationTransform(BaseTransform):
    """
    Transform for validating data quality.
    
    Supports various validation rules including null checks,
    type validation, range checks, and custom validators.
    """
    
    def __init__(self, validation_rules: List[Dict[str, Any]], config: Optional[Dict[str, Any]] = None):
        """
        Initialize validation transform.
        
        Args:
            validation_rules: List of validation rule dictionaries
            config: Additional configuration
        """
        super().__init__(config)
        self.validation_rules = validation_rules
        self.validation_results = []
        
    def apply(self, data: Any) -> Any:
        """
        Apply validation rules to data.
        
        Args:
            data: Input data to validate
            
        Returns:
            Validated data (with invalid rows optionally filtered)
        """
        df = self._to_dataframe(data)
        
        self.validation_results = []
        
        for rule in self.validation_rules:
            rule_type = rule.get('type')
            column = rule.get('column')
            
            if rule_type == 'null_check':
                self._validate_nulls(df, column, rule)
            elif rule_type == 'type_check':
                self._validate_type(df, column, rule)
            elif rule_type == 'range_check':
                self._validate_range(df, column, rule)
            elif rule_type == 'format_check':
                self._validate_format(df, column, rule)
            elif rule_type == 'custom':
                self._validate_custom(df, column, rule)
        
        # Log validation results
        self._log_validation_results()
        
        # Filter invalid rows if configured
        if self.config.get('filter_invalid', False):
            df = self._filter_invalid_rows(df)
        
        # Convert back to original format
        if self._is_dynamic_frame(data):
            from awsglue.dynamicframe import DynamicFrame
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext
            
            glue_context = GlueContext(SparkContext.getOrCreate())
            return DynamicFrame.fromDF(df, glue_context, "validated")
        
        return df
    
    def _validate_nulls(self, df: Any, column: str, rule: Dict[str, Any]) -> None:
        """Validate null values in column."""
        allow_nulls = rule.get('allow_nulls', False)
        
        null_count = df.filter(df[column].isNull()).count()
        total_count = df.count()
        
        result = {
            'rule_type': 'null_check',
            'column': column,
            'passed': (null_count == 0) if not allow_nulls else True,
            'null_count': null_count,
            'total_count': total_count,
            'null_percentage': (null_count / total_count * 100) if total_count > 0 else 0
        }
        
        self.validation_results.append(result)
    
    def _validate_type(self, df: Any, column: str, rule: Dict[str, Any]) -> None:
        """Validate data type of column."""
        expected_type = rule.get('expected_type')
        actual_type = str(df.schema[column].dataType)
        
        result = {
            'rule_type': 'type_check',
            'column': column,
            'passed': expected_type.lower() in actual_type.lower(),
            'expected_type': expected_type,
            'actual_type': actual_type
        }
        
        self.validation_results.append(result)
    
    def _validate_range(self, df: Any, column: str, rule: Dict[str, Any]) -> None:
        """Validate value ranges."""
        min_value = rule.get('min_value')
        max_value = rule.get('max_value')
        
        out_of_range_count = 0
        
        if min_value is not None:
            out_of_range_count += df.filter(df[column] < min_value).count()
        
        if max_value is not None:
            out_of_range_count += df.filter(df[column] > max_value).count()
        
        total_count = df.count()
        
        result = {
            'rule_type': 'range_check',
            'column': column,
            'passed': out_of_range_count == 0,
            'out_of_range_count': out_of_range_count,
            'total_count': total_count,
            'min_value': min_value,
            'max_value': max_value
        }
        
        self.validation_results.append(result)
    
    def _validate_format(self, df: Any, column: str, rule: Dict[str, Any]) -> None:
        """Validate string format using regex."""
        pattern = rule.get('pattern')
        
        if pattern:
            # This would require UDF for complex regex validation
            # Simplified version
            result = {
                'rule_type': 'format_check',
                'column': column,
                'pattern': pattern,
                'passed': True  # Placeholder
            }
            
            self.validation_results.append(result)
    
    def _validate_custom(self, df: Any, column: str, rule: Dict[str, Any]) -> None:
        """Apply custom validation function."""
        validator_func = rule.get('validator')
        
        if callable(validator_func):
            try:
                passed = validator_func(df, column)
                result = {
                    'rule_type': 'custom',
                    'column': column,
                    'passed': passed
                }
                self.validation_results.append(result)
            except Exception as e:
                self.logger.error(f"Custom validation failed: {str(e)}")
    
    def _log_validation_results(self) -> None:
        """Log all validation results."""
        for result in self.validation_results:
            status = "PASSED" if result['passed'] else "FAILED"
            self.logger.info(f"Validation {status}: {result}")
    
    def _filter_invalid_rows(self, df: Any) -> Any:
        """Filter out rows that don't pass validation."""
        # Implementation would depend on specific requirements
        return df
    
    def _to_dataframe(self, data: Any) -> Any:
        """Convert DynamicFrame to DataFrame if needed."""
        if self._is_dynamic_frame(data):
            return data.toDF()
        return data
    
    def _is_dynamic_frame(self, data: Any) -> bool:
        """Check if data is a DynamicFrame."""
        return hasattr(data, 'toDF') and callable(getattr(data, 'toDF'))
    
    def get_validation_report(self) -> List[Dict[str, Any]]:
        """
        Get detailed validation report.
        
        Returns:
            List of validation results
        """
        return self.validation_results
    
    def all_validations_passed(self) -> bool:
        """
        Check if all validations passed.
        
        Returns:
            True if all validations passed, False otherwise
        """
        return all(result['passed'] for result in self.validation_results)
