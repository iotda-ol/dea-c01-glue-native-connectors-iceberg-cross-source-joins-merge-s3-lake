"""
Data Masking Transform Module

Implements data masking for sensitive information (PII).
"""

from typing import Any, Dict, List, Optional
from src.transforms.base_transform import BaseTransform
import hashlib


class MaskingTransform(BaseTransform):
    """
    Transform for masking sensitive data.
    
    Supports various masking strategies: redaction, hashing, tokenization, encryption.
    """
    
    MASKING_STRATEGIES = ['redact', 'hash', 'partial', 'tokenize', 'encrypt']
    
    def __init__(self, masking_config: List[Dict[str, Any]], config: Optional[Dict[str, Any]] = None):
        """
        Initialize masking transform.
        
        Args:
            masking_config: List of masking configurations for columns
            config: Additional configuration
        """
        super().__init__(config)
        self.masking_config = masking_config
        
    def apply(self, data: Any) -> Any:
        """
        Apply masking to sensitive columns.
        
        Args:
            data: Input data
            
        Returns:
            Data with sensitive fields masked
        """
        from pyspark.sql import functions as F
        
        df = self._to_dataframe(data)
        
        for mask_spec in self.masking_config:
            column = mask_spec.get('column')
            strategy = mask_spec.get('strategy')
            
            if strategy not in self.MASKING_STRATEGIES:
                self.logger.warning(f"Unknown masking strategy: {strategy}")
                continue
            
            if strategy == 'redact':
                df = self._redact_column(df, column, mask_spec)
            elif strategy == 'hash':
                df = self._hash_column(df, column, mask_spec)
            elif strategy == 'partial':
                df = self._partial_mask_column(df, column, mask_spec)
            elif strategy == 'tokenize':
                df = self._tokenize_column(df, column, mask_spec)
            
            self.logger.info(f"Applied {strategy} masking to column {column}")
        
        # Convert back to original format
        if self._is_dynamic_frame(data):
            from awsglue.dynamicframe import DynamicFrame
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext
            
            glue_context = GlueContext(SparkContext.getOrCreate())
            return DynamicFrame.fromDF(df, glue_context, "masked")
        
        return df
    
    def _redact_column(self, df: Any, column: str, spec: Dict[str, Any]) -> Any:
        """Completely redact a column."""
        from pyspark.sql import functions as F
        
        replacement = spec.get('replacement', '***REDACTED***')
        return df.withColumn(column, F.lit(replacement))
    
    def _hash_column(self, df: Any, column: str, spec: Dict[str, Any]) -> Any:
        """Hash a column using SHA-256."""
        from pyspark.sql import functions as F
        
        algorithm = spec.get('algorithm', 'sha256')
        
        def hash_value(value):
            if value is None:
                return None
            if algorithm == 'sha256':
                return hashlib.sha256(str(value).encode()).hexdigest()
            elif algorithm == 'md5':
                return hashlib.md5(str(value).encode()).hexdigest()
            return value
        
        hash_udf = F.udf(hash_value)
        return df.withColumn(column, hash_udf(F.col(column)))
    
    def _partial_mask_column(self, df: Any, column: str, spec: Dict[str, Any]) -> Any:
        """Partially mask a column (e.g., show last 4 digits of SSN)."""
        from pyspark.sql import functions as F
        
        show_prefix = spec.get('show_prefix', 0)
        show_suffix = spec.get('show_suffix', 0)
        mask_char = spec.get('mask_char', '*')
        
        def partial_mask(value):
            if value is None:
                return None
            value_str = str(value)
            length = len(value_str)
            
            if length <= show_prefix + show_suffix:
                return value_str
            
            prefix = value_str[:show_prefix] if show_prefix > 0 else ''
            suffix = value_str[-show_suffix:] if show_suffix > 0 else ''
            masked_length = length - show_prefix - show_suffix
            masked_part = mask_char * masked_length
            
            return prefix + masked_part + suffix
        
        mask_udf = F.udf(partial_mask)
        return df.withColumn(column, mask_udf(F.col(column)))
    
    def _tokenize_column(self, df: Any, column: str, spec: Dict[str, Any]) -> Any:
        """Replace values with tokens (requires token mapping)."""
        from pyspark.sql import functions as F
        
        # This would typically use an external tokenization service
        # Simplified implementation generates deterministic tokens
        def tokenize(value):
            if value is None:
                return None
            return f"TOKEN_{hashlib.md5(str(value).encode()).hexdigest()[:8]}"
        
        token_udf = F.udf(tokenize)
        return df.withColumn(column, token_udf(F.col(column)))
    
    def _to_dataframe(self, data: Any) -> Any:
        """Convert DynamicFrame to DataFrame if needed."""
        if self._is_dynamic_frame(data):
            return data.toDF()
        return data
    
    def _is_dynamic_frame(self, data: Any) -> bool:
        """Check if data is a DynamicFrame."""
        return hasattr(data, 'toDF') and callable(getattr(data, 'toDF'))
