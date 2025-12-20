"""
Join Transform Module

Implements various join operations for combining data from multiple sources.
"""

from typing import Any, Dict, List, Optional
from src.transforms.base_transform import BaseTransform


class JoinTransform(BaseTransform):
    """
    Transform for joining data from multiple sources.
    
    Supports various join types: inner, left, right, full outer, cross.
    """
    
    VALID_JOIN_TYPES = ['inner', 'left', 'right', 'full', 'cross', 'left_semi', 'left_anti']
    
    def __init__(self, 
                 join_type: str = 'inner',
                 join_keys: Optional[List[str]] = None,
                 left_alias: str = 'left',
                 right_alias: str = 'right',
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize join transform.
        
        Args:
            join_type: Type of join ('inner', 'left', 'right', 'full', 'cross')
            join_keys: List of column names to join on
            left_alias: Alias for left dataset
            right_alias: Alias for right dataset
            config: Additional configuration
        """
        super().__init__(config)
        
        if join_type not in self.VALID_JOIN_TYPES:
            raise ValueError(f"Invalid join type: {join_type}. Must be one of {self.VALID_JOIN_TYPES}")
        
        self.join_type = join_type
        self.join_keys = join_keys or []
        self.left_alias = left_alias
        self.right_alias = right_alias
        
    def apply(self, left_data: Any, right_data: Any) -> Any:
        """
        Apply join transformation to two datasets.
        
        Args:
            left_data: Left dataset (DynamicFrame or DataFrame)
            right_data: Right dataset (DynamicFrame or DataFrame)
            
        Returns:
            Joined dataset
        """
        try:
            # Convert DynamicFrames to DataFrames if needed
            left_df = self._to_dataframe(left_data)
            right_df = self._to_dataframe(right_data)
            
            # Perform join
            if self.join_type == 'cross':
                # Cross join doesn't need join keys
                joined_df = left_df.crossJoin(right_df)
            else:
                # Build join condition
                join_condition = self._build_join_condition(left_df, right_df)
                joined_df = left_df.join(right_df, join_condition, self.join_type)
            
            self.logger.info(f"Performed {self.join_type} join on keys {self.join_keys}")
            
            # Convert back to DynamicFrame if input was DynamicFrame
            if self._is_dynamic_frame(left_data):
                from awsglue.dynamicframe import DynamicFrame
                from awsglue.context import GlueContext
                from pyspark.context import SparkContext
                
                glue_context = GlueContext(SparkContext.getOrCreate())
                return DynamicFrame.fromDF(joined_df, glue_context, "joined")
            
            return joined_df
            
        except Exception as e:
            self.logger.error(f"Join operation failed: {str(e)}")
            raise
    
    def _to_dataframe(self, data: Any) -> Any:
        """Convert DynamicFrame to DataFrame if needed."""
        if self._is_dynamic_frame(data):
            return data.toDF()
        return data
    
    def _is_dynamic_frame(self, data: Any) -> bool:
        """Check if data is a DynamicFrame."""
        return hasattr(data, 'toDF') and callable(getattr(data, 'toDF'))
    
    def _build_join_condition(self, left_df: Any, right_df: Any) -> Any:
        """
        Build join condition from join keys.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            
        Returns:
            Join condition expression
        """
        if not self.join_keys:
            raise ValueError("Join keys must be specified for non-cross joins")
        
        # Build condition: left.key1 == right.key1 AND left.key2 == right.key2 ...
        condition = None
        for key in self.join_keys:
            key_condition = left_df[key] == right_df[key]
            condition = key_condition if condition is None else condition & key_condition
        
        return condition
    
    def validate(self, *datasets) -> bool:
        """
        Validate input datasets for join.
        
        Args:
            *datasets: Variable number of datasets to validate
            
        Returns:
            True if datasets are valid for joining
        """
        if len(datasets) < 2:
            self.logger.error("At least two datasets required for join")
            return False
        
        for dataset in datasets:
            if dataset is None:
                self.logger.error("Dataset is None")
                return False
        
        # For non-cross joins, validate join keys exist
        if self.join_type != 'cross' and not self.join_keys:
            self.logger.error("Join keys required for non-cross joins")
            return False
        
        return True


class MultiSourceJoin(BaseTransform):
    """
    Performs sequential joins across multiple data sources.
    
    Useful for joining data from Redshift, Teradata, and BigQuery.
    """
    
    def __init__(self, join_config: List[Dict[str, Any]]):
        """
        Initialize multi-source join.
        
        Args:
            join_config: List of join configurations, each containing:
                - left_source: Name or index of left source
                - right_source: Name or index of right source
                - join_type: Type of join
                - join_keys: Keys to join on
        """
        super().__init__()
        self.join_config = join_config
        
    def apply(self, sources: Dict[str, Any]) -> Any:
        """
        Apply sequential joins across multiple sources.
        
        Args:
            sources: Dictionary of source name to data mapping
            
        Returns:
            Final joined dataset
        """
        result = None
        
        for i, join_spec in enumerate(self.join_config):
            left_source = join_spec.get('left_source')
            right_source = join_spec.get('right_source')
            join_type = join_spec.get('join_type', 'inner')
            join_keys = join_spec.get('join_keys', [])
            
            # Get left data (either from sources or previous result)
            if left_source == 'result' and result is not None:
                left_data = result
            else:
                left_data = sources.get(left_source)
            
            # Get right data
            right_data = sources.get(right_source)
            
            if left_data is None or right_data is None:
                raise ValueError(f"Invalid source reference in join {i+1}")
            
            # Perform join
            joiner = JoinTransform(join_type=join_type, join_keys=join_keys)
            result = joiner.apply(left_data, right_data)
            
            self.logger.info(f"Completed join {i+1}/{len(self.join_config)}: {left_source} {join_type} {right_source}")
        
        return result
