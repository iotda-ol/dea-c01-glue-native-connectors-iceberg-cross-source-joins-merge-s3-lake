"""
Cross-source join operations
Implements various join strategies for combining data from multiple sources
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col
from typing import List, Dict, Optional
from enum import Enum


class JoinStrategy(Enum):
    """Supported join strategies"""
    BROADCAST = "broadcast"
    SORT_MERGE = "sort_merge"
    SHUFFLE_HASH = "shuffle_hash"


class CrossSourceJoins:
    """Handles joining data from multiple sources"""
    
    @staticmethod
    def join_redshift_teradata(redshift_df: DataFrame,
                               teradata_df: DataFrame,
                               join_keys: List[str],
                               join_type: str = "inner",
                               use_broadcast: bool = True) -> DataFrame:
        """
        Join Redshift and Teradata data
        
        Args:
            redshift_df: DataFrame from Redshift
            teradata_df: DataFrame from Teradata
            join_keys: List of columns to join on
            join_type: Type of join (inner, left, right, outer)
            use_broadcast: Whether to use broadcast join for smaller table
        
        Returns:
            Joined DataFrame
        """
        if use_broadcast:
            # Assume teradata table is smaller (products, suppliers)
            return redshift_df.join(
                broadcast(teradata_df),
                join_keys,
                join_type
            )
        else:
            return redshift_df.join(teradata_df, join_keys, join_type)
    
    @staticmethod
    def three_way_join(customers_df: DataFrame,
                      orders_df: DataFrame,
                      transactions_df: DataFrame) -> DataFrame:
        """
        Perform three-way join across all sources
        
        Args:
            customers_df: Customer data (Redshift)
            orders_df: Order data (Redshift)
            transactions_df: Transaction data (BigQuery)
        
        Returns:
            Fully enriched DataFrame
        """
        # First join: orders with customers
        orders_customers = orders_df.join(
            customers_df,
            "customer_id",
            "left"
        )
        
        # Second join: add transactions
        enriched = transactions_df.join(
            orders_customers,
            "order_id",
            "left"
        )
        
        return enriched
    
    @staticmethod
    def join_with_dimension(fact_df: DataFrame,
                           dimension_df: DataFrame,
                           join_key: str,
                           dimension_columns: Optional[List[str]] = None) -> DataFrame:
        """
        Join fact table with dimension table (star schema pattern)
        
        Args:
            fact_df: Fact table DataFrame
            dimension_df: Dimension table DataFrame
            join_key: Column to join on
            dimension_columns: Specific columns to select from dimension
        
        Returns:
            Joined DataFrame
        """
        if dimension_columns:
            dimension_df = dimension_df.select([join_key] + dimension_columns)
        
        return fact_df.join(
            broadcast(dimension_df),
            join_key,
            "left"
        )
    
    @staticmethod
    def incremental_join(base_df: DataFrame,
                        updates_df: DataFrame,
                        key_columns: List[str]) -> DataFrame:
        """
        Join base data with incremental updates
        
        Args:
            base_df: Existing base DataFrame
            updates_df: New/updated records DataFrame
            key_columns: Columns to identify matching records
        
        Returns:
            Merged DataFrame
        """
        from pyspark.sql.functions import coalesce
        
        # Anti-join to get records not in updates
        unchanged = base_df.join(
            updates_df,
            key_columns,
            "left_anti"
        )
        
        # Union with updates
        return unchanged.union(updates_df)
