"""
Example: Iceberg MERGE Operation
Demonstrates incremental MERGE from Teradata to Iceberg
"""
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from datetime import datetime, timedelta

# Import custom modules
sys.path.append('s3://my-glue-scripts/libs/')
from src.connectors.connection_factory import ConnectionFactory
from src.transformations.data_transformer import DataTransformer
from src.iceberg.table_manager import IcebergTableManager
from lib.validation.data_quality import DataQualityValidator


def main():
    # Initialize
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    
    # Configuration
    teradata_config = {
        'connection_name': 'teradata-production',
        'database': 'prod_db'
    }
    
    # Create connector
    teradata = ConnectionFactory.create_connector('teradata', teradata_config, glue_context)
    
    # Extract incremental data (last 24 hours)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"Extracting data from Teradata since {yesterday}...")
    
    source_df = teradata.read_table(
        table_name='customers',
        schema='prod_db',
        predicates=[f"updated_at >= DATE '{yesterday}'"]
    )
    
    print(f"Extracted {source_df.count()} records")
    
    # Transform
    print("Transforming data...")
    transformer = DataTransformer()
    
    source_df = transformer.standardize_column_names(source_df, "snake_case")
    source_df = transformer.trim_strings(source_df)
    source_df = transformer.standardize_timestamps(source_df, ['updated_at', 'created_at'])
    source_df = transformer.add_audit_columns(source_df, 
                                             load_timestamp='etl_timestamp',
                                             source_system='teradata')
    
    # Validate data quality
    print("Validating data quality...")
    validator = DataQualityValidator()
    
    validator.validate_not_null(source_df, ['customer_id', 'email'])
    validator.validate_unique(source_df, ['customer_id'])
    validator.validate_format(source_df, 'email', r'^[\w\.-]+@[\w\.-]+\.\w+$')
    
    report = validator.get_validation_report()
    print(f"Validation report: {report}")
    
    if report['failed'] > 0:
        raise ValueError("Data quality validation failed")
    
    # MERGE into Iceberg
    print("Performing MERGE operation...")
    iceberg_manager = IcebergTableManager(spark)
    
    result = iceberg_manager.merge_data(
        source_df=source_df,
        target_database='iceberg_datalake',
        target_table='dim_customers',
        merge_keys=['customer_id'],
        update_condition="source.updated_at > target.updated_at"
    )
    
    print(f"MERGE completed: {result}")
    
    # Compact files if needed
    print("Running table maintenance...")
    iceberg_manager.compact_files(
        database='iceberg_datalake',
        table_name='dim_customers',
        target_file_size_mb=128
    )
    
    print("Job completed successfully!")


if __name__ == "__main__":
    main()
