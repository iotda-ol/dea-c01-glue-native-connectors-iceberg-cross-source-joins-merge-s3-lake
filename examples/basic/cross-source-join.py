"""
Example: Basic Cross-Source Join
Demonstrates joining data from Redshift and BigQuery
"""
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Import custom modules
sys.path.append('s3://my-glue-scripts/libs/')
from src.connectors.connection_factory import ConnectionFactory
from src.transformations.data_transformer import DataTransformer
from src.iceberg.table_manager import IcebergTableManager


def main():
    # Initialize Glue context
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    
    # Configuration
    redshift_config = {
        'connection_name': 'redshift-production',
        'database': 'glue_catalog',
        'temp_dir': 's3://aws-glue-temp/'
    }
    
    bigquery_config = {
        'project_id': 'my-gcp-project',
        'credentials_path': 's3://secrets/bigquery-key.json',
        'temp_bucket': 'temp-bucket'
    }
    
    # Create connectors
    redshift = ConnectionFactory.create_connector('redshift', redshift_config, glue_context)
    bigquery = ConnectionFactory.create_connector('bigquery', bigquery_config, glue_context)
    
    # Extract data
    print("Extracting data from Redshift...")
    sales_df = redshift.read_table(
        table_name='sales',
        schema='public',
        predicates=["sale_date >= '2024-01-01'"],
        columns=['sale_id', 'customer_id', 'product_id', 'amount', 'sale_date']
    )
    
    print("Extracting data from BigQuery...")
    products_df = bigquery.read_table(
        table_name='products',
        schema='analytics',
        columns=['product_id', 'product_name', 'category', 'price']
    )
    
    # Transform data
    print("Transforming data...")
    transformer = DataTransformer()
    
    # Standardize column names
    sales_df = transformer.standardize_column_names(sales_df, "snake_case")
    products_df = transformer.standardize_column_names(products_df, "snake_case")
    
    # Standardize dates
    sales_df = transformer.standardize_dates(sales_df, ['sale_date'])
    
    # Add audit columns
    sales_df = transformer.add_audit_columns(sales_df, 
                                            load_timestamp='etl_load_timestamp',
                                            source_system='redshift')
    products_df = transformer.add_audit_columns(products_df,
                                               load_timestamp='etl_load_timestamp',
                                               source_system='bigquery')
    
    # Perform join
    print("Joining data...")
    joined_df = sales_df.join(
        products_df,
        on='product_id',
        how='inner'
    )
    
    print(f"Joined data count: {joined_df.count()}")
    
    # Load to Iceberg
    print("Loading to Iceberg table...")
    iceberg_manager = IcebergTableManager(spark)
    
    iceberg_manager.write_data(
        df=joined_df,
        database='iceberg_datalake',
        table_name='sales_with_products',
        mode='append'
    )
    
    print("Job completed successfully!")


if __name__ == "__main__":
    main()
