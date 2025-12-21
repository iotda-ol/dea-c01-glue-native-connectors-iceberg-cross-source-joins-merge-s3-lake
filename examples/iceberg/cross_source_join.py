"""
Example: Cross-source join combining Redshift, Teradata, and BigQuery data
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from connectors import RedshiftConnector, TeradataConnector, BigQueryConnector
from transforms import CrossSourceJoins, IcebergTransforms, CommonTransforms
from utils import GlueLogger, ConfigManager


def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_path'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    logger = GlueLogger(args['JOB_NAME'])
    config = ConfigManager(s3_config_path=args['config_path'])
    
    logger.log_job_start()
    
    try:
        # Extract from Redshift (customers and orders)
        logger.info("Extracting from Redshift")
        redshift = RedshiftConnector(
            glueContext,
            config.get('connections.redshift.connection_name')
        )
        
        customers_df = redshift.extract_table('analytics', 'customers').toDF()
        orders_df = redshift.extract_table('analytics', 'orders').toDF()
        
        # Extract from Teradata (products)
        logger.info("Extracting from Teradata")
        teradata = TeradataConnector(
            glueContext,
            config.get('connections.teradata.connection_name')
        )
        
        products_df = teradata.extract_table('products').toDF()
        
        # Extract from BigQuery (transactions)
        logger.info("Extracting from BigQuery")
        bigquery = BigQueryConnector(
            spark,
            config.get('connections.bigquery.project_id'),
            config.get('connections.bigquery.secret_name')
        )
        
        transactions_df = bigquery.extract_table('analytics', 'transactions')
        
        # Perform cross-source joins
        logger.info("Performing cross-source joins")
        enriched_df = CrossSourceJoins.three_way_join(
            customers_df,
            orders_df,
            transactions_df
        )
        
        # Add product information
        enriched_df = CrossSourceJoins.join_with_dimension(
            enriched_df,
            products_df,
            join_key='product_id',
            dimension_columns=['product_name', 'category', 'price']
        )
        
        # Apply transformations
        enriched_df = CommonTransforms.add_audit_columns(enriched_df)
        enriched_df = CommonTransforms.standardize_column_names(enriched_df)
        
        record_count = enriched_df.count()
        logger.info(f"Created {record_count} enriched records")
        
        # Write to Iceberg
        logger.info("Writing to Iceberg")
        iceberg = IcebergTransforms(spark, config.get('iceberg.catalog'))
        
        enriched_df.createOrReplaceTempView("enriched_data")
        
        iceberg.merge_upsert(
            target_table='iceberg_curated.customer_transactions_enriched',
            source_df=enriched_df,
            merge_keys=['transaction_id']
        )
        
        logger.info("Cross-source join completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    
    finally:
        job.commit()


if __name__ == "__main__":
    main()
