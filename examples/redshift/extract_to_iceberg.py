"""
Example Glue job: Extract from Redshift and load to Iceberg
Demonstrates usage of reusable modules
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Import our reusable modules
from connectors import RedshiftConnector
from transforms import CommonTransforms, IcebergTransforms
from utils import GlueLogger, MetricsCollector, ConfigManager


def main():
    # Parse job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'config_path',
        'table_name'
    ])
    
    # Initialize Glue contexts
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Initialize utilities
    logger = GlueLogger(args['JOB_NAME'])
    config = ConfigManager(s3_config_path=args['config_path'])
    
    logger.log_job_start(table=args['table_name'])
    
    try:
        with MetricsCollector(job_name=args['JOB_NAME']) as metrics:
            # Get table configuration
            table_config = config.get_table_config(args['table_name'])
            
            # Initialize Redshift connector
            redshift_conn = RedshiftConnector(
                glueContext,
                config.get('connections.redshift.connection_name')
            )
            
            # Extract data
            logger.info(f"Extracting data from Redshift table: {args['table_name']}")
            df = redshift_conn.extract_table(
                database=config.get('connections.redshift.database'),
                table_name=table_config['source_table']
            ).toDF()
            
            input_count = df.count()
            logger.info(f"Extracted {input_count} records")
            metrics.record_records_processed(input_count)
            
            # Apply common transformations
            logger.info("Applying transformations")
            df = CommonTransforms.add_audit_columns(df)
            df = CommonTransforms.add_source_system_column(df, 'redshift')
            df = CommonTransforms.standardize_column_names(df)
            
            # Remove duplicates if configured
            if 'primary_key' in table_config:
                df = CommonTransforms.remove_duplicates(
                    df,
                    table_config['primary_key']
                )
            
            output_count = df.count()
            logger.log_transformation("common_transforms", input_count, output_count)
            
            # Write to Iceberg
            logger.info(f"Writing to Iceberg table: {table_config['target_table']}")
            iceberg = IcebergTransforms(spark, config.get('iceberg.catalog'))
            
            # Create temp view for merge
            df.createOrReplaceTempView("source_data")
            
            # Perform merge/upsert
            iceberg.merge_upsert(
                target_table=table_config['target_table'],
                source_df=df,
                merge_keys=table_config['primary_key']
            )
            
            logger.info("Data successfully loaded to Iceberg")
            
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    
    finally:
        logger.log_job_end(0, input_count)  # Duration will be calculated by metrics
        job.commit()


if __name__ == "__main__":
    main()
