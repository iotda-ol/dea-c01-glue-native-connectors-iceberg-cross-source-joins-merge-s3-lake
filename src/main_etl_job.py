"""
Main Glue ETL Job - Cross-Source Data Pipeline

This job extracts data from Redshift, Teradata, and BigQuery,
performs joins, and merges the result into an Iceberg table.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from src.connectors import RedshiftConnector, TeradataConnector, BigQueryConnector
from src.transforms import MultiSourceJoin, ValidationTransform, MaskingTransform
from src.iceberg import IcebergTableManager, IcebergMergeOperations
from src.utils import get_logger, ConfigManager, IncrementalProcessor

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'config_path',
    'database_connections_path'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize logger
logger = get_logger('GlueETLJob')
logger.info(f"Starting job: {args['JOB_NAME']}")

try:
    # Load configuration
    config = ConfigManager(args['config_path'])
    db_config = ConfigManager(args['database_connections_path'])
    
    # Initialize connectors
    logger.info("Initializing database connectors")
    
    redshift_conn = RedshiftConnector(db_config.get('connections.redshift'))
    teradata_conn = TeradataConnector(db_config.get('connections.teradata'))
    bigquery_conn = BigQueryConnector(db_config.get('connections.bigquery'))
    
    # Extract data from sources
    logger.info("Extracting data from sources")
    
    # Redshift data
    redshift_data = redshift_conn.read_table('customers')
    logger.info(f"Extracted {redshift_data.count()} rows from Redshift")
    
    # Teradata data  
    teradata_data = teradata_conn.read_table('transactions')
    logger.info(f"Extracted {teradata_data.count()} rows from Teradata")
    
    # BigQuery data
    bigquery_data = bigquery_conn.read_table('analytics.user_events')
    logger.info(f"Extracted {bigquery_data.count()} rows from BigQuery")
    
    # Perform multi-source join
    logger.info("Performing multi-source joins")
    
    join_config = config.get('joins', [])
    joiner = MultiSourceJoin(join_config)
    
    sources = {
        'redshift': redshift_data,
        'teradata': teradata_data,
        'bigquery': bigquery_data
    }
    
    joined_data = joiner.apply(sources)
    logger.info(f"Join completed: {joined_data.count()} rows")
    
    # Apply data validation
    if config.get('transformations.enable_validation', False):
        logger.info("Applying data validation")
        validation_rules = config.get('validation_rules', [])
        validator = ValidationTransform(validation_rules)
        joined_data = validator.apply(joined_data)
    
    # Apply PII masking
    if config.get('transformations.enable_masking', False):
        logger.info("Applying PII masking")
        pii_columns = config.get('transformations.pii_columns', [])
        masking_config = [
            {'column': col, 'strategy': 'hash'} for col in pii_columns
        ]
        masker = MaskingTransform(masking_config)
        joined_data = masker.apply(joined_data)
    
    # Initialize Iceberg table manager
    logger.info("Initializing Iceberg table")
    
    iceberg_config = {
        'warehouse_path': config.get('target.warehouse_path'),
        'catalog_name': config.get('target.catalog_name'),
        'table_name': config.get('target.table_name')
    }
    
    table_manager = IcebergTableManager(iceberg_config)
    table_manager.initialize_spark()
    
    # Perform MERGE operation
    logger.info("Performing MERGE into Iceberg table")
    
    merge_keys = config.get('target.merge_keys', [])
    merger = IcebergMergeOperations(
        catalog_name=iceberg_config['catalog_name'],
        table_name=iceberg_config['table_name'],
        merge_keys=merge_keys
    )
    
    merger.initialize_spark()
    merger.upsert(joined_data.toDF() if hasattr(joined_data, 'toDF') else joined_data)
    
    logger.info("Job completed successfully")
    
    # Commit job bookmark
    job.commit()
    
except Exception as e:
    logger.error(f"Job failed with error: {str(e)}", exc_info=True)
    raise

finally:
    # Cleanup
    logger.info("Cleaning up resources")
