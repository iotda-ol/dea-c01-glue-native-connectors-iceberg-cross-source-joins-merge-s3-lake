"""
Multi-Source Data Integration ETL Script for AWS Glue

This script demonstrates DEA-C01 best practices for:
- Reading from multiple data sources (Redshift, Teradata, BigQuery)
- Applying transformations and joins using Glue transforms
- Writing to S3 data lake using Apache Iceberg with MERGE operations
- Error handling and logging
- Performance optimization

Author: AWS Glue ETL Pipeline
Version: 1.0.0
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, coalesce
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType

# Initialize Glue context and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DATA_LAKE_BUCKET',
    'REDSHIFT_CONNECTION',
    'TERADATA_CONNECTION',
    'BIGQUERY_CONNECTION',
    'REDSHIFT_TABLE',
    'TERADATA_TABLE',
    'BIGQUERY_TABLE',
    'ICEBERG_OUTPUT_PATH',
    'ICEBERG_DATABASE',
    'ICEBERG_TABLE'
])

job.init(args['JOB_NAME'], args)

# Enable Iceberg format support
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args['ICEBERG_OUTPUT_PATH'])
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Logging helper
def log_info(message):
    """Log informational messages"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[INFO] {timestamp} - {message}")

def log_error(message):
    """Log error messages"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ERROR] {timestamp} - {message}")

# ============================
# Step 1: Read from Redshift
# ============================
log_info("Starting Redshift data extraction")

try:
    # Read from Redshift using native Glue connector
    redshift_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args.get('REDSHIFT_DATABASE', 'default'),
        table_name=args['REDSHIFT_TABLE'],
        transformation_ctx="redshift_source",
        additional_options={
            "connectionName": args['REDSHIFT_CONNECTION']
        }
    ) if args.get('REDSHIFT_CONNECTION') else None
    
    if redshift_dynamic_frame is None:
        # Fallback: Read from Redshift using JDBC
        redshift_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://your-cluster",
                "dbtable": args['REDSHIFT_TABLE'],
                "redshiftTmpDir": f"s3://{args['DATA_LAKE_BUCKET']}/temp/redshift/",
                "connectionName": args['REDSHIFT_CONNECTION']
            },
            transformation_ctx="redshift_source"
        )
    
    # Convert to DataFrame for transformations
    redshift_df = redshift_dynamic_frame.toDF()
    
    # Apply source-side transformations for Redshift data
    redshift_df = redshift_df \
        .withColumn("source_system", lit("REDSHIFT")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("data_quality_flag", 
                   when(col("amount").isNull() | (col("amount") < 0), "INVALID")
                   .otherwise("VALID"))
    
    # Select and rename columns for standardization
    redshift_df = redshift_df.select(
        col("transaction_id").cast(StringType()).alias("transaction_id"),
        col("customer_id").cast(StringType()).alias("customer_id"),
        col("product_id").cast(StringType()).alias("product_id"),
        col("amount").cast(DecimalType(18, 2)).alias("sales_amount"),
        col("transaction_date").cast(TimestampType()).alias("transaction_timestamp"),
        col("source_system"),
        col("ingestion_timestamp"),
        col("data_quality_flag")
    )
    
    log_info(f"Redshift data loaded: {redshift_df.count()} records")
    
except Exception as e:
    log_error(f"Error reading from Redshift: {str(e)}")
    raise

# ============================
# Step 2: Read from Teradata
# ============================
log_info("Starting Teradata data extraction")

try:
    # Read from Teradata using native Glue connector
    teradata_dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "url": "jdbc:teradata://your-host",
            "dbtable": args['TERADATA_TABLE'],
            "connectionName": args['TERADATA_CONNECTION']
        },
        transformation_ctx="teradata_source"
    )
    
    # Convert to DataFrame
    teradata_df = teradata_dynamic_frame.toDF()
    
    # Apply source-side transformations for Teradata data
    teradata_df = teradata_df \
        .withColumn("source_system", lit("TERADATA")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("data_quality_flag",
                   when(col("customer_name").isNull(), "INVALID")
                   .otherwise("VALID"))
    
    # Select and rename columns for standardization
    teradata_df = teradata_df.select(
        col("customer_id").cast(StringType()).alias("customer_id"),
        col("customer_name").cast(StringType()).alias("customer_name"),
        col("customer_email").cast(StringType()).alias("email"),
        col("customer_segment").cast(StringType()).alias("segment"),
        col("registration_date").cast(TimestampType()).alias("registration_timestamp"),
        col("source_system"),
        col("ingestion_timestamp"),
        col("data_quality_flag")
    )
    
    log_info(f"Teradata data loaded: {teradata_df.count()} records")
    
except Exception as e:
    log_error(f"Error reading from Teradata: {str(e)}")
    raise

# ============================
# Step 3: Read from BigQuery
# ============================
log_info("Starting BigQuery data extraction")

try:
    # Read from BigQuery using native connector
    bigquery_dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="marketplace.spark",
        connection_options={
            "connectionName": args['BIGQUERY_CONNECTION'],
            "table": args['BIGQUERY_TABLE']
        },
        transformation_ctx="bigquery_source"
    )
    
    # Convert to DataFrame
    bigquery_df = bigquery_dynamic_frame.toDF()
    
    # Apply source-side transformations for BigQuery data
    bigquery_df = bigquery_df \
        .withColumn("source_system", lit("BIGQUERY")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("data_quality_flag",
                   when(col("product_name").isNull(), "INVALID")
                   .otherwise("VALID"))
    
    # Select and rename columns for standardization
    bigquery_df = bigquery_df.select(
        col("product_id").cast(StringType()).alias("product_id"),
        col("product_name").cast(StringType()).alias("product_name"),
        col("category").cast(StringType()).alias("product_category"),
        col("unit_price").cast(DecimalType(18, 2)).alias("unit_price"),
        col("source_system"),
        col("ingestion_timestamp"),
        col("data_quality_flag")
    )
    
    log_info(f"BigQuery data loaded: {bigquery_df.count()} records")
    
except Exception as e:
    log_error(f"Error reading from BigQuery: {str(e)}")
    raise

# ============================
# Step 4: Perform Cross-Source Joins
# ============================
log_info("Performing cross-source joins")

try:
    # Join Redshift sales data with Teradata customer data
    sales_customer_df = redshift_df.join(
        teradata_df,
        redshift_df.customer_id == teradata_df.customer_id,
        "left"
    ).select(
        redshift_df["*"],
        teradata_df["customer_name"],
        teradata_df["email"],
        teradata_df["segment"],
        teradata_df["registration_timestamp"]
    )
    
    log_info(f"Sales-Customer join completed: {sales_customer_df.count()} records")
    
    # Join the result with BigQuery product data
    integrated_df = sales_customer_df.join(
        bigquery_df,
        sales_customer_df.product_id == bigquery_df.product_id,
        "left"
    ).select(
        sales_customer_df["transaction_id"],
        sales_customer_df["customer_id"],
        sales_customer_df["product_id"],
        sales_customer_df["sales_amount"],
        sales_customer_df["transaction_timestamp"],
        sales_customer_df["customer_name"],
        sales_customer_df["email"],
        sales_customer_df["segment"],
        bigquery_df["product_name"],
        bigquery_df["product_category"],
        bigquery_df["unit_price"],
        sales_customer_df["source_system"].alias("sales_source"),
        sales_customer_df["ingestion_timestamp"],
        sales_customer_df["data_quality_flag"]
    )
    
    log_info(f"Final integrated dataset: {integrated_df.count()} records")
    
except Exception as e:
    log_error(f"Error performing joins: {str(e)}")
    raise

# ============================
# Step 5: Apply Business Transformations
# ============================
log_info("Applying business transformations")

try:
    # Calculate derived metrics
    integrated_df = integrated_df \
        .withColumn("total_revenue", col("sales_amount")) \
        .withColumn("profit_margin", 
                   when(col("unit_price") > 0, 
                        (col("sales_amount") - col("unit_price")) / col("unit_price"))
                   .otherwise(0)) \
        .withColumn("customer_lifetime_value",
                   col("sales_amount"))  # Simplified - would normally aggregate
    
    # Add partition columns for efficient querying
    integrated_df = integrated_df \
        .withColumn("year", col("transaction_timestamp").substr(1, 4)) \
        .withColumn("month", col("transaction_timestamp").substr(6, 2)) \
        .withColumn("day", col("transaction_timestamp").substr(9, 2))
    
    log_info("Business transformations completed")
    
except Exception as e:
    log_error(f"Error applying transformations: {str(e)}")
    raise

# ============================
# Step 6: Write to Iceberg with MERGE
# ============================
log_info("Writing to Iceberg table with MERGE operation")

try:
    # Create Iceberg table if it doesn't exist
    iceberg_table_name = f"glue_catalog.{args['ICEBERG_DATABASE']}.{args['ICEBERG_TABLE']}"
    
    # Register DataFrame as temporary view for SQL operations
    integrated_df.createOrReplaceTempView("source_data")
    
    # Check if table exists
    table_exists = spark.catalog._jcatalog.tableExists(
        args['ICEBERG_DATABASE'], 
        args['ICEBERG_TABLE']
    )
    
    if not table_exists:
        log_info("Creating new Iceberg table")
        
        # Create Iceberg table with partitioning
        spark.sql(f"""
            CREATE TABLE {iceberg_table_name} (
                transaction_id STRING,
                customer_id STRING,
                product_id STRING,
                sales_amount DECIMAL(18,2),
                transaction_timestamp TIMESTAMP,
                customer_name STRING,
                email STRING,
                segment STRING,
                product_name STRING,
                product_category STRING,
                unit_price DECIMAL(18,2),
                sales_source STRING,
                ingestion_timestamp TIMESTAMP,
                data_quality_flag STRING,
                total_revenue DECIMAL(18,2),
                profit_margin DECIMAL(18,4),
                customer_lifetime_value DECIMAL(18,2),
                year STRING,
                month STRING,
                day STRING
            )
            USING iceberg
            PARTITIONED BY (year, month)
            LOCATION '{args['ICEBERG_OUTPUT_PATH']}{args['ICEBERG_TABLE']}/'
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.metadata.compression-codec' = 'gzip',
                'commit.retry.num-retries' = '3'
            )
        """)
        
        # Initial write
        integrated_df.writeTo(iceberg_table_name).append()
        log_info(f"Created Iceberg table and inserted {integrated_df.count()} records")
        
    else:
        log_info("Performing MERGE operation on existing Iceberg table")
        
        # Perform MERGE operation (upsert based on transaction_id)
        merge_sql = f"""
            MERGE INTO {iceberg_table_name} target
            USING source_data source
            ON target.transaction_id = source.transaction_id
            WHEN MATCHED THEN
                UPDATE SET
                    customer_id = source.customer_id,
                    product_id = source.product_id,
                    sales_amount = source.sales_amount,
                    transaction_timestamp = source.transaction_timestamp,
                    customer_name = source.customer_name,
                    email = source.email,
                    segment = source.segment,
                    product_name = source.product_name,
                    product_category = source.product_category,
                    unit_price = source.unit_price,
                    sales_source = source.sales_source,
                    ingestion_timestamp = source.ingestion_timestamp,
                    data_quality_flag = source.data_quality_flag,
                    total_revenue = source.total_revenue,
                    profit_margin = source.profit_margin,
                    customer_lifetime_value = source.customer_lifetime_value,
                    year = source.year,
                    month = source.month,
                    day = source.day
            WHEN NOT MATCHED THEN
                INSERT (
                    transaction_id, customer_id, product_id, sales_amount,
                    transaction_timestamp, customer_name, email, segment,
                    product_name, product_category, unit_price, sales_source,
                    ingestion_timestamp, data_quality_flag, total_revenue,
                    profit_margin, customer_lifetime_value, year, month, day
                )
                VALUES (
                    source.transaction_id, source.customer_id, source.product_id,
                    source.sales_amount, source.transaction_timestamp,
                    source.customer_name, source.email, source.segment,
                    source.product_name, source.product_category, source.unit_price,
                    source.sales_source, source.ingestion_timestamp,
                    source.data_quality_flag, source.total_revenue,
                    source.profit_margin, source.customer_lifetime_value,
                    source.year, source.month, source.day
                )
        """
        
        spark.sql(merge_sql)
        log_info("MERGE operation completed successfully")
    
    # Optimize Iceberg table (compaction)
    spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{iceberg_table_name}')")
    log_info("Table optimization completed")
    
except Exception as e:
    log_error(f"Error writing to Iceberg: {str(e)}")
    raise

# ============================
# Step 7: Data Quality Validation
# ============================
log_info("Performing data quality validation")

try:
    # Read back from Iceberg table for validation
    validation_df = spark.read.format("iceberg").load(
        f"{args['ICEBERG_OUTPUT_PATH']}{args['ICEBERG_TABLE']}/"
    )
    
    total_records = validation_df.count()
    invalid_records = validation_df.filter(col("data_quality_flag") == "INVALID").count()
    
    log_info(f"Total records in Iceberg table: {total_records}")
    log_info(f"Invalid records: {invalid_records}")
    log_info(f"Data quality percentage: {((total_records - invalid_records) / total_records * 100):.2f}%")
    
    # Log sample of the data
    log_info("Sample of integrated data:")
    validation_df.show(5, truncate=False)
    
except Exception as e:
    log_error(f"Error in data quality validation: {str(e)}")
    # Non-critical, don't raise

# ============================
# Cleanup and Job Completion
# ============================
log_info("ETL job completed successfully")
job.commit()
