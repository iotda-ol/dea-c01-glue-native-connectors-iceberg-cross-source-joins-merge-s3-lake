# Part 4: Glue Connectors Implementation (Steps 51-65)

## Implementing ETL with Native Connectors

### Step 51: Understanding Glue Job Structure
**Objective**: Learn the anatomy of a Glue ETL job

**Basic Structure**:
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Your ETL logic here

# Commit job
job.commit()
```

### Step 52: Create Redshift Extract Job
**Objective**: Extract data from Redshift to S3

```python
# extract_redshift.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import json

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'redshift_connection',
    'database_name',
    'table_name',
    'output_path'
])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Redshift using Glue connection
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=args['database_name'],
    table_name=args['table_name'],
    transformation_ctx="datasource"
)

# Apply basic transformations
mapped_df = ApplyMapping.apply(
    frame=datasource,
    mappings=[
        ("customer_id", "int", "customer_id", "long"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("email", "string", "email", "string"),
        ("country", "string", "country", "string"),
        ("created_date", "date", "created_date", "date"),
        ("last_updated", "timestamp", "last_updated", "timestamp")
    ],
    transformation_ctx="mapped_df"
)

# Write to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=mapped_df,
    connection_type="s3",
    connection_options={
        "path": args['output_path'],
        "partitionKeys": ["country"]
    },
    format="parquet",
    transformation_ctx="datasink"
)

job.commit()
```

### Step 53: Create Teradata Extract Job
**Objective**: Extract data from Teradata to S3

```python
# extract_teradata.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'teradata_connection',
    'table_name',
    'output_path'
])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Connection options for Teradata
connection_options = {
    "useConnectionProperties": "true",
    "connectionName": args['teradata_connection'],
    "dbtable": args['table_name']
}

# Read from Teradata
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="teradata",
    connection_options=connection_options,
    transformation_ctx="datasource"
)

# Convert to DataFrame for transformations
df = datasource.toDF()

# Data transformations
from pyspark.sql.functions import col, current_timestamp

df_transformed = df.withColumn("extraction_timestamp", current_timestamp()) \
                   .withColumn("source_system", lit("teradata"))

# Convert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df_transformed, glueContext, "dynamic_frame")

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": args['output_path'],
        "compression": "snappy"
    },
    format="parquet",
    transformation_ctx="datasink"
)

job.commit()
```

### Step 54: Create BigQuery Extract Job
**Objective**: Extract data from BigQuery to S3

```python
# extract_bigquery.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'gcp_project',
    'dataset',
    'table_name',
    'output_path'
])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from BigQuery using Spark connector
df = spark.read \
    .format("bigquery") \
    .option("project", args['gcp_project']) \
    .option("dataset", args['dataset']) \
    .option("table", args['table_name']) \
    .option("materializationDataset", "temp_dataset") \
    .load()

# Add metadata columns
df_with_metadata = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("bigquery"))

# Write to S3
df_with_metadata.write \
    .mode("overwrite") \
    .parquet(args['output_path'])

job.commit()
```

### Step 55: Implement Incremental Data Loading
**Objective**: Load only new/changed records

```python
# incremental_extract.py
from pyspark.sql.functions import col, max as spark_max
from datetime import datetime, timedelta

def get_last_extraction_timestamp(spark, metadata_path, source_table):
    """Get the last successful extraction timestamp"""
    try:
        metadata_df = spark.read.parquet(metadata_path)
        last_ts = metadata_df.filter(col("table_name") == source_table) \
                            .select(spark_max("extraction_timestamp")) \
                            .collect()[0][0]
        return last_ts
    except:
        # Return a default timestamp if no metadata exists
        return datetime(2020, 1, 1)

def extract_incremental(glueContext, connection_name, table_name, 
                        timestamp_column, last_timestamp, output_path):
    """
    Extract only records modified after last_timestamp
    
    Args:
        glueContext: GlueContext instance
        connection_name: Glue connection name
        table_name: Source table
        timestamp_column: Column to filter on
        last_timestamp: Last extraction timestamp
        output_path: S3 output path
    """
    # Build SQL query for incremental extraction
    query = f"""
        (SELECT * FROM {table_name} 
         WHERE {timestamp_column} > '{last_timestamp}')
    """
    
    connection_options = {
        "useConnectionProperties": "true",
        "connectionName": connection_name,
        "dbtable": query
    }
    
    # Read incremental data
    incremental_data = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options=connection_options
    )
    
    # Write to S3
    glueContext.write_dynamic_frame.from_options(
        frame=incremental_data,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet",
        format_options={"compression": "snappy"}
    )
    
    return incremental_data.count()
```

### Step 56: Implement Data Type Mapping
**Objective**: Standardize data types across sources

```python
# type_mapping.py
from awsglue.transforms import ApplyMapping

def standardize_types(dynamic_frame, source_system):
    """
    Map source-specific types to standard types
    
    Args:
        dynamic_frame: DynamicFrame to transform
        source_system: 'redshift', 'teradata', or 'bigquery'
    
    Returns:
        DynamicFrame with standardized types
    """
    
    # Common type mappings
    type_mappings = {
        'redshift': {
            'int': 'long',
            'bigint': 'long',
            'decimal': 'decimal(18,2)',
            'varchar': 'string',
            'date': 'date',
            'timestamp': 'timestamp'
        },
        'teradata': {
            'INTEGER': 'long',
            'DECIMAL': 'decimal(18,2)',
            'VARCHAR': 'string',
            'DATE': 'date',
            'TIMESTAMP': 'timestamp'
        },
        'bigquery': {
            'INT64': 'long',
            'NUMERIC': 'decimal(18,2)',
            'STRING': 'string',
            'DATE': 'date',
            'TIMESTAMP': 'timestamp'
        }
    }
    
    # Apply mapping based on source system
    # Implementation depends on your specific schema
    return dynamic_frame
```

### Step 57: Implement Error Handling and Retries
**Objective**: Make jobs resilient to failures

```python
# error_handler.py
import logging
from functools import wraps
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def retry_on_failure(max_retries=3, delay=5):
    """
    Decorator to retry function on failure
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.error(f"All {max_retries} attempts failed")
                        raise
        return wrapper
    return decorator

@retry_on_failure(max_retries=3, delay=10)
def extract_with_retry(glueContext, connection_name, table_name):
    """Extract data with automatic retry"""
    return glueContext.create_dynamic_frame.from_catalog(
        database="iceberg_raw",
        table_name=table_name,
        transformation_ctx=f"source_{table_name}"
    )
```

### Step 58: Implement Data Quality Checks
**Objective**: Validate data during extraction

```python
# data_quality.py
from pyspark.sql.functions import col, count, when, isnan
from awsglue.dynamicframe import DynamicFrame

class DataQualityChecker:
    def __init__(self, glueContext, cloudwatch_namespace="DataPipeline/Quality"):
        self.glueContext = glueContext
        self.cloudwatch_namespace = cloudwatch_namespace
        
    def check_nulls(self, df, column_name, threshold=0.1):
        """Check if null percentage is below threshold"""
        total_count = df.count()
        null_count = df.filter(col(column_name).isNull()).count()
        null_percentage = null_count / total_count if total_count > 0 else 0
        
        if null_percentage > threshold:
            raise ValueError(
                f"Column {column_name} has {null_percentage*100:.2f}% nulls, "
                f"exceeding threshold of {threshold*100:.2f}%"
            )
        return True
    
    def check_uniqueness(self, df, column_name):
        """Check if column values are unique"""
        total_count = df.count()
        distinct_count = df.select(column_name).distinct().count()
        
        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            raise ValueError(
                f"Column {column_name} has {duplicate_count} duplicate values"
            )
        return True
    
    def check_row_count(self, df, min_rows=1):
        """Check if DataFrame has minimum number of rows"""
        row_count = df.count()
        if row_count < min_rows:
            raise ValueError(
                f"DataFrame has only {row_count} rows, "
                f"minimum required is {min_rows}"
            )
        return True
    
    def run_all_checks(self, df, table_name, checks_config):
        """
        Run all configured quality checks
        
        Args:
            df: Spark DataFrame
            table_name: Table name for reporting
            checks_config: Dict with check configurations
        """
        results = {"table": table_name, "passed": True, "failures": []}
        
        try:
            for check in checks_config.get("checks", []):
                check_type = check["type"]
                if check_type == "null_check":
                    self.check_nulls(df, check["column"], check.get("threshold", 0.1))
                elif check_type == "uniqueness":
                    self.check_uniqueness(df, check["column"])
                elif check_type == "row_count":
                    self.check_row_count(df, check.get("min_rows", 1))
        except Exception as e:
            results["passed"] = False
            results["failures"].append(str(e))
            
        return results
```

### Step 59: Create Job Parameters Configuration
**Objective**: Externalize job configurations

```json
// job_configs/extract_customers.json
{
  "jobName": "extract-redshift-customers",
  "description": "Extract customers from Redshift to S3",
  "role": "GlueIcebergRole",
  "command": {
    "name": "glueetl",
    "scriptLocation": "s3://my-bucket/scripts/extract_redshift.py",
    "pythonVersion": "3"
  },
  "defaultArguments": {
    "--job-language": "python",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-continuous-log-filter": "true",
    "--redshift_connection": "redshift-connection",
    "--database_name": "iceberg_raw",
    "--table_name": "redshift_customers",
    "--output_path": "s3://my-bucket/raw/redshift/customers/",
    "--TempDir": "s3://my-bucket/temp/",
    "--enable-glue-datacatalog": "true"
  },
  "maxRetries": 0,
  "timeout": 60,
  "maxCapacity": 10,
  "glueVersion": "4.0",
  "numberOfWorkers": 10,
  "workerType": "G.1X"
}
```

### Step 60: Deploy Jobs Using AWS CLI
**Objective**: Create Glue jobs programmatically

```bash
#!/bin/bash
# deploy_glue_jobs.sh

# Upload scripts to S3
aws s3 cp extract_redshift.py s3://$BUCKET_NAME/scripts/
aws s3 cp extract_teradata.py s3://$BUCKET_NAME/scripts/
aws s3 cp extract_bigquery.py s3://$BUCKET_NAME/scripts/

# Create Redshift extraction job
aws glue create-job \
  --name extract-redshift-customers \
  --role GlueIcebergRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'$BUCKET_NAME'/scripts/extract_redshift.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--redshift_connection": "redshift-connection",
    "--database_name": "iceberg_raw",
    "--table_name": "redshift_customers",
    "--output_path": "s3://'$BUCKET_NAME'/raw/redshift/customers/",
    "--TempDir": "s3://'$BUCKET_NAME'/temp/"
  }' \
  --max-retries 1 \
  --timeout 60 \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X"

# Create Teradata extraction job
aws glue create-job \
  --name extract-teradata-products \
  --role GlueIcebergRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'$BUCKET_NAME'/scripts/extract_teradata.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--teradata_connection": "teradata-connection",
    "--table_name": "products",
    "--output_path": "s3://'$BUCKET_NAME'/raw/teradata/products/"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X"

echo "Jobs deployed successfully!"
```

### Step 61: Run and Monitor Jobs
**Objective**: Execute jobs and track progress

```bash
# Start a job run
RUN_ID=$(aws glue start-job-run \
  --job-name extract-redshift-customers \
  --query 'JobRunId' \
  --output text)

echo "Started job run: $RUN_ID"

# Monitor job status
while true; do
  STATUS=$(aws glue get-job-run \
    --job-name extract-redshift-customers \
    --run-id $RUN_ID \
    --query 'JobRun.JobRunState' \
    --output text)
  
  echo "Job status: $STATUS"
  
  if [[ "$STATUS" == "SUCCEEDED" || "$STATUS" == "FAILED" || "$STATUS" == "STOPPED" ]]; then
    break
  fi
  
  sleep 10
done

# Get job execution details
aws glue get-job-run \
  --job-name extract-redshift-customers \
  --run-id $RUN_ID
```

### Step 62: Implement Job Orchestration
**Objective**: Chain multiple jobs together

```python
# orchestrator.py
import boto3
import time
from typing import List, Dict

class GlueOrchestrator:
    def __init__(self):
        self.glue = boto3.client('glue')
        
    def start_job(self, job_name: str, arguments: Dict = None) -> str:
        """Start a Glue job and return run ID"""
        args = arguments or {}
        response = self.glue.start_job_run(
            JobName=job_name,
            Arguments=args
        )
        return response['JobRunId']
    
    def wait_for_job(self, job_name: str, run_id: str, poll_interval: int = 30):
        """Wait for job to complete"""
        while True:
            response = self.glue.get_job_run(
                JobName=job_name,
                RunId=run_id
            )
            status = response['JobRun']['JobRunState']
            
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                return status
            
            time.sleep(poll_interval)
    
    def run_pipeline(self, job_sequence: List[str]):
        """
        Run jobs in sequence
        
        Args:
            job_sequence: List of job names to run in order
        """
        for job_name in job_sequence:
            print(f"Starting job: {job_name}")
            run_id = self.start_job(job_name)
            
            status = self.wait_for_job(job_name, run_id)
            print(f"Job {job_name} completed with status: {status}")
            
            if status != 'SUCCEEDED':
                raise Exception(f"Job {job_name} failed with status: {status}")

# Usage
if __name__ == "__main__":
    orchestrator = GlueOrchestrator()
    
    pipeline = [
        'extract-redshift-customers',
        'extract-teradata-products',
        'extract-bigquery-transactions',
        'transform-and-join',
        'load-to-iceberg'
    ]
    
    orchestrator.run_pipeline(pipeline)
```

### Step 63: Set Up Job Scheduling with AWS EventBridge
**Objective**: Automate job execution

```bash
# Create EventBridge rule for daily execution
aws events put-rule \
  --name daily-data-extraction \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED \
  --description "Run data extraction daily at 2 AM UTC"

# Add Glue job as target
aws events put-targets \
  --rule daily-data-extraction \
  --targets '[{
    "Id": "1",
    "Arn": "arn:aws:glue:'$AWS_REGION':'$ACCOUNT_ID':job/extract-redshift-customers",
    "RoleArn": "arn:aws:iam::'$ACCOUNT_ID':role/GlueIcebergRole"
  }]'

# Create rule for hourly incremental loads
aws events put-rule \
  --name hourly-incremental-load \
  --schedule-expression "rate(1 hour)" \
  --state ENABLED

echo "Schedules created successfully!"
```

### Step 64: Implement Job Logging Best Practices
**Objective**: Comprehensive logging for debugging

```python
# logging_config.py
import logging
import sys
from awsglue.context import GlueContext

def setup_logging(job_name: str, log_level=logging.INFO):
    """
    Configure logging for Glue job
    
    Args:
        job_name: Name of the Glue job
        log_level: Logging level (default: INFO)
    
    Returns:
        Configured logger
    """
    logger = logging.getLogger(job_name)
    logger.setLevel(log_level)
    
    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    # Format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger

# Usage in Glue job
logger = setup_logging("extract-redshift-customers")

logger.info("Starting data extraction")
logger.debug(f"Connection: {connection_name}")
logger.warning("Large dataset detected, may take longer")
logger.error("Failed to connect to source")
```

### Step 65: Implement Performance Monitoring
**Objective**: Track job performance metrics

```python
# performance_monitor.py
import boto3
from datetime import datetime

class PerformanceMonitor:
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.cloudwatch = boto3.client('cloudwatch')
        self.start_time = None
        self.metrics = []
    
    def start(self):
        """Start performance monitoring"""
        self.start_time = datetime.utcnow()
    
    def record_metric(self, metric_name: str, value: float, unit: str = 'None'):
        """Record a custom metric"""
        self.metrics.append({
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.utcnow(),
            'Dimensions': [{'Name': 'JobName', 'Value': self.job_name}]
        })
    
    def publish_metrics(self):
        """Publish all collected metrics to CloudWatch"""
        if self.metrics:
            self.cloudwatch.put_metric_data(
                Namespace='GlueJobs/Performance',
                MetricData=self.metrics
            )
    
    def finish(self, record_count: int):
        """Finish monitoring and publish final metrics"""
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        
        self.record_metric('ExecutionTime', duration, 'Seconds')
        self.record_metric('RecordsProcessed', record_count, 'Count')
        self.record_metric('RecordsPerSecond', record_count / duration, 'Count/Second')
        
        self.publish_metrics()

# Usage
monitor = PerformanceMonitor("extract-redshift-customers")
monitor.start()

# ... ETL logic ...
record_count = df.count()

monitor.finish(record_count)
```
