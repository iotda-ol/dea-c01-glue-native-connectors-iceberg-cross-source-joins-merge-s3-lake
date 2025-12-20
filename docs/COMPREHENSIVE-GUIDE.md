# Complete Guide: AWS Glue Iceberg Cross-Source Data Pipeline
## From Novice to Expert - 100 Steps

This comprehensive guide takes you from basic understanding to expert-level implementation of a multi-source data pipeline using AWS Glue, Apache Iceberg, and S3 data lakes.

---

## Table of Contents
- [Part 1: Getting Started (Steps 1-10)](#part-1-getting-started-steps-1-10)
- [Part 2: Environment Setup (Steps 11-20)](#part-2-environment-setup-steps-11-20)
- [Part 3: AWS Configuration (Steps 21-30)](#part-3-aws-configuration-steps-21-30)
- [Part 4: Data Source Connectors (Steps 31-40)](#part-4-data-source-connectors-steps-31-40)
- [Part 5: Apache Iceberg Basics (Steps 41-50)](#part-5-apache-iceberg-basics-steps-41-50)
- [Part 6: ETL Pipeline Development (Steps 51-60)](#part-6-etl-pipeline-development-steps-51-60)
- [Part 7: Cross-Source Joins (Steps 61-70)](#part-7-cross-source-joins-steps-61-70)
- [Part 8: MERGE Operations (Steps 71-80)](#part-8-merge-operations-steps-71-80)
- [Part 9: Optimization & Performance (Steps 81-90)](#part-9-optimization--performance-steps-81-90)
- [Part 10: Production Deployment (Steps 91-100)](#part-10-production-deployment-steps-91-100)

---

## Part 1: Getting Started (Steps 1-10)

### Step 1: Understand the Architecture
**Objective:** Learn the high-level architecture of the data pipeline.

This project implements a serverless data pipeline that:
- Extracts data from Amazon Redshift, Teradata Vantage, and Google BigQuery
- Performs cross-source joins using AWS Glue native connectors
- Writes to an S3 data lake in Apache Iceberg format
- Implements MERGE operations for incremental updates

**Reference:** See `docs/getting-started/01-architecture-overview.md`

### Step 2: Review Prerequisites
**Objective:** Ensure you have the necessary accounts and permissions.

Required:
- AWS Account with administrative access
- Redshift cluster (or sample data)
- Teradata Vantage access (or sample data)
- Google Cloud Platform account with BigQuery access
- Basic Python knowledge
- Understanding of SQL

**Reference:** See `docs/getting-started/02-prerequisites.md`

### Step 3: Install Required Tools
**Objective:** Set up your local development environment.

Install:
```bash
# AWS CLI
pip install awscli --upgrade

# Python dependencies
pip install boto3 pyspark pandas

# Terraform (for infrastructure)
brew install terraform  # macOS
# or download from https://www.terraform.io/downloads
```

**Reference:** See `docs/getting-started/03-tool-installation.md`

### Step 4: Clone and Explore Repository
**Objective:** Get familiar with the project structure.

```bash
git clone https://github.com/iotda-ol/dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake.git
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake
```

Explore the folder structure:
- `src/` - Main source code
- `config/` - Configuration files
- `scripts/` - Utility scripts
- `docs/` - Documentation
- `examples/` - Sample implementations

**Reference:** See `docs/getting-started/04-repository-structure.md`

### Step 5: Understand AWS Glue Basics
**Objective:** Learn fundamental AWS Glue concepts.

Key concepts:
- **Crawlers:** Discover and catalog data
- **Jobs:** ETL scripts that transform data
- **Catalogs:** Metadata repository
- **Connections:** Links to data sources
- **Transforms:** Built-in data transformations

**Reference:** See `docs/getting-started/05-glue-fundamentals.md`

### Step 6: Learn Apache Iceberg Fundamentals
**Objective:** Understand why Iceberg is used for data lakes.

Iceberg benefits:
- ACID transactions on S3
- Schema evolution
- Time travel queries
- Hidden partitioning
- Efficient MERGE operations

**Reference:** See `docs/getting-started/06-iceberg-fundamentals.md`

### Step 7: Review DEA-C01 Best Practices
**Objective:** Understand AWS Data Engineer Associate certification principles.

Key principles:
- Data pipeline design patterns
- Cost optimization strategies
- Security best practices
- Data quality and governance
- Monitoring and observability

**Reference:** See `docs/getting-started/07-dea-c01-practices.md`

### Step 8: Understand Data Source Connectors
**Objective:** Learn how AWS Glue connects to different data sources.

Native connectors for:
- Amazon Redshift (JDBC)
- Teradata Vantage (JDBC)
- Google BigQuery (Spark connector)
- S3 (native)

**Reference:** See `docs/getting-started/08-connector-overview.md`

### Step 9: Set Up AWS Profile
**Objective:** Configure AWS credentials for CLI access.

```bash
aws configure --profile glue-iceberg
# Enter AWS Access Key ID
# Enter AWS Secret Access Key
# Default region: us-east-1
# Default output: json
```

**Reference:** See `docs/getting-started/09-aws-profile-setup.md`

### Step 10: Validate Environment
**Objective:** Ensure all prerequisites are properly configured.

Run validation script:
```bash
python scripts/setup/validate-environment.py
```

This checks:
- AWS credentials
- Required permissions
- Tool installations
- Network connectivity

**Reference:** See `docs/getting-started/10-environment-validation.md`

---

## Part 2: Environment Setup (Steps 11-20)

### Step 11: Create S3 Buckets
**Objective:** Set up storage for data lake and scripts.

```bash
# Data lake bucket
aws s3 mb s3://my-iceberg-datalake --profile glue-iceberg

# Scripts and temporary storage
aws s3 mb s3://my-glue-scripts --profile glue-iceberg
aws s3 mb s3://my-glue-temp --profile glue-iceberg
```

**Reference:** See `docs/intermediate/11-s3-bucket-setup.md`

### Step 12: Configure S3 Bucket Policies
**Objective:** Secure your S3 buckets with appropriate policies.

Apply bucket policies for:
- Encryption at rest (SSE-S3 or SSE-KMS)
- Access logging
- Versioning
- Lifecycle policies

**Reference:** See `docs/intermediate/12-s3-security.md`

### Step 13: Create IAM Roles for Glue
**Objective:** Set up service roles with least privilege access.

Create roles:
- `GlueServiceRole` - For Glue jobs
- `GlueCrawlerRole` - For crawlers
- `GlueConnectionRole` - For data source connections

**Reference:** See `templates/iam-policies/glue-service-role.json`

### Step 14: Set Up VPC and Security Groups
**Objective:** Configure network access for data sources.

Configure:
- VPC with public and private subnets
- Security groups for Glue ENIs
- NAT Gateway for internet access
- VPC endpoints for AWS services

**Reference:** See `docs/intermediate/14-vpc-configuration.md`

### Step 15: Configure Redshift Connection
**Objective:** Establish AWS Glue connection to Redshift.

Create connection:
```python
# Using boto3
import boto3
glue = boto3.client('glue')
glue.create_connection(
    ConnectionInput={
        'Name': 'redshift-connection',
        'ConnectionType': 'JDBC',
        'ConnectionProperties': {
            'JDBC_CONNECTION_URL': 'jdbc:redshift://...',
            'USERNAME': '...',
            'PASSWORD': '...'
        }
    }
)
```

**Reference:** See `config/redshift/connection-config.json`

### Step 16: Configure Teradata Connection
**Objective:** Establish AWS Glue connection to Teradata Vantage.

Configure JDBC connection:
- Install Teradata JDBC driver in Glue
- Create Glue connection
- Test connectivity

**Reference:** See `config/teradata/connection-config.json`

### Step 17: Configure BigQuery Connection
**Objective:** Set up Google BigQuery as a data source.

Setup steps:
- Create GCP service account
- Download JSON key
- Store in AWS Secrets Manager
- Configure Spark connector properties

**Reference:** See `config/bigquery/connection-config.json`

### Step 18: Create Glue Database
**Objective:** Set up metadata catalog for tables.

```python
glue.create_database(
    DatabaseInput={
        'Name': 'iceberg_datalake',
        'Description': 'Iceberg tables for cross-source data',
        'LocationUri': 's3://my-iceberg-datalake/'
    }
)
```

**Reference:** See `scripts/setup/create-glue-database.py`

### Step 19: Upload Reusable Libraries
**Objective:** Deploy shared Python libraries to S3.

```bash
# Package libraries
cd lib
zip -r ../libraries.zip .

# Upload to S3
aws s3 cp libraries.zip s3://my-glue-scripts/libs/
```

**Reference:** See `lib/README.md`

### Step 20: Test Connectivity
**Objective:** Verify all data source connections work.

Run connection tests:
```bash
python scripts/setup/test-connections.py --all
```

**Reference:** See `docs/intermediate/20-connectivity-testing.md`

---

## Part 3: AWS Configuration (Steps 21-30)

### Step 21: Configure Glue Job Parameters
**Objective:** Set up reusable job configuration templates.

Key parameters:
- `--datalake-database`: Target Iceberg database
- `--source-connection`: Data source connection name
- `--enable-metrics`: CloudWatch metrics flag
- `--enable-continuous-log`: Continuous logging
- `--enable-spark-ui`: Spark UI for debugging

**Reference:** See `config/glue/job-parameters.json`

### Step 22: Set Up CloudWatch Logging
**Objective:** Configure centralized logging for all jobs.

Configure:
- Log groups for each job
- Log retention policies (30 days default)
- Metric filters for errors
- Alarms for failures

**Reference:** See `docs/intermediate/22-cloudwatch-setup.md`

### Step 23: Configure Glue Data Catalog Settings
**Objective:** Optimize catalog for Iceberg tables.

Settings:
- Enable catalog encryption
- Configure catalog ID
- Set up resource policies
- Enable Lake Formation integration (optional)

**Reference:** See `config/glue/catalog-settings.json`

### Step 24: Set Up Secrets Manager
**Objective:** Securely store database credentials.

Store secrets for:
- Redshift credentials
- Teradata credentials
- BigQuery service account keys
- API tokens

```bash
aws secretsmanager create-secret \
  --name prod/redshift/credentials \
  --secret-string '{"username":"admin","password":"..."}'
```

**Reference:** See `scripts/setup/create-secrets.py`

### Step 25: Configure KMS Encryption
**Objective:** Set up encryption keys for data at rest.

Create KMS keys for:
- S3 bucket encryption
- Glue catalog encryption
- CloudWatch log encryption
- Secrets Manager encryption

**Reference:** See `docs/intermediate/25-kms-configuration.md`

### Step 26: Set Up AWS Glue Studio
**Objective:** Configure visual ETL development environment.

Access Glue Studio:
- Navigate to AWS Glue Console
- Open Glue Studio
- Create visual jobs
- Test with sample data

**Reference:** See `docs/intermediate/26-glue-studio-intro.md`

### Step 27: Configure Glue Triggers
**Objective:** Set up job scheduling and dependencies.

Create triggers:
- Scheduled (cron-based)
- On-demand
- Conditional (based on job success)
- Event-driven (S3, Lambda)

**Reference:** See `config/glue/triggers.json`

### Step 28: Set Up Glue Workflows
**Objective:** Orchestrate complex multi-job pipelines.

Design workflow:
1. Crawl source data
2. Run validation job
3. Execute transformation
4. Perform merge operation
5. Run quality checks

**Reference:** See `docs/intermediate/28-workflow-orchestration.md`

### Step 29: Configure Glue DevEndpoint (Optional)
**Objective:** Set up interactive development environment.

Create DevEndpoint for:
- Interactive PySpark development
- Testing transformation logic
- Debugging issues
- Library testing

**Reference:** See `docs/intermediate/29-dev-endpoint.md`

### Step 30: Validate AWS Configuration
**Objective:** Ensure all AWS resources are properly configured.

Run validation:
```bash
python scripts/setup/validate-aws-config.py
```

Validates:
- IAM roles and policies
- S3 buckets and permissions
- Network configuration
- Connections
- Glue database

**Reference:** See `docs/intermediate/30-config-validation.md`

---

## Part 4: Data Source Connectors (Steps 31-40)

### Step 31: Implement Redshift Connector Module
**Objective:** Create reusable connector for Redshift data extraction.

```python
# src/connectors/redshift_connector.py
class RedshiftConnector:
    def __init__(self, connection_name, glue_context):
        self.connection_name = connection_name
        self.glue_context = glue_context
    
    def read_table(self, schema, table, predicates=None):
        # Implementation
        pass
```

**Reference:** See `src/connectors/redshift_connector.py`

### Step 32: Implement Teradata Connector Module
**Objective:** Create reusable connector for Teradata data extraction.

Key features:
- FastExport for bulk reads
- Pushdown predicate support
- Partition-aware reading
- Schema inference

**Reference:** See `src/connectors/teradata_connector.py`

### Step 33: Implement BigQuery Connector Module
**Objective:** Create reusable connector for BigQuery data extraction.

Using Spark BigQuery connector:
- Service account authentication
- Project and dataset configuration
- Filter pushdown
- Schema mapping

**Reference:** See `src/connectors/bigquery_connector.py`

### Step 34: Create Connection Factory Pattern
**Objective:** Implement factory for creating data source connections.

```python
# src/connectors/connection_factory.py
class ConnectionFactory:
    @staticmethod
    def get_connector(source_type, config, glue_context):
        if source_type == 'redshift':
            return RedshiftConnector(config, glue_context)
        elif source_type == 'teradata':
            return TeradataConnector(config, glue_context)
        elif source_type == 'bigquery':
            return BigQueryConnector(config, glue_context)
```

**Reference:** See `src/connectors/connection_factory.py`

### Step 35: Implement Connection Pooling
**Objective:** Optimize connection reuse across operations.

Features:
- Connection lifecycle management
- Automatic retry logic
- Connection health checks
- Resource cleanup

**Reference:** See `lib/validation/connection_pool.py`

### Step 36: Add Schema Validation
**Objective:** Validate source schemas before extraction.

Validation checks:
- Required columns exist
- Data types match expectations
- Partition columns present
- Primary keys defined

**Reference:** See `lib/validation/schema_validator.py`

### Step 37: Implement Error Handling
**Objective:** Add robust error handling to connectors.

Error types:
- Connection failures
- Authentication errors
- Timeout errors
- Data quality issues

**Reference:** See `lib/logging/error_handler.py`

### Step 38: Add Connector Logging
**Objective:** Implement comprehensive logging for troubleshooting.

Log events:
- Connection attempts
- Data volume extracted
- Performance metrics
- Errors and warnings

**Reference:** See `lib/logging/connector_logger.py`

### Step 39: Create Connector Unit Tests
**Objective:** Test connector functionality in isolation.

Test coverage:
- Connection establishment
- Data reading
- Error handling
- Schema inference

**Reference:** See `tests/unit/test_connectors.py`

### Step 40: Document Connector API
**Objective:** Create comprehensive API documentation.

Documentation includes:
- Class interfaces
- Method signatures
- Usage examples
- Configuration options

**Reference:** See `docs/advanced/40-connector-api.md`

---

## Part 5: Apache Iceberg Basics (Steps 41-50)

### Step 41: Understand Iceberg Table Format
**Objective:** Learn the structure of Iceberg tables.

Components:
- Metadata files (JSON)
- Manifest lists
- Manifest files
- Data files (Parquet/ORC/Avro)

**Reference:** See `docs/advanced/41-iceberg-table-format.md`

### Step 42: Create First Iceberg Table
**Objective:** Create a basic Iceberg table in Glue Catalog.

```python
# Using PyIceberg or Spark
spark.sql("""
    CREATE TABLE iceberg_datalake.customers (
        customer_id BIGINT,
        name STRING,
        email STRING,
        created_at TIMESTAMP
    )
    USING iceberg
    LOCATION 's3://my-iceberg-datalake/customers'
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
""")
```

**Reference:** See `examples/basic/01-create-iceberg-table.py`

### Step 43: Configure Iceberg Table Properties
**Objective:** Optimize Iceberg tables with custom properties.

Key properties:
- `write.format.default`: File format (parquet)
- `write.metadata.compression-codec`: Metadata compression
- `write.target-file-size-bytes`: Target file size
- `commit.retry.num-retries`: Commit retry count

**Reference:** See `config/iceberg/table-properties.json`

### Step 44: Implement Partitioning Strategy
**Objective:** Design efficient partition schemes.

Partition strategies:
- Hidden partitioning (Iceberg feature)
- Time-based partitioning (day/month/year)
- Range partitioning
- Hash partitioning

**Reference:** See `docs/advanced/44-partitioning-strategies.md`

### Step 45: Add Schema Evolution Support
**Objective:** Enable schema changes without rewriting data.

Supported operations:
- Add columns
- Drop columns
- Rename columns
- Change column types (compatible)

```python
spark.sql("""
    ALTER TABLE iceberg_datalake.customers 
    ADD COLUMN phone STRING
""")
```

**Reference:** See `examples/basic/02-schema-evolution.py`

### Step 46: Implement Time Travel Queries
**Objective:** Query historical versions of data.

Time travel examples:
```sql
-- Query as of specific timestamp
SELECT * FROM iceberg_datalake.customers 
TIMESTAMP AS OF '2024-01-01 00:00:00';

-- Query as of specific snapshot
SELECT * FROM iceberg_datalake.customers 
VERSION AS OF 123456789;
```

**Reference:** See `examples/intermediate/01-time-travel.py`

### Step 47: Configure Snapshot Management
**Objective:** Manage table snapshots and history.

Operations:
- Expire old snapshots
- Retain minimum snapshots
- Remove orphan files
- Compact metadata

**Reference:** See `src/iceberg/snapshot_manager.py`

### Step 48: Implement Table Maintenance
**Objective:** Create maintenance routines for Iceberg tables.

Maintenance tasks:
- Compact data files
- Rewrite manifests
- Expire snapshots
- Remove orphan files

```python
# Compact small files
spark.sql("""
    CALL iceberg_datalake.system.rewrite_data_files(
        table => 'customers',
        strategy => 'binpack'
    )
""")
```

**Reference:** See `scripts/maintenance/iceberg-maintenance.py`

### Step 49: Add Iceberg Metrics Collection
**Objective:** Monitor Iceberg table performance and health.

Metrics:
- Table size
- Number of files
- Average file size
- Snapshot count
- Metadata size

**Reference:** See `lib/monitoring/iceberg_metrics.py`

### Step 50: Test Iceberg Operations
**Objective:** Validate Iceberg functionality.

Test scenarios:
- Create/drop tables
- Insert/update/delete operations
- Schema evolution
- Time travel
- Snapshot expiration

**Reference:** See `tests/integration/test_iceberg.py`

---

## Part 6: ETL Pipeline Development (Steps 51-60)

### Step 51: Design ETL Architecture
**Objective:** Plan the overall ETL pipeline structure.

Architecture components:
- Source layer (Redshift, Teradata, BigQuery)
- Staging layer (S3 raw data)
- Transformation layer (Glue jobs)
- Target layer (Iceberg tables)
- Metadata layer (Glue Catalog)

**Reference:** See `docs/advanced/51-etl-architecture.md`

### Step 52: Create Base ETL Job Template
**Objective:** Build reusable Glue job template.

```python
# templates/glue-jobs/base-etl-job.py
from awsglue.context import GlueContext
from pyspark.context import SparkContext

class BaseETLJob:
    def __init__(self, args):
        self.args = args
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
    
    def extract(self):
        raise NotImplementedError
    
    def transform(self):
        raise NotImplementedError
    
    def load(self):
        raise NotImplementedError
    
    def run(self):
        data = self.extract()
        transformed = self.transform(data)
        self.load(transformed)
```

**Reference:** See `templates/glue-jobs/base-etl-job.py`

### Step 53: Implement Data Extraction Logic
**Objective:** Create reusable extraction patterns.

Extraction patterns:
- Full load
- Incremental load (timestamp-based)
- Change Data Capture (CDC)
- Partition-based extraction

**Reference:** See `src/transformations/extractors.py`

### Step 54: Create Transformation Library
**Objective:** Build reusable transformation functions.

Common transformations:
- Data type conversions
- Null handling
- Deduplication
- Standardization
- Enrichment

```python
# lib/transformations/common.py
def standardize_dates(df, date_columns):
    for col in date_columns:
        df = df.withColumn(col, to_timestamp(col))
    return df
```

**Reference:** See `lib/transformations/common.py`

### Step 55: Implement Data Quality Checks
**Objective:** Add validation before loading to target.

Quality checks:
- Null checks
- Uniqueness constraints
- Referential integrity
- Value range validation
- Format validation

**Reference:** See `lib/validation/data_quality.py`

### Step 56: Create Loading Strategies
**Objective:** Implement different loading patterns.

Loading strategies:
- Append (insert only)
- Overwrite (replace all)
- Upsert (update or insert)
- Merge (complex logic)

**Reference:** See `src/transformations/loaders.py`

### Step 57: Add Job Parameterization
**Objective:** Make jobs configurable via parameters.

Parameters:
- Source connection name
- Target table name
- Batch date/timestamp
- Processing mode (full/incremental)
- Custom business logic flags

**Reference:** See `config/glue/job-parameters.json`

### Step 58: Implement Job Orchestration
**Objective:** Coordinate multiple ETL jobs.

Orchestration patterns:
- Sequential execution
- Parallel execution
- Conditional branching
- Error handling and retry

**Reference:** See `docs/advanced/58-job-orchestration.md`

### Step 59: Add Performance Optimization
**Objective:** Optimize Spark job performance.

Optimizations:
- Partition tuning
- Broadcast joins
- Caching strategy
- Resource allocation
- Pushdown predicates

**Reference:** See `docs/expert/59-performance-tuning.md`

### Step 60: Create ETL Monitoring Dashboard
**Objective:** Visualize ETL pipeline metrics.

Metrics to monitor:
- Job duration
- Records processed
- Error rates
- Data quality scores
- Cost per job

**Reference:** See `docs/advanced/60-monitoring-setup.md`

---

## Part 7: Cross-Source Joins (Steps 61-70)

### Step 61: Understand Cross-Source Join Challenges
**Objective:** Learn the complexities of joining data across systems.

Challenges:
- Different data formats
- Network latency
- Schema mismatches
- Data type incompatibilities
- Volume imbalances

**Reference:** See `docs/expert/61-cross-source-challenges.md`

### Step 62: Design Join Strategy
**Objective:** Plan efficient cross-source join approach.

Strategies:
- Extract both sources to S3, then join
- Join in Spark memory (broadcast join)
- Staged approach with intermediate tables
- Partition-aware joins

**Reference:** See `docs/expert/62-join-strategies.md`

### Step 63: Implement Broadcast Join Pattern
**Objective:** Join small dimension tables with large facts.

```python
# For small lookup tables
from pyspark.sql.functions import broadcast

df_large = extract_from_redshift('fact_sales')
df_small = extract_from_bigquery('dim_product')

result = df_large.join(
    broadcast(df_small),
    df_large.product_id == df_small.product_id
)
```

**Reference:** See `examples/advanced/01-broadcast-join.py`

### Step 64: Implement Hash Partitioned Join
**Objective:** Join large datasets efficiently.

```python
# Partition both datasets on join key
df1 = extract_from_redshift('customers') \
    .repartition(200, 'customer_id')

df2 = extract_from_teradata('orders') \
    .repartition(200, 'customer_id')

result = df1.join(df2, 'customer_id')
```

**Reference:** See `examples/advanced/02-partition-join.py`

### Step 65: Handle Schema Alignment
**Objective:** Normalize schemas before joining.

Alignment tasks:
- Column name standardization
- Data type harmonization
- Timezone normalization
- Null value handling
- Default value assignment

**Reference:** See `src/transformations/schema_alignment.py`

### Step 66: Implement Join Optimization
**Objective:** Optimize join performance.

Optimizations:
- Filter pushdown before join
- Select only required columns
- Cache intermediate results
- Use appropriate join hints
- Leverage statistics

**Reference:** See `docs/expert/66-join-optimization.md`

### Step 67: Add Join Validation
**Objective:** Ensure join results are correct.

Validation checks:
- Row count validation
- Duplicate detection
- Join key coverage
- Null join key handling
- Data quality post-join

**Reference:** See `lib/validation/join_validator.py`

### Step 68: Handle Join Failures
**Objective:** Implement robust error handling for joins.

Error scenarios:
- Memory overflow
- Skewed data
- Missing join keys
- Type mismatch
- Timeout issues

**Reference:** See `lib/logging/join_error_handler.py`

### Step 69: Create Multi-Source Join Example
**Objective:** Implement complete 3-way join.

```python
# Join Redshift + Teradata + BigQuery
redshift_data = extract_from_redshift('sales')
teradata_data = extract_from_teradata('customers')
bigquery_data = extract_from_bigquery('products')

# Join 1: Sales + Customers
sales_customers = redshift_data.join(
    teradata_data,
    redshift_data.customer_id == teradata_data.id
)

# Join 2: Add Products
final_result = sales_customers.join(
    bigquery_data,
    sales_customers.product_id == bigquery_data.id
)
```

**Reference:** See `examples/advanced/03-multi-source-join.py`

### Step 70: Document Join Patterns
**Objective:** Create comprehensive join pattern guide.

Document:
- When to use each pattern
- Performance characteristics
- Trade-offs
- Best practices
- Common pitfalls

**Reference:** See `docs/expert/70-join-patterns.md`

---

## Part 8: MERGE Operations (Steps 71-80)

### Step 71: Understand MERGE Semantics
**Objective:** Learn MERGE operation in Iceberg context.

MERGE combines:
- UPDATE: Modify existing records
- INSERT: Add new records
- DELETE: Remove records (optional)

Use cases:
- Slowly Changing Dimensions (SCD)
- Incremental data updates
- CDC processing
- Deduplication

**Reference:** See `docs/expert/71-merge-semantics.md`

### Step 72: Implement Basic MERGE Operation
**Objective:** Create simple MERGE into Iceberg table.

```python
# Using Spark SQL
spark.sql("""
    MERGE INTO iceberg_datalake.customers AS target
    USING staged_customers AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Reference:** See `examples/intermediate/02-basic-merge.py`

### Step 73: Implement Conditional MERGE
**Objective:** Add conditional logic to MERGE operations.

```python
spark.sql("""
    MERGE INTO iceberg_datalake.customers AS target
    USING staged_customers AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED AND source.updated_at > target.updated_at 
        THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Reference:** See `examples/intermediate/03-conditional-merge.py`

### Step 74: Implement SCD Type 2 with MERGE
**Objective:** Track historical changes using Type 2 dimensions.

```python
# Add effective_date, end_date, is_current columns
spark.sql("""
    MERGE INTO iceberg_datalake.dim_customer AS target
    USING staged_customers AS source
    ON target.customer_id = source.customer_id 
       AND target.is_current = true
    WHEN MATCHED AND (source.name != target.name OR source.email != target.email)
        THEN UPDATE SET 
            end_date = current_date(),
            is_current = false
    WHEN NOT MATCHED THEN INSERT (
        customer_id, name, email, 
        effective_date, end_date, is_current
    ) VALUES (
        source.customer_id, source.name, source.email,
        current_date(), NULL, true
    )
""")
```

**Reference:** See `examples/advanced/04-scd-type2.py`

### Step 75: Optimize MERGE Performance
**Objective:** Tune MERGE operations for large datasets.

Optimizations:
- Pre-aggregate source data
- Partition alignment
- Sort source data by merge key
- Use appropriate file sizes
- Leverage Iceberg's copy-on-write

**Reference:** See `docs/expert/75-merge-optimization.md`

### Step 76: Implement Incremental MERGE
**Objective:** Process only changed records.

```python
# Extract only records changed since last run
last_run_timestamp = get_last_run_timestamp()

source_data = extract_from_source(
    f"SELECT * FROM table WHERE updated_at > '{last_run_timestamp}'"
)

# MERGE only changed records
perform_merge(source_data, target_table)
```

**Reference:** See `src/iceberg/incremental_merge.py`

### Step 77: Handle MERGE Conflicts
**Objective:** Resolve conflicts during MERGE operations.

Conflict scenarios:
- Concurrent modifications
- Duplicate keys in source
- Missing required columns
- Data type mismatches
- Constraint violations

**Reference:** See `lib/validation/merge_validator.py`

### Step 78: Add MERGE Auditing
**Objective:** Track MERGE operation metadata.

Audit information:
- Records inserted
- Records updated
- Records deleted
- Execution timestamp
- Source data lineage

```python
audit_info = {
    'inserted': count_inserted,
    'updated': count_updated,
    'deleted': count_deleted,
    'execution_time': datetime.now(),
    'source': 'redshift.sales'
}
write_audit_log(audit_info)
```

**Reference:** See `lib/monitoring/merge_auditor.py`

### Step 79: Create MERGE Testing Framework
**Objective:** Validate MERGE operations thoroughly.

Test scenarios:
- New records (INSERT path)
- Existing records (UPDATE path)
- Unchanged records (no-op)
- Delete scenarios
- Edge cases (nulls, duplicates)

**Reference:** See `tests/integration/test_merge.py`

### Step 80: Document MERGE Best Practices
**Objective:** Create comprehensive MERGE guide.

Best practices:
- When to use MERGE vs. append
- Performance considerations
- Error handling strategies
- Monitoring and alerting
- Rollback procedures

**Reference:** See `docs/expert/80-merge-best-practices.md`

---

## Part 9: Optimization & Performance (Steps 81-90)

### Step 81: Implement Data Partitioning Strategy
**Objective:** Optimize data layout for query performance.

```python
# Create partitioned Iceberg table
spark.sql("""
    CREATE TABLE iceberg_datalake.sales (
        sale_id BIGINT,
        customer_id BIGINT,
        amount DECIMAL(10,2),
        sale_date DATE
    )
    USING iceberg
    PARTITIONED BY (days(sale_date))
""")
```

**Reference:** See `docs/expert/81-partitioning-strategy.md`

### Step 82: Optimize File Sizes
**Objective:** Configure optimal file sizes for Iceberg tables.

Configuration:
```python
spark.conf.set("spark.sql.files.maxRecordsPerFile", 500000)
spark.conf.set("write.target-file-size-bytes", 134217728)  # 128MB
```

Target: 128MB-512MB files for best performance

**Reference:** See `config/iceberg/file-size-config.json`

### Step 83: Implement Z-Ordering
**Objective:** Co-locate related data for faster queries.

```python
# Z-order data by frequently filtered columns
spark.sql("""
    CALL iceberg_datalake.system.rewrite_data_files(
        table => 'sales',
        strategy => 'sort',
        sort_order => 'zorder(customer_id, product_id)'
    )
""")
```

**Reference:** See `examples/advanced/05-z-ordering.py`

### Step 84: Configure Spark Memory Settings
**Objective:** Optimize Spark resource utilization.

Key settings:
```python
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**Reference:** See `config/glue/spark-config.json`

### Step 85: Implement Caching Strategy
**Objective:** Cache frequently accessed data.

```python
# Cache dimension tables
dim_products = spark.table("iceberg_datalake.dim_products")
dim_products.cache()
dim_products.count()  # Materialize cache

# Use in multiple joins
result1 = fact_sales.join(dim_products, "product_id")
result2 = fact_returns.join(dim_products, "product_id")
```

**Reference:** See `docs/expert/85-caching-strategies.md`

### Step 86: Add Predicate Pushdown
**Objective:** Filter data at source level.

```python
# Push filters to source database
redshift_df = glue_context.create_dynamic_frame.from_catalog(
    database="redshift_db",
    table_name="sales",
    push_down_predicate="sale_date >= '2024-01-01'"
).toDF()
```

**Reference:** See `examples/advanced/06-predicate-pushdown.py`

### Step 87: Implement Columnar Pruning
**Objective:** Read only required columns.

```python
# Select only needed columns
df = spark.table("iceberg_datalake.large_table") \
    .select("id", "name", "amount", "date")
```

**Reference:** See `docs/expert/87-columnar-pruning.md`

### Step 88: Monitor Query Performance
**Objective:** Track and analyze query execution.

Metrics to track:
- Query execution time
- Data scanned
- Shuffle read/write
- GC time
- Stage durations

**Reference:** See `lib/monitoring/query_profiler.py`

### Step 89: Implement Auto-Scaling
**Objective:** Dynamically adjust Glue DPU allocation.

```python
# Configure auto-scaling for Glue job
job_config = {
    'MaxCapacity': 10,
    'WorkerType': 'G.1X',
    'NumberOfWorkers': 5,
    'ExecutionProperty': {
        'MaxConcurrentRuns': 3
    }
}
```

**Reference:** See `docs/expert/89-auto-scaling.md`

### Step 90: Create Performance Testing Suite
**Objective:** Benchmark and validate optimizations.

Test scenarios:
- Baseline performance
- Partition pruning effectiveness
- Join performance
- MERGE operation speed
- Resource utilization

**Reference:** See `tests/performance/benchmark-suite.py`

---

## Part 10: Production Deployment (Steps 91-100)

### Step 91: Set Up CI/CD Pipeline
**Objective:** Automate deployment process.

Pipeline stages:
1. Code validation (linting, type checking)
2. Unit tests
3. Integration tests
4. Deployment to dev
5. Deployment to prod

**Reference:** See `docs/production/91-cicd-setup.md`

### Step 92: Implement Infrastructure as Code
**Objective:** Define all resources in Terraform.

Resources to define:
- S3 buckets
- IAM roles and policies
- Glue databases and tables
- Glue jobs and triggers
- CloudWatch alarms

**Reference:** See `docs/production/92-iac-terraform.md`

### Step 93: Configure Monitoring and Alerting
**Objective:** Set up comprehensive observability.

Monitoring:
- CloudWatch dashboards
- Custom metrics
- Log aggregation
- Error tracking
- Performance trends

Alerts:
- Job failures
- Data quality issues
- Performance degradation
- Cost anomalies

**Reference:** See `docs/production/93-monitoring-alerting.md`

### Step 94: Implement Data Quality Gates
**Objective:** Prevent bad data from entering data lake.

Quality gates:
- Pre-load validation
- Schema compliance
- Business rule validation
- Statistical anomaly detection
- Automated rollback on failure

**Reference:** See `lib/validation/quality_gates.py`

### Step 95: Set Up Disaster Recovery
**Objective:** Plan for failure scenarios.

DR components:
- S3 cross-region replication
- Glue catalog backups
- Job configuration backups
- Runbook documentation
- Recovery time objectives (RTO)

**Reference:** See `docs/production/95-disaster-recovery.md`

### Step 96: Implement Cost Optimization
**Objective:** Reduce operational costs.

Cost optimizations:
- Right-size Glue DPUs
- Implement S3 lifecycle policies
- Use Spot instances where applicable
- Optimize job schedules
- Clean up unused resources

**Reference:** See `docs/production/96-cost-optimization.md`

### Step 97: Add Security Hardening
**Objective:** Enhance security posture.

Security measures:
- Encrypt all data at rest
- Encrypt data in transit
- Implement least privilege IAM
- Enable audit logging
- Set up VPC endpoints
- Configure network ACLs

**Reference:** See `docs/production/97-security-hardening.md`

### Step 98: Create Operational Runbooks
**Objective:** Document operational procedures.

Runbooks for:
- Job failure recovery
- Data quality issue resolution
- Performance troubleshooting
- Schema evolution procedures
- Incident response

**Reference:** See `docs/production/98-operational-runbooks.md`

### Step 99: Implement Compliance Controls
**Objective:** Meet regulatory requirements.

Compliance areas:
- Data retention policies
- PII handling
- Access audit trails
- Data lineage tracking
- GDPR/CCPA compliance

**Reference:** See `docs/production/99-compliance-controls.md`

### Step 100: Final Production Checklist
**Objective:** Validate production readiness.

Checklist:
- [ ] All tests passing
- [ ] Performance benchmarks met
- [ ] Security review completed
- [ ] Documentation complete
- [ ] Monitoring configured
- [ ] Alerts set up
- [ ] Runbooks validated
- [ ] DR plan tested
- [ ] Cost optimization applied
- [ ] Stakeholder sign-off obtained

**Reference:** See `docs/production/100-production-checklist.md`

---

## Congratulations! 🎉

You have completed the comprehensive 100-step guide from novice to expert in building AWS Glue Iceberg cross-source data pipelines. You should now be proficient in:

- AWS Glue architecture and best practices
- Apache Iceberg table management
- Cross-source data integration
- ETL pipeline development
- Performance optimization
- Production deployment

## Next Steps

1. Implement your own use case using this guide
2. Contribute improvements to this repository
3. Share your learnings with the community
4. Pursue AWS Data Engineer Associate (DEA-C01) certification

## Additional Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [DEA-C01 Exam Guide](https://aws.amazon.com/certification/certified-data-engineer-associate/)

---

**Version:** 1.0  
**Last Updated:** 2024  
**Maintained by:** Data Engineering Team
