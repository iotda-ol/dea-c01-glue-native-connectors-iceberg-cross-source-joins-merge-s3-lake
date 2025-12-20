# Part 1: Fundamentals (Steps 1-20)

## Understanding the Basics

### Step 1: Understanding Data Lakes
**Objective**: Learn what a data lake is and why it matters
- A data lake is a centralized repository for storing structured and unstructured data at scale
- Amazon S3 serves as the storage layer for cost-effective, durable data storage
- Data lakes enable analytics, machine learning, and data science workflows
- Unlike data warehouses, data lakes store raw data in native format

**Key Concepts**:
- Schema-on-read vs schema-on-write
- Separation of compute and storage
- Cost-effective storage for petabyte-scale data

### Step 2: Introduction to AWS Glue
**Objective**: Understand AWS Glue's role in data integration
- AWS Glue is a serverless ETL (Extract, Transform, Load) service
- Automatically discovers and catalogs data
- Supports Apache Spark-based transformations
- Pay only for resources used during job execution
- No infrastructure to manage

**Components**:
- **Glue Data Catalog**: Centralized metadata repository
- **Glue ETL**: Spark-based data processing
- **Glue Crawlers**: Automatic schema discovery
- **Glue Connections**: Database and service connections

### Step 3: Understanding Apache Iceberg
**Objective**: Learn why Iceberg is essential for modern data lakes
- Iceberg is an open table format for large analytics datasets
- Provides ACID transactions on S3
- Supports schema evolution and time travel
- Enables efficient updates and deletes (MERGE operations)
- Manages metadata separately from data files

**Benefits**:
- Hidden partitioning (no partition predicates in queries)
- Time travel and snapshot isolation
- Concurrent writers without conflicts
- Schema evolution without data rewrites

### Step 4: Understanding Data Sources
**Objective**: Learn about the three source systems

**Amazon Redshift**:
- Cloud data warehouse for structured data
- Columnar storage optimized for analytics
- MPP (Massively Parallel Processing) architecture
- Best for structured, relational data

**Teradata Vantage**:
- Enterprise data warehouse platform
- Multi-cloud support
- Advanced analytics capabilities
- Legacy enterprise systems integration

**Google BigQuery**:
- Serverless, scalable analytics database
- Separated compute and storage
- SQL-based querying
- Integration with Google Cloud Platform

### Step 5: DEA-C01 Certification Overview
**Objective**: Understand AWS Data Engineer Associate certification requirements
- Covers data ingestion, transformation, and orchestration
- Focus on AWS Glue, Lake Formation, and data lake architecture
- Best practices for security, performance, and cost optimization
- Data modeling and schema design
- Data quality and governance

**Exam Domains**:
1. Data Ingestion and Transformation (34%)
2. Data Store Management (26%)
3. Data Operations and Support (22%)
4. Data Security and Governance (18%)

### Step 6: Architecture Overview
**Objective**: Visualize the complete solution architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Redshift   │  │  Teradata   │  │  BigQuery   │
│   Source    │  │   Source    │  │   Source    │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
                ┌───────▼────────┐
                │  AWS Glue ETL  │
                │  (Native      │
                │  Connectors)  │
                └───────┬────────┘
                        │
                ┌───────▼────────┐
                │  Cross-Source  │
                │  Joins &       │
                │  Transforms    │
                └───────┬────────┘
                        │
                ┌───────▼────────┐
                │  S3 Data Lake  │
                │  (Iceberg)     │
                └────────────────┘
```

### Step 7: Understanding ETL vs ELT
**Objective**: Learn the difference between approaches

**ETL (Extract, Transform, Load)**:
- Transform data before loading to destination
- Traditional approach
- Requires upfront schema definition
- Less storage needed in destination

**ELT (Extract, Load, Transform)**:
- Load raw data first, transform later
- Modern data lake approach
- More flexible for changing requirements
- Leverages destination's compute power
- Our solution uses ELT approach

### Step 8: AWS Account Prerequisites
**Objective**: Understand what you need to start

**Requirements**:
- Active AWS account with billing enabled
- IAM user with administrative or specific Glue permissions
- Access to AWS CLI and Console
- Basic understanding of AWS services
- Credit card for billing (Free tier available)

**Recommended**:
- Separate dev/test/prod accounts
- AWS Organizations for account management
- Billing alerts configured
- MFA (Multi-Factor Authentication) enabled

### Step 9: Understanding VPC and Networking
**Objective**: Learn networking requirements for Glue

**VPC Components**:
- **VPC**: Virtual Private Cloud provides network isolation
- **Subnets**: IP address ranges within VPC
- **Route Tables**: Network traffic routing
- **Internet Gateway**: Internet connectivity
- **NAT Gateway**: Outbound internet for private subnets
- **Security Groups**: Stateful firewall rules

**Glue Networking**:
- Glue can run in VPC or public mode
- VPC mode required for private databases
- Self-referencing security group rule required
- S3 VPC endpoints for private S3 access

### Step 10: Understanding IAM Roles and Policies
**Objective**: Learn AWS security model

**Key Concepts**:
- **IAM Users**: Individual accounts for people
- **IAM Roles**: Temporary credentials for services
- **IAM Policies**: JSON documents defining permissions
- **Principle of Least Privilege**: Grant minimum necessary permissions

**For Glue**:
- Service role for Glue to assume
- Policies for S3, Glue Catalog, CloudWatch
- Policies for source system access
- Encryption key permissions

### Step 11: Understanding S3 Bucket Organization
**Objective**: Learn best practices for S3 structure

**Recommended Structure**:
```
s3://my-data-lake/
  ├── raw/                    # Raw data from sources
  │   ├── redshift/
  │   ├── teradata/
  │   └── bigquery/
  ├── processed/              # Transformed data
  │   ├── staging/
  │   └── intermediate/
  ├── curated/                # Analytics-ready data
  │   ├── iceberg/
  │   │   ├── customers/
  │   │   ├── orders/
  │   │   └── products/
  │   └── reports/
  ├── temp/                   # Temporary processing files
  ├── scripts/                # Glue job scripts
  └── metadata/               # Iceberg metadata
```

### Step 12: Understanding Glue Catalog
**Objective**: Learn how Glue organizes metadata

**Components**:
- **Databases**: Logical grouping of tables
- **Tables**: Metadata about datasets (schema, location)
- **Partitions**: Subdivisions of table data
- **Connections**: Database connection information

**Best Practices**:
- Separate databases for raw, processed, curated
- Meaningful naming conventions
- Regular catalog cleanup
- Partition pruning for performance

### Step 13: Understanding Connectors
**Objective**: Learn about native vs custom connectors

**Native Connectors**:
- Built-in AWS Glue support
- Redshift (native)
- JDBC databases (native)
- No additional setup required

**AWS Marketplace Connectors**:
- Teradata connector
- BigQuery connector
- MongoDB, Snowflake, etc.
- Subscribe in AWS Marketplace

**Custom Connectors**:
- Build your own using Spark
- For proprietary systems
- More development effort

### Step 14: Understanding PySpark
**Objective**: Learn the programming model for Glue

**Key Concepts**:
- Glue jobs use Apache Spark with Python (PySpark)
- **DataFrame API**: Structured data processing
- **Transformations**: Lazy operations (map, filter, join)
- **Actions**: Trigger execution (show, count, write)
- Distributed processing across multiple nodes

**Example**:
```python
# Create DataFrame
df = spark.read.parquet("s3://bucket/data/")

# Transform (lazy)
filtered_df = df.filter(df.status == "active")

# Action (executes)
filtered_df.write.parquet("s3://bucket/output/")
```

### Step 15: Understanding Glue Job Types
**Objective**: Learn different job execution modes

**Spark Jobs**:
- Standard ETL jobs
- Full Spark functionality
- 2-100 DPUs (Data Processing Units)
- Best for large-scale transformations

**Streaming Jobs**:
- Continuous data processing
- Micro-batch processing
- Kafka, Kinesis integration
- Near real-time pipelines

**Python Shell**:
- Lightweight jobs
- No Spark overhead
- 0.0625 or 1 DPU
- Best for small datasets, API calls

**Ray Jobs**:
- For ML workloads
- Distributed Python
- TensorFlow, PyTorch support

### Step 16: Understanding Data Formats
**Objective**: Learn common data formats in data lakes

**Parquet**:
- Columnar format
- Efficient compression
- Best for analytics queries
- Schema embedded in file

**ORC (Optimized Row Columnar)**:
- Similar to Parquet
- Better for Hive workloads
- Built-in indexes

**Avro**:
- Row-based format
- Schema evolution support
- Good for streaming

**JSON**:
- Human-readable
- Flexible schema
- Higher storage cost
- Slower query performance

**Iceberg**:
- Table format (not file format)
- Manages Parquet/ORC files
- Metadata layer for ACID transactions

### Step 17: Understanding Partitioning
**Objective**: Learn how to organize data for performance

**Traditional Partitioning**:
```
s3://bucket/table/year=2024/month=12/day=20/data.parquet
```

**Benefits**:
- Partition pruning (skip irrelevant data)
- Faster queries on filtered columns
- Better compression per partition

**Anti-patterns**:
- Too many partitions (thousands)
- Skewed partition sizes
- Partitioning by high-cardinality columns

**Iceberg Partitioning**:
- Hidden partitioning (no folder structure)
- Automatic partition evolution
- No partition predicates in queries

### Step 18: Understanding Schema Evolution
**Objective**: Learn how schemas change over time

**Common Changes**:
- **Adding columns**: Most common and safest
- **Removing columns**: Requires careful handling
- **Changing data types**: Risky, may break compatibility
- **Renaming columns**: Treated as remove + add

**Iceberg Schema Evolution**:
- Add columns without rewriting data
- Column IDs track fields across renames
- Time travel to old schemas
- Compatible with downstream consumers

### Step 19: Cost Considerations
**Objective**: Understand cost factors

**Glue Costs**:
- **DPU-hours**: Primary cost driver
- Standard: $0.44 per DPU-hour
- Data Catalog: $1 per 100,000 requests
- Development endpoints: Separate pricing

**S3 Costs**:
- **Storage**: ~$0.023 per GB/month (Standard)
- **Requests**: GET, PUT request pricing
- **Data transfer**: Out to internet charges

**Optimization Tips**:
- Use appropriate DPU count
- Enable Glue auto-scaling
- Use S3 lifecycle policies
- Compress data
- Use columnar formats

### Step 20: Development Environment Setup Options
**Objective**: Choose your development approach

**AWS Console**:
- Web-based interface
- Visual workflow builder
- Good for beginners
- Limited for complex logic

**AWS CLI**:
- Command-line interface
- Scriptable and automatable
- Infrastructure as code
- Learning curve

**Glue Studio**:
- Visual ETL designer
- Drag-and-drop interface
- Generates PySpark code
- Limited customization

**Local Development**:
- Docker with Glue libraries
- Faster development cycle
- Debug locally
- Test before deployment

**Infrastructure as Code**:
- **Terraform**: Multi-cloud
- **CloudFormation**: AWS native
- **CDK**: Programming languages
- Version control friendly

**Recommended**: Start with Console/Glue Studio, graduate to IaC for production
