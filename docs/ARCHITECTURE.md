# Multi-Source Data Integration Pipeline - Architecture

## Overview

This solution implements a production-ready, multi-source data integration pipeline that follows AWS DEA-C01 (AWS Certified Data Engineer - Associate) best practices. The pipeline extracts data from three different source systems (Amazon Redshift, Teradata Vantage, and Google BigQuery), performs transformations and joins, and writes the integrated data to an Amazon S3 data lake using Apache Iceberg table format with MERGE operations.

## Architecture Diagram

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Amazon         │      │  Teradata       │      │  Google         │
│  Redshift       │      │  Vantage        │      │  BigQuery       │
│  (Sales Data)   │      │  (Customer Data)│      │  (Product Data) │
└────────┬────────┘      └────────┬────────┘      └────────┬────────┘
         │                        │                        │
         │ Glue Native           │ Glue JDBC              │ Glue Marketplace
         │ Connector             │ Connector              │ Connector
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │   AWS Glue ETL Job      │
                    │   - Extract             │
                    │   - Transform           │
                    │   - Join                │
                    │   - Load (MERGE)        │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   S3 Data Lake          │
                    │   (Apache Iceberg)      │
                    │   - Partitioned         │
                    │   - Versioned           │
                    │   - ACID Compliant      │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   AWS Glue Data Catalog │
                    │   (Metadata Store)      │
                    └─────────────────────────┘

         Monitoring & Logging
         ┌─────────────────────────┐
         │   Amazon CloudWatch     │
         │   - Job Metrics         │
         │   - Alarms              │
         │   - Logs                │
         └─────────────────────────┘
```

## Components

### 1. Data Sources

#### Amazon Redshift
- **Purpose**: Source of transactional sales data
- **Connection**: AWS Glue native Redshift connector
- **Data Volume**: High-volume transactional data
- **Key Features**: 
  - Column-oriented storage
  - Massively parallel processing
  - Optimized for analytical queries

#### Teradata Vantage
- **Purpose**: Source of customer master data
- **Connection**: AWS Glue JDBC connector
- **Data Volume**: Medium-volume dimensional data
- **Key Features**:
  - Enterprise data warehouse
  - Advanced analytics capabilities
  - Comprehensive customer profiles

#### Google BigQuery
- **Purpose**: Source of product catalog data
- **Connection**: AWS Glue Marketplace connector (Simba JDBC)
- **Data Volume**: Low to medium-volume reference data
- **Key Features**:
  - Serverless data warehouse
  - Real-time analytics
  - Standard SQL interface

### 2. AWS Glue ETL Job

The core processing engine that orchestrates the entire pipeline.

**Key Capabilities:**
- **Multi-source extraction**: Uses native connectors for optimal performance
- **Schema standardization**: Normalizes data from different source schemas
- **Data quality checks**: Validates data at ingestion time
- **Cross-source joins**: Combines data from all three sources
- **Business transformations**: Calculates derived metrics and KPIs
- **Incremental processing**: Job bookmarks for efficient processing

**Configuration:**
- **Glue Version**: 4.0 (latest features and optimizations)
- **Worker Type**: G.2X (8 vCPU, 32GB memory per worker)
- **Worker Count**: 10 workers (scalable based on data volume)
- **Timeout**: 2880 minutes (48 hours)
- **Max Concurrent Runs**: 1 (prevents resource contention)

### 3. Apache Iceberg on S3

Modern table format that brings ACID transactions to data lakes.

**Why Iceberg?**
- **ACID Transactions**: Ensures data consistency
- **Time Travel**: Query historical data versions
- **Schema Evolution**: Add/modify columns without rewriting data
- **Hidden Partitioning**: Automatic partition management
- **Efficient MERGE**: Upsert operations at scale
- **Snapshot Isolation**: Concurrent readers and writers

**Table Properties:**
- **Format**: Parquet (columnar storage)
- **Compression**: Snappy (balance between speed and size)
- **Partitioning**: By year and month (optimized for time-based queries)
- **Metadata Compression**: Gzip (reduced metadata storage costs)

### 4. AWS Glue Data Catalog

Centralized metadata repository for all data assets.

**Features:**
- **Schema Registry**: Stores table schemas and metadata
- **Partition Management**: Tracks partitions for efficient queries
- **Version Control**: Maintains schema evolution history
- **Integration**: Works seamlessly with Athena, EMR, Redshift Spectrum

### 5. Monitoring and Logging

#### Amazon CloudWatch
- **Metrics Tracked**:
  - Job success/failure rate
  - Job duration
  - Number of records processed
  - Data quality metrics
  - Worker utilization

- **CloudWatch Alarms**:
  - Job failure alarm (triggers on any failed tasks)
  - Job duration alarm (triggers if job exceeds threshold)

- **Log Groups**:
  - Continuous CloudWatch logging enabled
  - Log filtering for error detection
  - Retention period: 7 days (configurable)

## Data Flow

### Phase 1: Extraction
1. **Redshift Extraction**
   - Query: `SELECT * FROM public.sales_data WHERE updated_at > :bookmark`
   - Job bookmark tracks last processed timestamp
   - Pushdown predicates for efficient data transfer

2. **Teradata Extraction**
   - Query: `SELECT * FROM PROD.CUSTOMER_DATA WHERE last_modified > :bookmark`
   - JDBC connection with query pushdown
   - Batch reading for memory efficiency

3. **BigQuery Extraction**
   - Query: `SELECT * FROM project.dataset.product_data`
   - Uses BigQuery Storage API for optimal performance
   - Columnar data transfer

### Phase 2: Transformation
1. **Source-side Transformations**
   - Type casting and normalization
   - Data quality flag assignment
   - Source system tagging
   - Timestamp standardization

2. **Cross-source Joins**
   - Sales ⟕ Customer (left join on customer_id)
   - Result ⟕ Product (left join on product_id)
   - Broadcast join optimization for smaller tables

3. **Business Transformations**
   - Calculate total revenue
   - Calculate profit margins
   - Derive customer lifetime value
   - Add partition columns (year, month, day)

### Phase 3: Loading
1. **Table Creation** (first run)
   - Create Iceberg table with schema
   - Set partitioning strategy
   - Configure table properties

2. **MERGE Operation** (subsequent runs)
   - Match on transaction_id (primary key)
   - Update existing records
   - Insert new records
   - Atomic operation (all or nothing)

3. **Optimization**
   - Compact small files
   - Optimize metadata
   - Remove expired snapshots

### Phase 4: Validation
1. **Row Count Verification**
2. **Data Quality Metrics**
3. **Sample Data Review**
4. **Schema Validation**

## Security Architecture

### IAM Least Privilege

**Glue Job Role Permissions:**
```
✓ Read from source connections (minimal scope)
✓ Read/Write to specific S3 buckets
✓ Write logs to CloudWatch (specific log groups)
✓ Read secrets from Secrets Manager (tagged resources)
✓ Access Glue Data Catalog (specific databases)
✗ No administrative permissions
✗ No cross-account access
✗ No permission to modify IAM
```

### Data Encryption

**At Rest:**
- S3: AES-256 encryption
- Iceberg: Parquet with encryption support
- Secrets Manager: KMS encryption

**In Transit:**
- TLS 1.2+ for all connections
- HTTPS for S3 access
- SSL for database connections

### Network Security

**VPC Configuration** (when applicable):
- Private subnets for Glue ENIs
- Security groups with minimal ingress/egress
- VPC endpoints for AWS services
- No internet gateway for sensitive data

## Scalability Considerations

### Horizontal Scaling
- **Auto-scaling workers**: Glue automatically adds workers based on data volume
- **Partition pruning**: Iceberg partitions reduce scan size
- **Predicate pushdown**: Filters applied at source

### Vertical Scaling
- **Worker types**: G.025X → G.1X → G.2X → G.4X → G.8X
- **Memory management**: Tune spark.executor.memory
- **Parallelism**: Configure spark.default.parallelism

### Performance Optimization
- **Broadcast joins**: For small dimension tables
- **Bucketing**: Co-locate related data
- **Caching**: For repeatedly accessed data
- **Compression**: Reduces I/O and storage costs

## Cost Optimization

### Glue Job Costs
- **Pay per DPU-hour**: Only charged when job runs
- **Job bookmarks**: Process only new/changed data
- **Worker optimization**: Right-size worker type and count

### S3 Storage Costs
- **Lifecycle policies**: Move old data to Glacier
- **Compression**: Reduces storage footprint
- **Partitioning**: Enables selective deletion

### Data Transfer Costs
- **Same region**: Keep sources and targets in same region
- **VPC endpoints**: Avoid NAT gateway charges
- **Compression**: Reduces network transfer volume

## High Availability

### Fault Tolerance
- **Job retries**: Automatic retry on failure
- **Checkpointing**: Resume from last successful point
- **Data validation**: Detect and handle corrupt data

### Data Durability
- **S3 durability**: 99.999999999% (11 9's)
- **Versioning**: Recover from accidental deletions
- **Cross-region replication**: Optional for DR

### Monitoring
- **CloudWatch alarms**: Immediate notification of issues
- **SNS notifications**: Email/SMS alerts
- **Job metrics**: Track success rate and duration

## Disaster Recovery

### Backup Strategy
- **Iceberg snapshots**: Point-in-time recovery
- **S3 versioning**: File-level recovery
- **Metadata backup**: Glue Catalog exports

### Recovery Procedures
1. Identify failure point from logs
2. Restore from last known good snapshot
3. Rerun job with corrected configuration
4. Validate data consistency

## Future Enhancements

1. **Real-time Processing**: Add Kinesis/Kafka for streaming data
2. **Data Lineage**: Implement AWS Glue Studio for visual lineage
3. **ML Integration**: Add SageMaker for predictive analytics
4. **Advanced Governance**: Implement AWS Lake Formation
5. **Global Distribution**: Add S3 cross-region replication
6. **Cost Attribution**: Implement resource tagging strategy

## References

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [AWS DEA-C01 Exam Guide](https://aws.amazon.com/certification/certified-data-engineer-associate/)
- [AWS Well-Architected Framework](https://aws.amazon.com/architecture/well-architected/)
