# AWS Glue Iceberg Cross-Source Data Pipeline

[![DEA-C01](https://img.shields.io/badge/AWS-DEA--C01-orange)](https://aws.amazon.com/certification/certified-data-engineer-associate/)
[![Glue](https://img.shields.io/badge/AWS-Glue-green)](https://aws.amazon.com/glue/)
[![Iceberg](https://img.shields.io/badge/Apache-Iceberg-blue)](https://iceberg.apache.org/)

This repository implements a production-ready, low-operational-overhead data pipeline that aggregates data from **Amazon Redshift**, **Teradata Vantage**, and **Google BigQuery** into an **Amazon S3 data lake** using **Apache Iceberg**. AWS Glue native connectors and transforms are used to join data and perform Iceberg MERGE operations following **DEA-C01 best practices**.

## 🎯 Features

- **📚 Comprehensive 100-Step Manual** - From novice to expert training guide
- **🔧 Highly Modular Code** - Reusable components across all modules
- **📁 Organized Structure** - Clean folder hierarchy with minimal loose files
- **🔌 Multi-Source Connectors** - Redshift, Teradata, BigQuery support
- **❄️ Apache Iceberg Integration** - ACID transactions, time travel, schema evolution
- **🔄 MERGE Operations** - Incremental updates and SCD Type 2 support
- **✅ Data Quality Framework** - Comprehensive validation and quality checks
- **📊 Monitoring & Logging** - CloudWatch integration and structured logging
- **🚀 Production Ready** - CI/CD, IaC, security hardening included

## 📖 Documentation

### Quick Start
1. **[Comprehensive Guide](docs/COMPREHENSIVE-GUIDE.md)** - Complete 100-step manual from novice to expert
2. **[Getting Started](docs/getting-started/)** - Basic setup and prerequisites
3. **[Architecture Overview](docs/getting-started/01-architecture-overview.md)** - System design and components

### Advanced Topics
- **[Configuration Guide](config/README.md)** - All configuration options
- **[API Documentation](src/README.md)** - Code modules and APIs
- **[Examples](examples/README.md)** - Sample implementations
- **[Best Practices](docs/expert/)** - Production deployment and optimization

## 🏗️ Project Structure

```
dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake/
│
├── 📚 docs/                          # Comprehensive documentation
│   ├── COMPREHENSIVE-GUIDE.md        # 100-step manual (novice to expert)
│   ├── getting-started/              # Steps 1-10: Basics
│   ├── intermediate/                 # Steps 11-30: Setup & config
│   ├── advanced/                     # Steps 31-70: Advanced patterns
│   ├── expert/                       # Steps 71-90: Optimization
│   └── production/                   # Steps 91-100: Deployment
│
├── 💻 src/                           # Source code (reusable modules)
│   ├── connectors/                   # Data source connectors
│   │   ├── base_connector.py        # Abstract base class
│   │   ├── redshift_connector.py    # Amazon Redshift
│   │   ├── teradata_connector.py    # Teradata Vantage
│   │   ├── bigquery_connector.py    # Google BigQuery
│   │   └── connection_factory.py    # Factory pattern
│   ├── transformations/              # Data transformations
│   │   └── data_transformer.py      # Reusable transforms
│   ├── iceberg/                      # Iceberg operations
│   │   └── table_manager.py         # Table management
│   └── utils/                        # Utility functions
│
├── ⚙️ config/                        # Configuration files
│   ├── redshift/                     # Redshift configs
│   │   └── connection-config.json
│   ├── teradata/                     # Teradata configs
│   │   └── connection-config.json
│   ├── bigquery/                     # BigQuery configs
│   │   └── connection-config.json
│   ├── iceberg/                      # Iceberg configs
│   │   └── table-config.json
│   └── glue/                         # Glue job configs
│       └── job-config.json
│
├── 📦 lib/                           # Shared libraries
│   ├── validation/                   # Data quality
│   │   └── data_quality.py
│   ├── logging/                      # Enhanced logging
│   │   └── glue_logger.py
│   └── monitoring/                   # Metrics collection
│
├── 🔧 scripts/                       # Utility scripts
│   ├── setup/                        # Setup scripts
│   │   └── validate-environment.py
│   ├── deployment/                   # Deployment scripts
│   │   └── deploy-libraries.sh
│   └── maintenance/                  # Maintenance tasks
│
├── 📝 templates/                     # Reusable templates
│   ├── glue-jobs/                    # Glue job templates
│   │   └── base-etl-job.py
│   ├── etl-pipelines/                # Pipeline templates
│   └── iam-policies/                 # IAM policy templates
│
├── 💡 examples/                      # Sample implementations
│   ├── basic/                        # Basic examples
│   │   └── cross-source-join.py
│   ├── intermediate/                 # Intermediate examples
│   │   └── iceberg-merge.py
│   └── advanced/                     # Advanced examples
│
└── 🧪 tests/                         # Test suite
    ├── unit/                         # Unit tests
    ├── integration/                  # Integration tests
    └── e2e/                          # End-to-end tests
# AWS Glue Native Connectors with Iceberg - Cross-Source Data Integration

This repository implements a production-grade data pipeline that aggregates data from Amazon Redshift, Teradata Vantage, and Google BigQuery into an Amazon S3 data lake using Apache Iceberg. AWS Glue native connectors and transforms are used to join data and perform Iceberg MERGE operations following DEA-C01 best practices.

## 🎯 Features

- **Multi-Source Integration**: Extract from Redshift, Teradata, and BigQuery
- **Iceberg Format**: ACID transactions, schema evolution, time travel
- **Cross-Source Joins**: Combine data from different source systems
- **Modular Code**: Reusable connectors, transforms, and utilities
- **Production-Ready**: Monitoring, logging, error handling, data quality checks
- **DEA-C01 Aligned**: Follows AWS Data Engineer Associate certification best practices
- **100-Step Manual**: Comprehensive guide from novice to expert

## 📚 Documentation

Comprehensive 100-step manual from novice to expert:

- [Complete Manual](docs/manual/README.md)
- [Part 1: Fundamentals (Steps 1-20)](docs/manual/part1-fundamentals.md)
- [Part 2: AWS Setup & Configuration (Steps 21-35)](docs/manual/part2-aws-setup.md)
- [Part 3: Data Sources Setup (Steps 36-50)](docs/manual/part3-data-sources.md)
- [Part 4: Glue Connectors Implementation (Steps 51-65)](docs/manual/part4-glue-connectors.md)
- [Part 5: Iceberg Integration (Steps 66-80)](docs/manual/part5-iceberg.md)
- [Part 6: Advanced Operations & Optimization (Steps 81-95)](docs/manual/part6-advanced.md)
- [Part 7: Production & Monitoring (Steps 96-100)](docs/manual/part7-production.md)

## 🏗️ Architecture

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
                │  (Reusable     │
                │  Modules)      │
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
                │  - ACID        │
                │  - Time Travel │
                │  - MERGE Ops   │
                └────────────────┘
```

## 📁 Project Structure

```
.
├── docs/                          # Documentation
│   └── manual/                    # 100-step manual (novice to expert)
├── src/                           # Source code (reusable modules)
│   ├── connectors/                # Data source connectors
│   ├── transforms/                # Transformation modules
│   ├── utils/                     # Utility modules
│   └── config/                    # Configuration files
├── examples/                      # Example Glue jobs
├── scripts/                       # Utility scripts
│   ├── deployment/                # Deployment scripts
│   ├── setup/                     # Setup scripts
│   └── monitoring/                # Monitoring scripts
├── tests/                         # Test files
├── infrastructure/                # Infrastructure as Code
├── templates/                     # Job templates
├── .gitignore                     # Git ignore file
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## 🚀 Quick Start

### Prerequisites
- AWS Account with Glue, S3, and IAM access
- Python 3.7+
- AWS CLI configured
- Data sources: Redshift, Teradata, or BigQuery (optional for testing)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/iotda-ol/dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake.git
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake
```

2. **Validate your environment**
```bash
python scripts/setup/validate-environment.py
```

3. **Deploy shared libraries to S3**
```bash
./scripts/deployment/deploy-libraries.sh my-glue-scripts-bucket
```

4. **Configure data sources**
   - Update configuration files in `config/` directory
   - Store credentials in AWS Secrets Manager

5. **Follow the comprehensive guide**
   - Start with [docs/COMPREHENSIVE-GUIDE.md](docs/COMPREHENSIVE-GUIDE.md)
   - Complete all 100 steps for full proficiency

## 🔌 Usage Examples

### Basic Cross-Source Join
```python
from src.connectors.connection_factory import ConnectionFactory
from src.transformations.data_transformer import DataTransformer
from src.iceberg.table_manager import IcebergTableManager

# Create connectors
redshift = ConnectionFactory.create_connector('redshift', config, glue_context)
bigquery = ConnectionFactory.create_connector('bigquery', config, glue_context)

# Extract data
sales_df = redshift.read_table('sales', predicates=["date >= '2024-01-01'"])
products_df = bigquery.read_table('products', schema='analytics')

# Transform and join
transformer = DataTransformer()
sales_df = transformer.standardize_dates(sales_df, ['date'])
joined_df = sales_df.join(products_df, 'product_id')

# Load to Iceberg
iceberg = IcebergTableManager(spark)
iceberg.write_data(joined_df, 'iceberg_db', 'sales_products', mode='append')
```

### Incremental MERGE
```python
# Extract changed records
source_df = teradata.read_table('customers', 
    predicates=["updated_at >= CURRENT_DATE - 1"])

# Validate quality
validator = DataQualityValidator()
validator.validate_not_null(source_df, ['customer_id', 'email'])
validator.validate_unique(source_df, ['customer_id'])

# MERGE into Iceberg
iceberg.merge_data(
    source_df=source_df,
    target_database='iceberg_db',
    target_table='customers',
    merge_keys=['customer_id'],
    update_condition="source.updated_at > target.updated_at"
)
```

## 📋 Key Components

### Connectors
- **BaseConnector** - Abstract interface for all connectors
- **RedshiftConnector** - Optimized Redshift reads with pushdown predicates
- **TeradataConnector** - FastExport and parallel session support
- **BigQueryConnector** - Spark BigQuery connector integration
- **ConnectionFactory** - Centralized connector creation

### Transformations
- Column standardization and type casting
- Date/timestamp normalization
- Deduplication strategies
- Data quality filtering
- Audit column injection

### Iceberg Operations
- Table creation and management
- MERGE operations (INSERT/UPDATE/DELETE)
- File compaction and optimization
- Snapshot management and expiration
- Time travel queries

### Validation & Quality
- Null value checks
- Uniqueness validation
- Format validation (regex)
- Value range checks
- Referential integrity

### Monitoring
- Structured logging with CloudWatch
- Job metrics collection
- Performance profiling
- Error tracking and alerting

## 🎓 Learning Path

Follow the **100-step comprehensive guide** for structured learning:

1. **Novice (Steps 1-20)**: Fundamentals and environment setup
2. **Intermediate (Steps 21-40)**: Connectors and basic ETL
3. **Advanced (Steps 41-70)**: Iceberg, joins, and complex patterns
4. **Expert (Steps 71-90)**: Optimization and performance tuning
5. **Production (Steps 91-100)**: Deployment and operations

## 🛡️ Security Best Practices

- ✅ All credentials stored in AWS Secrets Manager
- ✅ Encryption at rest (S3, Glue Catalog)
- ✅ Encryption in transit (SSL/TLS)
- ✅ Least privilege IAM roles
- ✅ VPC isolation for data sources
- ✅ Audit logging enabled

## 🤝 Contributing

Contributions welcome! Please:
1. Follow existing code structure
2. Add comprehensive documentation
3. Include tests for new features
4. Update relevant README files
5. Follow DEA-C01 best practices

## 📄 License

This project is provided as-is for educational and production use.

## 🔗 Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [DEA-C01 Exam Guide](https://aws.amazon.com/certification/certified-data-engineer-associate/)
- [AWS Data Engineering Best Practices](https://aws.amazon.com/big-data/datalakes-and-analytics/)

## 💬 Support

For issues, questions, or contributions:
- 📝 Open an issue on GitHub
- 📖 Consult the [comprehensive guide](docs/COMPREHENSIVE-GUIDE.md)
- 🔍 Review [examples](examples/) for patterns

---

**Built with ❤️ for the Data Engineering Community**

- AWS Account with appropriate permissions
- AWS CLI configured
- Python 3.9+
- Access to data sources (Redshift, Teradata, BigQuery)

### 1. Setup AWS Resources

```bash
chmod +x scripts/setup/setup_resources.sh
./scripts/setup/setup_resources.sh
```

### 2. Configure Pipeline

Edit `src/config/pipeline_config.yaml` with your settings.

### 3. Deploy Glue Jobs

```bash
chmod +x scripts/deployment/deploy_jobs.sh
./scripts/deployment/deploy_jobs.sh dev
```

## 💻 Usage Examples

### Extract from Redshift

```python
from connectors import RedshiftConnector
from transforms import CommonTransforms, IcebergTransforms

redshift = RedshiftConnector(glueContext, "redshift-connection")
df = redshift.extract_table("analytics", "customers").toDF()
df = CommonTransforms.add_audit_columns(df)

iceberg = IcebergTransforms(spark)
iceberg.merge_upsert("iceberg_curated.customers", df, ["customer_id"])
```

### Cross-Source Join

```python
from transforms import CrossSourceJoins

enriched_df = CrossSourceJoins.three_way_join(
    customers_df, orders_df, transactions_df
)
```

## 📊 Monitoring

- CloudWatch metrics automatically published
- Structured logging to CloudWatch Logs
- Data quality monitoring
- Performance tracking

## 🔒 Security

- Credentials in AWS Secrets Manager
- S3 encryption enabled
- IAM least privilege
- VPC isolation
- PII data masking

## 📝 License

MIT License

---

**Built for data engineers following DEA-C01 best practices**
# Multi-Source Data Integration Pipeline with AWS Glue and Apache Iceberg

![AWS](https://img.shields.io/badge/AWS-Glue-orange) ![Terraform](https://img.shields.io/badge/IaC-Terraform-purple) ![Python](https://img.shields.io/badge/Python-3.9+-blue) ![Iceberg](https://img.shields.io/badge/Apache-Iceberg-blue) ![Security](https://img.shields.io/badge/Security-Reviewed-green)

This repository implements a production-ready, multi-source data integration pipeline that aggregates data from **Amazon Redshift**, **Teradata Vantage**, and **Google BigQuery** into an **Amazon S3 data lake** using **Apache Iceberg** table format. The solution uses **AWS Glue native connectors** for optimal performance, applies cross-source transformations and joins, and implements **MERGE operations** for efficient upserts.

This implementation follows **AWS DEA-C01 (AWS Certified Data Engineer - Associate)** best practices and demonstrates enterprise-grade data engineering patterns.

## 🎯 Key Features

- ✅ **Multi-Source Integration**: Seamlessly connects to Redshift, Teradata, and BigQuery
- ✅ **Native Connectors**: Uses AWS Glue native connectors for optimal performance
- ✅ **Cross-Source Joins**: Efficiently joins data from different source systems
- ✅ **Apache Iceberg**: Modern table format with ACID transactions
- ✅ **MERGE Operations**: Efficient upsert patterns for slowly changing dimensions
- ✅ **IAM Least Privilege**: Secure access with minimal permissions
- ✅ **Infrastructure as Code**: Complete Terraform configuration
- ✅ **Monitoring & Alerting**: CloudWatch integration with alarms
- ✅ **Scalability**: Auto-scaling Glue workers and partitioned storage
- ✅ **Cost Optimized**: Job bookmarks and efficient processing

## 📋 Architecture Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Redshift   │    │  Teradata   │    │  BigQuery   │
│ Sales Data  │    │ Customer    │    │  Product    │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                   │
       └──────────────────┴───────────────────┘
                          │
                  ┌───────▼────────┐
                  │  AWS Glue Job  │
                  │  ETL Pipeline  │
                  └───────┬────────┘
                          │
                  ┌───────▼────────┐
                  │   S3 Data Lake │
                  │ Apache Iceberg │
                  └────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- AWS Account with appropriate permissions
- Terraform >= 1.0
- AWS CLI v2 configured
- Python 3.9+ (for local testing)

### Deploy in 5 Minutes

```bash
# 1. Clone repository
git clone <repository-url>
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake

# 2. Configure variables
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your configuration

# 3. Deploy infrastructure
terraform init
terraform plan
terraform apply

# 4. Run the ETL job
aws glue start-job-run --job-name multi-source-pipeline-multi-source-etl
```

See [DEPLOYMENT.md](docs/DEPLOYMENT.md) for detailed instructions.

## 📁 Repository Structure

```
.
├── terraform/                  # Infrastructure as Code
│   ├── main.tf                # Main Terraform configuration
│   ├── variables.tf           # Variable definitions
│   ├── outputs.tf             # Output values
│   └── terraform.tfvars.example  # Example configuration
├── scripts/                   # ETL Scripts
│   └── multi_source_etl.py   # Main Glue ETL script
├── docs/                      # Documentation
│   ├── ARCHITECTURE.md        # Architecture deep dive
│   ├── DEA-C01-BEST-PRACTICES.md  # Best practices guide
│   └── DEPLOYMENT.md          # Deployment instructions
└── README.md                  # This file
```

## 🔧 Configuration

### Data Sources

The pipeline supports three data sources (all optional):

| Source | Connector Type | Configuration Required |
|--------|---------------|------------------------|
| Amazon Redshift | Native Glue Connector | JDBC URL, credentials, table name |
| Teradata Vantage | JDBC Connector | JDBC URL, credentials, table name |
| Google BigQuery | Marketplace Connector | JDBC driver, service account, project/dataset/table |

### Example Configuration

```hcl
# Enable/disable sources
enable_redshift_connection = true
enable_teradata_connection = true
enable_bigquery_connection = true

# Configure Glue job resources
worker_type       = "G.2X"     # 8 vCPU, 32GB RAM per worker
number_of_workers = 10         # Total workers
glue_version      = "4.0"      # Latest Glue version
```

## 🏗️ Infrastructure Components

### AWS Resources Created

- **S3 Buckets** (2)
  - Data lake bucket with versioning and encryption
  - Glue scripts bucket for ETL code
  
- **IAM Resources**
  - Glue job execution role
  - Least privilege policies for S3, CloudWatch, Secrets Manager
  
- **AWS Glue**
  - ETL job with PySpark
  - Database connections (Redshift, Teradata, BigQuery)
  - Data Catalog database for Iceberg tables
  
- **CloudWatch**
  - Log group for job logs
  - Alarms for failures and duration

## 📊 ETL Pipeline Details

### Data Flow

1. **Extract**: Read from multiple sources using native connectors
2. **Transform**: Apply source-side transformations and data quality checks
3. **Join**: Perform cross-source joins (Sales ⟕ Customer ⟕ Product)
4. **Load**: Write to Iceberg table using MERGE operation
5. **Validate**: Post-load data quality validation

### Key Features

- **Incremental Processing**: Job bookmarks track processed data
- **Data Quality**: Inline validation with quality flags
- **Partitioning**: By year/month for efficient queries
- **ACID Compliance**: Iceberg ensures transactional consistency
- **Schema Evolution**: Add columns without data rewrites

## 🔒 Security

### Implementation

- ✅ **IAM Least Privilege**: Minimal permissions for each component
- ✅ **Encryption at Rest**: S3 server-side encryption (AES-256)
- ✅ **Encryption in Transit**: TLS 1.2+ for all connections
- ✅ **Secrets Management**: AWS Secrets Manager for credentials
- ✅ **VPC Support**: Optional VPC isolation for connections
- ✅ **No Public Access**: All S3 buckets block public access

## 📈 Monitoring

### CloudWatch Integration

- **Continuous Logging**: Real-time log streaming
- **Job Metrics**: Success/failure rates, duration, records processed
- **Alarms**: Automated alerts for failures and anomalies
- **Job Insights**: Advanced troubleshooting information

### Key Metrics Tracked

- Job execution status
- Data quality percentage
- Processing duration
- Worker utilization
- Error rates

## 💰 Cost Optimization

- **Job Bookmarks**: Process only new/changed data
- **Right-Sized Workers**: Optimized DPU allocation
- **Compression**: Snappy compression reduces storage
- **Partitioning**: Reduces query scan costs
- **Lifecycle Policies**: Move old data to cheaper storage tiers

## 🎓 DEA-C01 Best Practices

This implementation demonstrates:

1. **Multi-source data ingestion** with native connectors
2. **PySpark transformations** for data processing
3. **Cross-source joins** with optimization
4. **Modern table formats** (Apache Iceberg)
5. **MERGE operations** for upserts
6. **IAM least privilege** security model
7. **Encryption** at rest and in transit
8. **Monitoring and alerting** with CloudWatch
9. **Cost optimization** techniques
10. **Infrastructure as Code** with Terraform

See [DEA-C01-BEST-PRACTICES.md](docs/DEA-C01-BEST-PRACTICES.md) for detailed explanations.

## 📚 Documentation

- [**ARCHITECTURE.md**](docs/ARCHITECTURE.md) - Detailed architecture and design decisions
- [**DEA-C01-BEST-PRACTICES.md**](docs/DEA-C01-BEST-PRACTICES.md) - AWS certification best practices
- [**DEPLOYMENT.md**](docs/DEPLOYMENT.md) - Step-by-step deployment guide
- [**SECURITY.md**](SECURITY.md) - Security policy and best practices

## 🔍 Testing

### Run ETL Job Manually

```bash
# Start job
aws glue start-job-run \
  --job-name multi-source-pipeline-multi-source-etl

# Check status
aws glue get-job-run \
  --job-name multi-source-pipeline-multi-source-etl \
  --run-id <run-id>

# View logs
aws logs tail /aws-glue/jobs/multi-source-pipeline-multi-source-etl --follow
```

### Query Iceberg Table

```bash
# Using Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM iceberg_db.integrated_data LIMIT 10" \
  --result-configuration "OutputLocation=s3://<bucket>/athena-results/"
```

## 🛠️ Maintenance

### Regular Tasks

- **Weekly**: Review CloudWatch metrics and alarms
- **Monthly**: Optimize Iceberg tables (compact files)
- **Quarterly**: Update dependencies and review IAM policies

### Update Pipeline

```bash
cd terraform
terraform plan
terraform apply
```

## 🗑️ Clean Up

To remove all resources:

```bash
cd terraform
terraform destroy
```

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Update documentation
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- AWS Glue team for excellent documentation
- Apache Iceberg community
- Terraform AWS provider maintainers

## 📞 Support

- 📖 [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- 🎯 [Apache Iceberg Docs](https://iceberg.apache.org/docs/latest/)
- 💬 [GitHub Issues](../../issues)
- 🌐 [AWS Support](https://aws.amazon.com/support/)

## 🏆 Use Cases

This pipeline is ideal for:

- **Data Warehousing**: Consolidate data from multiple sources
- **Analytics Platforms**: Build unified analytics layer
- **Customer 360**: Merge customer data from various systems
- **Compliance**: Centralized data with audit trail (Iceberg snapshots)
- **Machine Learning**: Prepare datasets from multiple sources

---

**Built with ❤️ for data engineers preparing for AWS DEA-C01 certification**
