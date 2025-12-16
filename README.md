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
