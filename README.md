# Data Pipeline: Multi-Source Integration with Iceberg

This repository implements a low-operational-overhead data pipeline that aggregates data from Amazon Redshift, Teradata Vantage, and Google BigQuery into an Amazon S3 data lake using Apache Iceberg. AWS Glue native connectors and transforms are used to join data and perform Iceberg MERGE operations following DEA-C01 best practices.

## 📚 Documentation

- **[Complete 100-Step Guide](docs/COMPLETE_GUIDE.md)** - Comprehensive instruction manual from novice to expert level
- [Architecture Overview](docs/architecture.md) - System architecture and design
- [API Reference](docs/api_reference.md) - Code API documentation

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Terraform 1.0+
- AWS CLI configured
- AWS account with appropriate permissions

### Installation

1. Clone the repository:
```bash
git clone https://github.com/iotda-ol/dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake.git
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake
```

2. Set up Python environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure your settings:
```bash
cp config/example.database_connections.yaml config/database_connections.yaml
# Edit config/database_connections.yaml with your database details
```

4. Deploy infrastructure:
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

## 📁 Project Structure

```
├── docs/                       # Documentation
│   └── COMPLETE_GUIDE.md      # 100-step instruction manual
├── src/                        # Python source code (modular & reusable)
│   ├── connectors/            # Database connectors
│   ├── transforms/            # Data transformations
│   ├── iceberg/               # Iceberg operations
│   ├── utils/                 # Utility functions
│   └── main_etl_job.py        # Main Glue ETL job
├── terraform/                  # Infrastructure as Code
│   ├── modules/               # Reusable Terraform modules
│   │   ├── networking/        # VPC and network setup
│   │   ├── iam/              # IAM roles and policies
│   │   ├── s3/               # S3 bucket configurations
│   │   ├── glue/             # Glue resources
│   │   └── monitoring/       # CloudWatch and SNS
│   ├── environments/          # Environment-specific configs
│   └── main.tf               # Main Terraform config
├── scripts/                   # Automation scripts
├── config/                    # Configuration files
├── examples/                  # Example configurations
└── requirements.txt          # Python dependencies
```

## 🎯 Key Features

### Modular Architecture
- **Reusable Components**: All code organized into reusable modules
- **Clean Separation**: Connectors, transforms, and utilities are independent
- **Easy Extension**: Add new connectors or transforms by extending base classes

### Multi-Source Integration
- **Amazon Redshift**: Native Glue connector with pushdown predicates
- **Teradata Vantage**: JDBC with partitioned reads
- **Google BigQuery**: Direct integration with authentication

### Apache Iceberg Support
- **ACID Transactions**: Full ACID compliance
- **Schema Evolution**: Add, drop, rename columns without downtime
- **Time Travel**: Query historical data snapshots
- **MERGE Operations**: Efficient upserts with conditional logic

### Production-Ready Features
- **Error Handling**: Comprehensive error handling with retry logic
- **Logging**: Structured logging with CloudWatch integration
- **Monitoring**: CloudWatch alarms and SNS notifications
- **Security**: Encryption at rest and in transit, IAM policies
- **Scalability**: Auto-scaling Glue jobs, partitioned reads

## 📖 Usage Examples

### Test Database Connections
```bash
python scripts/test_connections.py
```

### Create Glue Jobs
```bash
python scripts/create_glue_jobs.py --job-type all
```

### Upload Scripts to S3
```bash
python scripts/upload_scripts.py --bucket your-scripts-bucket
```

## 🔐 Security

- All credentials stored in AWS Secrets Manager
- Encryption at rest (S3, Glue catalog)
- Encryption in transit (TLS)
- VPC endpoints for AWS services
- Least privilege IAM policies

## 🆘 Support

For issues or questions:
1. Check the [Complete Guide](docs/COMPLETE_GUIDE.md)
2. Review the configuration examples
3. Open an issue in the GitHub repository

## 🔗 Additional Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [AWS DEA-C01 Certification](https://aws.amazon.com/certification/certified-data-engineer-associate/)
