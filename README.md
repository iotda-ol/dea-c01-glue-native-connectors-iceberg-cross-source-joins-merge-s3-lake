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
