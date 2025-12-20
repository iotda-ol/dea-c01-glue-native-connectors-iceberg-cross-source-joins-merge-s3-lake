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
