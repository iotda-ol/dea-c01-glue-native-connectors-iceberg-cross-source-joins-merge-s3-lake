# Quick Reference Guide

This guide provides quick access to the most commonly needed information.

## 🚀 Getting Started in 5 Minutes

### Step 1: Clone Repository
```bash
git clone https://github.com/iotda-ol/dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake.git
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake
```

### Step 2: Validate Environment
```bash
python scripts/setup/validate-environment.py
```

### Step 3: Configure AWS
```bash
aws configure --profile glue-iceberg
```

### Step 4: Review Structure
```bash
tree -L 2  # Or explore manually
```

### Step 5: Read Documentation
- Start: `docs/COMPREHENSIVE-GUIDE.md`
- Structure: `docs/PROJECT-STRUCTURE.md`
- Examples: `examples/README.md`

## 📁 Key Files Quick Access

| What | Where |
|------|-------|
| **100-Step Manual** | `docs/COMPREHENSIVE-GUIDE.md` |
| **Main README** | `README.md` |
| **Project Structure** | `docs/PROJECT-STRUCTURE.md` |
| **Contributing Guide** | `CONTRIBUTING.md` |
| **Connector Code** | `src/connectors/` |
| **Iceberg Manager** | `src/iceberg/table_manager.py` |
| **Data Transformer** | `src/transformations/data_transformer.py` |
| **Basic Example** | `examples/basic/cross-source-join.py` |
| **MERGE Example** | `examples/intermediate/iceberg-merge.py` |
| **Config Files** | `config/` |

## 🔧 Common Tasks

### Deploy Libraries to S3
```bash
./scripts/deployment/deploy-libraries.sh my-glue-scripts-bucket
```

### Create a New Connector
1. Inherit from `BaseConnector` in `src/connectors/base_connector.py`
2. Implement required methods
3. Register in `ConnectionFactory`
4. Add config to `config/`
5. Create example in `examples/`

### Run Basic Example
```python
# Update configs in examples/basic/cross-source-join.py
# Deploy to Glue:
aws glue create-job --name my-job --role GlueServiceRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://bucket/cross-source-join.py"}'
  
aws glue start-job-run --job-name my-job
```

### Configure Data Source
1. Choose source: Redshift, Teradata, or BigQuery
2. Edit `config/{source}/connection-config.json`
3. Store credentials in Secrets Manager:
```bash
aws secretsmanager create-secret \
  --name prod/redshift/credentials \
  --secret-string '{"username":"admin","password":"xxxxx"}'
```

## 📚 Documentation Map

### By Experience Level

**Novice** (Never used Glue/Iceberg):
1. `README.md` - Overview
2. `docs/COMPREHENSIVE-GUIDE.md` Steps 1-20
3. `examples/basic/` - Simple examples

**Intermediate** (Some Glue experience):
1. `docs/COMPREHENSIVE-GUIDE.md` Steps 21-50
2. `src/README.md` - Code structure
3. `config/README.md` - Configuration
4. `examples/intermediate/` - MERGE operations

**Advanced** (Building production pipelines):
1. `docs/COMPREHENSIVE-GUIDE.md` Steps 51-80
2. `src/connectors/` - Connector implementations
3. `templates/glue-jobs/` - Job templates
4. `lib/` - Shared libraries

**Expert** (Optimization and scale):
1. `docs/COMPREHENSIVE-GUIDE.md` Steps 81-100
2. `CONTRIBUTING.md` - Extend the framework
3. All source code for customization

### By Task

**Setting Up Environment**:
- `docs/COMPREHENSIVE-GUIDE.md` Steps 1-20
- `scripts/setup/validate-environment.py`

**Connecting Data Sources**:
- `config/{source}/connection-config.json`
- `src/connectors/{source}_connector.py`
- `docs/COMPREHENSIVE-GUIDE.md` Steps 31-40

**Building ETL Jobs**:
- `templates/glue-jobs/base-etl-job.py`
- `examples/basic/cross-source-join.py`
- `docs/COMPREHENSIVE-GUIDE.md` Steps 51-60

**Working with Iceberg**:
- `src/iceberg/table_manager.py`
- `config/iceberg/table-config.json`
- `docs/COMPREHENSIVE-GUIDE.md` Steps 41-50

**MERGE Operations**:
- `examples/intermediate/iceberg-merge.py`
- `docs/COMPREHENSIVE-GUIDE.md` Steps 71-80

**Deploying to Production**:
- `scripts/deployment/deploy-libraries.sh`
- `docs/COMPREHENSIVE-GUIDE.md` Steps 91-100

## 🎯 Common Code Snippets

### Import Modules
```python
import sys
sys.path.append('s3://my-glue-scripts/libs/libraries.zip')

from src.connectors.connection_factory import ConnectionFactory
from src.transformations.data_transformer import DataTransformer
from src.iceberg.table_manager import IcebergTableManager
from lib.validation.data_quality import DataQualityValidator
from lib.logging.glue_logger import GlueJobLogger
```

### Create Connector
```python
config = {
    'connection_name': 'redshift-prod',
    'database': 'analytics',
    'temp_dir': 's3://temp-bucket/'
}
connector = ConnectionFactory.create_connector('redshift', config, glue_context)
df = connector.read_table('sales', predicates=["date >= '2024-01-01'"])
```

### Transform Data
```python
transformer = DataTransformer()
df = transformer.standardize_column_names(df, "snake_case")
df = transformer.standardize_dates(df, ['created_at', 'updated_at'])
df = transformer.deduplicate(df, ['id'], 'updated_at', 'last')
df = transformer.add_audit_columns(df, 'etl_timestamp', 'source_system')
```

### Validate Quality
```python
validator = DataQualityValidator()
validator.validate_not_null(df, ['id', 'email'])
validator.validate_unique(df, ['id'])
validator.validate_format(df, 'email', r'^[\w\.-]+@[\w\.-]+\.\w+$')

report = validator.get_validation_report()
if report['failed'] > 0:
    raise ValueError("Validation failed")
```

### MERGE to Iceberg
```python
iceberg = IcebergTableManager(spark)
iceberg.merge_data(
    source_df=source_df,
    target_database='iceberg_db',
    target_table='customers',
    merge_keys=['customer_id'],
    update_condition="source.updated_at > target.updated_at"
)
```

### Enhanced Logging
```python
logger = GlueJobLogger("my-job")
logger.log_job_start(source='redshift', target='iceberg')
logger.log_dataframe_stats(df, "source_data")
logger.log_metric("rows_processed", df.count())
logger.log_job_end(status="success")
```

## 🆘 Troubleshooting

### "Module not found" Error
- Ensure libraries are uploaded to S3
- Check `sys.path.append()` in script
- Verify S3 path is correct

### Connection Failures
- Validate credentials in Secrets Manager
- Check security group rules
- Verify VPC configuration
- Test connection using `validate_connection()`

### Performance Issues
- Review Spark configuration in `config/glue/job-config.json`
- Enable pushdown predicates
- Check partition strategy
- Review file sizes (aim for 128-512MB)

### MERGE Failures
- Check merge keys exist in both tables
- Validate source data quality first
- Ensure target table exists
- Review Iceberg snapshot status

## 📞 Support

- **Documentation**: Start with `docs/COMPREHENSIVE-GUIDE.md`
- **Examples**: Check `examples/` for working code
- **Issues**: Open GitHub issue with details
- **Code**: Review source in `src/` for implementation

## ✅ Pre-Deployment Checklist

- [ ] Environment validated
- [ ] Credentials in Secrets Manager
- [ ] Libraries deployed to S3
- [ ] Configurations updated
- [ ] IAM roles created
- [ ] S3 buckets created
- [ ] Network/VPC configured
- [ ] Examples tested
- [ ] Monitoring configured
- [ ] Documentation reviewed

---

**Start with the basics, progress at your own pace, and reference this guide whenever needed!**
