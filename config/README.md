# Configuration Files

This directory contains all configuration files for the data pipeline.

## Structure

### `/redshift`
Amazon Redshift connection configurations
- `connection-config.json` - Redshift JDBC connection settings

### `/teradata`
Teradata Vantage connection configurations
- `connection-config.json` - Teradata JDBC connection settings with FastExport

### `/bigquery`
Google BigQuery connection configurations
- `connection-config.json` - BigQuery Spark connector settings

### `/iceberg`
Apache Iceberg table configurations
- `table-config.json` - Iceberg table properties and maintenance settings

### `/glue`
AWS Glue job configurations
- `job-config.json` - Default Glue job settings and Spark configuration

## Configuration Management

### Environment-Specific Configs
Create separate config files for different environments:
- `config-dev.json`
- `config-staging.json`
- `config-prod.json`

### Secret Management
Sensitive values use placeholder syntax:
- `${SECRET:path/to/secret}` - Retrieved from AWS Secrets Manager at runtime

### Usage in Jobs
```python
import json

# Load configuration
with open('config/redshift/connection-config.json') as f:
    config = json.load(f)

# Create connector with config
connector = RedshiftConnector(config, glue_context)
```

## Best Practices

1. Never commit sensitive credentials to version control
2. Use AWS Secrets Manager for all passwords and keys
3. Keep environment-specific configs separate
4. Document all configuration parameters
5. Validate configs before deploying to production
