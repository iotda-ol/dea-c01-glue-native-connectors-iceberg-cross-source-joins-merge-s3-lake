# Examples

Sample implementations demonstrating different use cases and patterns.

## Structure

### `/basic`
Simple examples for getting started:
- `cross-source-join.py` - Basic join between Redshift and BigQuery

### `/intermediate`
More complex scenarios:
- `iceberg-merge.py` - Incremental MERGE operations with validation

### `/advanced`
Production-ready patterns (to be added):
- Complex multi-source joins
- SCD Type 2 implementation
- Advanced optimization patterns

## Running Examples

### Prerequisites
1. Configure AWS credentials
2. Set up data source connections in Glue
3. Create target Iceberg tables
4. Upload shared libraries to S3

### Execution

**Using Glue Console:**
1. Create new Glue job
2. Copy example script
3. Configure job parameters
4. Run job

**Using AWS CLI:**
```bash
aws glue start-job-run \
  --job-name my-example-job \
  --arguments '{"--database":"iceberg_datalake"}'
# Examples README

This directory contains example Glue jobs demonstrating the usage of reusable modules.

## Structure

```
examples/
├── redshift/           # Redshift extraction examples
├── teradata/           # Teradata extraction examples
├── bigquery/           # BigQuery extraction examples
└── iceberg/            # Iceberg operations examples
```

## Running Examples

### Extract from Redshift to Iceberg

```bash
aws glue start-job-run \
    --job-name extract-redshift-to-iceberg-dev \
    --arguments '{
        "--table_name": "customers",
        "--config_path": "s3://your-bucket/config/pipeline_config.yaml"
    }'
```

### Cross-Source Join

```bash
aws glue start-job-run \
    --job-name cross-source-join-dev \
    --arguments '{
        "--config_path": "s3://your-bucket/config/pipeline_config.yaml"
    }'
```

## Customization

Examples are templates - customize for your needs:
1. Update connection configurations
2. Modify source/target table names
3. Adjust transformation logic
4. Add custom validation rules
5. Configure job parameters

## Best Practices Demonstrated

- Modular code with reusable components
- Comprehensive error handling
- Data quality validation
- Performance optimization
- Structured logging
- Configuration management
Each example can be customized by:
1. Modifying the configuration in `src/config/pipeline_config.yaml`
2. Adjusting job parameters in the `--arguments` flag
3. Extending the reusable modules in `src/`

## Best Practices

- Always use configuration files for environment-specific settings
- Leverage reusable modules for consistency
- Implement proper error handling and logging
- Monitor job performance and optimize as needed
