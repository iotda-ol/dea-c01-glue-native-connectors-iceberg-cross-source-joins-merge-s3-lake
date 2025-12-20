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

Each example can be customized by:
1. Modifying the configuration in `src/config/pipeline_config.yaml`
2. Adjusting job parameters in the `--arguments` flag
3. Extending the reusable modules in `src/`

## Best Practices

- Always use configuration files for environment-specific settings
- Leverage reusable modules for consistency
- Implement proper error handling and logging
- Monitor job performance and optimize as needed
