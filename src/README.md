# Source Code

This directory contains all reusable Python modules for the Glue data pipeline.

## Modules

### Connectors (`connectors/`)
Reusable connector classes for different data sources:
- `RedshiftConnector` - Amazon Redshift
- `TeradataConnector` - Teradata Vantage
- `BigQueryConnector` - Google BigQuery

### Transforms (`transforms/`)
Data transformation modules:
- `CommonTransforms` - Standard transformations (audit columns, deduplication, etc.)
- `CrossSourceJoins` - Multi-source join operations
- `IcebergTransforms` - Iceberg-specific operations (MERGE, schema evolution, etc.)

### Utils (`utils/`)
Utility modules:
- `GlueLogger` - Standardized logging
- `MetricsCollector` - CloudWatch metrics collection
- `ConfigManager` - Configuration management
- `error_handling` - Error handling and retry decorators

### Config (`config/`)
Configuration files:
- `pipeline_config.yaml` - Main pipeline configuration

## Usage

Import modules in your Glue jobs:

```python
from connectors import RedshiftConnector, TeradataConnector
from transforms import CommonTransforms, IcebergTransforms
from utils import GlueLogger, MetricsCollector, ConfigManager
```

## Development

When adding new modules:
1. Follow existing patterns
2. Add docstrings
3. Export in `__init__.py`
4. Create examples in `examples/`
5. Update documentation
