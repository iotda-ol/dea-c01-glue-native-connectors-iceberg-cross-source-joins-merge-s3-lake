# Source Code Directory

This directory contains all the main source code for the data pipeline.

## Structure

### `/connectors`
Data source connector implementations:
- `base_connector.py` - Abstract base class for all connectors
- `redshift_connector.py` - Amazon Redshift connector
- `teradata_connector.py` - Teradata Vantage connector
- `bigquery_connector.py` - Google BigQuery connector
- `connection_factory.py` - Factory pattern for creating connectors

### `/transformations`
Data transformation utilities:
- `data_transformer.py` - Reusable transformation functions

### `/iceberg`
Apache Iceberg table management:
- `table_manager.py` - Iceberg table operations (create, merge, compact, etc.)

### `/utils`
Utility modules and helper functions (to be added as needed)

## Usage

All modules are designed to be imported and reused across multiple Glue jobs:

```python
from src.connectors.connection_factory import ConnectionFactory
from src.transformations.data_transformer import DataTransformer
from src.iceberg.table_manager import IcebergTableManager
```

## Best Practices

1. Always use the factory pattern for creating connectors
2. Reuse transformation functions instead of duplicating logic
3. Follow the established patterns when adding new modules
4. Keep modules focused on single responsibility
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
