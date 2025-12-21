# Shared Libraries

Reusable libraries for logging, validation, and monitoring.

## Structure

### `/validation`
Data validation and quality check modules:
- `data_quality.py` - Comprehensive data quality validator

### `/logging`
Logging utilities:
- `glue_logger.py` - Enhanced logger with CloudWatch integration

### `/monitoring`
Monitoring and metrics collection (to be added)

## Installation

These libraries should be packaged and uploaded to S3 for use in Glue jobs:

```bash
# Package libraries
cd lib
zip -r ../libraries.zip .

# Upload to S3
aws s3 cp libraries.zip s3://my-glue-scripts/libs/
```

## Usage in Glue Jobs

```python
import sys
sys.path.append('s3://my-glue-scripts/libs/')

from lib.validation.data_quality import DataQualityValidator
from lib.logging.glue_logger import GlueJobLogger

# Use in your job
validator = DataQualityValidator()
logger = GlueJobLogger("my-job-name")
```

## Development

When adding new libraries:
1. Keep modules focused and reusable
2. Add comprehensive docstrings
3. Include unit tests
4. Update this README
5. Re-package and upload to S3

## Dependencies

Libraries may have external dependencies. Document them here:
- PySpark (provided by Glue)
- boto3 (provided by Glue)
- Standard Python libraries only
