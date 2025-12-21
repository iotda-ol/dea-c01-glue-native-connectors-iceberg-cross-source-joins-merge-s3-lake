#!/bin/bash
# Deployment script for Glue jobs
# Usage: ./deploy.sh <environment>

set -e

ENVIRONMENT=${1:-dev}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "=== Deploying Glue Jobs to $ENVIRONMENT ==="

# Load configuration
source "$PROJECT_ROOT/scripts/setup/load_config.sh" "$ENVIRONMENT"

# Upload Python modules to S3
echo "Uploading Python modules..."
aws s3 sync "$PROJECT_ROOT/src/" "s3://$S3_BUCKET/scripts/modules/" \
    --exclude "*.pyc" \
    --exclude "__pycache__/*" \
    --delete

# Upload configuration
echo "Uploading configuration..."
aws s3 cp "$PROJECT_ROOT/src/config/pipeline_config.yaml" \
    "s3://$S3_BUCKET/config/pipeline_config.yaml"

# Upload example jobs
echo "Uploading job scripts..."
aws s3 sync "$PROJECT_ROOT/examples/" "s3://$S3_BUCKET/scripts/jobs/" \
    --exclude "*.md" \
    --delete

# Create/Update Glue jobs
echo "Creating/Updating Glue jobs..."

# Extract to Iceberg job
aws glue create-job \
    --job-name "extract-redshift-to-iceberg-$ENVIRONMENT" \
    --role "$GLUE_ROLE" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://'"$S3_BUCKET"'/scripts/jobs/redshift/extract_to_iceberg.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--job-language": "python",
        "--config_path": "s3://'"$S3_BUCKET"'/config/pipeline_config.yaml",
        "--additional-python-modules": "pyyaml==6.0",
        "--extra-py-files": "s3://'"$S3_BUCKET"'/scripts/modules.zip",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--TempDir": "s3://'"$S3_BUCKET"'/temp/"
    }' \
    --max-retries 1 \
    --timeout 120 \
    --glue-version "4.0" \
    --number-of-workers 10 \
    --worker-type "G.1X" \
    2>/dev/null || aws glue update-job \
    --job-name "extract-redshift-to-iceberg-$ENVIRONMENT" \
    --job-update '{
        "Role": "'"$GLUE_ROLE"'",
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": "s3://'"$S3_BUCKET"'/scripts/jobs/redshift/extract_to_iceberg.py",
            "PythonVersion": "3"
        },
        "DefaultArguments": {
            "--config_path": "s3://'"$S3_BUCKET"'/config/pipeline_config.yaml",
            "--extra-py-files": "s3://'"$S3_BUCKET"'/scripts/modules.zip"
        },
        "MaxRetries": 1,
        "Timeout": 120,
        "GlueVersion": "4.0",
        "NumberOfWorkers": 10,
        "WorkerType": "G.1X"
    }'

echo "Deployment completed successfully!"
