#!/bin/bash
# Deploy shared libraries to S3
# Usage: ./deploy-libraries.sh <s3-bucket>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <s3-bucket>"
    echo "Example: $0 my-glue-scripts"
    exit 1
fi

S3_BUCKET=$1
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "Deploying libraries to S3..."
echo "Project root: $PROJECT_ROOT"
echo "Target bucket: s3://$S3_BUCKET"

# Create temporary directory for packaging
TEMP_DIR=$(mktemp -d)
echo "Using temp directory: $TEMP_DIR"

# Copy source code
echo "Copying source code..."
cp -r "$PROJECT_ROOT/src" "$TEMP_DIR/"
cp -r "$PROJECT_ROOT/lib" "$TEMP_DIR/"

# Create zip archive
echo "Creating archive..."
cd "$TEMP_DIR"
zip -r libraries.zip src/ lib/

# Upload to S3
echo "Uploading to S3..."
aws s3 cp libraries.zip "s3://$S3_BUCKET/libs/libraries.zip"

# Upload individual configs
echo "Uploading configurations..."
aws s3 sync "$PROJECT_ROOT/config" "s3://$S3_BUCKET/config/" --exclude "*.md"

# Upload templates
echo "Uploading templates..."
aws s3 sync "$PROJECT_ROOT/templates" "s3://$S3_BUCKET/templates/" --exclude "*.md"

# Cleanup
echo "Cleaning up..."
rm -rf "$TEMP_DIR"

echo "✓ Deployment complete!"
echo ""
echo "To use in Glue jobs, add this to your script:"
echo "  import sys"
echo "  sys.path.append('s3://$S3_BUCKET/libs/libraries.zip')"
