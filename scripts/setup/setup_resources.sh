#!/bin/bash
# Setup script for initial AWS resources
# Usage: ./setup_resources.sh

set -e

echo "=== Setting up AWS Resources for Glue Pipeline ==="

# Variables (customize these)
export AWS_REGION=${AWS_REGION:-us-east-1}
export S3_BUCKET=${S3_BUCKET:-my-iceberg-data-lake-$(date +%s)}
export GLUE_ROLE=${GLUE_ROLE:-GlueIcebergRole}

echo "Region: $AWS_REGION"
echo "S3 Bucket: $S3_BUCKET"
echo "Glue Role: $GLUE_ROLE"

# Create S3 bucket
echo "Creating S3 bucket..."
aws s3api create-bucket \
    --bucket $S3_BUCKET \
    --region $AWS_REGION \
    --create-bucket-configuration LocationConstraint=$AWS_REGION \
    2>/dev/null || echo "Bucket already exists"

# Create folder structure
echo "Creating S3 folder structure..."
for folder in raw processed curated temp scripts config metadata; do
    aws s3api put-object --bucket $S3_BUCKET --key $folder/ || true
done

# Enable encryption
echo "Enabling S3 encryption..."
aws s3api put-bucket-encryption \
    --bucket $S3_BUCKET \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            },
            "BucketKeyEnabled": true
        }]
    }'

# Create IAM role (if doesn't exist)
echo "Creating IAM role..."
cat > /tmp/glue-trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
EOF

aws iam create-role \
    --role-name $GLUE_ROLE \
    --assume-role-policy-document file:///tmp/glue-trust-policy.json \
    2>/dev/null || echo "Role already exists"

# Attach policies
echo "Attaching IAM policies..."
aws iam attach-role-policy \
    --role-name $GLUE_ROLE \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole \
    2>/dev/null || true

# Create Glue databases
echo "Creating Glue databases..."
for db in iceberg_raw iceberg_processed iceberg_curated; do
    aws glue create-database \
        --database-input "{
            \"Name\": \"$db\",
            \"Description\": \"Database for $db tables\"
        }" 2>/dev/null || echo "Database $db already exists"
done

echo ""
echo "=== Setup Complete! ==="
echo "S3 Bucket: s3://$S3_BUCKET"
echo "IAM Role: $GLUE_ROLE"
echo ""
echo "Next steps:"
echo "1. Configure your data source connections"
echo "2. Update src/config/pipeline_config.yaml with your settings"
echo "3. Run ./scripts/deployment/deploy_jobs.sh to deploy Glue jobs"
