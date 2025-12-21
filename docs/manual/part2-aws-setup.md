# Part 2: AWS Setup & Configuration (Steps 21-35)

## Setting Up Your AWS Environment

### Step 21: Create AWS Account
**Objective**: Set up your AWS account if you don't have one

**Process**:
1. Visit https://aws.amazon.com/
2. Click "Create an AWS Account"
3. Provide email address and account name
4. Enter payment information (credit card required)
5. Verify identity (phone verification)
6. Select support plan (Basic is free)

**Post-Creation**:
```bash
# Set up billing alerts
# Navigate to AWS Console > Billing > Preferences
# Enable "Receive Free Tier Usage Alerts"
# Enable "Receive Billing Alerts"
```

### Step 22: Install AWS CLI
**Objective**: Install command-line tools

**For Linux/Mac**:
```bash
# Download and install
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Verify installation
aws --version
# Expected output: aws-cli/2.x.x Python/3.x.x...
```

**For Windows**:
```powershell
# Download installer from:
# https://awscli.amazonaws.com/AWSCLIV2.msi
# Run the installer

# Verify in PowerShell
aws --version
```

**For macOS with Homebrew**:
```bash
brew install awscli
aws --version
```

### Step 23: Configure AWS CLI Credentials
**Objective**: Set up authentication

**Create IAM User**:
1. Log into AWS Console
2. Navigate to IAM > Users > Add User
3. Username: glue-developer
4. Access type: Programmatic access
5. Attach policies: AdministratorAccess (or specific Glue policies)
6. Save Access Key ID and Secret Access Key

**Configure CLI**:
```bash
# Configure credentials
aws configure

# Interactive prompts:
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: us-east-1
Default output format [None]: json

# Test configuration
aws sts get-caller-identity
# Should return your account ID and user ARN
```

**Alternative: Named Profiles**:
```bash
# Configure multiple profiles
aws configure --profile dev
aws configure --profile prod

# Use specific profile
aws s3 ls --profile dev
export AWS_PROFILE=dev  # Set default for session
```

### Step 24: Create S3 Bucket for Data Lake
**Objective**: Set up storage for your data lake

```bash
# Set variables
export BUCKET_NAME="iceberg-data-lake-$(date +%s)"
export AWS_REGION="us-east-1"

# Create bucket
aws s3api create-bucket \
  --bucket $BUCKET_NAME \
  --region $AWS_REGION

# Note: For regions other than us-east-1, add:
# --create-bucket-configuration LocationConstraint=$AWS_REGION

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket $BUCKET_NAME \
  --versioning-configuration Status=Enabled

# Block public access (security best practice)
aws s3api put-public-access-block \
  --bucket $BUCKET_NAME \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Create folder structure
aws s3api put-object --bucket $BUCKET_NAME --key raw/
aws s3api put-object --bucket $BUCKET_NAME --key raw/redshift/
aws s3api put-object --bucket $BUCKET_NAME --key raw/teradata/
aws s3api put-object --bucket $BUCKET_NAME --key raw/bigquery/
aws s3api put-object --bucket $BUCKET_NAME --key processed/
aws s3api put-object --bucket $BUCKET_NAME --key curated/
aws s3api put-object --bucket $BUCKET_NAME --key curated/iceberg/
aws s3api put-object --bucket $BUCKET_NAME --key temp/
aws s3api put-object --bucket $BUCKET_NAME --key scripts/
aws s3api put-object --bucket $BUCKET_NAME --key metadata/

# Verify structure
aws s3 ls s3://$BUCKET_NAME/ --recursive
```

### Step 25: Enable S3 Bucket Encryption
**Objective**: Secure data at rest

```bash
# Enable default encryption (SSE-S3)
aws s3api put-bucket-encryption \
  --bucket $BUCKET_NAME \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      },
      "BucketKeyEnabled": true
    }]
  }'

# Alternative: Use KMS for more control
# First, create KMS key
KEY_ID=$(aws kms create-key \
  --description "Data Lake Encryption Key" \
  --query 'KeyMetadata.KeyId' \
  --output text)

# Create alias
aws kms create-alias \
  --alias-name alias/data-lake-key \
  --target-key-id $KEY_ID

# Enable KMS encryption
aws s3api put-bucket-encryption \
  --bucket $BUCKET_NAME \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms",
        "KMSMasterKeyID": "'$KEY_ID'"
      },
      "BucketKeyEnabled": true
    }]
  }'

# Verify encryption
aws s3api get-bucket-encryption --bucket $BUCKET_NAME
```

### Step 26: Create IAM Role for Glue
**Objective**: Set up permissions for Glue jobs

```bash
# Create trust policy document
cat > /tmp/glue-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name GlueIcebergRole \
  --assume-role-policy-document file:///tmp/glue-trust-policy.json \
  --description "Role for AWS Glue to access S3 and Iceberg tables"

# Attach AWS managed policy for Glue
aws iam attach-role-policy \
  --role-name GlueIcebergRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Verify role creation
aws iam get-role --role-name GlueIcebergRole
```

### Step 27: Create Custom IAM Policy for S3 and Iceberg
**Objective**: Grant specific permissions

```bash
# Get account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Create custom policy for S3 access
cat > /tmp/glue-s3-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DataLakeAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:*Database*",
        "glue:*Table*",
        "glue:*Partition*",
        "glue:GetConnection",
        "glue:GetConnections"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogsAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:/aws-glue/*"
    },
    {
      "Sid": "SecretsManagerAccess",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:*:${ACCOUNT_ID}:secret:*"
    }
  ]
}
EOF

# Create policy
aws iam create-policy \
  --policy-name GlueS3IcebergPolicy \
  --policy-document file:///tmp/glue-s3-policy.json \
  --description "Custom policy for Glue S3 and Iceberg access"

# Attach custom policy to role
aws iam attach-role-policy \
  --role-name GlueIcebergRole \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/GlueS3IcebergPolicy

# Verify attached policies
aws iam list-attached-role-policies --role-name GlueIcebergRole
```

### Step 28: Set Up VPC for Glue (Optional but Recommended)
**Objective**: Configure network for secure access

```bash
# Create VPC
VPC_ID=$(aws ec2 create-vpc \
  --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=glue-vpc}]' \
  --query 'Vpc.VpcId' \
  --output text)

# Enable DNS hostnames and resolution
aws ec2 modify-vpc-attribute \
  --vpc-id $VPC_ID \
  --enable-dns-hostnames

aws ec2 modify-vpc-attribute \
  --vpc-id $VPC_ID \
  --enable-dns-support

# Create private subnet 1
SUBNET1_ID=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.1.0/24 \
  --availability-zone ${AWS_REGION}a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=glue-private-1}]' \
  --query 'Subnet.SubnetId' \
  --output text)

# Create private subnet 2 (Glue requires at least 2 subnets)
SUBNET2_ID=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.2.0/24 \
  --availability-zone ${AWS_REGION}b \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=glue-private-2}]' \
  --query 'Subnet.SubnetId' \
  --output text)

# Create S3 VPC endpoint for private S3 access
aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --service-name com.amazonaws.${AWS_REGION}.s3 \
  --route-table-ids $(aws ec2 describe-route-tables \
    --filters "Name=vpc-id,Values=$VPC_ID" \
    --query 'RouteTables[0].RouteTableId' \
    --output text)

echo "VPC_ID=$VPC_ID"
echo "SUBNET1_ID=$SUBNET1_ID"
echo "SUBNET2_ID=$SUBNET2_ID"
```

### Step 29: Create Security Group for Glue
**Objective**: Configure network security

```bash
# Create security group
SG_ID=$(aws ec2 create-security-group \
  --group-name glue-security-group \
  --description "Security group for AWS Glue ETL jobs" \
  --vpc-id $VPC_ID \
  --tag-specifications 'ResourceType=security-group,Tags=[{Key=Name,Value=glue-sg}]' \
  --query 'GroupId' \
  --output text)

# Add self-referencing ingress rule (required for Glue)
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 0-65535 \
  --source-group $SG_ID

# Add egress rule for HTTPS (for AWS services)
aws ec2 authorize-security-group-egress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 443 \
  --cidr 0.0.0.0/0

echo "SG_ID=$SG_ID"
```

### Step 30: Create Glue Databases
**Objective**: Set up metadata catalog

```bash
# Create database for raw data
aws glue create-database \
  --database-input '{
    "Name": "iceberg_raw",
    "Description": "Raw data from source systems (Redshift, Teradata, BigQuery)",
    "LocationUri": "s3://'$BUCKET_NAME'/raw/"
  }'

# Create database for processed data
aws glue create-database \
  --database-input '{
    "Name": "iceberg_processed",
    "Description": "Processed and transformed data",
    "LocationUri": "s3://'$BUCKET_NAME'/processed/"
  }'

# Create database for curated Iceberg tables
aws glue create-database \
  --database-input '{
    "Name": "iceberg_curated",
    "Description": "Production Iceberg tables for analytics",
    "LocationUri": "s3://'$BUCKET_NAME'/curated/iceberg/"
  }'

# List databases
aws glue get-databases --query 'DatabaseList[].Name'
```

### Step 31: Install Python and Required Libraries
**Objective**: Set up local development environment

```bash
# Check Python version (3.7+ required, 3.9 recommended)
python3 --version

# Create project directory
mkdir -p ~/glue-iceberg-project
cd ~/glue-iceberg-project

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install AWS SDK and Glue libraries
pip install boto3==1.34.0
pip install aws-glue-libs  # Glue ETL library for local development
pip install awswrangler==3.5.0  # AWS Data Wrangler
pip install pyiceberg==0.5.0  # PyIceberg library

# Install development tools
pip install pytest==7.4.0
pip install black==23.12.0  # Code formatter
pip install pylint==3.0.0  # Linter

# Create requirements.txt
pip freeze > requirements.txt

# Verify installations
python -c "import boto3; print(f'boto3 version: {boto3.__version__}')"
python -c "import pyiceberg; print('PyIceberg installed successfully')"
```

### Step 32: Set Up CloudWatch Logs
**Objective**: Configure monitoring and logging

```bash
# Create log group for Glue jobs
aws logs create-log-group \
  --log-group-name /aws-glue/jobs/iceberg-pipeline

# Set retention policy (30 days)
aws logs put-retention-policy \
  --log-group-name /aws-glue/jobs/iceberg-pipeline \
  --retention-in-days 30

# Create log group for continuous logging
aws logs create-log-group \
  --log-group-name /aws-glue/jobs/output

aws logs put-retention-policy \
  --log-group-name /aws-glue/jobs/output \
  --retention-in-days 7

# Create log group for errors
aws logs create-log-group \
  --log-group-name /aws-glue/jobs/error

aws logs put-retention-policy \
  --log-group-name /aws-glue/jobs/error \
  --retention-in-days 90

# List log groups
aws logs describe-log-groups \
  --log-group-name-prefix /aws-glue
```

### Step 33: Enable AWS CloudTrail (Optional)
**Objective**: Audit and compliance logging

```bash
# Create S3 bucket for CloudTrail logs
TRAIL_BUCKET="cloudtrail-logs-$(date +%s)"
aws s3api create-bucket \
  --bucket $TRAIL_BUCKET \
  --region $AWS_REGION

# Apply bucket policy for CloudTrail
cat > /tmp/trail-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AWSCloudTrailAclCheck",
      "Effect": "Allow",
      "Principal": {
        "Service": "cloudtrail.amazonaws.com"
      },
      "Action": "s3:GetBucketAcl",
      "Resource": "arn:aws:s3:::${TRAIL_BUCKET}"
    },
    {
      "Sid": "AWSCloudTrailWrite",
      "Effect": "Allow",
      "Principal": {
        "Service": "cloudtrail.amazonaws.com"
      },
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::${TRAIL_BUCKET}/*",
      "Condition": {
        "StringEquals": {
          "s3:x-amz-acl": "bucket-owner-full-control"
        }
      }
    }
  ]
}
EOF

aws s3api put-bucket-policy \
  --bucket $TRAIL_BUCKET \
  --policy file:///tmp/trail-policy.json

# Create trail
aws cloudtrail create-trail \
  --name glue-audit-trail \
  --s3-bucket-name $TRAIL_BUCKET \
  --is-multi-region-trail \
  --enable-log-file-validation

# Start logging
aws cloudtrail start-logging --name glue-audit-trail

# Verify trail status
aws cloudtrail get-trail-status --name glue-audit-trail
```

### Step 34: Set Up AWS Secrets Manager for Credentials
**Objective**: Securely store database passwords

```bash
# Create secret for Redshift credentials
aws secretsmanager create-secret \
  --name prod/redshift/credentials \
  --description "Redshift database credentials for production" \
  --secret-string '{
    "username": "admin",
    "password": "YourSecurePassword123!",
    "host": "your-cluster.us-east-1.redshift.amazonaws.com",
    "port": "5439",
    "database": "analytics"
  }' \
  --tags Key=Environment,Value=Production Key=Application,Value=DataPipeline

# Create secret for Teradata credentials
aws secretsmanager create-secret \
  --name prod/teradata/credentials \
  --description "Teradata database credentials for production" \
  --secret-string '{
    "username": "dbc",
    "password": "YourSecurePassword123!",
    "host": "teradata.example.com",
    "port": "1025"
  }' \
  --tags Key=Environment,Value=Production Key=Application,Value=DataPipeline

# For BigQuery, first download service account key JSON
# Then create secret
aws secretsmanager create-secret \
  --name prod/bigquery/credentials \
  --description "BigQuery service account credentials" \
  --secret-string file:///path/to/bigquery-key.json \
  --tags Key=Environment,Value=Production Key=Application,Value=DataPipeline

# List secrets
aws secretsmanager list-secrets \
  --filters Key=tag-key,Values=Application Key=tag-value,Values=DataPipeline

# Test retrieval
aws secretsmanager get-secret-value \
  --secret-id prod/redshift/credentials \
  --query SecretString \
  --output text | jq '.'
```

### Step 35: Verify AWS Setup
**Objective**: Ensure all components are ready

```bash
# Create verification script
cat > /tmp/verify-setup.sh << 'EOF'
#!/bin/bash
set -e

echo "=== AWS Setup Verification ==="
echo ""

# Check S3 bucket
echo "✓ Checking S3 bucket..."
aws s3 ls s3://$BUCKET_NAME/ || echo "✗ S3 bucket not found"

# Check IAM role
echo "✓ Checking IAM role..."
aws iam get-role --role-name GlueIcebergRole > /dev/null && echo "  GlueIcebergRole exists"

# Check Glue databases
echo "✓ Checking Glue databases..."
aws glue get-databases --query 'DatabaseList[].Name' --output text

# Check secrets
echo "✓ Checking Secrets Manager..."
aws secretsmanager list-secrets --query 'SecretList[].Name' --output text

# Check VPC (if created)
if [ ! -z "$VPC_ID" ]; then
  echo "✓ Checking VPC..."
  aws ec2 describe-vpcs --vpc-ids $VPC_ID --query 'Vpcs[0].VpcId' --output text
fi

# Check CloudWatch log groups
echo "✓ Checking CloudWatch log groups..."
aws logs describe-log-groups --log-group-name-prefix /aws-glue --query 'logGroups[].logGroupName' --output text

echo ""
echo "=== Setup Verification Complete ==="
echo "All components are ready for Glue development!"
EOF

chmod +x /tmp/verify-setup.sh
/tmp/verify-setup.sh

# Save environment variables for future use
cat > ~/glue-env-vars.sh << EOF
export BUCKET_NAME="$BUCKET_NAME"
export AWS_REGION="$AWS_REGION"
export VPC_ID="$VPC_ID"
export SUBNET1_ID="$SUBNET1_ID"
export SUBNET2_ID="$SUBNET2_ID"
export SG_ID="$SG_ID"
export ACCOUNT_ID="$ACCOUNT_ID"
EOF

echo ""
echo "Environment variables saved to ~/glue-env-vars.sh"
echo "Source it with: source ~/glue-env-vars.sh"
```
