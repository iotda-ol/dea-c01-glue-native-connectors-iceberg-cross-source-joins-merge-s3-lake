# Deployment Guide

This guide provides step-by-step instructions for deploying the multi-source data integration pipeline.

## Prerequisites

### Required Tools
- **Terraform**: v1.0 or later
- **AWS CLI**: v2.0 or later
- **Python**: 3.9 or later (for local testing)
- **Git**: For version control

### Required AWS Permissions
Your AWS IAM user/role needs permissions to create:
- S3 buckets and objects
- IAM roles and policies
- AWS Glue jobs, connections, and databases
- CloudWatch log groups and alarms
- Secrets Manager secrets (optional)

### AWS Account Setup
```bash
# Configure AWS credentials
aws configure

# Verify credentials
aws sts get-caller-identity
```

## Step 1: Clone Repository

```bash
git clone <repository-url>
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake
```

## Step 2: Network Configuration (Optional)

If your data sources require VPC connectivity:

### Create VPC Resources
```bash
# Create VPC (if not exists)
aws ec2 create-vpc --cidr-block 10.0.0.0/16 --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=glue-vpc}]'

# Create private subnet
aws ec2 create-subnet --vpc-id <vpc-id> --cidr-block 10.0.1.0/24 --availability-zone us-east-1a

# Create security group
aws ec2 create-security-group --group-name glue-sg --description "Security group for Glue connections" --vpc-id <vpc-id>

# Add security group rules
aws ec2 authorize-security-group-ingress --group-id <sg-id> --protocol tcp --port 5439 --source-group <sg-id>  # Redshift
aws ec2 authorize-security-group-ingress --group-id <sg-id> --protocol tcp --port 1025 --source-group <sg-id>  # Teradata
```

## Step 3: Store Database Credentials

### Option A: Using AWS Secrets Manager (Recommended)

```bash
# Store Redshift credentials
aws secretsmanager create-secret \
  --name multi-source-pipeline/redshift/credentials \
  --secret-string '{"username":"admin","password":"YourSecurePassword123!"}'

# Store Teradata credentials
aws secretsmanager create-secret \
  --name multi-source-pipeline/teradata/credentials \
  --secret-string '{"username":"tduser","password":"YourSecurePassword456!"}'

# Store BigQuery credentials (service account JSON)
aws secretsmanager create-secret \
  --name multi-source-pipeline/bigquery/credentials \
  --secret-string file://path/to/bigquery-service-account.json
```

### Option B: Using Terraform Variables (Not Recommended for Production)

Configure credentials in `terraform.tfvars` (see Step 4).

## Step 4: Configure Terraform Variables

Create `terraform.tfvars` from the example:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your values:

```hcl
# General Configuration
aws_region   = "us-east-1"
environment  = "dev"
project_name = "multi-source-pipeline"

# S3 Configuration (MUST BE GLOBALLY UNIQUE)
data_lake_bucket_name    = "mycompany-data-lake-dev-12345"
glue_scripts_bucket_name = "mycompany-glue-scripts-dev-12345"

# Network Configuration (if using VPC)
availability_zone   = "us-east-1a"
subnet_id           = "subnet-xxxxxxxxxxxxx"
security_group_ids  = ["sg-xxxxxxxxxxxxx"]

# Redshift Configuration
enable_redshift_connection = true
redshift_jdbc_url          = "jdbc:redshift://my-cluster.region.redshift.amazonaws.com:5439/mydb"
redshift_username          = "admin"
redshift_password          = "MySecurePassword123!"
redshift_source_table      = "public.sales_data"

# Teradata Configuration
enable_teradata_connection = true
teradata_jdbc_url          = "jdbc:teradata://teradata-host.example.com/DATABASE=PROD"
teradata_username          = "tduser"
teradata_password          = "MySecurePassword456!"
teradata_source_table      = "PROD.CUSTOMER_DATA"

# BigQuery Configuration
enable_bigquery_connection  = true
bigquery_jdbc_url           = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=my-project;"
bigquery_secret_id          = "multi-source-pipeline/bigquery/credentials"
bigquery_connector_s3_path  = "s3://mycompany-glue-scripts-dev-12345/connectors/GoogleBigQueryJDBC.jar"
bigquery_source_table       = "my-project.my_dataset.product_data"
```

**Important**: 
- Replace all placeholder values with your actual configuration
- Ensure S3 bucket names are globally unique
- Use AWS Secrets Manager for credentials in production

## Step 5: Upload BigQuery JDBC Connector (if using BigQuery)

If you're connecting to BigQuery, download and upload the JDBC driver:

```bash
# Download Simba BigQuery JDBC driver
wget https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.2.25.1029.zip

# Unzip
unzip SimbaJDBCDriverforGoogleBigQuery42_1.2.25.1029.zip

# Upload to S3 (will be created by Terraform, so do this after Step 6)
# This step will be done after the bucket is created
```

## Step 6: Deploy Infrastructure with Terraform

### Initialize Terraform

```bash
cd terraform
terraform init
```

This will:
- Download required providers (AWS)
- Initialize the backend
- Prepare the working directory

### Plan Deployment

```bash
terraform plan
```

Review the planned changes. You should see:
- 2 S3 buckets (data lake and scripts)
- IAM role and policies
- Glue connections (3)
- Glue job
- Glue database
- CloudWatch log group and alarms

### Apply Configuration

```bash
terraform apply
```

Type `yes` when prompted. This will:
- Create all AWS resources
- Upload the Glue ETL script to S3
- Configure monitoring and logging

**Expected Duration**: 2-5 minutes

### Verify Deployment

```bash
# Check outputs
terraform output

# Verify Glue job
aws glue get-job --job-name multi-source-pipeline-multi-source-etl

# Verify S3 buckets
aws s3 ls | grep multi-source
```

## Step 7: Upload BigQuery Connector (if applicable)

Now that the S3 bucket exists:

```bash
aws s3 cp GoogleBigQueryJDBC42.jar \
  s3://$(terraform output -raw glue_scripts_bucket_name)/connectors/GoogleBigQueryJDBC.jar
```

## Step 8: Create Source Tables (if needed)

### Redshift
```sql
-- Connect to Redshift
psql -h <cluster-endpoint> -U admin -d mydb

-- Create sample sales table
CREATE TABLE public.sales_data (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    amount DECIMAL(18,2),
    transaction_date TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO public.sales_data VALUES
    ('TXN001', 'CUST001', 'PROD001', 199.99, '2024-01-15 10:30:00', CURRENT_TIMESTAMP),
    ('TXN002', 'CUST002', 'PROD002', 299.99, '2024-01-15 11:45:00', CURRENT_TIMESTAMP);
```

### Teradata
```sql
-- Connect to Teradata
bteq

-- Create sample customer table
CREATE TABLE PROD.CUSTOMER_DATA (
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    customer_segment VARCHAR(50),
    registration_date TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO PROD.CUSTOMER_DATA VALUES
    ('CUST001', 'John Doe', 'john@example.com', 'Premium', '2023-01-10 09:00:00', CURRENT_TIMESTAMP),
    ('CUST002', 'Jane Smith', 'jane@example.com', 'Standard', '2023-02-15 10:30:00', CURRENT_TIMESTAMP);
```

### BigQuery
```sql
-- Using BigQuery console or bq CLI
CREATE TABLE my_project.my_dataset.product_data (
    product_id STRING,
    product_name STRING,
    category STRING,
    unit_price NUMERIC
);

INSERT INTO my_project.my_dataset.product_data VALUES
    ('PROD001', 'Laptop Computer', 'Electronics', 899.99),
    ('PROD002', 'Office Chair', 'Furniture', 249.99);
```

## Step 9: Test Glue Job

### Manual Test Run

```bash
# Start the Glue job
aws glue start-job-run \
  --job-name $(terraform output -raw glue_job_name)

# Get the job run ID from the output, then check status
aws glue get-job-run \
  --job-name $(terraform output -raw glue_job_name) \
  --run-id <run-id>
```

### Monitor Job Execution

```bash
# View CloudWatch logs
aws logs tail /aws-glue/jobs/multi-source-pipeline-multi-source-etl --follow
```

### Verify Output

```bash
# Check Iceberg table location
aws s3 ls s3://$(terraform output -raw data_lake_bucket_name)/iceberg/ --recursive

# Query using Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM iceberg_db.integrated_data LIMIT 10" \
  --result-configuration "OutputLocation=s3://$(terraform output -raw data_lake_bucket_name)/athena-results/"
```

## Step 10: Configure Scheduling (Optional)

### Create EventBridge Rule for Daily Execution

```bash
# Create rule
aws events put-rule \
  --name daily-etl-job \
  --schedule-expression "cron(0 2 * * ? *)" \
  --description "Trigger ETL job daily at 2 AM UTC"

# Add Glue job as target
aws events put-targets \
  --rule daily-etl-job \
  --targets "Id"="1","Arn"="$(terraform output -raw glue_job_arn)","RoleArn"="<events-role-arn>"
```

### Using Terraform (Recommended)

Add to `terraform/main.tf`:

```hcl
resource "aws_cloudwatch_event_rule" "daily_etl" {
  name                = "${var.project_name}-daily-etl"
  description         = "Trigger ETL job daily"
  schedule_expression = "cron(0 2 * * ? *)"
}

resource "aws_cloudwatch_event_target" "glue_job" {
  rule      = aws_cloudwatch_event_rule.daily_etl.name
  target_id = "GlueJobTarget"
  arn       = aws_glue_job.multi_source_etl.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}
```

## Troubleshooting

### Common Issues

#### 1. S3 Bucket Already Exists
**Error**: `BucketAlreadyExists` or `BucketAlreadyOwnedByYou`

**Solution**: Change bucket names in `terraform.tfvars` to be globally unique.

#### 2. Insufficient IAM Permissions
**Error**: `AccessDenied` or `UnauthorizedOperation`

**Solution**: Ensure your AWS credentials have necessary permissions. Check the Prerequisites section.

#### 3. VPC Connection Timeout
**Error**: Job fails with connection timeout

**Solution**: 
- Verify security group allows outbound traffic
- Check network ACLs
- Ensure NAT Gateway or VPC endpoints configured
- Test connectivity from a test instance in the same subnet

#### 4. BigQuery Connector Not Found
**Error**: `ClassNotFoundException: com.simba.googlebigquery.jdbc.Driver`

**Solution**: Upload the BigQuery JDBC connector to S3 at the path specified in `bigquery_connector_s3_path`.

#### 5. Job Bookmark State Issues
**Error**: Duplicate data or no data processed

**Solution**: 
```bash
# Reset job bookmark
aws glue reset-job-bookmark --job-name <job-name>
```

#### 6. Out of Memory Errors
**Error**: `OutOfMemoryError` or job failure

**Solution**: 
- Increase worker size (G.1X → G.2X → G.4X)
- Increase number of workers
- Optimize transformations (reduce caching, select fewer columns)

### View Detailed Logs

```bash
# CloudWatch Logs
aws logs tail /aws-glue/jobs/multi-source-pipeline-multi-source-etl \
  --follow \
  --filter-pattern "ERROR"

# Job metrics
aws cloudwatch get-metric-statistics \
  --namespace Glue \
  --metric-name glue.driver.aggregate.numFailedTasks \
  --dimensions Name=JobName,Value=multi-source-pipeline-multi-source-etl \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-02T00:00:00Z \
  --period 3600 \
  --statistics Sum
```

## Maintenance

### Regular Tasks

#### Weekly
- Review CloudWatch alarms and metrics
- Check data quality metrics
- Monitor job duration trends

#### Monthly
- Optimize Iceberg tables (compact files)
- Review and adjust worker configuration
- Update Terraform to latest versions
- Rotate database credentials

#### Quarterly
- Review IAM policies for least privilege
- Update JDBC drivers
- Review S3 lifecycle policies
- Audit CloudWatch log retention

### Updating the Pipeline

```bash
# Pull latest changes
git pull origin main

# Update Terraform
cd terraform
terraform plan
terraform apply

# If ETL script changed, Terraform will automatically update it
```

### Backup and Recovery

```bash
# Backup Terraform state
aws s3 cp terraform.tfstate s3://backup-bucket/terraform-state/$(date +%Y%m%d)/

# Backup Glue job definition
aws glue get-job --job-name <job-name> > glue-job-backup.json

# Backup Iceberg table metadata
aws s3 sync s3://<data-lake-bucket>/iceberg/metadata/ \
  s3://<backup-bucket>/iceberg-metadata-backup/$(date +%Y%m%d)/
```

## Clean Up

To remove all resources:

```bash
# Destroy Terraform resources
cd terraform
terraform destroy

# Verify all resources deleted
aws glue list-jobs --query "JobNames[?contains(@, 'multi-source-pipeline')]"
aws s3 ls | grep multi-source
```

**Note**: S3 buckets with versioning may require manual cleanup of all versions before deletion.

## Next Steps

1. Set up CI/CD pipeline for automated deployments
2. Implement data quality rules using AWS Glue Data Quality
3. Add data lineage tracking using AWS Glue Studio
4. Configure AWS Lake Formation for fine-grained access control
5. Set up cross-region replication for disaster recovery
6. Implement automated testing for ETL logic

## Support

For issues and questions:
- Check CloudWatch logs for error messages
- Review the troubleshooting section
- Consult AWS Glue documentation
- Open an issue in the repository

## Additional Resources

- [Terraform AWS Provider Documentation](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/latest/dg/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [AWS CLI Command Reference](https://docs.aws.amazon.com/cli/latest/)
