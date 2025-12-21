/**
 * Multi-Source Data Integration Pipeline with AWS Glue and Apache Iceberg
 * 
 * This Terraform configuration creates:
 * - IAM roles with least privilege access
 * - AWS Glue connections for Redshift, Teradata, and BigQuery
 * - AWS Glue job for ETL processing
 * - S3 bucket for data lake with Iceberg tables
 * - CloudWatch logging and monitoring
 * 
 * DEA-C01 Best Practices:
 * - Least privilege IAM policies
 * - Encrypted data at rest and in transit
 * - Scalable Glue job configuration
 * - Comprehensive monitoring and logging
 */

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "Multi-Source-Data-Integration"
      Environment = var.environment
      ManagedBy   = "Terraform"
      Purpose     = "DEA-C01-Demo"
    }
  }
}

# S3 Bucket for Data Lake (Iceberg Tables)
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name

  tags = {
    Name        = "Data Lake Bucket"
    Description = "S3 bucket for Apache Iceberg tables"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket for Glue Scripts
resource "aws_s3_bucket" "glue_scripts" {
  bucket = var.glue_scripts_bucket_name

  tags = {
    Name        = "Glue Scripts Bucket"
    Description = "S3 bucket for AWS Glue job scripts"
  }
}

resource "aws_s3_bucket_versioning" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload Glue ETL script to S3
resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/multi_source_etl.py"
  source = "${path.module}/../scripts/multi_source_etl.py"
  etag   = filemd5("${path.module}/../scripts/multi_source_etl.py")

  tags = {
    Name = "Multi-Source ETL Script"
  }
}

# IAM Role for Glue Job
resource "aws_iam_role" "glue_job_role" {
  name               = "${var.project_name}-glue-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  tags = {
    Name = "Glue Job Role"
  }
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"
    
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    
    actions = ["sts:AssumeRole"]
  }
}

# Attach AWS managed Glue service role
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom IAM policy for S3 access (least privilege)
resource "aws_iam_role_policy" "glue_s3_policy" {
  name   = "${var.project_name}-glue-s3-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    effect = "Allow"
    
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    
    resources = [
      "${aws_s3_bucket.data_lake.arn}/*",
      "${aws_s3_bucket.glue_scripts.arn}/*"
    ]
  }
  
  statement {
    effect = "Allow"
    
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    
    resources = [
      aws_s3_bucket.data_lake.arn,
      aws_s3_bucket.glue_scripts.arn
    ]
  }
}

# IAM policy for CloudWatch Logs
resource "aws_iam_role_policy" "glue_cloudwatch_policy" {
  name   = "${var.project_name}-glue-cloudwatch-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_cloudwatch_access.json
}

data "aws_iam_policy_document" "glue_cloudwatch_access" {
  statement {
    effect = "Allow"
    
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
    ]
  }
}

# IAM policy for Secrets Manager (for database credentials)
resource "aws_iam_role_policy" "glue_secrets_policy" {
  name   = "${var.project_name}-glue-secrets-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_secrets_access.json
}

data "aws_iam_policy_document" "glue_secrets_access" {
  statement {
    effect = "Allow"
    
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    
    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.project_name}/*"
    ]
  }
}

data "aws_caller_identity" "current" {}

# Glue Connection for Redshift
resource "aws_glue_connection" "redshift" {
  count = var.enable_redshift_connection ? 1 : 0
  
  name = "${var.project_name}-redshift-connection"
  
  connection_properties = {
    JDBC_CONNECTION_URL = var.redshift_jdbc_url
    USERNAME            = var.redshift_username
    PASSWORD            = var.redshift_password
  }
  
  connection_type = "JDBC"
  
  physical_connection_requirements {
    availability_zone      = var.availability_zone
    security_group_id_list = var.security_group_ids
    subnet_id              = var.subnet_id
  }
}

# Glue Connection for Teradata
resource "aws_glue_connection" "teradata" {
  count = var.enable_teradata_connection ? 1 : 0
  
  name = "${var.project_name}-teradata-connection"
  
  connection_properties = {
    JDBC_CONNECTION_URL = var.teradata_jdbc_url
    USERNAME            = var.teradata_username
    PASSWORD            = var.teradata_password
  }
  
  connection_type = "JDBC"
  
  physical_connection_requirements {
    availability_zone      = var.availability_zone
    security_group_id_list = var.security_group_ids
    subnet_id              = var.subnet_id
  }
}

# Glue Connection for BigQuery (using JDBC)
resource "aws_glue_connection" "bigquery" {
  count = var.enable_bigquery_connection ? 1 : 0
  
  name = "${var.project_name}-bigquery-connection"
  
  connection_properties = {
    JDBC_CONNECTION_URL      = var.bigquery_jdbc_url
    SECRET_ID                = var.bigquery_secret_id
    CONNECTOR_URL            = var.bigquery_connector_s3_path
    CONNECTOR_CLASS_NAME     = "com.simba.googlebigquery.jdbc.Driver"
  }
  
  connection_type = "MARKETPLACE"
}

# CloudWatch Log Group for Glue Job
resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/${var.project_name}-multi-source-etl"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "Glue Job Logs"
  }
}

# Glue Job
resource "aws_glue_job" "multi_source_etl" {
  name              = "${var.project_name}-multi-source-etl"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.job_timeout_minutes
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.glue_etl_script.key}"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-job-insights"              = "true"
    "--job-bookmark-option"              = var.job_bookmark_option
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.bucket}/temp/"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    
    # Custom parameters
    "--DATA_LAKE_BUCKET"                 = aws_s3_bucket.data_lake.bucket
    "--REDSHIFT_CONNECTION"              = var.enable_redshift_connection ? aws_glue_connection.redshift[0].name : ""
    "--TERADATA_CONNECTION"              = var.enable_teradata_connection ? aws_glue_connection.teradata[0].name : ""
    "--BIGQUERY_CONNECTION"              = var.enable_bigquery_connection ? aws_glue_connection.bigquery[0].name : ""
    "--REDSHIFT_TABLE"                   = var.redshift_source_table
    "--TERADATA_TABLE"                   = var.teradata_source_table
    "--BIGQUERY_TABLE"                   = var.bigquery_source_table
    "--ICEBERG_OUTPUT_PATH"              = "s3://${aws_s3_bucket.data_lake.bucket}/iceberg/"
    "--ICEBERG_DATABASE"                 = var.iceberg_database_name
    "--ICEBERG_TABLE"                    = var.iceberg_table_name
  }
  
  connections = compact([
    var.enable_redshift_connection ? aws_glue_connection.redshift[0].name : null,
    var.enable_teradata_connection ? aws_glue_connection.teradata[0].name : null,
    var.enable_bigquery_connection ? aws_glue_connection.bigquery[0].name : null
  ])
  
  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }
  
  tags = {
    Name = "Multi-Source ETL Job"
  }
  
  depends_on = [
    aws_iam_role_policy.glue_s3_policy,
    aws_iam_role_policy.glue_cloudwatch_policy,
    aws_s3_object.glue_etl_script
  ]
}

# CloudWatch Alarms for monitoring
resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "${var.project_name}-glue-job-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors Glue job failures"
  treat_missing_data  = "notBreaching"
  
  dimensions = {
    JobName = aws_glue_job.multi_source_etl.name
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_job_duration" {
  alarm_name          = "${var.project_name}-glue-job-duration"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "glue.driver.aggregate.elapsedTime"
  namespace           = "Glue"
  period              = "300"
  statistic           = "Maximum"
  threshold           = var.job_duration_threshold_seconds
  alarm_description   = "This metric monitors Glue job duration"
  treat_missing_data  = "notBreaching"
  
  dimensions = {
    JobName = aws_glue_job.multi_source_etl.name
  }
}

# Glue Data Catalog Database for Iceberg tables
resource "aws_glue_catalog_database" "iceberg_db" {
  name        = var.iceberg_database_name
  description = "Database for Apache Iceberg tables"
  
  location_uri = "s3://${aws_s3_bucket.data_lake.bucket}/iceberg/"
}
