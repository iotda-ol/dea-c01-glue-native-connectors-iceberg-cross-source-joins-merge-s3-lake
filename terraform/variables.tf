# General Configuration
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "multi-source-pipeline"
}

# S3 Configuration
variable "data_lake_bucket_name" {
  description = "S3 bucket name for data lake (must be globally unique)"
  type        = string
}

variable "glue_scripts_bucket_name" {
  description = "S3 bucket name for Glue scripts (must be globally unique)"
  type        = string
}

# Network Configuration (required for JDBC connections)
variable "availability_zone" {
  description = "Availability zone for Glue connections"
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "Subnet ID for Glue connections"
  type        = string
  default     = ""
}

variable "security_group_ids" {
  description = "Security group IDs for Glue connections"
  type        = list(string)
  default     = []
}

# Redshift Configuration
variable "enable_redshift_connection" {
  description = "Enable Redshift connection"
  type        = bool
  default     = true
}

variable "redshift_jdbc_url" {
  description = "Redshift JDBC connection URL"
  type        = string
  default     = "jdbc:redshift://redshift-cluster.example.com:5439/dev"
}

variable "redshift_username" {
  description = "Redshift username"
  type        = string
  default     = ""
  sensitive   = true
}

variable "redshift_password" {
  description = "Redshift password"
  type        = string
  default     = ""
  sensitive   = true
}

variable "redshift_source_table" {
  description = "Source table name in Redshift"
  type        = string
  default     = "public.sales_data"
}

# Teradata Configuration
variable "enable_teradata_connection" {
  description = "Enable Teradata connection"
  type        = bool
  default     = true
}

variable "teradata_jdbc_url" {
  description = "Teradata JDBC connection URL"
  type        = string
  default     = "jdbc:teradata://teradata-host.example.com/DATABASE=prod"
}

variable "teradata_username" {
  description = "Teradata username"
  type        = string
  default     = ""
  sensitive   = true
}

variable "teradata_password" {
  description = "Teradata password"
  type        = string
  default     = ""
  sensitive   = true
}

variable "teradata_source_table" {
  description = "Source table name in Teradata"
  type        = string
  default     = "PROD.CUSTOMER_DATA"
}

# BigQuery Configuration
variable "enable_bigquery_connection" {
  description = "Enable BigQuery connection"
  type        = bool
  default     = true
}

variable "bigquery_jdbc_url" {
  description = "BigQuery JDBC connection URL"
  type        = string
  default     = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=my-project;"
}

variable "bigquery_secret_id" {
  description = "AWS Secrets Manager secret ID containing BigQuery credentials"
  type        = string
  default     = ""
}

variable "bigquery_connector_s3_path" {
  description = "S3 path to BigQuery JDBC connector JAR file"
  type        = string
  default     = ""
}

variable "bigquery_source_table" {
  description = "Source table name in BigQuery (format: project.dataset.table)"
  type        = string
  default     = "my-project.my_dataset.product_data"
}

# Glue Job Configuration
variable "glue_version" {
  description = "AWS Glue version"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type (Standard, G.1X, G.2X, G.025X)"
  type        = string
  default     = "G.2X"
}

variable "number_of_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 10
}

variable "job_timeout_minutes" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 2880
}

variable "max_concurrent_runs" {
  description = "Maximum concurrent runs for Glue job"
  type        = number
  default     = 1
}

variable "job_bookmark_option" {
  description = "Job bookmark option (job-bookmark-enable, job-bookmark-disable, job-bookmark-pause)"
  type        = string
  default     = "job-bookmark-enable"
}

variable "job_duration_threshold_seconds" {
  description = "CloudWatch alarm threshold for job duration in seconds"
  type        = number
  default     = 7200
}

# Iceberg Configuration
variable "iceberg_database_name" {
  description = "Glue Data Catalog database name for Iceberg tables"
  type        = string
  default     = "iceberg_db"
}

variable "iceberg_table_name" {
  description = "Iceberg table name for merged output"
  type        = string
  default     = "integrated_data"
}

# Logging Configuration
variable "log_retention_days" {
  description = "CloudWatch log retention period in days"
  type        = number
  default     = 7
}
