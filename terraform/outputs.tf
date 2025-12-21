output "data_lake_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "glue_scripts_bucket_name" {
  description = "Name of the S3 Glue scripts bucket"
  value       = aws_s3_bucket.glue_scripts.id
}

output "glue_job_name" {
  description = "Name of the AWS Glue job"
  value       = aws_glue_job.multi_source_etl.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue job"
  value       = aws_glue_job.multi_source_etl.arn
}

output "glue_job_role_arn" {
  description = "ARN of the IAM role used by Glue job"
  value       = aws_iam_role.glue_job_role.arn
}

output "redshift_connection_name" {
  description = "Name of the Redshift Glue connection"
  value       = var.enable_redshift_connection ? aws_glue_connection.redshift[0].name : null
}

output "teradata_connection_name" {
  description = "Name of the Teradata Glue connection"
  value       = var.enable_teradata_connection ? aws_glue_connection.teradata[0].name : null
}

output "bigquery_connection_name" {
  description = "Name of the BigQuery Glue connection"
  value       = var.enable_bigquery_connection ? aws_glue_connection.bigquery[0].name : null
}

output "iceberg_database_name" {
  description = "Name of the Glue Data Catalog database for Iceberg tables"
  value       = aws_glue_catalog_database.iceberg_db.name
}

output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch log group for Glue job"
  value       = aws_cloudwatch_log_group.glue_job_logs.name
}

output "glue_job_failure_alarm_arn" {
  description = "ARN of the CloudWatch alarm for Glue job failures"
  value       = aws_cloudwatch_metric_alarm.glue_job_failure.arn
}

output "glue_job_duration_alarm_arn" {
  description = "ARN of the CloudWatch alarm for Glue job duration"
  value       = aws_cloudwatch_metric_alarm.glue_job_duration.arn
}

output "iceberg_table_location" {
  description = "S3 location of the Iceberg table"
  value       = "s3://${aws_s3_bucket.data_lake.bucket}/iceberg/${var.iceberg_table_name}/"
}
