# Monitoring Module - CloudWatch and SNS

# SNS Topic for Alerts
resource "aws_sns_topic" "alerts" {
  name_prefix = "${var.name_prefix}-alerts-"
  
  tags = var.tags
}

# SNS Topic Subscription (if email provided)
resource "aws_sns_topic_subscription" "email" {
  count     = var.sns_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.sns_email
}

# CloudWatch Log Group for Glue Jobs
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name_prefix       = "/aws/glue/${var.name_prefix}"
  retention_in_days = var.log_retention_days
  
  tags = var.tags
}

# CloudWatch Alarm for Glue Job Failures
resource "aws_cloudwatch_metric_alarm" "glue_job_failures" {
  alarm_name          = "${var.name_prefix}-glue-job-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobFailure"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alert when Glue jobs fail"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  
  tags = var.tags
}

# Outputs
output "sns_topic_arn" {
  description = "SNS topic ARN for alerts"
  value       = aws_sns_topic.alerts.arn
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.glue_jobs.name
}
