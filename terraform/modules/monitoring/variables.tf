variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "sns_email" {
  description = "Email for SNS notifications"
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
