variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "database_name" {
  description = "Glue catalog database name"
  type        = string
}

variable "scripts_bucket" {
  description = "S3 bucket for Glue scripts"
  type        = string
}

variable "temp_bucket" {
  description = "S3 bucket for temporary files"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  type        = string
}

variable "security_group_ids" {
  description = "Security group IDs for Glue connections"
  type        = list(string)
}

variable "subnet_ids" {
  description = "Subnet IDs for Glue connections"
  type        = list(string)
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
