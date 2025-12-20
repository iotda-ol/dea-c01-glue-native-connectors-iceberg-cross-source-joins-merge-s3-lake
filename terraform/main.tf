# Main Terraform Configuration
# Orchestrates all infrastructure modules for the data pipeline

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    # Configure remote state storage
    # bucket = "your-terraform-state-bucket"
    # key    = "datalake/terraform.tfstate"
    # region = "us-east-1"
    # encrypt = true
    # dynamodb_table = "terraform-state-lock"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = var.common_tags
  }
}

# Local variables
locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# VPC and Networking
module "networking" {
  source = "./modules/networking"
  
  name_prefix         = local.name_prefix
  vpc_cidr            = var.vpc_cidr
  availability_zones  = var.availability_zones
  private_subnets     = var.private_subnets
  public_subnets      = var.public_subnets
  enable_nat_gateway  = var.enable_nat_gateway
  enable_vpn_gateway  = var.enable_vpn_gateway
  
  tags = var.common_tags
}

# IAM Roles and Policies
module "iam" {
  source = "./modules/iam"
  
  name_prefix = local.name_prefix
  
  tags = var.common_tags
}

# S3 Buckets
module "s3" {
  source = "./modules/s3"
  
  name_prefix         = local.name_prefix
  enable_versioning   = var.enable_versioning
  enable_encryption   = var.enable_encryption
  lifecycle_rules     = var.lifecycle_rules
  
  tags = var.common_tags
}

# AWS Glue Resources
module "glue" {
  source = "./modules/glue"
  
  name_prefix           = local.name_prefix
  database_name         = var.glue_database_name
  scripts_bucket        = module.s3.scripts_bucket_id
  temp_bucket           = module.s3.temp_bucket_id
  glue_role_arn         = module.iam.glue_role_arn
  security_group_ids    = [module.networking.glue_security_group_id]
  subnet_ids            = module.networking.private_subnet_ids
  
  tags = var.common_tags
}

# CloudWatch Monitoring
module "monitoring" {
  source = "./modules/monitoring"
  
  name_prefix       = local.name_prefix
  sns_email         = var.alert_email
  log_retention_days = var.log_retention_days
  
  tags = var.common_tags
}

# Outputs
output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = module.glue.database_name
}

output "datalake_bucket" {
  description = "S3 bucket for data lake"
  value       = module.s3.datalake_bucket_id
}

output "scripts_bucket" {
  description = "S3 bucket for Glue scripts"
  value       = module.s3.scripts_bucket_id
}
