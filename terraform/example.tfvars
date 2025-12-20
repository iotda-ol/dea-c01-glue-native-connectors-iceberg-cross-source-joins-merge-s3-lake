# Example Terraform Variables File
# Copy this to terraform.tfvars and update with your values

aws_region   = "us-east-1"
project_name = "datalake"
environment  = "dev"

common_tags = {
  Project     = "DataLake"
  Environment = "dev"
  ManagedBy   = "Terraform"
  Owner       = "DataEngineering"
}

# Networking Configuration
vpc_cidr           = "10.0.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b"]
private_subnets    = ["10.0.1.0/24", "10.0.2.0/24"]
public_subnets     = ["10.0.101.0/24", "10.0.102.0/24"]
enable_nat_gateway = true
enable_vpn_gateway = false

# S3 Configuration
enable_versioning  = true
enable_encryption  = true

# Glue Configuration
glue_database_name = "datalake_db"

# Monitoring Configuration
alert_email        = "your-email@example.com"
log_retention_days = 30
