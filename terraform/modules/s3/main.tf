# S3 Module - Storage Buckets

# Data Lake Bucket
resource "aws_s3_bucket" "datalake" {
  bucket_prefix = "${var.name_prefix}-datalake-"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.name_prefix}-datalake"
      Purpose = "data-lake-storage"
    }
  )
}

# Bucket Versioning
resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# Bucket Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block Public Access
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Scripts Bucket
resource "aws_s3_bucket" "scripts" {
  bucket_prefix = "${var.name_prefix}-scripts-"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.name_prefix}-scripts"
      Purpose = "glue-scripts"
    }
  )
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Temporary Files Bucket
resource "aws_s3_bucket" "temp" {
  bucket_prefix = "${var.name_prefix}-temp-"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.name_prefix}-temp"
      Purpose = "temporary-files"
    }
  )
}

resource "aws_s3_bucket_server_side_encryption_configuration" "temp" {
  bucket = aws_s3_bucket.temp.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "temp" {
  bucket = aws_s3_bucket.temp.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle Policy for Temp Bucket
resource "aws_s3_bucket_lifecycle_configuration" "temp" {
  bucket = aws_s3_bucket.temp.id
  
  rule {
    id     = "delete-old-temp-files"
    status = "Enabled"
    
    expiration {
      days = 7
    }
  }
}

# Outputs
output "datalake_bucket_id" {
  description = "Data lake bucket ID"
  value       = aws_s3_bucket.datalake.id
}

output "datalake_bucket_arn" {
  description = "Data lake bucket ARN"
  value       = aws_s3_bucket.datalake.arn
}

output "scripts_bucket_id" {
  description = "Scripts bucket ID"
  value       = aws_s3_bucket.scripts.id
}

output "scripts_bucket_arn" {
  description = "Scripts bucket ARN"
  value       = aws_s3_bucket.scripts.arn
}

output "temp_bucket_id" {
  description = "Temp bucket ID"
  value       = aws_s3_bucket.temp.id
}

output "temp_bucket_arn" {
  description = "Temp bucket ARN"
  value       = aws_s3_bucket.temp.arn
}
