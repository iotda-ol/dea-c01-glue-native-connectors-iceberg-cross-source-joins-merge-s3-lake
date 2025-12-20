# IAM Module - Roles and Policies

# Glue Service Role
resource "aws_iam_role" "glue" {
  name_prefix = "${var.name_prefix}-glue-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

# Attach AWS managed Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 access
resource "aws_iam_role_policy" "glue_s3" {
  name_prefix = "${var.name_prefix}-glue-s3-policy"
  role        = aws_iam_role.glue.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::*datalake*",
          "arn:aws:s3:::*datalake*/*",
          "arn:aws:s3:::*glue*",
          "arn:aws:s3:::*glue*/*"
        ]
      }
    ]
  })
}

# Policy for Secrets Manager access
resource "aws_iam_role_policy" "glue_secrets" {
  name_prefix = "${var.name_prefix}-glue-secrets-policy"
  role        = aws_iam_role.glue.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:*:*:secret:*"
      }
    ]
  })
}

# Policy for CloudWatch Logs
resource "aws_iam_role_policy" "glue_cloudwatch" {
  name_prefix = "${var.name_prefix}-glue-cloudwatch-policy"
  role        = aws_iam_role.glue.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Outputs
output "glue_role_arn" {
  description = "ARN of Glue service role"
  value       = aws_iam_role.glue.arn
}

output "glue_role_name" {
  description = "Name of Glue service role"
  value       = aws_iam_role.glue.name
}
