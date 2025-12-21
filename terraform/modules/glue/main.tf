# Glue Module - AWS Glue Resources

# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  name        = var.database_name
  description = "Data lake catalog database"
  
  tags = var.tags
}

# Example Glue Connection for Redshift (requires additional configuration)
# resource "aws_glue_connection" "redshift" {
#   name = "${var.name_prefix}-redshift-connection"
#   
#   connection_properties = {
#     JDBC_CONNECTION_URL = "jdbc:redshift://..."
#     USERNAME            = "admin"
#     PASSWORD            = "..." # Use Secrets Manager
#   }
#   
#   physical_connection_requirements {
#     availability_zone      = var.availability_zone
#     security_group_id_list = var.security_group_ids
#     subnet_id              = var.subnet_ids[0]
#   }
# }

# Outputs
output "database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.main.name
}

output "database_arn" {
  description = "Glue catalog database ARN"
  value       = aws_glue_catalog_database.main.arn
}
