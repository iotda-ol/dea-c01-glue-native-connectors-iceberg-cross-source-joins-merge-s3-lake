# Quick Start Guide

Get up and running with the multi-source data integration pipeline in under 10 minutes.

## Prerequisites Checklist

- [ ] AWS Account with admin or appropriate permissions
- [ ] AWS CLI installed and configured (`aws configure`)
- [ ] Terraform >= 1.0 installed
- [ ] Access to source databases (Redshift, Teradata, BigQuery)

## 5-Minute Setup

### Step 1: Clone and Configure (2 min)

```bash
# Clone repository
git clone <repo-url>
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake

# Copy and edit configuration
cd terraform
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your values
# IMPORTANT: Change bucket names to be globally unique!
nano terraform.tfvars
```

**Minimum Required Changes** in `terraform.tfvars`:
```hcl
# These MUST be globally unique
data_lake_bucket_name    = "your-company-data-lake-12345"
glue_scripts_bucket_name = "your-company-glue-scripts-12345"

# Update with your actual database details
redshift_jdbc_url = "jdbc:redshift://your-cluster.region.redshift.amazonaws.com:5439/db"
redshift_username = "your-username"
redshift_password = "your-password"
```

### Step 2: Deploy Infrastructure (2 min)

```bash
# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Deploy (type 'yes' when prompted)
terraform apply
```

Expected output:
```
Apply complete! Resources: 15 added, 0 changed, 0 destroyed.

Outputs:
data_lake_bucket_name = "your-company-data-lake-12345"
glue_job_name = "multi-source-pipeline-multi-source-etl"
```

### Step 3: Run ETL Job (1 min)

```bash
# Get job name from Terraform output
JOB_NAME=$(terraform output -raw glue_job_name)

# Start the job
aws glue start-job-run --job-name $JOB_NAME

# Output will show job run ID
{
    "JobRunId": "jr_abc123..."
}
```

### Step 4: Monitor Execution (ongoing)

```bash
# Check job status
aws glue get-job-run \
  --job-name $JOB_NAME \
  --run-id jr_abc123...

# Watch logs in real-time
aws logs tail /aws-glue/jobs/multi-source-pipeline-multi-source-etl --follow
```

### Step 5: Verify Results (1 min)

```bash
# Query the Iceberg table using Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM iceberg_db.integrated_data LIMIT 10" \
  --result-configuration "OutputLocation=s3://your-company-data-lake-12345/athena-results/" \
  --query-execution-context "Database=iceberg_db"
```

## Common Issues

### Issue: "BucketAlreadyExists"
**Solution**: Bucket names must be globally unique. Change `data_lake_bucket_name` and `glue_scripts_bucket_name` in terraform.tfvars.

### Issue: "Connection timeout"
**Solution**: Ensure your database is accessible. If using VPC:
1. Provide `subnet_id`, `availability_zone`, and `security_group_ids`
2. Ensure security group allows traffic from Glue

### Issue: "Job fails immediately"
**Solution**: Check CloudWatch logs:
```bash
aws logs tail /aws-glue/jobs/multi-source-pipeline-multi-source-etl --since 5m
```

## What Gets Created

✅ **2 S3 Buckets**: Data lake and scripts  
✅ **1 IAM Role**: Glue job execution role with least privilege  
✅ **3 Glue Connections**: Redshift, Teradata, BigQuery  
✅ **1 Glue Job**: ETL pipeline with 10 G.2X workers  
✅ **1 Glue Database**: Iceberg table catalog  
✅ **1 CloudWatch Log Group**: Job logs  
✅ **2 CloudWatch Alarms**: Failure and duration alerts  

## Next Steps

1. **Schedule Job**: Set up daily runs with EventBridge
   ```bash
   aws events put-rule \
     --name daily-etl \
     --schedule-expression "cron(0 2 * * ? *)"
   ```

2. **Set Up Alerts**: Configure SNS for alarm notifications
   ```bash
   aws sns create-topic --name glue-alerts
   aws sns subscribe --topic-arn <topic-arn> --protocol email --notification-endpoint you@example.com
   ```

3. **Optimize Performance**: Monitor job metrics and adjust workers
   ```bash
   # View execution times
   aws glue get-job-runs --job-name $JOB_NAME --max-results 10
   ```

4. **Query Data**: Use Athena to analyze integrated data
   ```sql
   SELECT 
     customer_segment,
     product_category,
     SUM(sales_amount) as total_sales,
     COUNT(*) as transaction_count
   FROM iceberg_db.integrated_data
   WHERE year = '2024' AND month = '01'
   GROUP BY customer_segment, product_category
   ORDER BY total_sales DESC;
   ```

## Costs Estimate

For typical usage (daily runs, 10GB data):
- **Glue Job**: ~$0.44 per DPU-hour × 10 workers × 0.5 hours = ~$2.20/day
- **S3 Storage**: ~$0.023 per GB × 100GB = ~$2.30/month
- **CloudWatch**: ~$0.50/month for logs
- **Total**: ~$70-80/month

## Clean Up

To remove all resources and stop incurring charges:

```bash
cd terraform
terraform destroy
```

Type `yes` when prompted. This will delete all resources except S3 buckets with versioning (must be emptied manually first).

## Documentation

📖 [Full Documentation](docs/)
- [Architecture Details](docs/ARCHITECTURE.md)
- [DEA-C01 Best Practices](docs/DEA-C01-BEST-PRACTICES.md)
- [Deployment Guide](docs/DEPLOYMENT.md)
- [Troubleshooting](docs/MONITORING-AND-TROUBLESHOOTING.md)

## Support

- 🐛 [Report Issues](../../issues)
- 💬 [Ask Questions](../../discussions)
- 📧 Contact maintainers

---

**Time to First Pipeline Run: ~5-10 minutes** ⚡
