# Monitoring and Troubleshooting Guide

This guide provides comprehensive instructions for monitoring the multi-source data integration pipeline and troubleshooting common issues.

## Table of Contents
- [Monitoring Overview](#monitoring-overview)
- [CloudWatch Metrics](#cloudwatch-metrics)
- [CloudWatch Alarms](#cloudwatch-alarms)
- [Log Analysis](#log-analysis)
- [Performance Monitoring](#performance-monitoring)
- [Common Issues and Solutions](#common-issues-and-solutions)
- [Debugging Techniques](#debugging-techniques)

---

## Monitoring Overview

### Key Monitoring Areas

1. **Job Execution**: Success rate, duration, frequency
2. **Data Quality**: Validation metrics, error rates
3. **Resource Utilization**: Worker usage, memory, CPU
4. **Costs**: DPU-hours consumed, S3 storage
5. **Data Freshness**: Time since last successful run

### Monitoring Tools

- **AWS Glue Console**: Visual job monitoring
- **CloudWatch Dashboard**: Custom metrics dashboard
- **CloudWatch Logs**: Detailed execution logs
- **CloudWatch Alarms**: Automated alerting
- **AWS Cost Explorer**: Cost tracking

---

## CloudWatch Metrics

### Glue Job Metrics

#### Built-in Metrics

```bash
# View job metrics
aws cloudwatch get-metric-statistics \
  --namespace Glue \
  --metric-name glue.driver.aggregate.elapsedTime \
  --dimensions Name=JobName,Value=multi-source-pipeline-multi-source-etl \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-02T00:00:00Z \
  --period 3600 \
  --statistics Average,Maximum
```

**Key Metrics**:

| Metric Name | Description | Unit | Threshold |
|-------------|-------------|------|-----------|
| `glue.driver.aggregate.elapsedTime` | Total job execution time | Milliseconds | < 7200000 (2 hours) |
| `glue.driver.aggregate.numFailedTasks` | Number of failed tasks | Count | = 0 |
| `glue.driver.aggregate.numCompletedTasks` | Completed tasks | Count | > 0 |
| `glue.driver.BlockManager.disk.diskSpaceUsed_MB` | Disk usage | MB | < 80% capacity |
| `glue.driver.jvm.heap.usage` | JVM heap utilization | Ratio | < 0.85 |
| `glue.ALL.s3.filesystem.read_bytes` | S3 bytes read | Bytes | Monitor trends |
| `glue.ALL.s3.filesystem.write_bytes` | S3 bytes written | Bytes | Monitor trends |

#### Custom Application Metrics

The ETL script logs custom metrics:

```python
# In the ETL script
log_info(f"Total records in Iceberg table: {total_records}")
log_info(f"Invalid records: {invalid_records}")
log_info(f"Data quality percentage: {quality_percentage:.2f}%")
```

Extract these from CloudWatch Logs Insights:

```sql
fields @timestamp, @message
| filter @message like /Total records in Iceberg table/
| parse @message "Total records in Iceberg table: *" as record_count
| stats latest(record_count) by bin(5m)
```

### Creating a CloudWatch Dashboard

```bash
# Create a custom dashboard
aws cloudwatch put-dashboard \
  --dashboard-name multi-source-pipeline-dashboard \
  --dashboard-body file://dashboard-config.json
```

**dashboard-config.json**:
```json
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["Glue", "glue.driver.aggregate.elapsedTime", {"stat": "Average"}],
          [".", "glue.driver.aggregate.numFailedTasks", {"stat": "Sum"}]
        ],
        "period": 300,
        "stat": "Average",
        "region": "us-east-1",
        "title": "Job Execution Metrics",
        "yAxis": {
          "left": {
            "min": 0
          }
        }
      }
    },
    {
      "type": "log",
      "properties": {
        "query": "SOURCE '/aws-glue/jobs/multi-source-pipeline-multi-source-etl'\n| fields @timestamp, @message\n| filter @message like /ERROR/\n| sort @timestamp desc\n| limit 20",
        "region": "us-east-1",
        "title": "Recent Errors"
      }
    }
  ]
}
```

---

## CloudWatch Alarms

### Pre-configured Alarms

The Terraform configuration creates two alarms:

#### 1. Job Failure Alarm

```hcl
resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "glue-job-failure"
  comparison_operator = "GreaterThanThreshold"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  threshold           = "0"
}
```

**Triggers when**: Any task fails
**Action**: Immediate investigation required

#### 2. Job Duration Alarm

```hcl
resource "aws_cloudwatch_metric_alarm" "glue_job_duration" {
  alarm_name          = "glue-job-duration"
  comparison_operator = "GreaterThanThreshold"
  metric_name         = "glue.driver.aggregate.elapsedTime"
  threshold           = "7200000"  # 2 hours in milliseconds
}
```

**Triggers when**: Job runs longer than 2 hours
**Action**: Check for data volume increase or performance issues

### Adding SNS Notifications

```bash
# Create SNS topic
aws sns create-topic --name glue-job-alerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:123456789012:glue-job-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com

# Update alarm to send notifications
aws cloudwatch put-metric-alarm \
  --alarm-name glue-job-failure \
  --alarm-actions arn:aws:sns:us-east-1:123456789012:glue-job-alerts
```

### Additional Recommended Alarms

#### Data Quality Alarm

```bash
# Create a custom metric for data quality
aws cloudwatch put-metric-data \
  --namespace MultiSourcePipeline \
  --metric-name DataQualityPercentage \
  --value 98.5 \
  --timestamp $(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Create alarm
aws cloudwatch put-metric-alarm \
  --alarm-name data-quality-low \
  --comparison-operator LessThanThreshold \
  --metric-name DataQualityPercentage \
  --namespace MultiSourcePipeline \
  --threshold 95 \
  --evaluation-periods 1 \
  --period 300
```

#### Job Not Running Alarm

```bash
# Alarm if no job runs in 24 hours
aws cloudwatch put-metric-alarm \
  --alarm-name glue-job-not-running \
  --comparison-operator LessThanThreshold \
  --metric-name JobRuns \
  --namespace AWS/Glue \
  --threshold 1 \
  --evaluation-periods 1 \
  --period 86400 \
  --statistic Sum
```

---

## Log Analysis

### Accessing Logs

#### Via AWS Console
1. Navigate to CloudWatch → Log Groups
2. Find `/aws-glue/jobs/multi-source-pipeline-multi-source-etl`
3. View log streams (one per job run)

#### Via AWS CLI

```bash
# List recent log streams
aws logs describe-log-streams \
  --log-group-name /aws-glue/jobs/multi-source-pipeline-multi-source-etl \
  --order-by LastEventTime \
  --descending \
  --max-items 10

# Tail logs in real-time
aws logs tail /aws-glue/jobs/multi-source-pipeline-multi-source-etl --follow

# Filter for errors
aws logs tail /aws-glue/jobs/multi-source-pipeline-multi-source-etl \
  --follow \
  --filter-pattern "ERROR"
```

### CloudWatch Logs Insights Queries

#### 1. Error Analysis

```sql
fields @timestamp, @message
| filter @message like /ERROR/
| parse @message "[ERROR] * - *" as timestamp, error_message
| stats count() by error_message
| sort count() desc
```

#### 2. Performance Metrics

```sql
fields @timestamp, @message
| filter @message like /records/
| parse @message "* records" as record_count
| stats sum(record_count) as total_records by bin(1h)
```

#### 3. Data Quality Tracking

```sql
fields @timestamp, @message
| filter @message like /Data quality percentage/
| parse @message "Data quality percentage: *%" as quality
| stats avg(quality) as avg_quality, min(quality) as min_quality by bin(1d)
```

#### 4. Job Duration Trend

```sql
fields @timestamp, @message
| filter @message like /ETL job completed/
| stats count() as successful_runs by bin(1d)
```

#### 5. Source-Specific Issues

```sql
fields @timestamp, @message
| filter @message like /Error reading from/
| parse @message "Error reading from *:" as source_system
| stats count() by source_system
```

### Log Retention and Export

```bash
# Set log retention
aws logs put-retention-policy \
  --log-group-name /aws-glue/jobs/multi-source-pipeline-multi-source-etl \
  --retention-in-days 30

# Export logs to S3
aws logs create-export-task \
  --log-group-name /aws-glue/jobs/multi-source-pipeline-multi-source-etl \
  --from 1640995200000 \
  --to 1641081600000 \
  --destination my-log-export-bucket \
  --destination-prefix glue-logs/
```

---

## Performance Monitoring

### Job Performance Metrics

#### Execution Time Analysis

```bash
# Get job run history
aws glue get-job-runs \
  --job-name multi-source-pipeline-multi-source-etl \
  --max-results 50 \
  --query 'JobRuns[*].[Id,ExecutionTime,JobRunState]' \
  --output table
```

#### Data Processing Throughput

Calculate records per second:
```
Throughput = Total Records / Execution Time (seconds)
```

Target: > 1000 records/second for G.2X workers

#### Worker Utilization

Monitor via CloudWatch:
- CPU utilization per executor
- Memory usage per executor
- Shuffle read/write metrics
- GC time percentage

### Optimization Recommendations

| Metric | Current | Recommendation | Action |
|--------|---------|----------------|--------|
| Job Duration | > 2 hours | < 1 hour | Increase workers or optimize queries |
| DPU Utilization | < 50% | 70-85% | Reduce worker count |
| GC Time | > 10% | < 5% | Increase worker memory (G.2X → G.4X) |
| Shuffle Size | > 1GB | < 200MB | Repartition data or use broadcast joins |

### Performance Testing

```bash
# Run with different worker configurations
for workers in 5 10 15 20; do
  echo "Testing with $workers workers"
  aws glue start-job-run \
    --job-name multi-source-pipeline-multi-source-etl \
    --arguments '{"--number-of-workers":"'$workers'"}'
  
  # Wait and check duration
  sleep 3600
done
```

---

## Common Issues and Solutions

### Issue 1: Connection Timeout

**Symptoms**:
```
ERROR: Connection timeout connecting to Redshift/Teradata
```

**Root Causes**:
- Security group not allowing traffic
- Network ACL blocking connections
- Database not accessible from VPC

**Solutions**:

1. **Check Security Groups**:
```bash
aws ec2 describe-security-groups \
  --group-ids sg-xxxxx \
  --query 'SecurityGroups[*].IpPermissions'
```

Ensure inbound rules allow traffic from Glue security group.

2. **Test Connection**:
```bash
# Create test Glue job or use EC2 in same subnet
telnet redshift-cluster.region.redshift.amazonaws.com 5439
```

3. **Verify VPC Configuration**:
- Ensure private subnet has route to NAT Gateway
- Check VPC endpoints for S3 and Glue

### Issue 2: Out of Memory (OOM)

**Symptoms**:
```
ERROR: java.lang.OutOfMemoryError: Java heap space
ERROR: Container killed by YARN for exceeding memory limits
```

**Solutions**:

1. **Increase Worker Size**:
```hcl
worker_type = "G.4X"  # from G.2X
```

2. **Optimize DataFrame Operations**:
```python
# Before: Loading entire table
df = spark.read.format("jdbc").load()

# After: Predicate pushdown
df = spark.read.format("jdbc") \
    .option("predicates", ["date >= '2024-01-01'"]) \
    .load()
```

3. **Partition Data**:
```python
df.repartition(100).write.save()
```

4. **Cache Strategically**:
```python
# Only cache if data reused multiple times
if reuse_count > 2:
    df.cache()
```

### Issue 3: Slow Performance

**Symptoms**:
- Job takes much longer than expected
- High shuffle read/write
- Uneven task distribution

**Solutions**:

1. **Check for Data Skew**:
```python
# Identify skew
df.groupBy("partition_key").count().show()
```

2. **Optimize Joins**:
```python
# Use broadcast for small tables (< 10MB)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

3. **Adjust Parallelism**:
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")
```

4. **Enable Adaptive Query Execution**:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Issue 4: Iceberg Write Failures

**Symptoms**:
```
ERROR: CommitFailedException: Failed to commit table update
ERROR: Table already exists
```

**Solutions**:

1. **Concurrent Write Conflict**:
```bash
# Ensure max_concurrent_runs = 1
aws glue update-job \
  --job-name multi-source-pipeline-multi-source-etl \
  --execution-property MaxConcurrentRuns=1
```

2. **Stale Snapshot**:
```sql
-- Expire old snapshots
CALL glue_catalog.system.expire_snapshots(
  table => 'iceberg_db.integrated_data',
  older_than => TIMESTAMP '2024-01-01 00:00:00'
)
```

3. **Orphaned Files**:
```sql
-- Remove orphaned files
CALL glue_catalog.system.remove_orphan_files(
  table => 'iceberg_db.integrated_data'
)
```

### Issue 5: Job Bookmark Issues

**Symptoms**:
- Duplicate data
- No new data processed
- Bookmark state corruption

**Solutions**:

1. **Reset Bookmark**:
```bash
aws glue reset-job-bookmark \
  --job-name multi-source-pipeline-multi-source-etl
```

2. **Verify Bookmark State**:
```bash
aws glue get-job-bookmark \
  --job-name multi-source-pipeline-multi-source-etl
```

3. **Disable Bookmarks Temporarily**:
```bash
aws glue start-job-run \
  --job-name multi-source-pipeline-multi-source-etl \
  --arguments '{"--job-bookmark-option":"job-bookmark-disable"}'
```

### Issue 6: Credentials/Authentication Errors

**Symptoms**:
```
ERROR: Invalid credentials
ERROR: Access denied
```

**Solutions**:

1. **Check IAM Role**:
```bash
aws iam get-role --role-name multi-source-pipeline-glue-job-role
```

2. **Test S3 Access**:
```bash
aws s3 ls s3://data-lake-bucket/ --profile glue-role
```

3. **Verify Secrets Manager**:
```bash
aws secretsmanager get-secret-value \
  --secret-id multi-source-pipeline/redshift/credentials
```

4. **Update Connection Credentials**:
```bash
aws glue update-connection \
  --name redshift-connection \
  --connection-input '{"ConnectionProperties": {"USERNAME": "new_user"}}'
```

---

## Debugging Techniques

### 1. Enable Verbose Logging

Add to Glue job arguments:
```hcl
"--enable-continuous-cloudwatch-log" = "true"
"--enable-continuous-log-filter" = "true"
"--continuous-log-logGroup" = "/aws-glue/jobs/output"
```

### 2. Add Debug Print Statements

```python
# In ETL script
print(f"DEBUG: DataFrame schema: {df.printSchema()}")
print(f"DEBUG: Row count: {df.count()}")
print(f"DEBUG: Sample data:")
df.show(5, truncate=False)
```

### 3. Save Intermediate Results

```python
# Save checkpoints
df.write.mode("overwrite").parquet("s3://bucket/debug/checkpoint1/")
```

### 4. Use Explain Plan

```python
# Analyze query execution plan
df.explain(extended=True)
```

### 5. Local Testing with Docker

```bash
# Use AWS Glue Docker container
docker run -it -v ~/.aws:/home/glue_user/.aws \
  -v $PWD:/home/glue_user/workspace \
  amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
  /home/glue_user/workspace/scripts/multi_source_etl.py \
  --JOB_NAME test
```

### 6. Profiling

```python
# Add timing
import time
start = time.time()
# ... operation ...
print(f"Operation took {time.time() - start:.2f} seconds")
```

---

## Monitoring Checklist

### Daily
- [ ] Check job execution status
- [ ] Review CloudWatch alarms
- [ ] Verify data freshness
- [ ] Check error logs

### Weekly
- [ ] Analyze performance trends
- [ ] Review data quality metrics
- [ ] Check resource utilization
- [ ] Verify cost trends

### Monthly
- [ ] Optimize worker configuration
- [ ] Compact Iceberg tables
- [ ] Review and update alarms
- [ ] Audit IAM permissions
- [ ] Review S3 storage growth

### Quarterly
- [ ] Performance benchmarking
- [ ] Disaster recovery test
- [ ] Security audit
- [ ] Cost optimization review

---

## Additional Resources

- [AWS Glue Monitoring Documentation](https://docs.aws.amazon.com/glue/latest/dg/monitoring-glue.html)
- [CloudWatch Logs Insights Query Syntax](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html)
- [Apache Iceberg Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [PySpark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
