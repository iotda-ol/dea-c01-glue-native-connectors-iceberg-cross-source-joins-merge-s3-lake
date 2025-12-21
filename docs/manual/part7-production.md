# Part 7: Production & Monitoring (Steps 96-100)

## Deploying to Production

### Step 96: Production Deployment Checklist
**Objective**: Prepare for production deployment

**Pre-Deployment Checklist**:
- [ ] All tests passing
- [ ] Security scan completed
- [ ] IAM roles reviewed (least privilege)
- [ ] Encryption enabled (at rest and in transit)
- [ ] Monitoring and alerting configured
- [ ] Backup and disaster recovery tested
- [ ] Documentation complete
- [ ] Cost estimation reviewed
- [ ] SLA requirements defined
- [ ] Runbook created

**Deployment Process**:
```bash
# 1. Tag release
git tag -a v1.0.0 -m "Production release 1.0.0"
git push origin v1.0.0

# 2. Deploy infrastructure
cd infrastructure/terraform
terraform plan -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars

# 3. Deploy Glue jobs
./scripts/deployment/deploy_jobs.sh prod

# 4. Verify deployment
./scripts/deployment/verify_deployment.sh prod
```

### Step 97: Implement Comprehensive Monitoring
**Objective**: Set up production monitoring

```python
# monitoring.py
import boto3
from datetime import datetime, timedelta

class ProductionMonitor:
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')
        self.glue = boto3.client('glue')
        self.sns = boto3.client('sns')
    
    def create_alarms(self):
        """Create CloudWatch alarms"""
        # Job failure alarm
        self.cloudwatch.put_metric_alarm(
            AlarmName='GlueJobFailure',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=1,
            MetricName='JobFailure',
            Namespace='AWS/Glue',
            Period=300,
            Statistic='Sum',
            Threshold=0,
            ActionsEnabled=True,
            AlarmActions=['arn:aws:sns:region:account:GlueAlerts'],
            AlarmDescription='Alert when Glue job fails'
        )
        
        # Long running job alarm
        self.cloudwatch.put_metric_alarm(
            AlarmName='GlueJobLongRunning',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=1,
            MetricName='ExecutionTime',
            Namespace='GlueJobs/Performance',
            Period=3600,
            Statistic='Average',
            Threshold=3600,  # 1 hour
            ActionsEnabled=True,
            AlarmActions=['arn:aws:sns:region:account:GlueAlerts']
        )
    
    def monitor_data_freshness(self, table_name, max_age_hours=24):
        """Check if data is fresh"""
        # Get latest snapshot timestamp from Iceberg
        # Implement based on your requirements
        pass
    
    def generate_daily_report(self):
        """Generate daily pipeline report"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=1)
        
        # Get job run metrics
        metrics = self.cloudwatch.get_metric_statistics(
            Namespace='AWS/Glue',
            MetricName='glue.driver.aggregate.numCompletedTasks',
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Sum']
        )
        
        # Format and send report
        report = f"Daily Pipeline Report - {end_time.date()}\n"
        report += f"Total tasks completed: {sum(m['Sum'] for m in metrics['Datapoints'])}\n"
        
        # Send via SNS
        self.sns.publish(
            TopicArn='arn:aws:sns:region:account:DailyReports',
            Subject='Daily Pipeline Report',
            Message=report
        )
```

### Step 98: Implement Alerting Strategy
**Objective**: Set up comprehensive alerting

```python
# alerting.py
import boto3
from enum import Enum

class AlertSeverity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"

class AlertManager:
    def __init__(self, sns_topic_arn):
        self.sns = boto3.client('sns')
        self.topic_arn = sns_topic_arn
    
    def send_alert(self, title, message, severity=AlertSeverity.MEDIUM):
        """Send alert via SNS"""
        subject = f"[{severity.value}] {title}"
        
        self.sns.publish(
            TopicArn=self.topic_arn,
            Subject=subject,
            Message=message,
            MessageAttributes={
                'severity': {'DataType': 'String', 'StringValue': severity.value}
            }
        )
    
    def alert_on_job_failure(self, job_name, run_id, error_message):
        """Alert when job fails"""
        message = f"""
        Job Failure Alert
        
        Job Name: {job_name}
        Run ID: {run_id}
        Error: {error_message}
        
        Please investigate immediately.
        """
        
        self.send_alert(
            f"Glue Job Failed: {job_name}",
            message,
            AlertSeverity.CRITICAL
        )
    
    def alert_on_data_quality_issue(self, table_name, issue_details):
        """Alert on data quality problems"""
        message = f"""
        Data Quality Issue Detected
        
        Table: {table_name}
        Issues: {issue_details}
        
        Action Required: Review data quality rules
        """
        
        self.send_alert(
            f"DQ Issue: {table_name}",
            message,
            AlertSeverity.HIGH
        )

# Usage
alerter = AlertManager("arn:aws:sns:us-east-1:123456789012:GlueAlerts")
alerter.alert_on_job_failure("extract-customers", "jr_123", "Connection timeout")
```

### Step 99: Create Operational Runbook
**Objective**: Document operational procedures

**Runbook Template**:
```markdown
# Glue Data Pipeline Operational Runbook

## Daily Operations

### Morning Checks (8 AM)
1. Check CloudWatch dashboard for overnight job executions
2. Verify data freshness in Iceberg tables
3. Review any failed jobs and rerun if needed
4. Check for data quality alerts

### End of Day (6 PM)
1. Review daily metrics and trends
2. Check S3 storage costs
3. Clean up temp files older than 7 days
4. Update status report

## Troubleshooting

### Job Failure
1. Check CloudWatch Logs for error details
2. Verify source system connectivity
3. Check IAM role permissions
4. Review recent code changes
5. Rerun with increased logging if needed

### Performance Issues
1. Check DPU utilization
2. Review partition strategy
3. Analyze Spark UI (if enabled)
4. Consider file compaction
5. Review join strategies

### Data Quality Issues
1. Run data quality validation
2. Check source data
3. Review transformation logic
4. Verify schema compatibility
5. Check for upstream changes

## Emergency Procedures

### Complete Pipeline Failure
1. Alert team via PagerDuty
2. Switch to backup data sources
3. Investigate root cause
4. Document incident
5. Implement fix
6. Post-mortem review

## Maintenance Tasks

### Weekly
- Review and expire old Iceberg snapshots
- Compact small files
- Update documentation

### Monthly
- Review and optimize costs
- Update dependencies
- Security patch review
- Disaster recovery test

## Contact Information
- On-call engineer: [phone/email]
- AWS Support: [case system]
- Team Slack: #data-pipeline
```

### Step 100: Continuous Improvement Process
**Objective**: Establish improvement cycle

**Metrics to Track**:
```python
# metrics_tracker.py
class PipelineMetrics:
    def __init__(self):
        self.metrics = {
            'reliability': {
                'sla_compliance': 0.99,
                'job_success_rate': 0.95,
                'data_quality_score': 0.98
            },
            'performance': {
                'avg_execution_time_minutes': 30,
                'records_per_second': 10000,
                'cost_per_gb': 0.05
            },
            'efficiency': {
                'dpu_utilization': 0.80,
                'storage_efficiency': 0.75,
                'code_reuse_percentage': 0.60
            }
        }
    
    def track_kpis(self):
        """Track key performance indicators"""
        kpis = {
            'Pipeline Health': self.metrics['reliability']['job_success_rate'],
            'Cost Efficiency': self.metrics['performance']['cost_per_gb'],
            'Data Quality': self.metrics['reliability']['data_quality_score'],
            'Performance': self.metrics['performance']['avg_execution_time_minutes']
        }
        return kpis
    
    def improvement_suggestions(self):
        """Suggest improvements based on metrics"""
        suggestions = []
        
        if self.metrics['efficiency']['dpu_utilization'] < 0.70:
            suggestions.append("Consider reducing DPU allocation")
        
        if self.metrics['performance']['cost_per_gb'] > 0.10:
            suggestions.append("Review compression and file formats")
        
        if self.metrics['reliability']['data_quality_score'] < 0.95:
            suggestions.append("Enhance data quality checks")
        
        return suggestions

# Post-mortem template
postmortem_template = """
# Post-Mortem: [Incident Title]

**Date**: [Date]
**Duration**: [Start - End]
**Impact**: [Description]

## Timeline
- [Time]: [Event]
- [Time]: [Event]

## Root Cause
[Detailed explanation]

## Resolution
[What was done to fix]

## Action Items
1. [ ] [Action with owner and deadline]
2. [ ] [Action with owner and deadline]

## Lessons Learned
- [Lesson 1]
- [Lesson 2]

## Prevention
[How to prevent similar issues]
"""
```

**Completion Message**:
```
🎉 Congratulations! You've completed all 100 steps!

You now have comprehensive knowledge of:
✓ AWS Glue fundamentals and advanced features
✓ Apache Iceberg table format and operations
✓ Cross-source data integration
✓ Production-grade data pipelines
✓ Monitoring and operational excellence

Next Steps:
1. Practice implementing in a sandbox environment
2. Build a proof-of-concept with real data
3. Prepare for DEA-C01 certification
4. Join AWS data engineering communities
5. Contribute to open-source Iceberg projects

Remember: Data engineering is a journey, not a destination!
Keep learning, keep optimizing, and keep building great data solutions.
```
