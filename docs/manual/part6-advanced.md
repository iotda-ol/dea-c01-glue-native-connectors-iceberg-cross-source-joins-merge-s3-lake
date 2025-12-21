# Part 6: Advanced Operations & Optimization (Steps 81-95)

## Advanced Techniques and Performance Tuning

### Step 81: Implement Advanced Join Strategies
**Objective**: Optimize multi-source joins

```python
# advanced_joins.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def broadcast_small_tables(spark, large_df, small_df, join_key):
    """Use broadcast join for small dimension tables"""
    from pyspark.sql.functions import broadcast
    
    return large_df.join(broadcast(small_df), join_key)

def bucket_join_optimization(spark, df1, df2, num_buckets=100):
    """Pre-bucket tables for faster joins"""
    # Bucket tables
    df1.write.bucketBy(num_buckets, "customer_id").saveAsTable("bucketed_customers")
    df2.write.bucketBy(num_buckets, "customer_id").saveAsTable("bucketed_orders")
    
    # Join bucketed tables (no shuffle needed)
    result = spark.table("bucketed_customers").join(
        spark.table("bucketed_orders"), 
        "customer_id"
    )
    return result
```

### Step 82: Implement Data Caching Strategies
**Objective**: Cache frequently accessed data

```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Cache frequently used dataframes
customers_df = spark.table("iceberg_curated.customers")
customers_df.cache()
customers_df.count()  # Materialize cache

# Use persist for more control
from pyspark import StorageLevel
products_df = spark.table("iceberg_curated.products")
products_df.persist(StorageLevel.MEMORY_AND_DISK)
```

### Step 83: Implement Pushdown Predicates
**Objective**: Filter data at source

```python
# Predicate pushdown to Redshift
df = spark.read \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "(SELECT * FROM customers WHERE country = 'USA') as filtered") \
    .load()

# Partition pruning with Iceberg
df = spark.table("iceberg_curated.customers") \
    .filter(F.col("created_date") >= "2024-01-01") \  # Partition pruning
    .filter(F.col("country") == "USA")  # Additional filtering
```

### Step 84: Implement Custom UDFs
**Objective**: Create reusable transformation functions

```python
# custom_udfs.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
import hashlib

@udf(returnType=StringType())
def hash_email(email):
    """Hash email for PII protection"""
    if email:
        return hashlib.sha256(email.encode()).hexdigest()
    return None

@udf(returnType=StringType())
def categorize_customer(total_purchases):
    """Categorize customer by purchase amount"""
    if total_purchases > 10000:
        return "VIP"
    elif total_purchases > 5000:
        return "Premium"
    elif total_purchases > 1000:
        return "Regular"
    else:
        return "New"

# Usage
df_transformed = df.withColumn("email_hash", hash_email(F.col("email"))) \
                   .withColumn("customer_tier", categorize_customer(F.col("total_amount")))
```

### Step 85: Implement Window Functions
**Objective**: Perform advanced analytics

```python
# window_analytics.py
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

# Ranking customers by purchase amount per country
window_spec = Window.partitionBy("country").orderBy(F.desc("total_amount"))

result = df.withColumn("rank_in_country", rank().over(window_spec)) \
           .withColumn("row_num", row_number().over(window_spec))

# Calculate running totals
running_total_spec = Window.partitionBy("customer_id") \
                           .orderBy("order_date") \
                           .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_running_total = df.withColumn(
    "cumulative_amount", 
    F.sum("amount").over(running_total_spec)
)

# Previous/next order analysis
customer_window = Window.partitionBy("customer_id").orderBy("order_date")
df_with_prev_next = df.withColumn("previous_order_amount", lag("amount").over(customer_window)) \
                      .withColumn("next_order_amount", lead("amount").over(customer_window))
```

### Step 86: Implement Data Quality Framework
**Objective**: Comprehensive data validation

```python
# dq_framework.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull
from typing import List, Dict
import json

class DataQualityFramework:
    def __init__(self, spark):
        self.spark = spark
        self.results = []
    
    def check_completeness(self, df: DataFrame, columns: List[str], threshold: float = 0.95):
        """Check data completeness"""
        total_rows = df.count()
        for column in columns:
            non_null_count = df.filter(col(column).isNotNull()).count()
            completeness = non_null_count / total_rows if total_rows > 0 else 0
            
            result = {
                "check": "completeness",
                "column": column,
                "value": completeness,
                "threshold": threshold,
                "passed": completeness >= threshold
            }
            self.results.append(result)
    
    def check_uniqueness(self, df: DataFrame, columns: List[str]):
        """Check for duplicates"""
        for column in columns:
            total_count = df.count()
            distinct_count = df.select(column).distinct().count()
            uniqueness = distinct_count / total_count if total_count > 0 else 0
            
            result = {
                "check": "uniqueness",
                "column": column,
                "value": uniqueness,
                "passed": uniqueness == 1.0
            }
            self.results.append(result)
    
    def check_validity(self, df: DataFrame, column: str, valid_values: List):
        """Check if values are in valid set"""
        invalid_count = df.filter(~col(column).isin(valid_values)).count()
        total_count = df.count()
        validity = (total_count - invalid_count) / total_count if total_count > 0 else 0
        
        result = {
            "check": "validity",
            "column": column,
            "value": validity,
            "passed": validity == 1.0
        }
        self.results.append(result)
    
    def generate_report(self) -> Dict:
        """Generate DQ report"""
        passed_checks = sum(1 for r in self.results if r["passed"])
        total_checks = len(self.results)
        
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_checks": total_checks,
            "passed": passed_checks,
            "failed": total_checks - passed_checks,
            "success_rate": passed_checks / total_checks if total_checks > 0 else 0,
            "results": self.results
        }
        
        return report

# Usage
dq = DataQualityFramework(spark)
dq.check_completeness(customers_df, ["customer_id", "email", "country"])
dq.check_uniqueness(customers_df, ["customer_id", "email"])
dq.check_validity(customers_df, "country", ["USA", "UK", "Canada"])
report = dq.generate_report()
```

### Step 87: Implement Cost Optimization
**Objective**: Reduce AWS costs

```python
# cost_optimizer.py
class CostOptimizer:
    @staticmethod
    def optimize_dpu_count(record_count, avg_record_size_kb):
        """Calculate optimal DPU count"""
        data_size_gb = (record_count * avg_record_size_kb) / (1024 * 1024)
        
        # Rule of thumb: 1 DPU per 10GB
        recommended_dpus = max(2, min(100, int(data_size_gb / 10) + 2))
        return recommended_dpus
    
    @staticmethod
    def enable_auto_scaling(glue_client, job_name):
        """Enable Glue auto-scaling"""
        response = glue_client.update_job(
            JobName=job_name,
            JobUpdate={
                'MaxCapacity': 10.0,
                'ExecutionProperty': {
                    'MaxConcurrentRuns': 1
                },
                'GlueVersion': '4.0'
            }
        )
        return response
    
    @staticmethod
    def use_spot_instances():
        """Configure job bookmarks to avoid reprocessing"""
        job_args = {
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-metrics": "true",
            "--enable-spark-ui": "false"  # Disable when not debugging
        }
        return job_args
```

### Step 88: Implement Parallel Processing
**Objective**: Process multiple tables concurrently

```python
# parallel_processor.py
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

class ParallelProcessor:
    def __init__(self, max_workers=5):
        self.max_workers = max_workers
        self.glue = boto3.client('glue')
    
    def process_table(self, table_config):
        """Process single table"""
        job_name = table_config['job_name']
        args = table_config.get('arguments', {})
        
        response = self.glue.start_job_run(
            JobName=job_name,
            Arguments=args
        )
        
        return {
            'table': table_config['table_name'],
            'run_id': response['JobRunId'],
            'status': 'started'
        }
    
    def process_tables_parallel(self, table_configs):
        """Process multiple tables in parallel"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_table = {
                executor.submit(self.process_table, config): config['table_name']
                for config in table_configs
            }
            
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"Started processing: {table_name}")
                except Exception as e:
                    print(f"Error processing {table_name}: {str(e)}")
                    results.append({
                        'table': table_name,
                        'status': 'failed',
                        'error': str(e)
                    })
        
        return results

# Usage
processor = ParallelProcessor(max_workers=5)
tables = [
    {'job_name': 'extract-customers', 'table_name': 'customers'},
    {'job_name': 'extract-orders', 'table_name': 'orders'},
    {'job_name': 'extract-products', 'table_name': 'products'},
    {'job_name': 'extract-transactions', 'table_name': 'transactions'}
]
results = processor.process_tables_parallel(tables)
```

### Step 89: Implement Data Lineage Tracking
**Objective**: Track data flow and transformations

```python
# lineage_tracker.py
import json
from datetime import datetime

class LineageTracker:
    def __init__(self, s3_client, bucket_name):
        self.s3 = s3_client
        self.bucket = bucket_name
        self.lineage = []
    
    def record_transformation(self, source_table, target_table, 
                            transformation_type, job_name):
        """Record a transformation step"""
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": source_table,
            "target": target_table,
            "transformation": transformation_type,
            "job": job_name
        }
        self.lineage.append(record)
    
    def save_lineage(self, execution_id):
        """Save lineage to S3"""
        lineage_data = {
            "execution_id": execution_id,
            "execution_time": datetime.utcnow().isoformat(),
            "lineage": self.lineage
        }
        
        key = f"metadata/lineage/{execution_id}.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(lineage_data, indent=2)
        )

# Usage
tracker = LineageTracker(s3_client, bucket_name)
tracker.record_transformation(
    "redshift.customers", 
    "iceberg_curated.customers",
    "extract_and_load",
    "extract-customers-job"
)
tracker.save_lineage("exec-20241220-001")
```

### Step 90: Implement Monitoring Dashboard
**Objective**: Create CloudWatch dashboard

```python
# create_dashboard.py
import boto3
import json

def create_monitoring_dashboard():
    cloudwatch = boto3.client('cloudwatch')
    
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "properties": {
                    "metrics": [
                        ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", 
                         {"stat": "Sum"}]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-east-1",
                    "title": "Glue Job Tasks"
                }
            },
            {
                "type": "metric",
                "properties": {
                    "metrics": [
                        ["GlueJobs/Performance", "RecordsProcessed", 
                         {"stat": "Sum"}],
                        [".", "ExecutionTime", {"stat": "Average", "yAxis": "right"}]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-east-1",
                    "title": "Job Performance"
                }
            }
        ]
    }
    
    response = cloudwatch.put_dashboard(
        DashboardName='GlueDataPipeline',
        DashboardBody=json.dumps(dashboard_body)
    )
    
    return response
```

### Step 91-95: Additional Advanced Topics

**Step 91**: Implement CDC (Change Data Capture) using DMS
**Step 92**: Set up data encryption at rest and in transit
**Step 93**: Implement data masking for PII
**Step 94**: Create disaster recovery procedures
**Step 95**: Implement A/B testing for pipeline changes
