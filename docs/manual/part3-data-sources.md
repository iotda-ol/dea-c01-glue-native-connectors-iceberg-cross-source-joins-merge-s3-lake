# Part 3: Data Sources Setup (Steps 36-50)

## Configuring Source Systems

### Step 36: Redshift Cluster Setup
**Objective**: Create and configure Amazon Redshift cluster

```bash
# Create Redshift cluster
aws redshift create-cluster \
  --cluster-identifier my-redshift-cluster \
  --node-type dc2.large \
  --master-username admin \
  --master-user-password 'YourPassword123!' \
  --cluster-type single-node \
  --db-name analytics \
  --publicly-accessible false \
  --vpc-security-group-ids $SG_ID \
  --cluster-subnet-group-name my-subnet-group

# Wait for cluster to be available
aws redshift wait cluster-available \
  --cluster-identifier my-redshift-cluster

# Get cluster endpoint
REDSHIFT_ENDPOINT=$(aws redshift describe-clusters \
  --cluster-identifier my-redshift-cluster \
  --query 'Clusters[0].Endpoint.Address' \
  --output text)

echo "Redshift Endpoint: $REDSHIFT_ENDPOINT"
```

### Step 37: Create Sample Tables in Redshift
**Objective**: Set up test data in Redshift

```sql
-- Connect to Redshift using psql or query editor
-- Create customers table
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    country VARCHAR(50),
    created_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO customers VALUES
(1, 'John', 'Doe', 'john.doe@example.com', 'USA', '2023-01-15', CURRENT_TIMESTAMP),
(2, 'Jane', 'Smith', 'jane.smith@example.com', 'UK', '2023-02-20', CURRENT_TIMESTAMP),
(3, 'Bob', 'Johnson', 'bob.j@example.com', 'Canada', '2023-03-10', CURRENT_TIMESTAMP);

INSERT INTO orders VALUES
(101, 1, '2024-01-15', 150.00, 'completed', CURRENT_TIMESTAMP),
(102, 1, '2024-02-20', 200.50, 'completed', CURRENT_TIMESTAMP),
(103, 2, '2024-03-10', 75.25, 'pending', CURRENT_TIMESTAMP);

-- Grant read permissions to Glue user
GRANT SELECT ON customers TO glue_user;
GRANT SELECT ON orders TO glue_user;
```

### Step 38: Configure Teradata Connection
**Objective**: Set up Teradata access for Glue

**Prerequisites**:
- Teradata instance accessible from AWS
- Teradata JDBC driver
- Network connectivity (VPN or Direct Connect)

```bash
# Upload Teradata JDBC driver to S3
aws s3 cp terajdbc4.jar s3://$BUCKET_NAME/drivers/

# Create Glue connection for Teradata
aws glue create-connection \
  --connection-input '{
    "Name": "teradata-connection",
    "Description": "Connection to Teradata Vantage",
    "ConnectionType": "JDBC",
    "ConnectionProperties": {
      "JDBC_CONNECTION_URL": "jdbc:teradata://teradata.example.com/DATABASE=analytics",
      "USERNAME": "dbc",
      "PASSWORD": "password",
      "JDBC_DRIVER_CLASS_NAME": "com.teradata.jdbc.TeraDriver",
      "JDBC_DRIVER_JAR_URI": "s3://'$BUCKET_NAME'/drivers/terajdbc4.jar"
    },
    "PhysicalConnectionRequirements": {
      "SubnetId": "'$SUBNET1_ID'",
      "SecurityGroupIdList": ["'$SG_ID'"],
      "AvailabilityZone": "'${AWS_REGION}'a"
    }
  }'

# Test connection
aws glue get-connection --name teradata-connection
```

### Step 39: Create Sample Tables in Teradata
**Objective**: Set up test data in Teradata

```sql
-- Create products table in Teradata
CREATE TABLE products (
    product_id INTEGER NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    supplier_id INTEGER,
    created_date DATE,
    PRIMARY KEY (product_id)
);

-- Insert sample data
INSERT INTO products VALUES
(1001, 'Laptop Pro', 'Electronics', 1299.99, 50, 5001, DATE '2023-01-01'),
(1002, 'Wireless Mouse', 'Electronics', 29.99, 200, 5001, DATE '2023-01-01'),
(1003, 'Desk Chair', 'Furniture', 199.99, 30, 5002, DATE '2023-02-01');

-- Create suppliers table
CREATE TABLE suppliers (
    supplier_id INTEGER NOT NULL,
    supplier_name VARCHAR(200),
    country VARCHAR(50),
    contact_email VARCHAR(200),
    PRIMARY KEY (supplier_id)
);

INSERT INTO suppliers VALUES
(5001, 'Tech Supplies Inc', 'USA', 'contact@techsupplies.com'),
(5002, 'Office Furniture Co', 'Canada', 'info@officefurniture.com');

-- Grant access
GRANT SELECT ON products TO glue_user;
GRANT SELECT ON suppliers TO glue_user;
```

### Step 40: Configure BigQuery Access
**Objective**: Set up Google BigQuery integration

**Prerequisites**:
1. Google Cloud Platform account
2. BigQuery project created
3. Service account with BigQuery permissions
4. Service account key JSON file

```bash
# Install BigQuery Glue connector from AWS Marketplace
# Navigate to AWS Marketplace and subscribe to Google BigQuery connector

# Create Glue connection for BigQuery
aws glue create-connection \
  --connection-input '{
    "Name": "bigquery-connection",
    "Description": "Connection to Google BigQuery",
    "ConnectionType": "MARKETPLACE",
    "ConnectionProperties": {
      "CONNECTOR_TYPE": "Marketplace",
      "CONNECTOR_CLASS_NAME": "com.google.cloud.spark.bigquery.connector.BigQueryConnector",
      "dataSource": "bigquery",
      "projectId": "your-gcp-project-id",
      "credentials": "'$(cat bigquery-key.json | base64)'",
      "parentProject": "your-gcp-project-id"
    }
  }'
```

### Step 41: Create Sample Tables in BigQuery
**Objective**: Set up test data in BigQuery

```sql
-- Create dataset in BigQuery
CREATE SCHEMA analytics OPTIONS(
  description="Analytics data warehouse",
  location="US"
);

-- Create transactions table
CREATE OR REPLACE TABLE analytics.transactions (
  transaction_id INT64,
  order_id INT64,
  payment_method STRING,
  payment_status STRING,
  transaction_date DATE,
  amount NUMERIC,
  currency STRING,
  created_at TIMESTAMP
);

-- Insert sample data
INSERT INTO analytics.transactions VALUES
(20001, 101, 'credit_card', 'completed', DATE '2024-01-15', 150.00, 'USD', CURRENT_TIMESTAMP()),
(20002, 102, 'paypal', 'completed', DATE '2024-02-20', 200.50, 'USD', CURRENT_TIMESTAMP()),
(20003, 103, 'credit_card', 'pending', DATE '2024-03-10', 75.25, 'USD', CURRENT_TIMESTAMP());

-- Create customer_preferences table
CREATE OR REPLACE TABLE analytics.customer_preferences (
  customer_id INT64,
  preference_type STRING,
  preference_value STRING,
  updated_at TIMESTAMP
);

INSERT INTO analytics.customer_preferences VALUES
(1, 'newsletter', 'subscribed', CURRENT_TIMESTAMP()),
(1, 'notifications', 'email', CURRENT_TIMESTAMP()),
(2, 'newsletter', 'unsubscribed', CURRENT_TIMESTAMP()),
(3, 'notifications', 'sms', CURRENT_TIMESTAMP());

-- Grant access to service account
GRANT `roles/bigquery.dataViewer` ON SCHEMA analytics TO 'serviceAccount:glue-sa@your-project.iam.gserviceaccount.com';
```

### Step 42: Test Redshift Connection from Glue
**Objective**: Verify Redshift connectivity

```python
# Create test Glue job: test_redshift_connection.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Redshift
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:redshift://your-cluster.region.redshift.amazonaws.com:5439/analytics") \
    .option("dbtable", "customers") \
    .option("user", "admin") \
    .option("password", "YourPassword123!") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .load()

# Show sample data
df.show(5)
print(f"Total rows: {df.count()}")

job.commit()
```

### Step 43: Test Teradata Connection from Glue
**Objective**: Verify Teradata connectivity

```python
# Create test job: test_teradata_connection.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Use Glue connection
connection_options = {
    "useConnectionProperties": "true",
    "connectionName": "teradata-connection",
    "dbtable": "products"
}

# Read from Teradata using Glue connection
df = glueContext.create_dynamic_frame.from_options(
    connection_type="teradata",
    connection_options=connection_options
).toDF()

df.show(5)
print(f"Total products: {df.count()}")
```

### Step 44: Test BigQuery Connection from Glue
**Objective**: Verify BigQuery connectivity

```python
# Create test job: test_bigquery_connection.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Read from BigQuery
df = spark.read \
    .format("bigquery") \
    .option("project", "your-gcp-project-id") \
    .option("dataset", "analytics") \
    .option("table", "transactions") \
    .option("credentials", "/path/to/key.json") \
    .load()

df.show(5)
print(f"Total transactions: {df.count()}")
```

### Step 45: Create Glue Crawlers for Source Systems
**Objective**: Automatically discover schemas

```bash
# Create crawler for Redshift
aws glue create-crawler \
  --name redshift-crawler \
  --role GlueIcebergRole \
  --database-name iceberg_raw \
  --targets '{
    "JdbcTargets": [{
      "ConnectionName": "redshift-connection",
      "Path": "analytics/%"
    }]
  }' \
  --table-prefix "redshift_"

# Create crawler for Teradata
aws glue create-crawler \
  --name teradata-crawler \
  --role GlueIcebergRole \
  --database-name iceberg_raw \
  --targets '{
    "JdbcTargets": [{
      "ConnectionName": "teradata-connection",
      "Path": "analytics/%"
    }]
  }' \
  --table-prefix "teradata_"

# Run crawlers
aws glue start-crawler --name redshift-crawler
aws glue start-crawler --name teradata-crawler

# Check crawler status
aws glue get-crawler --name redshift-crawler --query 'Crawler.State'
```

### Step 46: Configure Network Access Between AWS and External Sources
**Objective**: Ensure secure connectivity

**For Teradata (On-premises)**:
1. Set up AWS Direct Connect or VPN
2. Configure route tables
3. Update security groups
4. Test connectivity

```bash
# Test connectivity from Glue VPC
# Create test endpoint
aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --service-name com.amazonaws.vpce.${AWS_REGION}.vpce-svc-teradata

# Verify routing
aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID"
```

**For BigQuery**:
- Configure service account permissions
- Set up Cloud Interconnect (optional)
- Use public internet with encryption

### Step 47: Set Up Data Sampling Strategy
**Objective**: Test with subset of data first

```python
# Sample data extraction utility
def sample_table(spark, connection_url, table_name, sample_fraction=0.01):
    """
    Read a sample of data from source table
    
    Args:
        spark: SparkSession
        connection_url: JDBC connection URL
        table_name: Source table name
        sample_fraction: Fraction of data to sample (default 1%)
    
    Returns:
        DataFrame with sampled data
    """
    # Read with pushdown predicate for sampling
    df = spark.read.jdbc(
        url=connection_url,
        table=f"(SELECT * FROM {table_name} SAMPLE {sample_fraction*100}) as sample",
        properties={"driver": "com.amazon.redshift.jdbc42.Driver"}
    )
    return df

# Usage
sampled_customers = sample_table(
    spark, 
    redshift_url, 
    "customers", 
    sample_fraction=0.1
)
```

### Step 48: Create Data Validation Scripts
**Objective**: Validate source data quality

```python
# data_validation.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when

def validate_data_quality(df: DataFrame, table_name: str):
    """
    Perform basic data quality checks
    
    Args:
        df: Spark DataFrame to validate
        table_name: Name of table for reporting
    
    Returns:
        dict with validation results
    """
    results = {
        "table": table_name,
        "total_rows": df.count(),
        "total_columns": len(df.columns),
        "null_counts": {},
        "duplicate_count": 0
    }
    
    # Check for nulls in each column
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        results["null_counts"][column] = null_count
    
    # Check for duplicates (if primary key exists)
    # results["duplicate_count"] = df.count() - df.dropDuplicates().count()
    
    print(f"\\n=== Data Quality Report: {table_name} ===")
    print(f"Total Rows: {results['total_rows']}")
    print(f"Total Columns: {results['total_columns']}")
    print(f"\\nNull Counts:")
    for col_name, null_cnt in results["null_counts"].items():
        if null_cnt > 0:
            print(f"  {col_name}: {null_cnt}")
    
    return results
```

### Step 49: Document Source Schema Mappings
**Objective**: Create schema mapping documentation

```yaml
# schema_mappings.yaml
source_systems:
  redshift:
    database: analytics
    tables:
      customers:
        columns:
          customer_id: {type: INTEGER, pk: true}
          first_name: {type: VARCHAR, nullable: false}
          last_name: {type: VARCHAR, nullable: false}
          email: {type: VARCHAR, unique: true}
          country: {type: VARCHAR}
          created_date: {type: DATE}
          last_updated: {type: TIMESTAMP}
      orders:
        columns:
          order_id: {type: INTEGER, pk: true}
          customer_id: {type: INTEGER, fk: customers.customer_id}
          order_date: {type: DATE}
          total_amount: {type: DECIMAL}
          status: {type: VARCHAR}
  
  teradata:
    database: analytics
    tables:
      products:
        columns:
          product_id: {type: INTEGER, pk: true}
          product_name: {type: VARCHAR}
          category: {type: VARCHAR}
          price: {type: DECIMAL}
          stock_quantity: {type: INTEGER}
          supplier_id: {type: INTEGER, fk: suppliers.supplier_id}
      suppliers:
        columns:
          supplier_id: {type: INTEGER, pk: true}
          supplier_name: {type: VARCHAR}
          country: {type: VARCHAR}
  
  bigquery:
    project: your-project
    dataset: analytics
    tables:
      transactions:
        columns:
          transaction_id: {type: INT64, pk: true}
          order_id: {type: INT64, fk: redshift.orders.order_id}
          payment_method: {type: STRING}
          payment_status: {type: STRING}
          amount: {type: NUMERIC}
```

### Step 50: Set Up Source System Monitoring
**Objective**: Monitor source system health and availability

```python
# source_monitor.py
import boto3
from datetime import datetime

def check_source_availability(glue_client, connection_name):
    """
    Test if source system is accessible
    
    Args:
        glue_client: boto3 Glue client
        connection_name: Glue connection name
    
    Returns:
        bool: True if available, False otherwise
    """
    try:
        response = glue_client.get_connection(Name=connection_name)
        print(f"✓ Connection {connection_name} is configured")
        return True
    except Exception as e:
        print(f"✗ Connection {connection_name} failed: {str(e)}")
        return False

def monitor_all_sources():
    """Monitor all configured source systems"""
    glue = boto3.client('glue')
    cloudwatch = boto3.client('cloudwatch')
    
    sources = ['redshift-connection', 'teradata-connection', 'bigquery-connection']
    
    for source in sources:
        available = check_source_availability(glue, source)
        
        # Send metric to CloudWatch
        cloudwatch.put_metric_data(
            Namespace='DataPipeline/Sources',
            MetricData=[{
                'MetricName': 'SourceAvailability',
                'Value': 1.0 if available else 0.0,
                'Unit': 'None',
                'Timestamp': datetime.utcnow(),
                'Dimensions': [{'Name': 'Source', 'Value': source}]
            }]
        )

if __name__ == "__main__":
    monitor_all_sources()
```
