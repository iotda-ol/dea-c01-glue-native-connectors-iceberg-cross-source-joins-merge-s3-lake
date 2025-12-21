# Part 5: Iceberg Integration (Steps 66-80)

## Working with Apache Iceberg Tables

### Step 66: Understanding Iceberg Table Format
**Objective**: Learn Iceberg architecture

**Key Components**:
- **Metadata Files**: JSON files tracking table state
- **Manifest Lists**: Snapshot metadata
- **Manifest Files**: Lists of data files
- **Data Files**: Actual data in Parquet/ORC

### Step 67: Create First Iceberg Table
**Objective**: Create an Iceberg table using Glue

```python
# create_iceberg_table.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Configure Spark for Iceberg
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{bucket_name}/curated/iceberg/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

# Create table schema
schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("last_updated", TimestampType(), True)
])

# Create Iceberg table
spark.sql("""
    CREATE TABLE glue_catalog.iceberg_curated.customers (
        customer_id BIGINT,
        first_name STRING,
        last_name STRING,
        email STRING,
        country STRING,
        created_date DATE,
        last_updated TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (country)
    LOCATION 's3://{}/curated/iceberg/customers'
""".format(bucket_name))
```

### Step 68: Load Data into Iceberg Table
**Objective**: Populate Iceberg table from S3

```python
# load_to_iceberg.py
# Read from S3 (Parquet files from extraction)
df = spark.read.parquet("s3://bucket/raw/redshift/customers/")

# Write to Iceberg table
df.writeTo("glue_catalog.iceberg_curated.customers") \
  .using("iceberg") \
  .createOrReplace()

# Or append mode
df.writeTo("glue_catalog.iceberg_curated.customers") \
  .using("iceberg") \
  .append()
```

### Step 69: Implement Cross-Source Join
**Objective**: Join data from multiple sources

```python
# cross_source_join.py
from pyspark.sql import functions as F

# Read from Redshift extract (customers)
customers_df = spark.read.parquet("s3://bucket/raw/redshift/customers/")

# Read from Teradata extract (products)
products_df = spark.read.parquet("s3://bucket/raw/teradata/products/")

# Read from BigQuery extract (transactions)
transactions_df = spark.read.parquet("s3://bucket/raw/bigquery/transactions/")

# Read Redshift orders
orders_df = spark.read.parquet("s3://bucket/raw/redshift/orders/")

# Join all sources
enriched_df = transactions_df \
    .join(orders_df, "order_id", "left") \
    .join(customers_df, "customer_id", "left") \
    .join(products_df, 
          transactions_df.product_id == products_df.product_id, 
          "left") \
    .select(
        transactions_df.transaction_id,
        customers_df.first_name,
        customers_df.last_name,
        customers_df.email,
        customers_df.country,
        orders_df.order_date,
        orders_df.total_amount,
        products_df.product_name,
        products_df.category,
        transactions_df.payment_method,
        transactions_df.payment_status
    )

# Write to Iceberg
enriched_df.writeTo("glue_catalog.iceberg_curated.customer_transactions") \
    .using("iceberg") \
    .createOrReplace()
```

### Step 70: Implement MERGE Operations
**Objective**: Upsert data using Iceberg MERGE

```python
# iceberg_merge.py
# Read new/updated records
updates_df = spark.read.parquet("s3://bucket/processed/customer_updates/")

# Register as temp view
updates_df.createOrReplaceTempView("customer_updates")

# Perform MERGE operation
spark.sql("""
    MERGE INTO glue_catalog.iceberg_curated.customers t
    USING customer_updates s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("MERGE operation completed successfully")
```

### Step 71: Implement Schema Evolution
**Objective**: Add columns to existing table

```python
# Add new column to Iceberg table
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_curated.customers
    ADD COLUMN phone_number STRING AFTER email
""")

# Add multiple columns
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_curated.customers
    ADD COLUMNS (
        loyalty_points INT,
        customer_tier STRING
    )
""")
```

### Step 72: Implement Time Travel
**Objective**: Query historical data

```python
# Query table at specific timestamp
historical_df = spark.read \
    .option("as-of-timestamp", "2024-01-01 00:00:00") \
    .table("glue_catalog.iceberg_curated.customers")

# Query specific snapshot
snapshot_df = spark.read \
    .option("snapshot-id", 1234567890) \
    .table("glue_catalog.iceberg_curated.customers")

# View snapshots
spark.sql("""
    SELECT * FROM glue_catalog.iceberg_curated.customers.snapshots
""").show()
```

### Step 73: Implement Partition Evolution
**Objective**: Change partitioning strategy

```python
# Change partition spec
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_curated.customers
    REPLACE PARTITION FIELD country WITH months(created_date)
""")

# Hidden partitioning - no changes to queries needed
df = spark.table("glue_catalog.iceberg_curated.customers") \
    .filter("created_date >= '2024-01-01'")
```

### Step 74: Optimize Iceberg Tables
**Objective**: Compact files for better performance

```python
# Compact small files
spark.sql("""
    CALL glue_catalog.system.rewrite_data_files(
        table => 'iceberg_curated.customers',
        options => map('target-file-size-bytes', '536870912')
    )
""")

# Rewrite manifests
spark.sql("""
    CALL glue_catalog.system.rewrite_manifests('iceberg_curated.customers')
""")

# Expire old snapshots (keep 30 days)
spark.sql("""
    CALL glue_catalog.system.expire_snapshots(
        table => 'iceberg_curated.customers',
        older_than => TIMESTAMP '2024-01-01 00:00:00',
        retain_last => 10
    )
""")
```

### Step 75: Implement Table Maintenance
**Objective**: Regular table maintenance tasks

```python
# maintenance.py
from datetime import datetime, timedelta

class IcebergMaintenance:
    def __init__(self, spark, catalog="glue_catalog"):
        self.spark = spark
        self.catalog = catalog
    
    def expire_snapshots(self, table_name, days_to_keep=30):
        """Remove old snapshots"""
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
        
        self.spark.sql(f"""
            CALL {self.catalog}.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{cutoff_date} 00:00:00',
                retain_last => 5
            )
        """)
    
    def remove_orphan_files(self, table_name):
        """Remove files not referenced by any snapshot"""
        self.spark.sql(f"""
            CALL {self.catalog}.system.remove_orphan_files(
                table => '{table_name}'
            )
        """)
    
    def compact_files(self, table_name, target_size_mb=512):
        """Compact small files"""
        self.spark.sql(f"""
            CALL {self.catalog}.system.rewrite_data_files(
                table => '{table_name}',
                options => map('target-file-size-bytes', '{target_size_mb * 1024 * 1024}')
            )
        """)
```

### Step 76: Implement Data Versioning Strategy
**Objective**: Track data lineage and versions

```python
# versioning.py
def create_versioned_snapshot(spark, table_name, version_tag):
    """Create a tagged snapshot for data version"""
    # Get current snapshot ID
    snapshot_df = spark.sql(f"""
        SELECT snapshot_id, committed_at 
        FROM {table_name}.snapshots 
        ORDER BY committed_at DESC 
        LIMIT 1
    """)
    
    snapshot_id = snapshot_df.first()['snapshot_id']
    
    # Tag snapshot
    spark.sql(f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES ('snapshot.{version_tag}' = '{snapshot_id}')
    """)
    
    print(f"Created version tag: {version_tag} -> snapshot {snapshot_id}")

# Usage
create_versioned_snapshot(spark, "iceberg_curated.customers", "v1.0")
```

### Step 77: Implement Incremental Processing
**Objective**: Process only new data efficiently

```python
# incremental_processing.py
def process_incremental_changes(spark, table_name, last_snapshot_id):
    """
    Process only changes since last snapshot
    
    Args:
        spark: SparkSession
        table_name: Iceberg table name
        last_snapshot_id: Last processed snapshot ID
    
    Returns:
        DataFrame with incremental changes
    """
    # Read incremental data
    incremental_df = spark.read \
        .format("iceberg") \
        .option("start-snapshot-id", last_snapshot_id) \
        .option("end-snapshot-id", "current") \
        .table(table_name)
    
    return incremental_df

# Get current snapshot for next iteration
current_snapshot = spark.sql(f"""
    SELECT snapshot_id 
    FROM {table_name}.snapshots 
    ORDER BY committed_at DESC 
    LIMIT 1
""").first()['snapshot_id']
```

### Step 78: Set Up Table Statistics
**Objective**: Collect statistics for query optimization

```python
# Collect table statistics
spark.sql("""
    ANALYZE TABLE glue_catalog.iceberg_curated.customers
    COMPUTE STATISTICS
""")

# View table statistics
spark.sql("""
    DESCRIBE EXTENDED glue_catalog.iceberg_curated.customers
""").show(truncate=False)
```

### Step 79: Implement Row-Level Security
**Objective**: Filter data based on user permissions

```python
# row_level_security.py
def apply_row_filters(df, user_role, user_country=None):
    """
    Apply row-level filters based on user permissions
    
    Args:
        df: DataFrame to filter
        user_role: User role (admin, regional, country)
        user_country: User's country (for country role)
    
    Returns:
        Filtered DataFrame
    """
    if user_role == "admin":
        # Admin sees all data
        return df
    elif user_role == "regional":
        # Regional sees specific regions
        return df.filter(df.country.isin(['USA', 'Canada', 'Mexico']))
    elif user_role == "country":
        # Country user sees only their country
        return df.filter(df.country == user_country)
    else:
        # Default: no data
        return df.filter(F.lit(False))

# Usage
filtered_df = apply_row_filters(
    customers_df, 
    user_role="country", 
    user_country="USA"
)
```

### Step 80: Validate Iceberg Implementation
**Objective**: Ensure Iceberg tables are working correctly

```python
# validation.py
class IcebergValidator:
    def __init__(self, spark):
        self.spark = spark
    
    def validate_table_exists(self, table_name):
        """Check if table exists"""
        tables = self.spark.sql(f"SHOW TABLES IN iceberg_curated").collect()
        table_list = [row.tableName for row in tables]
        assert table_name.split('.')[-1] in table_list, f"Table {table_name} not found"
        print(f"✓ Table {table_name} exists")
    
    def validate_row_count(self, table_name, expected_min=0):
        """Validate minimum row count"""
        count = self.spark.table(table_name).count()
        assert count >= expected_min, f"Table has {count} rows, expected at least {expected_min}"
        print(f"✓ Table has {count} rows")
    
    def validate_partitions(self, table_name):
        """Check partition health"""
        partitions = self.spark.sql(f"SHOW PARTITIONS {table_name}").count()
        print(f"✓ Table has {partitions} partitions")
    
    def validate_snapshots(self, table_name):
        """Validate snapshot metadata"""
        snapshots = self.spark.sql(f"SELECT * FROM {table_name}.snapshots").count()
        assert snapshots > 0, "No snapshots found"
        print(f"✓ Table has {snapshots} snapshots")
    
    def run_all_validations(self, table_name):
        """Run all validation checks"""
        print(f"\n=== Validating {table_name} ===")
        self.validate_table_exists(table_name)
        self.validate_row_count(table_name)
        self.validate_snapshots(table_name)
        print("All validations passed!\n")

# Usage
validator = IcebergValidator(spark)
validator.run_all_validations("glue_catalog.iceberg_curated.customers")
```
