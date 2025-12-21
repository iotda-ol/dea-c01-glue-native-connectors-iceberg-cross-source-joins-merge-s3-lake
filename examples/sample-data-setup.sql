-- Sample Data Setup Scripts
-- Run these scripts to create sample data in your source systems

-- ============================================
-- Amazon Redshift - Sales Data
-- ============================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS public;

-- Create sales_data table
CREATE TABLE IF NOT EXISTS public.sales_data (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    region VARCHAR(50),
    payment_method VARCHAR(50),
    status VARCHAR(20)
);

-- Insert sample sales data
INSERT INTO public.sales_data 
    (transaction_id, customer_id, product_id, amount, transaction_date, region, payment_method, status)
VALUES
    ('TXN001', 'CUST001', 'PROD001', 199.99, '2024-01-15 10:30:00', 'US-EAST', 'CREDIT_CARD', 'COMPLETED'),
    ('TXN002', 'CUST002', 'PROD002', 299.99, '2024-01-15 11:45:00', 'US-WEST', 'DEBIT_CARD', 'COMPLETED'),
    ('TXN003', 'CUST001', 'PROD003', 89.99, '2024-01-16 09:15:00', 'US-EAST', 'CREDIT_CARD', 'COMPLETED'),
    ('TXN004', 'CUST003', 'PROD001', 199.99, '2024-01-16 14:20:00', 'US-CENTRAL', 'PAYPAL', 'COMPLETED'),
    ('TXN005', 'CUST004', 'PROD004', 449.99, '2024-01-17 16:30:00', 'US-WEST', 'CREDIT_CARD', 'PENDING'),
    ('TXN006', 'CUST002', 'PROD005', 159.99, '2024-01-17 10:00:00', 'US-WEST', 'CREDIT_CARD', 'COMPLETED'),
    ('TXN007', 'CUST005', 'PROD002', 299.99, '2024-01-18 11:30:00', 'US-EAST', 'DEBIT_CARD', 'COMPLETED'),
    ('TXN008', 'CUST003', 'PROD006', 79.99, '2024-01-18 15:45:00', 'US-CENTRAL', 'CREDIT_CARD', 'COMPLETED'),
    ('TXN009', 'CUST006', 'PROD001', 199.99, '2024-01-19 09:00:00', 'US-WEST', 'PAYPAL', 'COMPLETED'),
    ('TXN010', 'CUST001', 'PROD007', 349.99, '2024-01-19 13:20:00', 'US-EAST', 'CREDIT_CARD', 'COMPLETED');

-- Verify data
SELECT COUNT(*) as total_transactions FROM public.sales_data;
SELECT * FROM public.sales_data ORDER BY transaction_date DESC LIMIT 5;

-- ============================================
-- Teradata Vantage - Customer Data
-- ============================================

-- Create database if not exists
DATABASE PROD;

-- Create customer_data table
CREATE TABLE PROD.CUSTOMER_DATA (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(100) NOT NULL,
    customer_segment VARCHAR(50),
    registration_date TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    lifetime_value DECIMAL(18,2)
);

-- Insert sample customer data
INSERT INTO PROD.CUSTOMER_DATA 
    (customer_id, customer_name, customer_email, customer_segment, registration_date, 
     phone, address, city, state, country, lifetime_value)
VALUES
    ('CUST001', 'John Doe', 'john.doe@example.com', 'Premium', '2023-01-10 09:00:00',
     '+1-555-0101', '123 Main St', 'New York', 'NY', 'USA', 2500.00),
    ('CUST002', 'Jane Smith', 'jane.smith@example.com', 'Standard', '2023-02-15 10:30:00',
     '+1-555-0102', '456 Oak Ave', 'Los Angeles', 'CA', 'USA', 1800.00),
    ('CUST003', 'Bob Johnson', 'bob.johnson@example.com', 'Premium', '2023-03-20 14:15:00',
     '+1-555-0103', '789 Pine Rd', 'Chicago', 'IL', 'USA', 3200.00),
    ('CUST004', 'Alice Williams', 'alice.w@example.com', 'VIP', '2023-01-05 08:00:00',
     '+1-555-0104', '321 Elm St', 'San Francisco', 'CA', 'USA', 5000.00),
    ('CUST005', 'Charlie Brown', 'charlie.b@example.com', 'Standard', '2023-04-12 11:45:00',
     '+1-555-0105', '654 Maple Dr', 'Boston', 'MA', 'USA', 1500.00),
    ('CUST006', 'Diana Prince', 'diana.p@example.com', 'Premium', '2023-02-28 16:20:00',
     '+1-555-0106', '987 Cedar Ln', 'Seattle', 'WA', 'USA', 2800.00);

-- Verify data
SELECT COUNT(*) as total_customers FROM PROD.CUSTOMER_DATA;
SELECT * FROM PROD.CUSTOMER_DATA ORDER BY registration_date DESC;

-- ============================================
-- Google BigQuery - Product Data
-- ============================================

-- Note: Run this in BigQuery console or using bq CLI

-- Create dataset if not exists
CREATE SCHEMA IF NOT EXISTS `my-project.my_dataset`;

-- Create product_data table
CREATE TABLE IF NOT EXISTS `my-project.my_dataset.product_data` (
    product_id STRING NOT NULL,
    product_name STRING NOT NULL,
    category STRING,
    unit_price NUMERIC,
    stock_quantity INT64,
    supplier_id STRING,
    description STRING,
    weight_kg NUMERIC,
    dimensions STRING,
    created_date TIMESTAMP
);

-- Insert sample product data
INSERT INTO `my-project.my_dataset.product_data` 
    (product_id, product_name, category, unit_price, stock_quantity, 
     supplier_id, description, weight_kg, dimensions, created_date)
VALUES
    ('PROD001', 'Laptop Computer', 'Electronics', 899.99, 50, 'SUPP001', 
     '15-inch laptop with 16GB RAM', 2.5, '35x25x2 cm', CURRENT_TIMESTAMP()),
    ('PROD002', 'Office Chair', 'Furniture', 249.99, 100, 'SUPP002',
     'Ergonomic office chair with lumbar support', 15.0, '60x60x120 cm', CURRENT_TIMESTAMP()),
    ('PROD003', 'Wireless Mouse', 'Electronics', 29.99, 200, 'SUPP001',
     'Bluetooth wireless mouse', 0.1, '10x6x3 cm', CURRENT_TIMESTAMP()),
    ('PROD004', 'Monitor 27-inch', 'Electronics', 399.99, 75, 'SUPP001',
     '4K Ultra HD monitor', 5.5, '61x40x20 cm', CURRENT_TIMESTAMP()),
    ('PROD005', 'Desk Lamp', 'Furniture', 79.99, 150, 'SUPP003',
     'LED desk lamp with adjustable brightness', 1.2, '20x20x40 cm', CURRENT_TIMESTAMP()),
    ('PROD006', 'Keyboard Mechanical', 'Electronics', 129.99, 120, 'SUPP001',
     'RGB mechanical gaming keyboard', 1.0, '45x15x3 cm', CURRENT_TIMESTAMP()),
    ('PROD007', 'Webcam HD', 'Electronics', 89.99, 80, 'SUPP001',
     '1080p HD webcam with microphone', 0.3, '10x5x5 cm', CURRENT_TIMESTAMP());

-- Verify data
SELECT COUNT(*) as total_products FROM `my-project.my_dataset.product_data`;
SELECT * FROM `my-project.my_dataset.product_data` ORDER BY created_date DESC;

-- ============================================
-- Expected Join Results
-- ============================================

/*
After the ETL pipeline runs, the Iceberg table should contain:
- All transactions from Redshift (10 records)
- Joined with matching customers from Teradata (6 customers)
- Joined with matching products from BigQuery (7 products)

Expected output columns:
- transaction_id, customer_id, product_id
- sales_amount, transaction_timestamp
- customer_name, email, segment
- product_name, product_category, unit_price
- Derived fields: total_revenue, profit_margin, customer_lifetime_value
- Metadata: source_system, ingestion_timestamp, data_quality_flag
- Partitions: year, month, day

Sample query to validate (run in Athena):
SELECT 
    t.transaction_id,
    c.customer_name,
    p.product_name,
    t.sales_amount,
    c.segment as customer_segment
FROM iceberg_db.integrated_data
WHERE year = '2024' AND month = '01'
ORDER BY transaction_timestamp DESC
LIMIT 10;
*/

-- ============================================
-- Data Validation Queries
-- ============================================

-- Redshift: Check for data quality issues
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN amount IS NULL OR amount < 0 THEN 1 END) as invalid_amount,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as missing_customer,
    COUNT(CASE WHEN product_id IS NULL THEN 1 END) as missing_product
FROM public.sales_data;

-- Teradata: Check for data quality issues
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN customer_name IS NULL THEN 1 END) as missing_name,
    COUNT(CASE WHEN customer_email IS NULL THEN 1 END) as missing_email
FROM PROD.CUSTOMER_DATA;

-- BigQuery: Check for data quality issues
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN product_name IS NULL THEN 1 END) as missing_name,
    COUNT(CASE WHEN unit_price IS NULL OR unit_price <= 0 THEN 1 END) as invalid_price
FROM `my-project.my_dataset.product_data`;
