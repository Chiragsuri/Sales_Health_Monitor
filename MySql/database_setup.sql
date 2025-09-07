/*
=============================================================
SALES HEALTH MONITOR - DATABASE SETUP SCRIPT
=============================================================
Project: Sales Health Monitor - Phase 5 MySQL Integration
Purpose: Create database, users, tables, and initial structure
Author: Chirag Suri
Created: September 3, 2025
Version: 1.0 - Initial setup with idempotent guards

Prerequisites:
- MySQL Server 8.0+ installed and running
- MySQL Workbench connected as root user
- Sufficient disk space for 777K+ transaction records

Execution Time: ~15-30 minutes
Database Size: ~500MB when fully loaded

This script is IDEMPOTENT - safe to run multiple times
=============================================================
*/

-- =============================================================
-- ENVIRONMENT VERIFICATION & INITIAL CHECKS
-- =============================================================
-- Check MySQL version compatibility (requires 8.0+)

SELECT VERSION();
SHOW DATABASES;

-- =============================================================
-- DATABASE CREATION & SELECTION
-- =============================================================
-- Create dedicated database for Sales Health Monitor project
-- Uses IF NOT EXISTS for idempotent execution
CREATE DATABASE IF NOT EXISTS sales_health_monitor CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE sales_health_monitor;

-- Verify database creation and selection
SHOW DATABASES;
SELECT DATABASE();

-- =============================================================
-- USER MANAGEMENT & SECURITY SETUP
-- =============================================================
-- Create dedicated project user with secure credentials
-- Allows both local and external connections for Power BI integration

CREATE USER IF NOT EXISTS 'sales_admin'@'localhost' IDENTIFIED BY 'SalesHealth2025!';

-- Grant all privileges on our database
GRANT ALL PRIVILEGES ON sales_health_monitor.* TO 'sales_admin'@'localhost';

-- Allow external connections (for Power BI later)
CREATE USER IF NOT EXISTS 'sales_admin'@'%' IDENTIFIED BY 'SalesHealth2025!';
GRANT ALL PRIVILEGES ON sales_health_monitor.* TO 'sales_admin'@'%';

-- Apply changes
FLUSH PRIVILEGES;

-- =============================================================
-- TABLE 1: SALES TRANSACTIONS (MAIN FACT TABLE)
-- =============================================================
-- Primary transactional data table - handles 700K+ records
-- Includes computed columns for automatic time-based analysis
-- Optimized with strategic indexes for query performance

CREATE TABLE IF NOT EXISTS sales_transactions (
    -- Primary transaction identifiers
    transaction_id VARCHAR(20) PRIMARY KEY COMMENT 'Unique transaction identifier',
    transaction_date DATE NOT NULL COMMENT 'Transaction date for temporal analysis',
    transaction_datetime DATETIME NOT NULL COMMENT 'Exact transaction timestamp',
    
    -- Customer relationship (allows NULL for data quality issues)
    customer_id VARCHAR(20) COMMENT 'Links to customers table - NULL allowed for missing IDs',
    customer_segment ENUM('Premium', 'Standard', 'Budget') COMMENT 'Customer tier classification',
    
    -- Product relationship
    product_id VARCHAR(20) NOT NULL COMMENT 'Links to products table',
    product_category ENUM('Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors', 'Books & Media') 
        COMMENT 'Product category for analysis',
    
    -- Geographic and channel dimensions
    region ENUM('North', 'South', 'East', 'West', 'Central') COMMENT 'Geographic region',
    sales_channel ENUM('Online', 'Retail Store', 'Phone Orders', 'Mobile App') 
        COMMENT 'Sales channel classification',
    
    -- Financial metrics
    quantity INT NOT NULL COMMENT 'Number of items purchased',
    unit_price DECIMAL(10,2) NOT NULL COMMENT 'Price per unit before discount',
    discount_percent DECIMAL(5,2) DEFAULT 0 COMMENT 'Discount percentage applied',
    total_amount DECIMAL(12,2) NOT NULL COMMENT 'Final transaction amount after discount',
    
    -- COMPUTED COLUMNS - Automatic time-based features from Phase 3 analysis
    -- These eliminate need for repeated date calculations in queries
    year INT GENERATED ALWAYS AS (YEAR(transaction_date)) STORED COMMENT 'Extracted year for grouping',
    month INT GENERATED ALWAYS AS (MONTH(transaction_date)) STORED COMMENT 'Extracted month (1-12)',
    quarter INT GENERATED ALWAYS AS (QUARTER(transaction_date)) STORED COMMENT 'Business quarter (1-4)',
    day_of_week INT GENERATED ALWAYS AS (DAYOFWEEK(transaction_date)) STORED COMMENT 'Day of week (1=Sunday)',
    week_of_year INT GENERATED ALWAYS AS (WEEK(transaction_date)) STORED COMMENT 'Week number (1-52)',
    
    -- PERFORMANCE OPTIMIZATION INDEXES
    -- Strategic indexes based on expected query patterns from Phase 3/4 analysis
    INDEX idx_date (transaction_date) COMMENT 'Temporal analysis queries',
    INDEX idx_customer (customer_id) COMMENT 'Customer-based analysis',
    INDEX idx_product (product_id) COMMENT 'Product performance queries',
    INDEX idx_region_category (region, product_category) COMMENT 'Geographic-product cross analysis',
    INDEX idx_year_month (year, month) COMMENT 'Time-series aggregations'
) 
ENGINE=InnoDB 
COMMENT='Main sales transactions - supports 777K+ records with computed time features';

-- =============================================================
-- TABLE 2: CUSTOMERS (DIMENSION TABLE)
-- =============================================================
-- Customer master data - 50,000 customer records
-- Provides demographic and segmentation context for transactions

CREATE TABLE IF NOT EXISTS customers (
    -- Primary customer identification
    customer_id VARCHAR(20) PRIMARY KEY COMMENT 'Unique customer identifier',
    
    -- Personal information
    first_name VARCHAR(100) COMMENT 'Customer first name',
    last_name VARCHAR(100) COMMENT 'Customer last name', 
    email VARCHAR(255) COMMENT 'Customer email address',
    
    -- Business classification
    segment ENUM('Premium', 'Standard', 'Budget') COMMENT 'Customer value tier',
    region ENUM('North', 'South', 'East', 'West', 'Central') COMMENT 'Customer home region',
    
    -- Temporal information
    acquisition_date DATE COMMENT 'Date customer was acquired',
    
    -- INDEXES for customer analysis queries
    INDEX idx_segment (segment) COMMENT 'Segment-based analysis',
    INDEX idx_region (region) COMMENT 'Regional customer distribution'
)
ENGINE=InnoDB
COMMENT='Customer master data - 50,000 customer records with segmentation';

-- =============================================================
-- TABLE 3: PRODUCTS (DIMENSION TABLE)
-- =============================================================
-- Product catalog - 500 product records across 5 categories
-- Contains pricing, cost, and profitability information

CREATE TABLE IF NOT EXISTS products (
    -- Primary product identification  
    product_id VARCHAR(20) PRIMARY KEY COMMENT 'Unique product identifier',
    
    -- Product information
    product_name VARCHAR(255) COMMENT 'Full product name/description',
    category ENUM('Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors', 'Books & Media') 
        COMMENT 'Product category classification',
    
    -- Financial attributes
    price DECIMAL(10,2) COMMENT 'Standard retail price',
    cost DECIMAL(10,2) COMMENT 'Product cost basis',
    margin_percent DECIMAL(5,2) COMMENT 'Profit margin percentage',
    
    -- Lifecycle information
    launch_date DATE COMMENT 'Product launch date',
    
    -- INDEX for category-based analysis
    INDEX idx_category (category) COMMENT 'Category performance analysis'
)
ENGINE=InnoDB
COMMENT='Product catalog - 500 products with pricing and profitability data';

-- =============================================================
-- TABLE 4: ML BASELINES (ANALYTICS SUPPORT)
-- =============================================================
-- Stores machine learning baselines and thresholds from Phase 3/4 analysis
-- Supports automated anomaly detection and KPI monitoring

CREATE TABLE IF NOT EXISTS ml_baselines (
    -- Primary key
    baseline_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique baseline record identifier',
    
    -- Classification dimensions
    dimension VARCHAR(50) COMMENT 'Analysis dimension: temporal, geographic, product, customer',
    metric_name VARCHAR(100) COMMENT 'Specific metric name from Phase 3/4 analysis',
    
    -- Baseline values and thresholds
    baseline_value DECIMAL(15,4) COMMENT 'Established baseline value',
    threshold_upper DECIMAL(15,4) COMMENT 'Upper threshold for anomaly detection',
    threshold_lower DECIMAL(15,4) COMMENT 'Lower threshold for anomaly detection',
    
    -- Metadata
    data_source VARCHAR(100) COMMENT 'Source JSON file from Phase 3/4',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP 
        COMMENT 'Automatic timestamp tracking',
    
    -- INDEXES for baseline queries
    INDEX idx_dimension (dimension) COMMENT 'Dimension-based baseline lookup',
    INDEX idx_metric (metric_name) COMMENT 'Specific metric retrieval'
)
ENGINE=InnoDB
COMMENT='ML baselines and thresholds from Phase 3/4 EDA analysis';

-- =============================================================
-- TABLE 5: KPI RESULTS (PERFORMANCE CACHE)
-- =============================================================
-- Caches calculated KPI results for dashboard performance
-- Supports real-time business intelligence without recalculation

CREATE TABLE IF NOT EXISTS kpi_results (
    -- Primary identification
    kpi_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique KPI result identifier',
    
    -- KPI classification
    kpi_name VARCHAR(100) COMMENT 'Name of calculated KPI',
    kpi_category VARCHAR(50) COMMENT 'KPI grouping: financial, operational, quality',
    
    -- Results and timing
    kpi_value DECIMAL(15,4) COMMENT 'Calculated KPI value',
    period_type VARCHAR(20) COMMENT 'Time period: daily, monthly, quarterly, yearly',
    period_start DATE COMMENT 'Period start date',
    period_end DATE COMMENT 'Period end date',
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Calculation timestamp',
    
    -- INDEXES for KPI retrieval
    INDEX idx_kpi_name (kpi_name) COMMENT 'KPI-specific queries',
    INDEX idx_period (period_start, period_end) COMMENT 'Time-based KPI analysis'
)
ENGINE=InnoDB
COMMENT='Cached KPI results for dashboard performance optimization';

-- =============================================================
-- SETUP VERIFICATION & VALIDATION
-- =============================================================
-- Display all created tables for verification
SHOW TABLES;

-- Verify detailed structure of each table
DESCRIBE sales_transactions;
DESCRIBE customers;
DESCRIBE products; 
DESCRIBE ml_baselines;
DESCRIBE kpi_results;

-- Confirm storage engine and character set configuration
SELECT 
    TABLE_NAME,
    ENGINE,
    TABLE_COLLATION,
    TABLE_COMMENT
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'sales_health_monitor'
ORDER BY TABLE_NAME;

-- =============================================================
-- FUNCTIONALITY TESTING (OPTIONAL - SAFE TO RUN)
-- =============================================================
-- Test data insertion to verify table functionality
-- This section includes cleanup - no permanent test data remains

-- Test customer table functionality
INSERT INTO customers VALUES 
('CUST000001', 'John', 'Doe', 'john.doe@email.com', 'Standard', 'North', '2022-01-15');

-- Test product table functionality  
INSERT INTO products VALUES
('PROD0001', 'Test Laptop', 'Electronics', 999.99, 650.00, 35.0, '2022-01-01');

-- Test sales_transactions table with computed columns
INSERT INTO sales_transactions (
    transaction_id, transaction_date, transaction_datetime, 
    customer_id, customer_segment, product_id, product_category, 
    region, sales_channel, quantity, unit_price, discount_percent, total_amount
) VALUES (
    'TXN00000001', '2022-01-15', '2022-01-15 14:30:00', 
    'CUST000001', 'Standard', 'PROD0001', 'Electronics', 
    'North', 'Online', 1, 999.99, 0.0, 999.99
);

-- Verify computed columns are working correctly
SELECT 
    transaction_id,
    transaction_date,
    year,
    month, 
    quarter,
    day_of_week
FROM sales_transactions
WHERE transaction_id = 'TXN00000001';

-- =============================================================
-- TEST DATA CLEANUP
-- =============================================================
-- Remove all test data to keep database clean for production data
DELETE FROM sales_transactions WHERE transaction_id = 'TXN00000001';
DELETE FROM customers WHERE customer_id = 'CUST000001';
DELETE FROM products WHERE product_id = 'PROD0001';

-- =============================================================
-- SETUP COMPLETION CONFIRMATION
-- =============================================================
SELECT 'DATABASE SETUP COMPLETE' as Status,
       'Ready for Phase 5 Section 2 - Data Import' as Next_Step,
       NOW() as Completed_At;

/*
=============================================================
SETUP COMPLETE - NEXT STEPS
=============================================================
✅ Database 'sales_health_monitor' created successfully
✅ User 'sales_admin' configured with appropriate permissions  
✅ All 5 core tables created with optimized structure
✅ Computed columns and indexes configured for performance
✅ Test functionality verified and cleanup completed

READY FOR:
- Core data import (sales_cleaned.xlsx, customers.xlsx, products.xlsx)
- ML baseline data import from Phase 3/4 JSON files
- KPI views and business intelligence queries
- Future: Power BI integration and automated monitoring

DATABASE STATISTICS:
- Tables: 5 (optimized for 777K+ transactions)
- Storage Engine: InnoDB (ACID compliant)
- Character Set: UTF8MB4 (full Unicode support)
- Estimated Size: ~500MB when fully loaded
=============================================================
*/




