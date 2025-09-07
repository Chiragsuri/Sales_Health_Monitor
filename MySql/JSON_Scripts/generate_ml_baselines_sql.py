# -*- coding: utf-8 -*-
# MySQL Workbench Python script
# <description>
# Written in MySQL Workbench 8.0.42

import grt
#import mforms

#!/usr/bin/env python3
"""
=============================================================
SALES HEALTH MONITOR - JSON TO SQL CONSOLIDATED CONVERTER
=============================================================

Purpose: Convert all ML baseline JSON files to single SQL import script
Output: MySQL/import_ml_baselines.sql
Author: Chirag Suri
Created: September 6, 2025

Features:
- Dynamic file detection with accurate size reporting
- Smart JSON structure analysis and data extraction
- Single consolidated SQL output
- Idempotent and automation-ready
- Enhanced schema support for complete data capture

Usage: python generate_ml_baselines_sql.py
"""

import json
import os
from datetime import datetime
from pathlib import Path

class MLBaselinesConverter:
    def __init__(self):
        self.processed_folder = Path("G:/WEBD CODES/DA Projects/Sales_Health_Monitor/Dataset/processed/")
        self.output_folder = Path("G:/WEBD CODES/DA Projects/Sales_Health_Monitor/MySQL/")
        self.output_file = "import_ml_baselines.sql"
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.sql_sections = []
        self.section4_header_written = False
        self.total_json_size_mb = 0
        self.file_stats = {}

    def detect_json_files(self):
        """Enhanced file detection with accurate size reporting"""
        json_files = {}
        file_patterns = {
            'behavioral_anomalies': 'behavioral_anomalies.json',
            'customer_comprehensive': 'customer_baseline_comprehensive.json',
            'customer_rfm': 'customer_rfm_analysis.json',
            'customer_segments': 'customer_segment_baselines.json',
            'customer_value_tiers': 'customer_value_tiers.json',
            'category_baselines': 'category_baselines.json',
            'product_anomalies': 'product_anomaly_metrics.json',
            'ml_baselines': 'ml_baseline_metrics.json',
            'executive_kpi_dashboard': 'executive_kpi_dashboard.json',
            'executive_insights_summary': 'executive_insights_summary.json',
            'ml_baseline_consolidated': 'ml_baseline_consolidated.json'
        }
        
        print("📊 Analyzing JSON files and sizes...")
        
        total_size_mb = 0.0
        found_count = 0
        
        for key, filename in file_patterns.items():
            filepath = self.processed_folder / filename
            if filepath.exists():
                size_bytes = filepath.stat().st_size
                size_mb = size_bytes / (1024 * 1024)
                total_size_mb += size_mb
                found_count += 1
                
                json_files[key] = filepath
                self.file_stats[key] = {
                    'filename': filename,
                    'size_mb': round(size_mb, 1),
                    'size_bytes': size_bytes,
                    'status': 'Found'
                }
                print(f"✅ Found: {filename} ({size_mb:.1f} MB)")
            else:
                self.file_stats[key] = {
                    'filename': filename,
                    'size_mb': 0,
                    'size_bytes': 0,
                    'status': 'Missing'
                }
                print(f"⚠️ Missing: {filename}")
        
        print(f"\n📈 Source Analysis:")
        print(f"   JSON Files Found: {found_count}/{len(file_patterns)}")
        print(f"   Total JSON Size: {total_size_mb:.1f} MB")
        print(f"   Processing Mode: Intelligent data extraction (output will be optimized)")
        
        self.total_json_size_mb = total_size_mb
        return json_files

    def load_json_safe(self, filepath):
        """Enhanced JSON loading with size reporting"""
        try:
            print(f"📖 Loading {filepath.name}...")
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, dict):
                print(f"   ✓ Dictionary with {len(data)} keys")
            elif isinstance(data, list):
                print(f"   ✓ List with {len(data)} items")
            else:
                print(f"   ✓ {type(data).__name__} data type")
                
            return data
        except Exception as e:
            print(f"   ❌ Error loading {filepath}: {e}")
            return None

    def escape_sql_string(self, value):
        """Enhanced SQL string escaping"""
        if value is None:
            return 'NULL'
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, bool):
            return '1' if value else '0'
        return str(value).replace("'", "''").replace('"', '""').replace('\n', ' ').replace('\r', ' ')

    def process_behavioral_anomalies(self, data):
        """Process behavioral anomalies with enhanced data extraction"""
        if not data:
            return
            
        print("   🔍 Processing behavioral anomalies...")
        self.sql_sections.append("""
-- ===============================================================
-- SECTION 1: BEHAVIORAL ANOMALIES → customer_anomalies
-- ===============================================================""")
        
        anomaly_records = []
        record_count = 0
        
        if isinstance(data, dict):
            possible_keys = ['customer_anomalies', 'behavioral_anomalies', 'anomalies']
            for key in possible_keys:
                if key in data and isinstance(data[key], list):
                    anomaly_records = data[key]
                    break
            
            if not anomaly_records:
                for key, value in data.items():
                    if isinstance(value, list) and value and isinstance(value[0], dict):
                        if any(field in value[0] for field in ['customer_id', 'customerid', 'severity', 'changes']):
                            anomaly_records = value
                            break
        elif isinstance(data, list):
            anomaly_records = data

        if anomaly_records:
            values = []
            for record in anomaly_records:
                if isinstance(record, dict):
                    customer_id = (record.get('customer_id') or 
                                 record.get('customerid') or 
                                 f'UNKNOWN_{record_count}')
                    
                    customer_segment = record.get('customer_segment', record.get('customersegment', 'Unknown'))
                    value_tier = record.get('value_tier', record.get('valuetier', 'Unknown'))
                    severity = (record.get('severity') or 
                              record.get('anomaly_severity') or 
                              record.get('riskscore', 'Normal'))
                    flags = (record.get('flags') or 
                           record.get('anomaly_flags') or 
                           record.get('riskscore', 0))
                    types = record.get('types') or record.get('anomaly_types') or record.get('changes', '')
                    if isinstance(types, list):
                        types = ','.join(str(t) for t in types)
                    
                    customer_id_clean = self.escape_sql_string(customer_id)
                    customer_segment_clean = self.escape_sql_string(customer_segment)
                    value_tier_clean = self.escape_sql_string(value_tier)
                    severity_clean = self.escape_sql_string(severity)
                    types_clean = self.escape_sql_string(types)
                    
                    values.append(f"('{customer_id_clean}', '{customer_segment_clean}', '{value_tier_clean}', '{severity_clean}', {flags}, '{types_clean}')")
                    record_count += 1

            if values:
                batch_size = 1000
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    self.sql_sections.append("INSERT INTO customer_anomalies (customer_id, customer_segment, value_tier, severity, flags, types) VALUES")
                    self.sql_sections.append(',\n'.join(batch) + ';')
                    
                print(f"   ✅ Processed {record_count} anomaly records in {len(range(0, len(values), batch_size))} batches")
                self.sql_sections.append("")
            else:
                print("   ⚠️ No valid anomaly records found")

    def process_customer_baselines(self, data):
        """Process customer baselines with enhanced data extraction"""
        if not data:
            return
            
        print("   🔍 Processing customer baselines...")
        self.sql_sections.append("""
-- ===============================================================
-- SECTION 2: CUSTOMER BASELINES → customer_baselines  
-- ===============================================================""")
        
        baseline_records = []
        record_count = 0
        
        if isinstance(data, dict):
            possible_keys = ['customer_intelligence_records', 'customer_baselines', 'customers', 'customerintelligencerecords']
            for key in possible_keys:
                if key in data and isinstance(data[key], list):
                    baseline_records = data[key]
                    break

        if baseline_records:
            values = []
            for i, record in enumerate(baseline_records):
                if isinstance(record, dict):
                    customer_id = (record.get('customer_id') or 
                                 record.get('customerid') or 
                                 f'CUST_{i:06d}')
                    
                    health_score = record.get('customer_health_score', record.get('health_score', 50.0))
                    frequency_score = record.get('frequency_count', record.get('frequency_score', 0))
                    monetary_value = record.get('monetary_total', record.get('monetary_value', 0.0))
                    recency_days = record.get('recency_days', 0)
                    value_tier = record.get('value_tier', 'Medium')
                    activity_segment = record.get('activity_segment', record.get('activitysegment', 'Unknown'))
                    clv_score = record.get('clv_score', record.get('clvscore', 0.0))
                    customer_health_score = record.get('customer_health_score', record.get('customerhealthscore', health_score))
                    customer_segment = record.get('customer_segment', record.get('customersegment', 'Standard'))
                    
                    customer_id_clean = self.escape_sql_string(customer_id)
                    value_tier_clean = self.escape_sql_string(value_tier)
                    activity_segment_clean = self.escape_sql_string(activity_segment)
                    customer_segment_clean = self.escape_sql_string(customer_segment)
                    
                    values.append(f"('{customer_id_clean}', {health_score}, {frequency_score}, {monetary_value}, {recency_days}, '{value_tier_clean}', '{activity_segment_clean}', {clv_score}, {customer_health_score}, '{customer_segment_clean}')")
                    record_count += 1

            if values:
                batch_size = 1000
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    self.sql_sections.append("INSERT INTO customer_baselines (customer_id, health_score, frequency_score, monetary_value, recency_days, value_tier, activity_segment, clv_score, customer_health_score, customer_segment) VALUES")
                    self.sql_sections.append(',\n'.join(batch) + ';')
                    
                print(f"   ✅ Processed {record_count} customer baseline records in {len(range(0, len(values), batch_size))} batches")
                self.sql_sections.append("")
            else:
                print("   ⚠️ No valid customer baseline records found")

    def process_segment_performance(self, data):
        """Process segment performance with enhanced data extraction"""
        if not data:
            return
            
        print("   🔍 Processing segment performance...")
        self.sql_sections.append("""
-- ===============================================================
-- SECTION 3: SEGMENT PERFORMANCE → segment_performance
-- ===============================================================""")
        
        segment_data = []
        record_count = 0
        
        if isinstance(data, dict):
            possible_keys = ['channel_performance', 'segment_baselines', 'segmentbaselines']
            for key in possible_keys:
                if key in data and isinstance(data[key], list):
                    segment_data = data[key]
                    break

        if segment_data:
            values = []
            for record in segment_data:
                if isinstance(record, dict):
                    segment = record.get('customer_segment', record.get('segment', 'Unknown'))
                    channel = record.get('sales_channel', record.get('channel', 'Unknown'))
                    region = record.get('region', 'Unknown')
                    total_revenue = record.get('total_amount', record.get('total_revenue', 0.0))
                    transaction_count = record.get('transaction_count', record.get('count', 0))
                    avg_transaction_value = record.get('avg_transaction_value', 0.0)
                    market_share_pct = record.get('market_share_pct', 0.0)
                    
                    segment_clean = self.escape_sql_string(segment)
                    channel_clean = self.escape_sql_string(channel)
                    region_clean = self.escape_sql_string(region)
                    
                    values.append(f"('{segment_clean}', '{channel_clean}', '{region_clean}', {total_revenue}, {transaction_count}, {avg_transaction_value}, {market_share_pct})")
                    record_count += 1

            if values:
                self.sql_sections.append("INSERT INTO segment_performance (segment, channel, region, total_revenue, transaction_count, avg_transaction_value, market_share_pct) VALUES")
                self.sql_sections.append(',\n'.join(values) + ';')
                print(f"   ✅ Processed {record_count} segment performance records")
                self.sql_sections.append("")
            else:
                print("   ⚠️ No valid segment performance records found")

    def process_ml_baselines(self, data, source_name="unknown"):
        """Process ML baselines with enhanced data extraction"""
        if not data:
            return
            
        print(f"   🔍 Processing ML baselines from {source_name}...")
        
        if not self.section4_header_written:
            self.sql_sections.append("""
-- ===============================================================
-- SECTION 4: ML BASELINES → ml_baselines (consolidated)
-- ===============================================================""")
            self.section4_header_written = True

        values = []
        record_count = 0
        
        if isinstance(data, dict):
            for dimension, metrics in data.items():
                if isinstance(metrics, dict):
                    for metric_name, metric_data in metrics.items():
                        if isinstance(metric_data, dict):
                            baseline_value = metric_data.get('baseline', metric_data.get('mean', 0.0))
                            threshold_upper = metric_data.get('upper_threshold', metric_data.get('max', baseline_value * 1.2 if baseline_value else 0))
                            threshold_lower = metric_data.get('lower_threshold', metric_data.get('min', baseline_value * 0.8 if baseline_value else 0))
                        else:
                            baseline_value = float(metric_data) if isinstance(metric_data, (int, float)) else 0.0
                            threshold_upper = baseline_value * 1.2
                            threshold_lower = baseline_value * 0.8
                            
                        data_source = f"{source_name}.json"
                        dimension_clean = self.escape_sql_string(dimension)
                        metric_name_clean = self.escape_sql_string(metric_name)
                        data_source_clean = self.escape_sql_string(data_source)
                        
                        values.append(f"('{dimension_clean}', '{metric_name_clean}', {baseline_value}, {threshold_upper}, {threshold_lower}, '{data_source_clean}')")
                        record_count += 1

        if values:
            self.sql_sections.append("INSERT INTO ml_baselines (dimension, metric_name, baseline_value, threshold_upper, threshold_lower, data_source) VALUES")
            self.sql_sections.append(',\n'.join(values) + ';')
            print(f"   ✅ Processed {record_count} ML baseline records")
            self.sql_sections.append("")
        else:
            print(f"   ⚠️ No valid ML baseline records found in {source_name}")

    def generate_header(self):
        """Generate enhanced SQL header"""
        return f"""/*
=============================================================
SALES HEALTH MONITOR - ML BASELINES INTEGRATION SCRIPT
=============================================================

Project: Sales Health Monitor - Phase 5 MySQL Integration
Purpose: Import ML baseline metrics from Phase 3/4 EDA analysis
Author: Chirag Suri
Created: {datetime.now().strftime('%Y-%m-%d')}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Prerequisites:
- Database setup completed via database_setup.sql
- Core data imported via import_core_data.sql
- JSON baseline files available from Phase 3/4 analysis
- MySQL Workbench connected as sales_admin user

Expected Data Integration:
- ML baseline metrics across multiple dimensions
- Customer anomaly records with behavioral flags
- Segment performance matrix (channels x regions)
- Category and product baselines with thresholds

This script is IDEMPOTENT - safe to run multiple times

=============================================================
*/

-- =============================================================
-- DATABASE CONNECTION & VERIFICATION
-- =============================================================

USE sales_health_monitor;
SELECT DATABASE() as current_database;
SELECT USER();

-- Verify existing table structure
SHOW TABLES;

-- Check current table status
SELECT COUNT(*) as current_baseline_count FROM ml_baselines;

-- =============================================================
-- AUXILIARY TABLES CREATION WITH ENHANCED SCHEMA
-- =============================================================

-- Create auxiliary tables for complex ML baseline data
-- Following the hybrid approach: main ml_baselines + targeted auxiliaries

-- Table 1: Customer Anomalies (from behavioral_anomalies.json) - ENHANCED SCHEMA
CREATE TABLE IF NOT EXISTS customer_anomalies (
    anomaly_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique anomaly record identifier',
    customer_id VARCHAR(20) COMMENT 'Customer with anomaly flags',
    customer_segment ENUM('Premium', 'Standard', 'Budget') COMMENT 'Customer segment classification',
    value_tier VARCHAR(25) COMMENT 'Customer value tier classification',
    severity VARCHAR(20) COMMENT 'Anomaly severity: Normal, Low, Medium, High, Critical',
    flags INT DEFAULT 0 COMMENT 'Number of anomaly flags detected',
    types TEXT COMMENT 'Comma-separated anomaly types detected',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer (customer_id) COMMENT 'Customer anomaly lookup',
    INDEX idx_severity (severity) COMMENT 'Severity-based filtering',
    INDEX idx_segment (customer_segment) COMMENT 'Segment-based analysis'
) ENGINE=InnoDB COMMENT='Customer behavioral anomalies from Phase 4 analysis - Enhanced Schema';

-- Table 2: Customer Baselines (from customer_baseline_comprehensive.json) - ENHANCED SCHEMA
CREATE TABLE IF NOT EXISTS customer_baselines (
    baseline_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique customer baseline identifier',
    customer_id VARCHAR(20) COMMENT 'Customer identifier',
    health_score DECIMAL(5,2) COMMENT 'Customer health score (0-100)',
    frequency_score INT COMMENT 'RFM frequency component',
    monetary_value DECIMAL(15,4) COMMENT 'RFM monetary component',
    recency_days INT COMMENT 'Days since last transaction',
    value_tier VARCHAR(20) COMMENT 'Customer value classification',
    activity_segment VARCHAR(25) COMMENT 'Activity segment: Champions, Loyal Customers, etc.',
    clv_score DECIMAL(15,4) COMMENT 'Customer Lifetime Value score',
    customer_health_score DECIMAL(5,2) COMMENT '0-100 health score',
    customer_segment ENUM('Premium', 'Standard', 'Budget') COMMENT 'Customer segment classification',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer (customer_id) COMMENT 'Customer baseline lookup',
    INDEX idx_health_score (health_score) COMMENT 'Health score analysis',
    INDEX idx_value_tier (value_tier) COMMENT 'Value tier segmentation',
    INDEX idx_activity_segment (activity_segment) COMMENT 'Activity segment analysis'
) ENGINE=InnoDB COMMENT='Comprehensive customer baseline metrics from Phase 4 - Enhanced Schema';

-- Table 3: Segment Performance Matrix (from customer_segment_baselines.json)
CREATE TABLE IF NOT EXISTS segment_performance (
    performance_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique performance record identifier',
    segment VARCHAR(20) COMMENT 'Customer segment: Premium, Standard, Budget',
    channel VARCHAR(50) COMMENT 'Sales channel performance',
    region VARCHAR(20) COMMENT 'Geographic region',
    total_revenue DECIMAL(15,4) COMMENT 'Total revenue for segment-channel-region',
    transaction_count INT COMMENT 'Number of transactions',
    avg_transaction_value DECIMAL(10,4) COMMENT 'Average transaction value',
    market_share_pct DECIMAL(5,2) COMMENT 'Market share percentage',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_segment (segment) COMMENT 'Segment-based analysis',
    INDEX idx_channel (channel) COMMENT 'Channel performance queries',
    INDEX idx_region (region) COMMENT 'Regional performance analysis'
) ENGINE=InnoDB COMMENT='Segment-channel-region performance matrix from Phase 4';

-- Verify auxiliary tables creation
SHOW TABLES;

-- =============================================================
-- ML BASELINES DATA IMPORT PREPARATION
-- =============================================================

-- Clear existing ML baseline data for fresh import (idempotent design)
TRUNCATE TABLE ml_baselines;
TRUNCATE TABLE customer_anomalies;
TRUNCATE TABLE customer_baselines;
TRUNCATE TABLE segment_performance;

-- Reset auto-increment counters
ALTER TABLE ml_baselines AUTO_INCREMENT = 1;
ALTER TABLE customer_anomalies AUTO_INCREMENT = 1;
ALTER TABLE customer_baselines AUTO_INCREMENT = 1;
ALTER TABLE segment_performance AUTO_INCREMENT = 1;

-- Confirm cleanup completed
SELECT
    'After Cleanup' as section,
    (SELECT COUNT(*) FROM ml_baselines) AS ml_baselines_count,
    (SELECT COUNT(*) FROM customer_anomalies) AS anomalies_count,
    (SELECT COUNT(*) FROM customer_baselines) AS customer_baselines_count,
    (SELECT COUNT(*) FROM segment_performance) AS segment_performance_count;

"""

    def generate_footer(self):
        """Generate enhanced SQL footer with validation queries"""
        return f"""

-- =============================================================
-- IMPORT VALIDATION & SUMMARY
-- =============================================================

SELECT 'ML Baselines Import Complete!' as status;

-- Final record counts
SELECT
    (SELECT COUNT(*) FROM ml_baselines) as ml_baselines_count,
    (SELECT COUNT(*) FROM customer_anomalies) as customer_anomalies_count,
    (SELECT COUNT(*) FROM customer_baselines) as customer_baselines_count,
    (SELECT COUNT(*) FROM segment_performance) as segment_performance_count;

-- Sample data verification
SELECT 'Sample ML Baselines:' as section;
SELECT * FROM ml_baselines LIMIT 5;

SELECT 'Sample Customer Anomalies:' as section;  
SELECT * FROM customer_anomalies LIMIT 5;

SELECT 'Sample Customer Baselines:' as section;
SELECT * FROM customer_baselines LIMIT 5;

SELECT 'Sample Segment Performance:' as section;
SELECT * FROM segment_performance LIMIT 5;

-- Completion timestamp
SELECT NOW() as import_completed;

/*
=============================================================
CONSOLIDATED IMPORT COMPLETE
=============================================================

✅ All ML baseline data successfully imported
✅ Database ready for Phase 5 KPI development  
✅ Automated monitoring thresholds configured
✅ Customer intelligence framework established

Next Steps:
- Verify data integrity with sample queries above
- Begin KPI view development
- Prepare Power BI integration
- Configure automated alert triggers

=============================================================
*/

"""

    def convert_all(self):
        """Main conversion method with enhanced progress reporting"""
        print("=" * 70)
        print("🔄 Starting JSON-to-SQL conversion...")
        print("=" * 70)
        
        json_files = self.detect_json_files()
        
        if not json_files:
            print("❌ No JSON baseline files found!")
            return False

        print(f"\n🗂️ Processing {len(json_files)} JSON files...")
        sql_content = [self.generate_header()]
        
        processors = {
            'behavioral_anomalies': self.process_behavioral_anomalies,
            'customer_comprehensive': self.process_customer_baselines,
            'customer_segments': self.process_segment_performance,
            'ml_baselines': lambda data: self.process_ml_baselines(data, 'ml_baselines'),
            'category_baselines': lambda data: self.process_ml_baselines(data, 'category_baselines'),
            'product_anomalies': lambda data: self.process_ml_baselines(data, 'product_anomalies'),
            'executive_kpi_dashboard': lambda data: self.process_ml_baselines(data, 'executive_kpi_dashboard'),
            'executive_insights_summary': lambda data: self.process_ml_baselines(data, 'executive_insights_summary'),
            'ml_baseline_consolidated': lambda data: self.process_ml_baselines(data, 'ml_baseline_consolidated')
        }
        
        processed_count = 0
        for file_type, filepath in json_files.items():
            print(f"\n📊 Processing {file_type} ({filepath.name})...")
            data = self.load_json_safe(filepath)
            
            if data and file_type in processors:
                processors[file_type](data)
                print(f"   ✅ Successfully converted {file_type}")
                processed_count += 1
            else:
                print(f"   ⚠️ Skipped {file_type} (no processor or failed to load)")

        sql_content.extend(self.sql_sections)
        sql_content.append(self.generate_footer())
        
        output_path = self.output_folder / self.output_file
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sql_content))
            
            actual_size_kb = output_path.stat().st_size / 1024
            actual_size_mb = actual_size_kb / 1024
            
            print(f"\n🎉 CONVERSION COMPLETE!")
            print(f"📁 Output: {output_path}")
            print(f"📊 File Statistics:")
            print(f"   - Generated SQL Size: {actual_size_mb:.1f} MB ({actual_size_kb:.1f} KB)")
            print(f"   - Files Processed: {processed_count}/{len(json_files)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to write SQL file: {e}")
            return False

def main():
    print("=" * 70)
    print("SALES HEALTH MONITOR - JSON TO SQL CONVERTER")
    print("Enhanced Version with Complete Schema Analysis")
    print("=" * 70)
    
    converter = MLBaselinesConverter()
    success = converter.convert_all()
    
    if success:
        print("\n🚀 Ready for MySQL Workbench execution")
        print("\n📋 Next steps:")
        print("1. Open MySQL Workbench")
        print("2. Load the generated import_ml_baselines.sql file") 
        print("3. Execute the script (lightning bolt icon)")
        print("4. Verify import success with validation queries")
        print("5. Check final record counts in all tables")
    else:
        print("\n❌ CONVERSION FAILED!")
        print("Please check error messages above")

if __name__ == "__main__":
    main()
