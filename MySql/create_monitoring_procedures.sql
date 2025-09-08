/*
=============================================================
SALES HEALTH MONITOR - AUTOMATED MONITORING PROCEDURES
=============================================================

Project: Sales Health Monitor - Phase 5 MySQL Integration
Purpose: Create automated monitoring procedures and alert systems
Author: Chirag Suri
Created: September 8, 2025
Version: 1.0 - Automated monitoring with ML baseline integration

Prerequisites:
- Database setup completed via database_setup.sql
- Core data imported via import_core_data.sql
- ML baselines imported via import_ml_baselines.sql
- Foundation KPI views created via create_foundation_kpi_views.sql
- MySQL Workbench connected as sales_admin user

Key Monitoring Framework:
- Real-time revenue anomaly detection using ML baselines
- Customer health score monitoring and alerts
- Regional performance deviation tracking
- Product category anomaly detection
- Automated alert generation with severity classification
- Business intelligence health checks
This script is IDEMPOTENT - safe to run multiple times
=============================================================
*/

-- =============================================================
-- SECTION 1: DATABASE CONNECTION & VERIFICATION
-- =============================================================

USE sales_health_monitor;
SELECT DATABASE() as current_database;
SELECT USER();

-- Verify table structure and data availability
SHOW TABLES;

-- Verify foundation views and data availability
SELECT 
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS 
     WHERE TABLE_SCHEMA = 'sales_health_monitor' AND TABLE_NAME LIKE 'v_%') as foundation_views,
    (SELECT COUNT(*) FROM ml_baselines) as ml_baseline_metrics,
    (SELECT COUNT(*) FROM customer_baselines) as customer_intelligence_records,
    (SELECT COUNT(*) FROM sales_transactions) as transaction_records;

-- Check actual ML baseline dimensions (not hardcoded expectations)
SELECT 
    COUNT(DISTINCT dimension) as monitoring_dimensions,
    COUNT(*) as total_thresholds,
    MIN(last_updated) as oldest_baseline,
    MAX(last_updated) as newest_baseline
FROM ml_baselines;

-- Show available dimensions for monitoring setup
SELECT DISTINCT dimension FROM ml_baselines ORDER BY dimension;

-- =============================================================
-- SECTION 2: MONITORING CONFIGURATION TABLES
-- =============================================================

-- Drop existing monitoring tables for idempotent execution
DROP TABLE IF EXISTS monitoring_alerts;
DROP TABLE IF EXISTS monitoring_config;
DROP TABLE IF EXISTS monitoring_log;

-- Table 1: Monitoring Configuration
CREATE TABLE monitoring_config (
    config_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique configuration identifier',
    monitor_type VARCHAR(50) NOT NULL COMMENT 'Type: revenue, customer, regional, product',
    monitor_name VARCHAR(100) NOT NULL COMMENT 'Descriptive name for the monitor',
    baseline_dimension VARCHAR(50) COMMENT 'Links to ml_baselines.dimension',
    baseline_metric VARCHAR(100) COMMENT 'Links to ml_baselines.metric_name',
    threshold_upper DECIMAL(15,4) COMMENT 'Upper alert threshold',
    threshold_lower DECIMAL(15,4) COMMENT 'Lower alert threshold',
    check_frequency ENUM('real-time', 'hourly', 'daily', 'weekly') DEFAULT 'daily',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Enable/disable monitoring',
    alert_severity ENUM('low', 'medium', 'high', 'critical') DEFAULT 'medium',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_monitor_type (monitor_type),
    INDEX idx_active_monitors (is_active, check_frequency),
    INDEX idx_baseline_lookup (baseline_dimension, baseline_metric)
) ENGINE=InnoDB COMMENT='Monitoring system configuration and thresholds';

-- Table 2: Monitoring Alerts
CREATE TABLE monitoring_alerts (
    alert_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique alert identifier',
    config_id INT NOT NULL COMMENT 'Links to monitoring_config',
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    alert_type VARCHAR(50) NOT NULL COMMENT 'Type of anomaly detected',
    entity_id VARCHAR(50) COMMENT 'Affected entity (customer_id, region, etc.)',
    current_value DECIMAL(15,4) COMMENT 'Current measured value',
    baseline_value DECIMAL(15,4) COMMENT 'Expected baseline value',
    deviation_pct DECIMAL(8,2) COMMENT 'Percentage deviation from baseline',
    severity ENUM('low', 'medium', 'high', 'critical') NOT NULL,
    status ENUM('new', 'acknowledged', 'investigating', 'resolved') DEFAULT 'new',
    alert_message TEXT COMMENT 'Human-readable alert description',
    resolved_at TIMESTAMP NULL,
    resolved_by VARCHAR(100) NULL,
    
    INDEX idx_alert_timestamp (alert_timestamp),
    INDEX idx_alert_status (status, severity),
    INDEX idx_entity_alerts (entity_id, alert_type),
    FOREIGN KEY (config_id) REFERENCES monitoring_config(config_id)
) ENGINE=InnoDB COMMENT='Active and historical monitoring alerts';

-- Table 3: Monitoring Execution Log
CREATE TABLE monitoring_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique log entry identifier',
    procedure_name VARCHAR(100) NOT NULL COMMENT 'Name of monitoring procedure executed',
    execution_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_end TIMESTAMP NULL,
    execution_status ENUM('running', 'completed', 'failed', 'timeout') DEFAULT 'running',
    records_checked INT DEFAULT 0 COMMENT 'Number of records processed',
    alerts_generated INT DEFAULT 0 COMMENT 'Number of new alerts created',
    error_message TEXT NULL,
    
    INDEX idx_execution_time (execution_start),
    INDEX idx_procedure_status (procedure_name, execution_status)
) ENGINE=InnoDB COMMENT='Monitoring system execution tracking and performance';

-- Verify monitoring tables creation
SHOW TABLES LIKE 'monitoring_%';

-- =============================================================
-- SECTION 3: MONITORING CONFIGURATION SETUP
-- =============================================================

-- Insert monitoring configurations using available ML baseline data
INSERT INTO monitoring_config (monitor_type, monitor_name, baseline_dimension, baseline_metric, threshold_upper, threshold_lower, check_frequency, alert_severity)
SELECT 
    'revenue' as monitor_type,
    CONCAT('Revenue Monitor - ', UPPER(LEFT(dimension, 15))) as monitor_name,
    dimension,
    metric_name,
    threshold_upper,
    threshold_lower,
    'daily' as check_frequency,
    CASE 
        WHEN ABS(threshold_upper - baseline_value) > baseline_value * 0.5 THEN 'high'
        WHEN ABS(threshold_upper - baseline_value) > baseline_value * 0.25 THEN 'medium'
        ELSE 'low'
    END as alert_severity
FROM ml_baselines 
WHERE dimension IN ('revenue_intelligence', 'temporal_intelligence', 'revenue_thresholds')
AND metric_name IS NOT NULL
LIMIT 5;

-- Customer health monitoring configuration
INSERT INTO monitoring_config (monitor_type, monitor_name, threshold_upper, threshold_lower, check_frequency, alert_severity)
VALUES 
('customer', 'Critical Customer Health Score', 100, 25, 'daily', 'high'),
('customer', 'Customer Churn Risk Monitor', 365, 90, 'daily', 'medium'),
('customer', 'High Value Customer Alert', NULL, 75, 'daily', 'critical');

-- Regional performance monitoring configuration  
INSERT INTO monitoring_config (monitor_type, monitor_name, threshold_upper, threshold_lower, check_frequency, alert_severity)
VALUES 
('regional', 'Regional Market Share Deviation', 25.0, 15.0, 'daily', 'medium'),
('regional', 'Regional Revenue Drop Alert', NULL, -15.0, 'daily', 'high');

-- Product category monitoring configuration
INSERT INTO monitoring_config (monitor_type, monitor_name, threshold_upper, threshold_lower, check_frequency, alert_severity)
VALUES 
('product', 'Category Performance Anomaly', 150.0, 50.0, 'daily', 'medium'),
('product', 'Product Revenue Spike Alert', 200.0, NULL, 'daily', 'low');

-- Verify configuration setup
SELECT COUNT(*) as total_monitors,
       COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_monitors,
       COUNT(DISTINCT monitor_type) as monitor_types
FROM monitoring_config;

-- =============================================================
-- SECTION 4: MONITORING PROCEDURES WITH DYNAMIC DATE HANDLING
-- =============================================================

-- Drop existing procedures for idempotent execution
DROP PROCEDURE IF EXISTS sp_monitor_daily_revenue;
DROP PROCEDURE IF EXISTS sp_monitor_regional_performance;
DROP PROCEDURE IF EXISTS sp_monitor_customer_health;
DROP PROCEDURE IF EXISTS sp_monitor_category_performance;
DROP PROCEDURE IF EXISTS sp_run_all_monitoring;
DROP PROCEDURE IF EXISTS sp_monitoring_health_check;

-- Change delimiter for procedure creation
DELIMITER //

-- Procedure 1: Daily Revenue Monitoring (Dynamic Date Version)
CREATE PROCEDURE sp_monitor_daily_revenue()
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Monitor daily revenue against ML baselines with dynamic date handling'
BEGIN
    DECLARE v_log_id INT;
    DECLARE v_total_checked INT DEFAULT 0;
    DECLARE v_alerts_created INT DEFAULT 0;
    DECLARE v_baseline_revenue DECIMAL(15,4);
    DECLARE v_current_revenue DECIMAL(15,4);
    DECLARE v_deviation_pct DECIMAL(8,2);
    DECLARE v_start_date DATE;
    DECLARE v_end_date DATE;
    DECLARE v_eval_date DATE;
    
    -- Log procedure start
    INSERT INTO monitoring_log (procedure_name, execution_status, records_checked, alerts_generated)
    VALUES ('sp_monitor_daily_revenue', 'running', 0, 0);
    
    SET v_log_id = LAST_INSERT_ID();
    
    -- Get dynamic date range from actual data
    SELECT MIN(transaction_date), MAX(transaction_date) INTO v_start_date, v_end_date 
    FROM sales_transactions;
    
    -- Use latest date as evaluation date
    SET v_eval_date = v_end_date;
    
    -- Get baseline revenue from available ML baselines
    SELECT baseline_value INTO v_baseline_revenue
    FROM ml_baselines 
    WHERE dimension IN ('revenue_intelligence', 'revenue_thresholds', 'temporal_intelligence')
    AND metric_name LIKE '%revenue%'
    ORDER BY baseline_value DESC
    LIMIT 1;
    
    -- Get actual revenue for evaluation date
    SELECT COALESCE(SUM(total_amount), 0) INTO v_current_revenue
    FROM sales_transactions 
    WHERE transaction_date = v_eval_date;
    
    SET v_total_checked = 1;
    
    -- Calculate deviation percentage
    IF v_baseline_revenue > 0 THEN
        SET v_deviation_pct = ((v_current_revenue - v_baseline_revenue) / v_baseline_revenue) * 100;
    ELSE
        SET v_deviation_pct = 0;
    END IF;
    
    -- Check for revenue spike alert
    IF v_deviation_pct > 200 THEN
        INSERT INTO monitoring_alerts (config_id, alert_type, entity_id, current_value, baseline_value, deviation_pct, severity, alert_message)
        SELECT config_id, 'revenue_spike', 'daily_revenue', v_current_revenue, v_baseline_revenue, v_deviation_pct, 'high',
               CONCAT('Revenue spike detected on ', v_eval_date, ': ', FORMAT(v_current_revenue, 2), ' vs baseline ', FORMAT(v_baseline_revenue, 2), ' (', ROUND(v_deviation_pct, 1), '% increase)')
        FROM monitoring_config 
        WHERE monitor_type = 'revenue' AND is_active = TRUE
        LIMIT 1;
        
        SET v_alerts_created = v_alerts_created + 1;
    END IF;
    
    -- Check for revenue drop alert
    IF v_deviation_pct < -50 THEN
        INSERT INTO monitoring_alerts (config_id, alert_type, entity_id, current_value, baseline_value, deviation_pct, severity, alert_message)
        SELECT config_id, 'revenue_drop', 'daily_revenue', v_current_revenue, v_baseline_revenue, v_deviation_pct, 'critical',
               CONCAT('Revenue drop detected on ', v_eval_date, ': ', FORMAT(v_current_revenue, 2), ' vs baseline ', FORMAT(v_baseline_revenue, 2), ' (', ROUND(v_deviation_pct, 1), '% decrease)')
        FROM monitoring_config 
        WHERE monitor_type = 'revenue' AND is_active = TRUE
        LIMIT 1;
        
        SET v_alerts_created = v_alerts_created + 1;
    END IF;
    
    -- Update monitoring log
    UPDATE monitoring_log 
    SET execution_end = NOW(), 
        execution_status = 'completed',
        records_checked = v_total_checked,
        alerts_generated = v_alerts_created
    WHERE log_id = v_log_id;
    
    -- Return summary
    SELECT 'Daily Revenue Monitoring Complete' as status,
           v_eval_date as evaluation_date,
           v_current_revenue as current_revenue,
           v_baseline_revenue as baseline_revenue,
           v_deviation_pct as deviation_percentage,
           v_alerts_created as alerts_generated;
END //

-- Procedure 2: Regional Performance Monitoring (Dynamic Date Version)
CREATE PROCEDURE sp_monitor_regional_performance()
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Monitor regional performance deviations with dynamic date handling'
BEGIN
    DECLARE v_log_id INT;
    DECLARE v_total_checked INT DEFAULT 0;
    DECLARE v_alerts_created INT DEFAULT 0;
    DECLARE v_done INT DEFAULT FALSE;
    DECLARE v_region_name VARCHAR(20);
    DECLARE v_region_share DECIMAL(5,2);
    DECLARE v_expected_share DECIMAL(5,2) DEFAULT 20.0;
    DECLARE v_deviation_pct DECIMAL(8,2);
    DECLARE v_eval_date DATE;
    
    -- Cursor for regional performance data
    DECLARE region_cursor CURSOR FOR 
        SELECT region, market_share_pct 
        FROM v_geographic_performance;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
    
    -- Log procedure start
    INSERT INTO monitoring_log (procedure_name, execution_status)
    VALUES ('sp_monitor_regional_performance', 'running');
    
    SET v_log_id = LAST_INSERT_ID();
    
    -- Get evaluation date
    SELECT MAX(transaction_date) INTO v_eval_date FROM sales_transactions;
    
    -- Open cursor and process each region
    OPEN region_cursor;
    region_loop: LOOP
        FETCH region_cursor INTO v_region_name, v_region_share;
        
        IF v_done THEN
            LEAVE region_loop;
        END IF;
        
        SET v_total_checked = v_total_checked + 1;
        SET v_deviation_pct = ((v_region_share - v_expected_share) / v_expected_share) * 100;
        
        -- Alert for regions significantly below expected market share
        IF v_region_share < 17.0 THEN
            INSERT INTO monitoring_alerts (config_id, alert_type, entity_id, current_value, baseline_value, deviation_pct, severity, alert_message)
            SELECT config_id, 'regional_underperformance', v_region_name, v_region_share, v_expected_share, v_deviation_pct, 'medium',
                   CONCAT('Region ', v_region_name, ' underperforming: ', ROUND(v_region_share, 1), '% market share vs expected ', v_expected_share, '% (Eval Date: ', v_eval_date, ')')
            FROM monitoring_config 
            WHERE monitor_type = 'regional' AND is_active = TRUE
            LIMIT 1;
            
            SET v_alerts_created = v_alerts_created + 1;
        END IF;
        
    END LOOP;
    CLOSE region_cursor;
    
    -- Update monitoring log
    UPDATE monitoring_log 
    SET execution_end = NOW(), 
        execution_status = 'completed',
        records_checked = v_total_checked,
        alerts_generated = v_alerts_created
    WHERE log_id = v_log_id;
    
    -- Return summary
    SELECT 'Regional Performance Monitoring Complete' as status,
           v_eval_date as evaluation_date,
           v_total_checked as regions_analyzed,
           v_alerts_created as alerts_generated;
END //

-- Procedure 3: Customer Health Monitoring (Dynamic Date Version)
CREATE PROCEDURE sp_monitor_customer_health()
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Monitor customer health scores and churn risk indicators'
BEGIN
    DECLARE v_log_id INT;
    DECLARE v_total_checked INT DEFAULT 0;
    DECLARE v_alerts_created INT DEFAULT 0;
    DECLARE v_eval_date DATE;
    
    -- Log procedure start
    INSERT INTO monitoring_log (procedure_name, execution_status)
    VALUES ('sp_monitor_customer_health', 'running');
    
    SET v_log_id = LAST_INSERT_ID();
    
    -- Get evaluation date
    SELECT MAX(transaction_date) INTO v_eval_date FROM sales_transactions;
    
    -- Monitor critical customer health scores
    INSERT INTO monitoring_alerts (config_id, alert_type, entity_id, current_value, severity, alert_message)
    SELECT 
        mc.config_id,
        'critical_health_score',
        cb.customer_id,
        cb.customer_health_score,
        'critical',
        CONCAT('Customer ', cb.customer_id, ' has critical health score: ', ROUND(cb.customer_health_score, 1), ' (Value Tier: ', cb.value_tier, ', Eval Date: ', v_eval_date, ')')
    FROM customer_baselines cb
    JOIN monitoring_config mc ON mc.monitor_type = 'customer' AND mc.monitor_name = 'Critical Customer Health Score' AND mc.is_active = TRUE
    WHERE cb.customer_health_score < 25 
    AND cb.customer_id NOT IN (
        SELECT entity_id FROM monitoring_alerts 
        WHERE alert_type = 'critical_health_score' 
        AND status = 'new' 
        AND alert_timestamp >= DATE_SUB(v_eval_date, INTERVAL 1 DAY)
    );
    
    SET v_alerts_created = ROW_COUNT();
    SET v_total_checked = (SELECT COUNT(*) FROM customer_baselines WHERE customer_health_score < 25);
    
    -- Monitor high-value customers at risk
    INSERT INTO monitoring_alerts (config_id, alert_type, entity_id, current_value, severity, alert_message)
    SELECT 
        mc.config_id,
        'high_value_at_risk',
        cb.customer_id,
        cb.customer_health_score,
        'high',
        CONCAT('High-value customer at risk: ', cb.customer_id, ' (Health: ', ROUND(cb.customer_health_score, 1), ', CLV: ', FORMAT(cb.clv_score, 0), ', Eval Date: ', v_eval_date, ')')
    FROM customer_baselines cb
    JOIN monitoring_config mc ON mc.monitor_type = 'customer' AND mc.monitor_name = 'High Value Customer Alert' AND mc.is_active = TRUE
    WHERE cb.value_tier IN ('High Value', 'Premium') 
    AND cb.customer_health_score < 75
    AND cb.customer_id NOT IN (
        SELECT entity_id FROM monitoring_alerts 
        WHERE alert_type = 'high_value_at_risk' 
        AND status IN ('new', 'acknowledged')
        AND alert_timestamp >= DATE_SUB(v_eval_date, INTERVAL 7 DAY)
    );
    
    SET v_alerts_created = v_alerts_created + ROW_COUNT();
    
    -- Update monitoring log
    UPDATE monitoring_log 
    SET execution_end = NOW(), 
        execution_status = 'completed',
        records_checked = v_total_checked,
        alerts_generated = v_alerts_created
    WHERE log_id = v_log_id;
    
    -- Return summary
    SELECT 'Customer Health Monitoring Complete' as status,
           v_eval_date as evaluation_date,
           v_total_checked as customers_analyzed,
           v_alerts_created as alerts_generated;
END //

-- Procedure 4: Category Performance Monitoring (Dynamic Date Version)
CREATE PROCEDURE sp_monitor_category_performance()
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Monitor product category performance anomalies with dynamic date handling'
BEGIN
    DECLARE v_log_id INT;
    DECLARE v_total_checked INT DEFAULT 0;
    DECLARE v_alerts_created INT DEFAULT 0;
    DECLARE v_current_revenue DECIMAL(15,4);
    DECLARE v_avg_revenue DECIMAL(15,4);
    DECLARE v_deviation_pct DECIMAL(8,2);
    DECLARE v_eval_date DATE;
    DECLARE v_start_date DATE;
    
    -- Log procedure start
    INSERT INTO monitoring_log (procedure_name, execution_status)
    VALUES ('sp_monitor_category_performance', 'running');
    
    SET v_log_id = LAST_INSERT_ID();
    
    -- Get dynamic date range
    SELECT MIN(transaction_date), MAX(transaction_date) INTO v_start_date, v_eval_date 
    FROM sales_transactions;
    
    -- Check Electronics category for anomalies using dynamic dates
    SELECT 
        COALESCE(SUM(CASE WHEN transaction_date = v_eval_date THEN total_amount END), 0),
        AVG(daily_electronics)
    INTO v_current_revenue, v_avg_revenue
    FROM (
        SELECT DATE(transaction_date) as date, SUM(total_amount) as daily_electronics
        FROM sales_transactions 
        WHERE product_category = 'Electronics'
        AND transaction_date >= DATE_SUB(v_eval_date, INTERVAL 30 DAY)
        AND transaction_date <= v_eval_date
        GROUP BY DATE(transaction_date)
    ) avg_calc;
    
    SET v_total_checked = 1;
    
    -- Calculate deviation
    IF v_avg_revenue > 0 THEN
        SET v_deviation_pct = ((v_current_revenue - v_avg_revenue) / v_avg_revenue) * 100;
        
        -- Alert for significant Electronics category anomalies
        IF ABS(v_deviation_pct) > 50 THEN
            INSERT INTO monitoring_alerts (config_id, alert_type, entity_id, current_value, baseline_value, deviation_pct, severity, alert_message)
            SELECT 
                config_id, 
                CASE WHEN v_deviation_pct > 0 THEN 'category_spike' ELSE 'category_drop' END,
                'Electronics',
                v_current_revenue,
                v_avg_revenue,
                v_deviation_pct,
                CASE WHEN ABS(v_deviation_pct) > 100 THEN 'high' ELSE 'medium' END,
                CONCAT('Electronics category anomaly on ', v_eval_date, ': ', FORMAT(v_current_revenue, 0), ' vs 30-day avg ', FORMAT(v_avg_revenue, 0), ' (', ROUND(v_deviation_pct, 1), '% change)')
            FROM monitoring_config 
            WHERE monitor_type = 'product' AND is_active = TRUE
            LIMIT 1;
            
            SET v_alerts_created = 1;
        END IF;
    END IF;
    
    -- Update monitoring log
    UPDATE monitoring_log 
    SET execution_end = NOW(), 
        execution_status = 'completed',
        records_checked = v_total_checked,
        alerts_generated = v_alerts_created
    WHERE log_id = v_log_id;
    
    -- Return summary
    SELECT 'Category Performance Monitoring Complete' as status,
           v_eval_date as evaluation_date,
           v_current_revenue as electronics_current,
           v_avg_revenue as electronics_avg_30d,
           v_deviation_pct as deviation_percentage,
           v_alerts_created as alerts_generated;
END //

-- Master monitoring procedure (Dynamic Date Version)
CREATE PROCEDURE sp_run_all_monitoring()
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Execute all active monitoring procedures with dynamic date handling'
BEGIN
    DECLARE v_master_log_id INT;
    DECLARE v_total_alerts INT DEFAULT 0;
    DECLARE v_eval_date DATE;
    
    -- Log master procedure start
    INSERT INTO monitoring_log (procedure_name, execution_status)
    VALUES ('sp_run_all_monitoring', 'running');
    
    SET v_master_log_id = LAST_INSERT_ID();
    
    -- Get evaluation date
    SELECT MAX(transaction_date) INTO v_eval_date FROM sales_transactions;
    
    -- Execute all monitoring procedures
    CALL sp_monitor_daily_revenue();
    CALL sp_monitor_regional_performance();
    CALL sp_monitor_customer_health();
    CALL sp_monitor_category_performance();
    
    -- Count total alerts generated for evaluation period
    SELECT COUNT(*) INTO v_total_alerts
    FROM monitoring_alerts 
    WHERE alert_timestamp >= DATE_SUB(NOW(), INTERVAL 1 DAY);
    
    -- Update master log
    UPDATE monitoring_log 
    SET execution_end = NOW(), 
        execution_status = 'completed',
        records_checked = 4,
        alerts_generated = v_total_alerts
    WHERE log_id = v_master_log_id;
    
    -- Return monitoring summary
    SELECT 
        'All Monitoring Procedures Complete' as status,
        v_eval_date as evaluation_date,
        NOW() as execution_time,
        v_total_alerts as total_alerts_today,
        (SELECT COUNT(*) FROM monitoring_alerts WHERE status = 'new') as unresolved_alerts,
        (SELECT COUNT(*) FROM monitoring_config WHERE is_active = TRUE) as active_monitors;
END //

-- Health check procedure for monitoring system
CREATE PROCEDURE sp_monitoring_health_check()
READS SQL DATA
DETERMINISTIC
COMMENT 'Perform health check on monitoring system components'
BEGIN
    DECLARE v_eval_date DATE;
    
    -- Get evaluation date
    SELECT MAX(transaction_date) INTO v_eval_date FROM sales_transactions;
    
    -- Check table health
    SELECT 
        (SELECT COUNT(*) FROM monitoring_config) as config_records,
        (SELECT COUNT(*) FROM monitoring_alerts) as total_alerts,
        (SELECT COUNT(*) FROM monitoring_log) as log_entries,
        v_eval_date as data_evaluation_date;
    
    -- Check recent monitoring activity
    SELECT 
        (SELECT COUNT(*) FROM monitoring_log WHERE execution_start >= DATE_SUB(NOW(), INTERVAL 24 HOUR)) as procedures_24h,
        (SELECT COUNT(*) FROM monitoring_alerts WHERE alert_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)) as alerts_24h,
        CASE 
            WHEN (SELECT COUNT(*) FROM monitoring_log WHERE execution_start >= DATE_SUB(NOW(), INTERVAL 24 HOUR)) > 0 
            THEN 'ACTIVE' 
            ELSE 'INACTIVE' 
        END as status;
    
    -- Check alert distribution by severity
    SELECT 
        severity,
        COUNT(*) as alert_count,
        COUNT(CASE WHEN status = 'new' THEN 1 END) as unresolved_count
    FROM monitoring_alerts 
    WHERE alert_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
    GROUP BY severity
    ORDER BY FIELD(severity, 'critical', 'high', 'medium', 'low');
    
    -- Check monitoring configuration status
    SELECT 
        monitor_type,
        COUNT(*) as total_monitors,
        COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_monitors
    FROM monitoring_config 
    GROUP BY monitor_type;
END //

DELIMITER ;

-- =============================================================
-- SECTION 5: MONITORING VIEWS FOR DASHBOARD INTEGRATION
-- =============================================================

-- Drop existing monitoring views for idempotent execution
DROP VIEW IF EXISTS v_active_alerts;
DROP VIEW IF EXISTS v_monitoring_summary;
DROP VIEW IF EXISTS v_alert_trends;

-- View 1: Active Alerts Dashboard
CREATE VIEW v_active_alerts AS
SELECT 
    ma.alert_id,
    ma.alert_timestamp,
    mc.monitor_name,
    ma.alert_type,
    ma.entity_id,
    ma.current_value,
    ma.baseline_value,
    ma.deviation_pct,
    ma.severity,
    ma.status,
    ma.alert_message,
    TIMESTAMPDIFF(HOUR, ma.alert_timestamp, NOW()) as hours_since_alert
FROM monitoring_alerts ma
JOIN monitoring_config mc ON ma.config_id = mc.config_id
WHERE ma.status IN ('new', 'acknowledged')
ORDER BY FIELD(ma.severity, 'critical', 'high', 'medium', 'low'), ma.alert_timestamp DESC;

-- View 2: Monitoring System Summary
CREATE VIEW v_monitoring_summary AS
SELECT 
    (SELECT COUNT(*) FROM monitoring_config WHERE is_active = TRUE) as active_monitors,
    (SELECT COUNT(*) FROM monitoring_alerts WHERE status = 'new') as new_alerts,
    (SELECT COUNT(*) FROM monitoring_alerts WHERE alert_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)) as alerts_today,
    (SELECT COUNT(*) FROM monitoring_alerts WHERE alert_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)) as alerts_7days,
    (SELECT execution_start FROM monitoring_log WHERE procedure_name = 'sp_run_all_monitoring' ORDER BY execution_start DESC LIMIT 1) as last_monitoring_run,
    (SELECT COUNT(*) FROM monitoring_log WHERE execution_status = 'completed' AND execution_start >= DATE_SUB(NOW(), INTERVAL 24 HOUR)) as successful_runs_24h,
    (SELECT MAX(transaction_date) FROM sales_transactions) as data_evaluation_date;

-- View 3: Alert Trends Analysis
CREATE VIEW v_alert_trends AS
SELECT 
    DATE(alert_timestamp) as alert_date,
    COUNT(*) as total_alerts,
    COUNT(CASE WHEN severity = 'critical' THEN 1 END) as critical_alerts,
    COUNT(CASE WHEN severity = 'high' THEN 1 END) as high_alerts,
    COUNT(CASE WHEN severity = 'medium' THEN 1 END) as medium_alerts,
    COUNT(CASE WHEN severity = 'low' THEN 1 END) as low_alerts,
    COUNT(DISTINCT entity_id) as affected_entities
FROM monitoring_alerts 
WHERE alert_timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY DATE(alert_timestamp)
ORDER BY alert_date DESC;

-- =============================================================
-- SECTION 6: VALIDATION & TESTING
-- =============================================================

-- Verify procedures were created
SELECT 
    COUNT(*) as procedures_created
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'sales_health_monitor' 
AND ROUTINE_TYPE = 'PROCEDURE'
AND ROUTINE_NAME LIKE 'sp_monitor_%';

-- List all created procedures
SELECT 
    ROUTINE_NAME as procedure_name,
    CREATED as created_date
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'sales_health_monitor' 
AND ROUTINE_TYPE = 'PROCEDURE'
AND ROUTINE_NAME LIKE 'sp_%'
ORDER BY CREATED;

-- Verify monitoring tables have data
SELECT 
    COUNT(*) as total_configs,
    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_configs
FROM monitoring_config;

-- Verify monitoring views are accessible
SELECT 
    COUNT(*) as monitoring_views
FROM INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'sales_health_monitor' 
AND (TABLE_NAME LIKE 'v_%alert%' OR TABLE_NAME LIKE 'v_%monitoring%');

-- Run initial health check
CALL sp_monitoring_health_check();

-- Display monitoring summary
SELECT * FROM v_monitoring_summary;

-- Test data availability for monitoring
SELECT 
    (SELECT COUNT(*) FROM ml_baselines WHERE dimension IN ('revenue_intelligence', 'temporal_intelligence', 'revenue_thresholds')) as revenue_baselines_available,
    (SELECT COUNT(*) FROM sales_transactions WHERE transaction_date >= (SELECT MAX(transaction_date) FROM sales_transactions) - INTERVAL 30 DAY) as recent_transactions_30d,
    (SELECT MIN(transaction_date) FROM sales_transactions) as data_start_date,
    (SELECT MAX(transaction_date) FROM sales_transactions) as data_end_date;

-- Test customer health monitoring data
SELECT 
    (SELECT COUNT(*) FROM customer_baselines) as customer_baselines_available,
    (SELECT COUNT(*) FROM customer_baselines WHERE customer_health_score < 50) as customers_at_risk;

-- Test regional monitoring data  
SELECT * FROM v_geographic_performance LIMIT 3;

-- =============================================================
-- SECTION 7: COMPLETION STATUS & NEXT STEPS
-- =============================================================

SELECT 
    'Monitoring System Setup Complete!' as status,
    NOW() as completed_at;

-- Final system statistics
SELECT 
    (SELECT COUNT(*) FROM monitoring_config) as monitoring_rules_configured,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'sales_health_monitor' AND ROUTINE_NAME LIKE 'sp_monitor_%') as monitoring_procedures_created,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'sales_health_monitor' AND (TABLE_NAME LIKE 'v_%alert%' OR TABLE_NAME LIKE 'v_%monitoring%')) as monitoring_views_created;

-- Verify system readiness
SELECT 
    'System Readiness Check' as section,
    CASE 
        WHEN (SELECT COUNT(*) FROM monitoring_config WHERE is_active = TRUE) > 0 THEN 'READY'
        ELSE 'NOT READY'
    END as monitoring_status,
    CASE 
        WHEN (SELECT COUNT(*) FROM ml_baselines) > 0 THEN 'READY'
        ELSE 'NOT READY'
    END as baselines_status,
    CASE 
        WHEN (SELECT COUNT(*) FROM customer_baselines) > 0 THEN 'READY'
        ELSE 'NOT READY'
    END as customer_intelligence_status,
    CASE 
        WHEN (SELECT COUNT(*) FROM sales_transactions) > 0 THEN 'READY'
        ELSE 'NOT READY'
    END as sales_data_status;

/*

=============================================================
AUTOMATED MONITORING SYSTEM COMPLETE
=============================================================

✅ Key Improvements Made:
- Dynamic date handling (no hardcoded CURDATE())
- Uses actual ML baseline dimensions from your data
- Evaluation based on MAX(transaction_date) from your dataset
- Compatible with historical data (2022-2024 range)

✅ Monitoring Infrastructure Created:
- monitoring_config: Configuration and thresholds
- monitoring_alerts: Alert tracking and management  
- monitoring_log: Execution history and performance

✅ Automated Monitoring Procedures:
- sp_monitor_daily_revenue: Revenue anomaly detection
- sp_monitor_regional_performance: Geographic monitoring
- sp_monitor_customer_health: Customer health alerts
- sp_monitor_category_performance: Category anomalies
- sp_run_all_monitoring: Master execution procedure
- sp_monitoring_health_check: System health validation

✅ Business Intelligence Integration:
- v_active_alerts: Real-time alert dashboard
- v_monitoring_summary: System status overview
- v_alert_trends: Historical alert analysis

✅ Enterprise Features:
- ML baseline integration with actual dimension names
- Severity classification with escalation rules
- Comprehensive logging and audit trail
- Dashboard-ready views for Power BI integration

READY FOR:
- Automated scheduling (MySQL Event Scheduler)
- Integration with external alert systems
- Power BI monitoring dashboards
- Email/SMS alert notifications

USAGE EXAMPLES:

-- Disable safe updates first:
SET SQL_SAFE_UPDATES = 0;

-- Run all monitoring checks:
CALL sp_run_all_monitoring();

-- Check system health:
CALL sp_monitoring_health_check();

-- View active alerts:
SELECT * FROM v_active_alerts;

-- View monitoring summary:
SELECT * FROM v_monitoring_summary;

-- Re-enable safe updates:
SET SQL_SAFE_UPDATES = 1;

=============================================================

*/
