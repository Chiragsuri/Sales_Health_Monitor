# Sales Health Monitor - MySQL Integration

**Adaptive monitoring system with percentile-based alerting**

Author: Chirag Suri  
Repository: [Sales_Health_Monitor](https://github.com/Chiragsuri/Sales_Health_Monitor)  
Last Updated: November 2025

---

## 📋 Overview

Enterprise-grade MySQL database with **adaptive monitoring** that automatically scales to any dataset. Uses percentile-based thresholds instead of hardcoded values, ensuring the system remains effective as your business grows.

### What Makes This Unique

**Traditional systems:**
```sql
-- ❌ Breaks when business scales
IF customer_health_score < 75 THEN alert
```

**This system:**
```sql
-- ✅ Always relevant
IF customer IN (bottom 10% of tier) THEN alert
```

---

## 🚀 Quick Start

### Prerequisites
- MySQL Workbench 8.0+
- Sales data CSVs from [`/Dataset`](https://github.com/Chiragsuri/Sales_Health_Monitor/tree/main/Dataset)

### Installation (5 Minutes)

```bash
# 1. Clone repository
git clone https://github.com/Chiragsuri/Sales_Health_Monitor.git

# 2. Open MySQL Workbench, execute scripts in order:
```

**Execution Order:**
1. `database_setup.sql` - Creates tables and infrastructure
2. `import_core_data.sql` - Loads transaction data *(update file paths first!)*
3. `import_ml_baselines.sql` - Loads ML metrics *(if available)*
4. `create_foundation_kpi_views.sql` - Creates analytical views
5. `create_monitoring_procedures.sql` - Installs adaptive monitoring
6. `validate_eda_insights.sql` - Validates setup *(optional)*

### Verify Installation

```sql
USE sales_health_monitor;
CALL sp_monitoring_health_check();
```

**Expected:** All checks return `READY` status.

---

## 🎯 Adaptive Monitoring System

### How It Works

The system calculates alert thresholds **dynamically** based on your data:

**Critical Alerts (Bottom 1%):**
```sql
-- Finds bottom 1% of customers by health score
SELECT FLOOR(COUNT(*) * 0.01) INTO v_threshold_offset
FROM customer_baselines;
```

**Result:** Always flags the worst 1%, whether you have 100 or 100,000 customers.

**High-Value At Risk (Bottom 10%):**
```sql
-- Finds bottom 10% within high-value tier only
SELECT FLOOR(COUNT(*) * 0.10) INTO v_threshold_offset
FROM customer_baselines
WHERE value_tier IN ('High Value', 'Premium');
```

**Result:** Monitors your most valuable at-risk customers proportionally.

### Why This Matters

| Scenario | Hardcoded (75 threshold) | Adaptive (10th percentile) |
|----------|--------------------------|----------------------------|
| **Startup:** 100 customers, avg health = 60 | 90 alerts (noise) | 10 alerts (actionable) |
| **Growth:** 10K customers, avg health = 65 | 3K alerts (overwhelming) | 1K alerts (manageable) |
| **Mature:** 100K customers, avg health = 70 | 80K alerts (broken) | 10K alerts (scalable) |

**Your system adapts automatically - no maintenance needed.**

---

## 🔄 Adjusting Alert Sensitivity

To change alert volume, edit **percentiles** in `create_monitoring_procedures.sql`:

### Location: `sp_monitor_customer_health()` procedure

**Current (10% = ~1,000 alerts per 10K customers):**
```sql
SELECT FLOOR(COUNT(*) * 0.10) INTO v_hv_offset
```

**More strict (5% = ~500 alerts):**
```sql
SELECT FLOOR(COUNT(*) * 0.05) INTO v_hv_offset
```

**Less strict (15% = ~1,500 alerts):**
```sql
SELECT FLOOR(COUNT(*) * 0.15) INTO v_hv_offset
```

### Percentile Guide

| Percentile | Alert Volume | Best For |
|------------|--------------|----------|
| 1% | Very few | Critical emergencies only |
| 5% | Low | Important issues |
| **10%** | **Moderate** | **Standard monitoring (recommended)** |
| 15% | High | Proactive warnings |
| 25% | Very high | Comprehensive coverage |

---

## 🔔 Running Monitoring

### Execute All Monitors

```sql
SET SQL_SAFE_UPDATES = 0;
CALL sp_run_all_monitoring();
SET SQL_SAFE_UPDATES = 1;
```

### Check Results

```sql
-- View active alerts
SELECT * FROM v_active_alerts;

-- Alert summary
SELECT 
    alert_type,
    severity,
    COUNT(*) as count
FROM monitoring_alerts
WHERE status = 'new'
GROUP BY alert_type, severity;
```

### Individual Monitors

```sql
CALL sp_monitor_daily_revenue();        -- Revenue anomalies
CALL sp_monitor_customer_health();      -- Customer health
CALL sp_monitor_regional_performance(); -- Regional issues
CALL sp_monitor_category_performance(); -- Product categories
```

---

## 📊 Understanding Alerts

### Critical Health Score
**What:** Customers in bottom 1% of health scores  
**Action:** Immediate retention intervention  

```sql
SELECT * FROM monitoring_alerts 
WHERE alert_type = 'critical_health_score' 
AND status = 'new';
```

### High Value At Risk
**What:** Premium customers in bottom 10% of their tier  
**Action:** Account manager outreach  

```sql
SELECT 
    entity_id as customer_id,
    current_value as health_score,
    alert_message
FROM monitoring_alerts 
WHERE alert_type = 'high_value_at_risk';
```

**Example Alert:**
```
High-value in bottom 10%: CUST_047040 
(Health: 37.5, CLV: $10,954,941)
```

This customer is worth **$10.9M** but has declining engagement - critical business risk!

---

## 🔧 Common Issues

### Too Many Alerts

**Symptom:** Thousands of alerts generated

**Solution:** Lower percentile from 10% to 5% or 3%

```sql
-- In create_monitoring_procedures.sql, change:
SELECT FLOOR(COUNT(*) * 0.05) INTO v_hv_offset  -- Was 0.10
```

### No Alerts Generated

**Check thresholds are reasonable:**
```sql
SELECT 
    monitor_name,
    threshold_lower,
    is_active
FROM monitoring_config;
```

**Re-enable monitors if needed:**
```sql
UPDATE monitoring_config 
SET is_active = TRUE 
WHERE is_active = FALSE;
```

### Import Fails

**File path error:** Update CSV paths in `import_core_data.sql` before running

**Permission error:** Check `secure_file_priv` setting
```sql
SHOW VARIABLES LIKE 'secure_file_priv';
```

---

## 🔄 Maintenance

### Daily
```sql
CALL sp_run_all_monitoring();
SELECT * FROM v_active_alerts;
```

### Weekly
```sql
-- Clean old resolved alerts
DELETE FROM monitoring_alerts 
WHERE status = 'resolved' 
AND resolved_at < DATE_SUB(CURDATE(), INTERVAL 90 DAY);

-- Optimize tables
ANALYZE TABLE sales_transactions;
```

### Adding New Data

```sql
-- Load into main table
LOAD DATA INFILE '/path/to/new_data.csv' 
INTO TABLE sales_transactions ...;

-- Re-run monitoring
CALL sp_run_all_monitoring();
```

**System automatically adapts to new date ranges and data volumes.**

---

## 📈 Key Views

```sql
-- Geographic performance
SELECT * FROM v_geographic_performance;

-- Customer intelligence
SELECT * FROM v_customer_intelligence LIMIT 10;

-- Active alerts dashboard
SELECT * FROM v_active_alerts;

-- System health
SELECT * FROM v_monitoring_summary;
```

---

## 🚀 Next Steps

### Phase 6: Power BI Integration
1. Connect Power BI to `sales_health_monitor` database
2. Import views: `v_active_alerts`, `v_geographic_performance`, `v_customer_intelligence`
3. Build dashboards for alert monitoring and customer health tracking

---

## 🎓 Technical Highlights

**What This Demonstrates:**

✅ Adaptive percentile-based thresholds (unique contribution)  
✅ Dynamic date handling (works with any date range)  
✅ Scalable architecture (1K to 1M+ customers)  
✅ Production-ready stored procedures  
✅ Comprehensive monitoring system  
✅ Future-proof design (no hardcoded values)

**Portfolio Value:**
- Advanced SQL (stored procedures, dynamic calculations)
- Enterprise patterns (monitoring, alerting, logging)
- Performance optimization (indexing, query design)
- Production-ready code quality

---

## 📞 Support

**Issues:** [GitHub Issues](https://github.com/Chiragsuri/Sales_Health_Monitor/issues)  
**Author:** Chirag Suri

---

**Status:** Production-Ready ✅  
**Phase:** 5 Complete

---
