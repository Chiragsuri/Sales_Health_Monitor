# Sales Health Monitor - Power BI Analytics

**Enterprise Business Intelligence Dashboard System**

A comprehensive Power BI solution transforming transactional data into actionable insights across four specialized analytics dashboards, designed for data-driven decision-making at multiple organizational levels.

---

## 📊 Project Overview

### Purpose
Multi-dashboard BI system analyzing sales performance, customer behavior, product portfolio, and geographic distribution with real-time KPI monitoring, anomaly detection, and strategic planning capabilities.

### Technical Stack
- **BI Platform:** Microsoft Power BI Desktop
- **Database:** MySQL 8.0+
- **Data Model:** Star Schema
- **Languages:** DAX, SQL
- **Visuals:** 34 interactive components across 4 dashboards
- **Custom Measures:** 20+ DAX formulas

### Key Capabilities
✅ Real-time MySQL data integration  
✅ Advanced time intelligence (YoY, growth calculations)  
✅ ML-driven anomaly detection integration  
✅ Customer risk scoring and retention analytics  
✅ Geographic and product portfolio optimization  
✅ Executive KPI monitoring with conditional formatting  
✅ Interactive cross-filtering across all visuals  

---

## 🗂️ Dashboard Architecture

### Dashboard 1: Executive Overview
**Audience:** C-suite, VP Sales, Executive Leadership

**Purpose:** High-level performance monitoring with year-over-year comparisons

**Components (10 visuals):**
- KPI cards with sparklines (Revenue, Customers, Transactions)
- YoY growth indicators with conditional formatting
- Revenue breakdown by channel and segment
- Monthly trend analysis with forecasting
- Interactive year selection buttons

**Key Features:**
- Time intelligence DAX measures
- Growth arrows with color coding (green/red)
- Executive summary insights
- Dynamic time period filtering

---

### Dashboard 2: Anomaly & Risk Monitor
**Audience:** Operations Managers, Data Analysts, Risk Teams

**Purpose:** Proactive identification of unusual patterns for early intervention

**Components (6 visuals):**
- Anomaly detection counter
- Risk score distribution analysis
- At-risk customer priority table
- Product anomaly timeline
- Geographic anomaly heatmap
- Severity-based filtering

**Key Features:**
- ML model output integration
- Automated alert thresholds
- Customer risk scoring system
- Drill-through to customer details

---

### Dashboard 3: Customer Intelligence
**Audience:** Marketing Teams, Customer Success, Sales Operations

**Purpose:** Deep customer analytics for retention and segmentation strategies

**Components (10 visuals):**
- Customer Lifetime Value (CLV) metrics
- Segment performance analysis
- Retention rate trends
- Value tier distribution (Premium/Standard/Budget)
- Cohort analysis
- Churn risk identification
- Purchase frequency patterns
- Average order value by segment

**Key Features:**
- RFM (Recency, Frequency, Monetary) scoring
- Complex CLV calculations
- Cohort retention tracking
- Targeted marketing insights

---

### Dashboard 4: Geographic & Product Performance
**Audience:** Product Managers, Regional Sales Leaders, Strategic Planners

**Purpose:** Portfolio optimization and regional strategy development

**Components (8 visuals):**
- Leading product category indicator
- Category revenue with YoY growth indicators (▲/▼)
- Revenue distribution by category (donut chart)
- Category-Region performance matrix (heatmap)
- Regional revenue comparison
- Top 10 products with dynamic filtering
- Monthly revenue trends by category (stacked area)
- Interactive slicers (Category, Region, Year)

**Key Features:**
- Prior year comparison logic with context awareness
- Matrix heatmap with gradient conditional formatting
- Unicode arrow trend indicators
- HASONEVALUE filter detection
- Dynamic Top N product filtering

**Technical Highlights:**
- 6 custom DAX measures for growth analysis
- Text/numeric measure separation pattern
- Edge case handling (no prior year, "All" filters)
- Error-proof calculations with ISBLANK logic

---

## 🛠️ Technical Implementation

**Relationships:**
- Many-to-One from fact to dimensions
- Single-direction cross-filtering
- Referential integrity enabled for performance

### Advanced DAX Patterns

**Time Intelligence:**
```dax
PY Category Revenue =
VAR HasYearFilter = HASONEVALUE(sales_transactions[year])
VAR SelectedYear = 
    IF(
        HasYearFilter,
        SELECTEDVALUE(sales_transactions[year]),
        MAX(sales_transactions[year])
    )
VAR PYYear = 
    IF(
        HasYearFilter,
        SelectedYear - 1,
        SelectedYear
    )
RETURN
    CALCULATE(
        [Leading Category Revenue],
        sales_transactions[year] = PYYear
    )
```

**Growth Calculations:**
```dax
Category Revenue Growth % =
VAR CurrentRevenue = [Leading Category Revenue]
VAR PreviousRevenue = [PY Category Revenue]
VAR GrowthPct =
    DIVIDE(
        CurrentRevenue - PreviousRevenue,
        PreviousRevenue,
        BLANK()
    )
RETURN
    IF(
        NOT HASONEVALUE(sales_transactions[year]),
        BLANK(),
        GrowthPct
    )
```

**Conditional Formatting:**
```dax
Category Growth Color = 
VAR _Growth = [Category Revenue Growth %]
RETURN
    SWITCH(
        TRUE(),
        ISBLANK(_Growth), "#FFFFFF",
        _Growth > 0, "#00FF7F",
        _Growth < 0, "#FF3131",
        "#FFFFFF"
    )
```

### DAX Best Practices Demonstrated
✅ Filter context awareness (HASONEVALUE)  
✅ Error handling (ISBLANK, DIVIDE with defaults)  
✅ Variable usage (VAR) for performance  
✅ Measure separation (numeric vs text)  
✅ Time intelligence patterns  
✅ Conditional formatting logic  

---

## 🎨 Design System

### Visual Design Principles
- **Theme:** Dark background with purple accents (#9D4EDD)
- **Typography:** Segoe UI with clear hierarchy
- **Colors:** Consistent 5-color category palette
- **Layout:** Grid-based, 95-98% canvas utilization
- **Spacing:** 15-20px between visuals

### Color Palette
**Status Colors:**
- Positive/Growth: Green (#00FF7F)
- Negative/Decline: Red (#FF3131)
- Warning: Yellow (#FFD700)
- Neutral: White (#FFFFFF)

**Category Colors:**
- Purple (#9D4EDD)
- Cyan (#00D4FF)
- Green (#00FF7F)
- Orange (#FFA500)
- Gray (#6E6E6E)

### Typography Hierarchy
- Dashboard Title: 24pt Bold
- Visual Titles: 16pt Bold
- KPI Main Values: 36pt Bold (Gold)
- Context Text: 9-10pt (Light Gray)
- Data Labels: 10-11pt

---

## 🎯 Key Features & Achievements

### Technical Achievements
✅ **Advanced DAX:** Implemented time intelligence, filter awareness, and complex calculations  
✅ **Performance Optimization:** <3 second dashboard load times  
✅ **ML Integration:** Connected anomaly detection outputs to interactive visuals  
✅ **Conditional Formatting:** Gradient heatmaps and dynamic color coding  
✅ **Dynamic Filtering:** Top N products adjust based on user selections  
✅ **Error Handling:** Graceful null handling and edge case management  

### Business Impact
✅ **Executive Insights:** Real-time KPI monitoring with growth indicators  
✅ **Customer Retention:** Identified at-risk customers with priority scoring  
✅ **Portfolio Optimization:** Revealed category and regional performance patterns  
✅ **Anomaly Detection:** Automated unusual pattern identification  
✅ **Strategic Planning:** Data-driven product and regional investment decisions  

### Problem-Solving Examples

**Challenge:** Year-over-year calculations when "All" years selected  
**Solution:** Implemented HASONEVALUE to detect filter state and adjust logic  
**Result:** Clean displays regardless of filter context, no errors

**Challenge:** Percentage formatting causing 100x multiplication  
**Solution:** Changed format from Percentage to Decimal Number  
**Result:** Correct discount display (2-3% range)

**Challenge:** Text measures breaking mathematical calculations  
**Solution:** Separated numeric (for math) from text (for display) measures  
**Result:** Calculations work correctly, displays show formatted text

---

## 🚀 Getting Started

### Prerequisites
- Power BI Desktop (latest version)
- MySQL database with appropriate credentials
- Dataset: 800K+ transaction records

### Setup Instructions
1. Clone repository
2. Open `SalesHealthMonitor.pbix`
3. Update data source credentials:
   - Transform Data → Data Source Settings
   - Enter MySQL connection details
4. Refresh dataset
5. Explore dashboards

### Usage
- **Executives:** Dashboard 1 for KPI overview
- **Operations:** Dashboard 2 for anomaly monitoring
- **Marketing:** Dashboard 3 for customer insights
- **Product/Regional:** Dashboard 4 for portfolio analysis

---

## 📊 Performance Metrics

- **Dashboard Load Time:** <3 seconds
- **Slicer Response:** <1 second
- **Cross-Filter Refresh:** <0.5 seconds
- **Data Refresh:** Daily scheduled (configurable)

---

## 🎓 Skills Demonstrated

### Business Intelligence
- Multi-dashboard system design
- Stakeholder-specific visualizations
- KPI framework development
- Strategic insight generation

### Technical Skills
- Advanced DAX (time intelligence, filter context)
- Star schema data modeling
- MySQL integration
- Performance optimization
- Conditional formatting
- Interactive visualizations

### Problem-Solving
- Complex filter logic
- Edge case handling
- Error prevention
- Performance tuning

---

## 📈 Future Enhancements

### Planned Features
- Drill-through detail pages
- Custom tooltips with additional context
- Forecasting models integration
- Mobile-optimized layouts
- Power BI Service deployment with scheduled refresh
- Automated email alerts for anomalies

---

## 📝 License

This project is part of a portfolio demonstration. Feel free to use concepts and patterns for learning purposes.

---

## 📧 Contact

**Project Showcase:** [https://chiragsuri.github.io/]  
**LinkedIn:** [https://www.linkedin.com/in/chirag-suri/]  
**GitHub:** [https://github.com/Chiragsuri]

---

## 🏆 Project Status

**Status:** ✅ Complete  
**Version:** 1.0  
**Last Updated:** November 2025  
**Dashboards:** 4/4 Complete  
**Visuals:** 34 interactive components  
**DAX Measures:** 20+ custom formulas  

---

**⭐ If you find this project useful, please consider starring the repository!**

---

*Built with Power BI Desktop | Powered by MySQL | Designed for Impact*
