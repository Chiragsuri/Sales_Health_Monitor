# Sales Health Monitor

Sales Health Monitor is an end-to-end analytics project built to turn raw sales data into business-ready insights. It brings together data generation, cleaning, exploratory analysis, SQL-based modeling, and Power BI dashboards to monitor sales, customers, products, and regional performance in one place.

The project was built to show the full workflow an analyst follows in a real business setting, from messy data to structured reporting. It also includes anomaly detection logic and monitoring views so the project goes beyond static dashboards.

## Project Highlights

- Built a realistic retail dataset with customers, products, regions, and transactions.
- Cleaned and validated the data before analysis.
- Explored trends across time, geography, products, and customer segments.
- Designed a MySQL star schema for reporting and dashboarding.
- Created KPI views and monitoring procedures for business tracking.
- Built Power BI dashboards for executive, customer, risk, and geographic views.

## Tools Used

- Python
- Pandas
- NumPy
- Matplotlib
- Seaborn
- Faker
- MySQL
- Power BI
- DAX
- Generative AI

## Repository Structure

```text
Sales-Health-Monitor/
├── Dataset/
│   ├── raw/
│   ├── processed/
│   └── sample/
├── MYSQL/
│   ├── database_setup.sql
│   ├── import_core_data.sql
│   ├── import_ml_baselines.sql
│   ├── create_foundation_kpi_views.sql
│   ├── create_monitoring_procedures.sql
│   ├── validate_eda_insights.sql
│   └── README.md
├── Python/
│   ├── 01_Data_Generation.ipynb
│   ├── 02_Data_Cleaning_Validation.ipynb
│   ├── 03_EDA_Analysis.ipynb
│   ├── 04_EDA_Advanced_Analysis.ipynb
├── PowerBI_Dashboard/
└── README.md
```

## Workflow

### 1. Data Generation
The project starts by creating a realistic retail dataset for customers, products, and sales transactions. The data was designed to reflect common business patterns such as seasonality, region-wise differences, and customer segment behavior.

### 2. Data Cleaning and Validation
The generated data was intentionally made messy in places so the cleaning step would mirror real-world work. Duplicate rows, missing values, inconsistent labels, and outliers were handled through a structured validation process.

### 3. Exploratory Data Analysis
The cleaned data was analyzed to understand performance across time, regions, products, and customer groups. This helped identify seasonal spikes, regional differences, high-value customers, and product-level trends.

### 4. MySQL Integration
The analysis-ready data was moved into MySQL using a structured schema. KPI views and monitoring procedures were created to support fast reporting and business tracking.

### 5. Power BI Dashboards
Power BI was used to build interactive dashboards for different business needs. These dashboards help track revenue, customer behavior, risk signals, and regional performance through a single reporting layer.

## Key Features

- Time-based sales analysis.
- Customer segmentation and value tracking.
- Product and category performance analysis.
- Geographic performance monitoring.
- Adaptive anomaly detection.
- KPI views for dashboard use.
- Interactive Power BI reporting.

## Dataset Summary

This project uses synthetic business data created to simulate a realistic retail environment.

- 50,000 customers.
- 500 products.
- 3 years of transaction data.
- 5 regions.
- Multiple sales channels.
- Seasonal buying patterns.

## Why This Project Matters

This project shows more than just dashboard building. It shows how raw data can be prepared, analyzed, modeled, and turned into a system that supports business decisions. It reflects the kind of workflow used in analytics and BI roles.

## How To Run

1. Open the Python notebooks in order.
2. Run the cleaning and analysis notebooks first.
3. Load the SQL scripts into MySQL.
4. Connect Power BI to the database.
5. Explore the dashboards and supporting outputs.

## Future Work

- ML-based anomaly detection.
- AI-generated text insights.
- Automated alerts and scheduled monitoring.
- More advanced dashboard refresh workflows.

## Contact

Created by Chirag Suri  
Portfolio: [chiragsuri.github.io](https://chiragsuri.github.io)  
GitHub: [github.com/Chiragsuri](https://github.com/Chiragsuri)
