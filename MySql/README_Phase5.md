
# Sales Health Monitor - Phase 5

## Overview

This folder contains SQL scripts to implement Phase 5 of the Sales Monitor project.

The goal is to build the MySQL database integrating all cleaned data and ML baselines.

## Scripts Included

- `database_setup.sql`: Creates database, users, tables with optimized schema.
- `import_data.sql`: Applies data import commands for sales, customers, products.
- `load_ml_baselines.sql`: Loads JSON ML baseline data.
- `create_views.sql`: Defines KPI and analytic views.
- `data_validation.sql`: Contains data quality and validation queries.

## Database Highlights

- Uses InnoDB for reliability, ACID compliance, and performance.
- Computed columns speed up time-based queries.
- Indexed tables for query optimization.

## Current Status

- `database_setup.sql` completed with idempotent guards.
- Remaining scripts to be developed following modular approach.

## Next Steps

- Execute `database_setup.sql` fully.
- Begin preparations for `import_data.sql` and others.
- Maintain modular script discipline for easy maintenance.

## Notes

- Automation and external service integrations are deferred.
- No Google account or calendar linking prompts will be issued.

---

Contact: Sales Health Monitor Team
