# Automated Financial Data ETL & BI Pipeline

An end-to-end Airflow-driven ETL and BI solution for intraday trading data.
- **Hourly** ingestion from REST API into S3 staging.
- **Data drift** & **schema** checks via Great Expectations & Slack alerts.
- **Transform**, **load** into Redshift, **batch** Glue jobs, and **Power BI** refresh automation.
