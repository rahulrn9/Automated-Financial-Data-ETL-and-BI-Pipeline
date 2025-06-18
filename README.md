# Unified Data Governance & Automation Platform

An end-to-end, automated framework for enterprise data governance using Apache Atlas, Python, SQL, and Excel templates.

---

## 🚀 Features

* **Asset Discovery**: Auto-discover and register AWS S3 buckets and RDS tables in Apache Atlas.
* **Data Lineage**: Parse ETL or audit logs to build and ingest lineage graphs.
* **Access Policies**: Manage data-access policies via Excel templates, enforce via Python jobs.
* **Data Quality**: Define quality rules (null thresholds, value ranges) and alert on violations.
* **Automated Classification**: Use ML/NLP (Presidio) to detect and tag PII or sensitive assets.
* **Data Profiling**: Generate profiling reports (row counts, null %, top values) with pandas-profiling.
* **Usage Analytics**: Aggregate audit logs to surface most/least-used datasets.
* **Slack Approvals**: Real-time access-request notifications and approvals in Slack.
* **Self‑Service UI**: Basic FastAPI search portal to explore and request assets.
* **Retention & Deletion**: Automate data retention policies and cleanup.

---

## 📋 Prerequisites

* Python 3.8+
* Docker (for Apache Atlas) or Atlas server access
* AWS credentials with S3 & IAM permissions
* RDS instance (PostgreSQL/MySQL) and connection details
* Slack app token (for notifications)
* Excel installed (for policy templates)

---

## 📦 Installation

1. **Clone the repo**

   ```bash
   git clone https://github.com/rahulrn9/Unified-Data-Governance-and-Automation-Platform.git
   cd Unified-Data-Governance-and-Automation-Platform
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Start Apache Atlas (if using Docker)**

   ```bash
   docker-compose up -d  # from atlas/ directory or your custom setup
   ```

4. **Configure environment**

   * Copy `.env.example` to `.env` and update:

     * `ATLAS_HOST`, `ATLAS_PORT`, `ATLAS_USER`, `ATLAS_PASS`
     * `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
     * `RDS_HOST`, `RDS_PORT`, `RDS_DB`, `RDS_USER`, `RDS_PASS`
     * `SLACK_TOKEN`

---

## 🛠️ Usage

Each component lives in `scripts/`. Example workflows:

* **Discover and register S3 buckets**

  ```bash
  python scripts/discover_s3.py
  ```

* **Register RDS tables**

  ```bash
  python scripts/discover_rds.py
  ```

* **Ingest lineage**

  ```bash
  python scripts/lineage_ingest.py
  ```

* **Enforce access policies**

  ```bash
  python scripts/enforce_policies.py
  ```

* **Run data quality checks**

  ```bash
  python scripts/quality_rules_engine.py
  ```

* **Automated classification**

  ```bash
  python scripts/auto_classification.py
  ```

* **Generate profiling reports**

  ```bash
  python scripts/data_profiling.py
  ```

* **Parse usage logs**

  ```bash
  python scripts/usage_analytics.py
  ```

* **Send Slack alerts**

  ```bash
  python scripts/slack_alerts.py
  ```

* **Start self-service UI**

  ```bash
  uvicorn scripts/web_ui:app --reload
  ```

* **Retention & deletion**

  ```bash
  python scripts/retention_deletion.py
  ```

---

## 📂 Project Structure

```
├── scripts/                  # Core Python scripts
├── templates/                # Excel templates for policies & rules
├── profiles/                 # Generated profiling HTML reports
├── .env.example              # Environment variables template
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

---

## 🤝 Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add feature'`)
4. Push to branch (`git push origin feature/your-feature`)
5. Open a pull request

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
