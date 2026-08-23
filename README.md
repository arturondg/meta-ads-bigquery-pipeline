# 🚀 Serverless Meta Ads Data Pipeline (Medallion Architecture)

An enterprise-grade, serverless ETL pipeline built on Google Cloud Platform that extracts marketing data from the Meta Graph API, manages state and incremental loads, and models the data into a BigQuery Data Warehouse using the Medallion Architecture.

---

## 📌 Business Problem & Solution
Marketing teams often rely on manual CSV exports or expensive third-party connectors (like Fivetran or Supermetrics) to analyze campaign performance. 

This project implements a custom, highly scalable internal connector that:
1. **Bypasses third-party costs** by interacting directly with the Meta Graph API (v25.0).
2. **Handles API complexities** such as pagination, rate limits, and dynamic date-preset extraction (fetching only last 14 days for existing campaigns or full history for new ones).
3. **Ensures Data Quality** through a Medallion architecture, handling retroactive conversion updates (e.g., Meta updating purchase attribution days later) via MD5 hashing.

---

## 🏗️ Architecture & Tech Stack

* **Language:** Python 3.10+
* **Compute:** Google Cloud Functions (HTTP Triggered)
* **Data Lake (Raw):** Google Cloud Storage (GCS)
* **Data Warehouse (Silver & Gold):** Google BigQuery
* **Data Modeling:** SQL (MERGE statements, Star Schema)

### Pipeline Flow (Medallion Approach)
1. **Bronze / Raw Layer (GCS):** Python scripts extract raw JSON responses from the Meta API and store them in partitioned GCS buckets (`/raw/meta/insights/YYYY-MM-DD/data.json`).
2. **Silver Layer (BigQuery):** JSON data is flattened, cast to strictly typed tables (`campaigns`, `campaign_insights`), and deduplicated using row hashing to capture retroactive changes.
3. **Gold Layer (BigQuery):** Data is modeled into a Star Schema (`dim_campaigns`, `fact_campaign_performance`, `dim_fecha`) clustered and partitioned by date for optimized BI querying.

---

## ⚙️ Key Technical Features Highlight

### 1. Smart Incremental Extraction
Instead of dropping and reloading data, the pipeline queries a control table to check the `last_insight_date`. If a campaign is new, it extracts `maximum` history. If it exists, it requests `last_14d`, significantly reducing API calls and execution time.

### 2. Retroactive Update Handling via MD5 Hashing
Ad platforms often attribute conversions days after the click. The pipeline generates an MD5 hash of the metrics row:
```python
def generate_hash(row):
    row_str = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(row_str.encode()).hexdigest()
