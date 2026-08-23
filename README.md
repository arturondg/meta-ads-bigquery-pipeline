# 🚀 Serverless Meta Ads Data Pipeline (Medallion Architecture)

<div align="left">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white" alt="GCP" />
  <img src="https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=google-bigquery&logoColor=white" alt="BigQuery" />
  <img src="https://img.shields.io/badge/Meta_API-0668E1?style=for-the-badge&logo=meta&logoColor=white" alt="Meta" />
</div>
<br>

An enterprise-grade, serverless ETL pipeline built on Google Cloud Platform that extracts marketing data from the Meta Graph API, manages state and incremental loads, and models the data into a BigQuery Data Warehouse using the Medallion Architecture.

---

## 💡 Business Problem & Solution

Marketing teams often rely on manual CSV exports or expensive third-party connectors (like Fivetran or Supermetrics) to analyze campaign performance. This project implements a custom, highly scalable internal connector to solve that:

| Without this Pipeline ❌ | With this Pipeline ✅ |
| :--- | :--- |
| **High Costs:** Paying thousands annually for SaaS connectors. | **Cost-Effective:** Bypasses third-party costs by interacting directly with the Meta Graph API (v25.0). |
| **Manual Updates:** Analysts spend hours downloading and cleaning CSVs. | **Fully Automated:** Data is refreshed daily and ready for Looker Studio/BI automatically. |
| **Data Mismatches:** Ad platforms attribute purchases days later, breaking old reports. | **Self-Healing Data:** MD5 hashing detects retroactive updates and corrects historical data automatically. |

---

## 🏗️ Architecture Flow & Tech Stack

* **Language:** Python 3.10+
* **Compute:** Google Cloud Functions (HTTP Triggered)
* **Data Lake (Raw):** Google Cloud Storage (GCS)
* **Data Warehouse (Silver & Gold):** Google BigQuery
* **Data Modeling:** SQL (MERGE statements, Star Schema)

### The Medallion Approach

```mermaid
graph TD
    A[Meta Graph API] -->|Python / Cloud Functions| B[(GCS Data Lake <br/> Raw Layer)]
    B -->|Python Flattening| C[(BigQuery <br/> Silver Layer)]
    C -->|SQL MERGE / Hashing| D[(BigQuery <br/> Gold Layer)]
    D --> E[📊 Looker Studio / BI]
    
    style A fill:#0668E1,stroke:#fff,stroke-width:2px,color:#fff
    style B fill:#E37400,stroke:#fff,stroke-width:2px,color:#fff
    style C fill:#C0C0C0,stroke:#fff,stroke-width:2px,color:#000
    style D fill:#FFD700,stroke:#fff,stroke-width:2px,color:#000
    style E fill:#4285F4,stroke:#fff,stroke-width:2px,color:#fff
