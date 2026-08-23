CREATE OR REPLACE TABLE your_gcp_project_id.silver_layer.campaigns (
  account_id            STRING NOT NULL,
  campaign_id           STRING NOT NULL,
  campaign_name         STRING,
  status                STRING,
  objective             STRING,
  budget_rebalance_flag BOOLEAN,
  daily_budget          FLOAT64,
  lifetime_budget       FLOAT64,
  start_time            TIMESTAMP,
  stop_time             TIMESTAMP,
  created_time          TIMESTAMP,
  updated_time          TIMESTAMP,
  platform              STRING,
  loaded_at             TIMESTAMP
);

CREATE OR REPLACE TABLE your_gcp_project_id.silver_layer.campaign_insights (
  account_id        STRING NOT NULL,
  campaign_id       STRING NOT NULL,
  date_information  DATE NOT NULL,
  impressions       INT64,
  clicks            INT64,
  spend             FLOAT64,
  purchases         INT64,
  value_purchases   FLOAT64,
  platform          STRING NOT NULL,
  row_hash          STRING NOT NULL,
  updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date_information  -- Ultra crítico: segmenta por día para que Looker Studio solo pague por los días que consulta
CLUSTER BY account_id, campaign_id; -- Acelera los filtros por campaña
