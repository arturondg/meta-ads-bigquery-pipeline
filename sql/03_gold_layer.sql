-- DDL: Tabla de Hechos de Rendimiento Publicitario
CREATE TABLE IF NOT EXISTS `your_project_id.gold_layer.fact_campaign_performance` (
    date_information DATE NOT NULL OPTIONS(description="Llave primaria de tiempo. Fecha diaria del insight publicitario"),
    campaign_id STRING NOT NULL OPTIONS(description="Llave foránea. Conecta con dim_campaigns"),
    account_id STRING OPTIONS(description="Cuenta publicitaria de origen"),
    impressions INT64 OPTIONS(description="Número de impresiones acumuladas en el día"),
    clicks INT64 OPTIONS(description="Número de clics acumulados en el día"),
    spend FLOAT64 OPTIONS(description="Dinero invertido en el día"),
    purchases FLOAT64 OPTIONS(description="Cantidad de conversiones/compras atribuidas"),
    value_purchases FLOAT64 OPTIONS(description="Valor monetario de las compras"),
    platform STRING OPTIONS(description="Plataforma publicitaria (Meta, TikTok, etc.)"),
    row_hash STRING OPTIONS(description="Hash de control para detectar cambios retroactivos"),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Auditoría: Fecha de inserción")
)
PARTITION BY date_information
CLUSTER BY platform, campaign_id;


-- DML: MERGE para carga incremental e idempotente
MERGE `your_project_id.gold_layer.fact_campaign_performance` AS target
USING (
  SELECT 
    CAST(date_information AS DATE) AS date_information,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(account_id AS STRING) AS account_id,
    CAST(impressions AS INT64) AS impressions,
    CAST(clicks AS INT64) AS clicks,
    CAST(spend AS FLOAT64) AS spend,
    CAST(purchases AS FLOAT64) AS purchases,
    CAST(value_purchases AS FLOAT64) AS value_purchases,
    CAST(platform AS STRING) AS platform,
    CAST(row_hash AS STRING) AS row_hash
  FROM `your_project_id.silver_layer.campaign_insights`
) AS source
ON target.campaign_id = source.campaign_id 
AND target.date_information = source.date_information

-- Si el día ya existe pero el Hash cambió (actualización retroactiva de Meta)
WHEN MATCHED AND target.row_hash != source.row_hash THEN
  UPDATE SET 
    impressions = source.impressions,
    clicks = source.clicks,
    spend = source.spend,
    purchases = source.purchases,
    value_purchases = source.value_purchases,
    row_hash = source.row_hash,
    inserted_at = CURRENT_TIMESTAMP()

-- Si es un registro nuevo para ese día y campaña
WHEN NOT MATCHED THEN
  INSERT (
    date_information, campaign_id, account_id, impressions, clicks, 
    spend, purchases, value_purchases, platform, row_hash, inserted_at
  )
  VALUES (
    source.date_information, source.campaign_id, source.account_id, source.impressions, 
    source.clicks, source.spend, source.purchases, source.value_purchases, 
    source.platform, source.row_hash, CURRENT_TIMESTAMP()
  );

