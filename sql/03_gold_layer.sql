CREATE TABLE IF NOT EXISTS `your_gcp_project_id.gold_layer.dim_campaigns` (
    campaign_id STRING NOT NULL OPTIONS(description="Llave primaria. ID único de la campaña en la plataforma publicitaria"),
    account_name STRING OPTIONS(description="Nombre de la cuenta publicitaria (proveniente del Mapping Layer)"),
    campaign_name STRING OPTIONS(description="Nombre real de la campaña extraído de la API (Meta/TikTok)"),
    platform STRING OPTIONS(description="Plataforma de origen (Facebook, TikTok, etc.) proveniente de Monday"),
    status STRING OPTIONS(description="Estado actual de la campaña (ACTIVE, PAUSED, etc.)"),
    objective STRING OPTIONS(description="Objetivo publicitario (OUTCOME_SALES, TRAFFIC, etc.)"),
    start_time TIMESTAMP OPTIONS(description="Fecha y hora de inicio de la campaña"),
    stop_time TIMESTAMP OPTIONS(description="Fecha y hora de fin de la campaña (puede ser nulo si es continua)"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Fecha de primera inserción en la capa Gold"),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Fecha de última actualización")
)
CLUSTER BY platform, status; 
-- El cluster por plataforma y estado acelerará las consultas al filtrar en Looker Studio.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.gold_layer.fact_campaign_performance` (
    date_information DATE NOT NULL OPTIONS(description="Llave primaria de tiempo. Fecha diaria del insight publicitario"),
    campaign_id STRING NOT NULL OPTIONS(description="Llave foránea. Conecta con dim_campaigns"),
    account_name STRING OPTIONS(description="Cuenta publicitaria de origen"),
    impressions INT64 OPTIONS(description="Número de impresiones acumuladas en el día"),
    clicks INT64 OPTIONS(description="Número de clics acumulados en el día"),
    spend FLOAT64 OPTIONS(description="Dinero invertido en el día (Moneda real)"),
    purchases FLOAT64 OPTIONS(description="Cantidad de compras/boletos atribuidos en el día"),
    value_purchases FLOAT64 OPTIONS(description="Valor monetario de las compras atribuidas en el día"),
    platform STRING OPTIONS(description="Plataforma publicitaria (Facebook, TikTok, etc.)"),
    row_hash STRING OPTIONS(description="Hash de control para detectar cambios retroactivos en las métricas"),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Auditoría: Fecha de inserción en el Data Warehouse")
)
PARTITION BY date_information
CLUSTER BY  campaign_id;
-- Nota: Particionar por fecha asegura que Looker Studio solo pague por los días que el usuario está viendo en el filtro.

