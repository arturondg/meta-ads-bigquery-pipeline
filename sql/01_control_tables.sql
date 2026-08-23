CREATE OR REPLACE TABLE `your_gcp_project_id.pipeline_management.extraction_errors_v2` (
  error_id         STRING DEFAULT GENERATE_UUID(), -- Conservamos tu UUID
  proceso          STRING NOT NULL,
  platform         STRING NOT NULL,
  parent_id        STRING,
  parent_tipo      STRING,
  child_id         STRING,
  child_tipo       STRING,
  entidad_id       STRING,
  entidad_tipo     STRING,
  tipo_error       STRING,
  mensaje_error    STRING,
  fecha_ejecucion  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  intentos         INT64 DEFAULT 1,
  resuelto         BOOL DEFAULT FALSE,
  resuelto_at      TIMESTAMP
);

CREATE OR REPLACE TABLE `your_gcp_project_id.pipeline_management.load_control (
    load_id         STRING    DEFAULT GENERATE_UUID(),
    gcs_path        STRING    NOT NULL,
    business_line   STRING    NOT NULL,
    group_id        STRING    NOT NULL,
    fecha_archivo   DATE      NOT NULL,
    status          STRING    DEFAULT 'pendiente',
    fecha_carga     TIMESTAMP,
    error_mensaje   STRING
);


CREATE OR REPLACE TABLE datawarehouseevents-demo.pipeline_management.account_catalog (
  account_id      STRING NOT NULL,   --  según plataforma
  account_name    STRING,
  platform        STRING NOT NULL,   -- "meta", "tiktok", "amazon", "spotify"
  area            STRING,            -- depende del area de producto
  tipo            STRING,            -- "advertiser", "agency", etc.
  currency        STRING,            -- "MXN", "USD" — útil para normalizar spend
  timezone        STRING,            -- "America/Mexico_City" — importante para fechas
  activo          BOOL DEFAULT TRUE,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  extraction_status STRING,
  ultima_extraccion TIMESTAMP
);

