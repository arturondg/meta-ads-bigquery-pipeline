import os
import json
import hashlib
import time
import requests
import dateutil.parser
from pathlib import Path
from datetime import date, datetime, timezone
from google.cloud import bigquery, storage
import functions_framework

# ==============================================================================
# 1. CONSTANTES Y CONFIGURACIONES
# ==============================================================================
HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9"
}

# Lectura segura desde las variables de entorno del contenedor
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN")
NOMBRE_BUCKET = "datawarehouseevents-demo-raw"

def name_path(source, tipo, id_elemento):
    return f"raw/{source}/{tipo}/{id_elemento}/{date.today()}/data.json"

def debe_extraer(ultima_extraccion, extraction_status):
    hoy = date.today()
    if not ultima_extraccion: 
        return True, "primera_extraccion"
    
    ultima_fecha = ultima_extraccion.date()
    if ultima_fecha == hoy and extraction_status == 'exitoso': 
        return False, "ya_completo_hoy"
    if ultima_fecha == hoy and extraction_status in ('en_proceso', 'fallido'): 
        return True, "reintento"
    if ultima_fecha < hoy: 
        return True, "extraccion_nueva"
    
    return False, "sin_cambios"

def parse_fb_timestamp(value):
    """Convierte timestamp de Meta a UTC string para BigQuery"""
    if not value:
        return None
    dt = dateutil.parser.parse(value)
    return dt.astimezone(timezone.utc).isoformat()

def generate_hash(row):
    row_str = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(row_str.encode()).hexdigest()

# ==============================================================================
# 2. MANEJO DE ERRORES Y CONTROL DE ESTADOS
# ==============================================================================
def registrar_error(bq_client, proceso, entity_name, error, intentos=1,
                    platform=None, entity_id=None, parent_id=None, parent_name=None,
                    child_id=None, child_name=None):
    """Registra errores estructurados en la tabla de control central V2."""
    query = """
        INSERT INTO `datawarehouseevents-demo.pipeline_management.extraction_errors_v2`
        (proceso, platform, parent_id, parent_tipo, child_id, child_tipo, 
         entidad_id, entidad_tipo, fecha_ejecucion, tipo_error, mensaje_error, intentos, resuelto)
        VALUES 
        (@proceso, @platform, @parent_id, @parent_tipo, @child_id, @child_tipo, 
         @entity_id, @entity_name, CURRENT_TIMESTAMP(), @tipo_error, @mensaje_error, @intentos, False)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("proceso", "STRING", proceso),
            bigquery.ScalarQueryParameter("platform", "STRING", platform),
            bigquery.ScalarQueryParameter("parent_id", "STRING", str(parent_id) if parent_id else None),
            bigquery.ScalarQueryParameter("parent_tipo", "STRING", parent_name),
            bigquery.ScalarQueryParameter("child_id", "STRING", str(child_id) if child_id else None),
            bigquery.ScalarQueryParameter("child_tipo", "STRING", child_name),
            bigquery.ScalarQueryParameter("entity_id", "STRING", str(entity_id) if entity_id else None),
            bigquery.ScalarQueryParameter("entity_name", "STRING", entity_name),
            bigquery.ScalarQueryParameter("tipo_error", "STRING", type(error).__name__),
            bigquery.ScalarQueryParameter("mensaje_error", "STRING", str(error)),
            bigquery.ScalarQueryParameter("intentos", "INT64", intentos),
        ]
    )
    bq_client.query(query, job_config=job_config).result()

def actualizar_status_cuenta(bq_client, account_id, status):
    query = """
        UPDATE pipeline_management.account_catalog
        SET extraction_status = @status, ultima_extraccion = CURRENT_TIMESTAMP()
        WHERE account_id = @account_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
        ]
    )
    bq_client.query(query, job_config=job_config).result()

def registrar_carga(bq_client, path, business_line, entity_id, fecha):
    query = """
        MERGE pipeline_management.load_control AS target
        USING (
            SELECT @path AS gcs_path, @business_line AS business_line, 
                   @entity_id AS entity_id, CAST(@fecha AS DATE) AS fecha_archivo
        ) AS source ON target.gcs_path = source.gcs_path
        WHEN MATCHED THEN UPDATE SET 
            status = 'en_proceso', fecha_carga = CURRENT_TIMESTAMP(), error_mensaje = NULL
        WHEN NOT MATCHED THEN INSERT (gcs_path, business_line, entity_id, fecha_archivo, status, fecha_carga)
            VALUES (source.gcs_path, source.business_line, source.entity_id, source.fecha_archivo, 'en_proceso', CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("path", "STRING", path),
        bigquery.ScalarQueryParameter("business_line", "STRING", business_line),
        bigquery.ScalarQueryParameter("entity_id", "STRING", entity_id),
        bigquery.ScalarQueryParameter("fecha", "STRING", fecha),
    ])
    bq_client.query(query, job_config=job_config).result()

def actualizar_carga(bq_client, path, status, error=None):
    if error:
        query = """UPDATE pipeline_management.load_control 
                   SET status = @status, error_mensaje = @error WHERE gcs_path = @path AND status = 'en_proceso'"""
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("error", "STRING", str(error)),
            bigquery.ScalarQueryParameter("path", "STRING", path),
        ])
    else:
        query = """UPDATE pipeline_management.load_control 
                   SET status = @status WHERE gcs_path = @path AND status = 'en_proceso'"""
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("path", "STRING", path),
        ])
    bq_client.query(query, job_config=job_config).result()

def listar_archivos_del_dia(source, business_line, fecha=None, usar_gcs=False, gcs_client=None):
    if not fecha:
        fecha = date.today().strftime('%Y-%m-%d')
    prefix = f"raw/{source}/{business_line}"
    
    if usar_gcs:
        blobs = gcs_client.list_blobs(prefix=prefix, bucket_or_name=NOMBRE_BUCKET)
        return [b.name for b in blobs if fecha in b.name]
    else:
        base_path = Path(prefix)
        if base_path.exists():
            return [str(p).replace('\\', '/') for p in base_path.rglob("data.json") if fecha in str(p)]
        return []

# ==============================================================================
# 3. FASE 1: EXTRACCIÓN (META API A RAW STORAGE)
# ==============================================================================
def get_facebook_data(proceso, cuenta=None, campaigns=None, date_preset=None):
    BASE = "https://graph.facebook.com/v25.0"
    url = ""
    params_campaings = {}

    if proceso == "campaigns_general":
        url = f"{BASE}/act_{cuenta}/campaigns"
        params_campaings = {
            "fields": "id,name,status,objective,daily_budget,lifetime_budget,start_time,stop_time,created_time,updated_time",
            "limit": 1000,
        }
    return url, params_campaings

def facebook_query(url, params):
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            msg = data["error"].get("message", "Error desconocido")
            return None, f"Meta API Error: {msg}"
        return data, None
    except Exception as e:
        return None, str(e)

def extraer_campanas(bq_client, cuenta_id, access_token):
    url, params = get_facebook_data("campaigns_general", cuenta=cuenta_id)
    params["access_token"] = access_token

    data, error = facebook_query(url, params)
    if error:
        registrar_error(
            bq_client    = bq_client,
            proceso      = "get_campaigns_meta",
            platform     = "meta",
            parent_id    = cuenta_id,
            parent_name  = "ad_account",
            entity_id    = cuenta_id,
            entity_name  = "ad_account",
            error        = Exception(error)
        )
        return []
    return data.get("data", [])

def extraer_campanas_meta(bq_client, gcs_client=None):
    cuentas = bq_client.query("""
        SELECT account_id, account_name, platform
        FROM pipeline_management.account_catalog
        WHERE activo = TRUE AND platform = 'Meta'
    """).result()

    for cuenta in cuentas:
        account_id   = cuenta['account_id']
        account_name = cuenta['account_name']
        platform     = cuenta['platform']

        # Validamos si corresponde extracción incremental hoy
        # (Para simular la consulta local pasamos un mock rápido si no hay campos de auditoría)
        extraer, motivo = debe_extraer(None, 'pendiente') 
        if not extraer:
            print(f"Cuenta {account_name} omitida: {motivo}")
            continue

        actualizar_status_cuenta(bq_client, account_id, 'en_proceso')

        try:
            if not META_ACCESS_TOKEN:
                raise Exception("Falta la variable de entorno META_ACCESS_TOKEN")

            campanas = extraer_campanas(bq_client, account_id, META_ACCESS_TOKEN)
            if not campanas:
                raise Exception("No se obtuvieron campañas o la lista venía vacía")

            # Escribir temporalmente en la función antes de enviarlo a GCS
            path_gcs = name_path(platform.lower(), "campaigns", account_id)
            path_local = f"/tmp/campaigns_{account_id}.json"
            
            with open(path_local, "w", encoding="utf-8") as f:
                json.dump(campanas, f, indent=2)

            # Subida directa a Cloud Storage
            bucket = gcs_client.bucket(NOMBRE_BUCKET)
            blob = bucket.blob(path_gcs)
            blob.upload_from_filename(path_local)
            
            os.remove(path_local)

            actualizar_status_cuenta(bq_client, account_id, 'exitoso')
            print(f"✓ {account_name}: {len(campanas)} campañas persistidas en gs://{NOMBRE_BUCKET}/{path_gcs}")

        except Exception as e:
            actualizar_status_cuenta(bq_client, account_id, 'fallido')
            registrar_error(
                bq_client    = bq_client,
                proceso      = "get_campaigns_meta",
                platform     = "meta",
                parent_id    = account_id,
                parent_name  = "ad_account",
                entity_id    = account_id,
                entity_name  = "ad_account",
                error        = e
            )
            continue
        time.sleep(0.5)

# ==============================================================================
# 4. FASE 2: TRANSFORMACIÓN (RAW JSON A MODELO SILVER)
# ==============================================================================
def transformar_campaigns(raw_data, platform, account):
    if platform.lower() == "meta":
        return transformar_meta(raw_data, platform, account)
    else:
        raise Exception(f"Estrategia de transformación no implementada para: {platform}")

def transformar_meta(raw_data, platform, account):
    campaings_general = []
    errors = []

    for item in raw_data:
        try:
            item_row = {
                'account_id': str(account),
                'campaign_id': str(item.get('id')),
                'campaign_name': item.get('name'),
                'status': item.get('status'),
                'objective': item.get('objective'),
                'budget_rebalance_flag': item.get('budget_rebalance_flag', False),
                'daily_budget': float(item['daily_budget']) / 100 if item.get('daily_budget') else None,
                'lifetime_budget': float(item['lifetime_budget']) / 100 if item.get('lifetime_budget') else None,
                "start_time":   parse_fb_timestamp(item.get("start_time")),
                "stop_time":    parse_fb_timestamp(item.get("stop_time")),
                "created_time": parse_fb_timestamp(item.get("created_time")),
                "updated_time": parse_fb_timestamp(item.get("updated_time")),
                'platform': platform
            }
            campaings_general.append(item_row)
        except Exception as e:
            errors.append({
                'nivel': 'item',
                'item_id': item.get('id', 'Desconocido'),
                'error': str(e)
            })
            continue
            
    return campaings_general, errors

def cargar_a_silver_campaigns(bq_client, campaings, entity_id):
    fecha     = date.today().strftime('%Y%m%d')
    temp_base = f"datawarehouseevents-demo.silver_layer.temp_camp_{entity_id}_{fecha}"
    
    try:
        _cargar_temporal(bq_client, campaings, f"{temp_base}_campaigns")
        _merge_campaigns(bq_client, f"{temp_base}_campaigns")
    finally:
        bq_client.delete_table(f"{temp_base}_campaigns", not_found_ok=True)

def _cargar_temporal(bq_client, datos, tabla):
    job_config = bigquery.LoadJobConfig(
        write_disposition  = "WRITE_TRUNCATE",
        create_disposition = "CREATE_IF_NEEDED"
    )
    bq_client.load_table_from_json(datos, tabla, job_config=job_config).result()

def _merge_campaigns(bq_client, temp_table):
    bq_client.query(f"""
        MERGE `datawarehouseevents-demo.silver_layer.campaigns` AS target
        USING (
            SELECT
                CAST(account_id            AS STRING)    AS account_id,
                CAST(campaign_id           AS STRING)    AS campaign_id,
                CAST(campaign_name         AS STRING)    AS campaign_name,
                CAST(status                AS STRING)    AS status,
                CAST(objective             AS STRING)    AS objective,
                CAST(budget_rebalance_flag AS BOOL)      AS budget_rebalance_flag,
                CAST(daily_budget          AS FLOAT64)   AS daily_budget,
                CAST(lifetime_budget       AS FLOAT64)   AS lifetime_budget,
                CAST(start_time            AS TIMESTAMP) AS start_time,
                CAST(stop_time             AS TIMESTAMP) AS stop_time,
                CAST(created_time          AS TIMESTAMP) AS created_time,
                CAST(updated_time          AS TIMESTAMP) AS updated_time,
                CAST(platform              AS STRING)    AS platform
            FROM `{temp_table}`
        ) AS source
        ON target.campaign_id = source.campaign_id
        WHEN MATCHED AND COALESCE(source.updated_time, '1970-01-01') > COALESCE(target.updated_time, '1970-01-01') THEN UPDATE SET
            status                = source.status,
            stop_time             = source.stop_time,
            daily_budget          = source.daily_budget,
            lifetime_budget       = source.lifetime_budget,
            budget_rebalance_flag = source.budget_rebalance_flag,
            campaign_name         = source.campaign_name,
            loaded_at             = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (
                account_id, campaign_id, campaign_name, status, objective, 
                budget_rebalance_flag, daily_budget, lifetime_budget, 
                start_time, stop_time, created_time, updated_time, platform, loaded_at
            )
            VALUES (
                source.account_id, source.campaign_id, source.campaign_name, source.status, source.objective, 
                source.budget_rebalance_flag, source.daily_budget, source.lifetime_budget, 
                source.start_time, source.stop_time, source.created_time, source.updated_time, source.platform, CURRENT_TIMESTAMP()
            )
    """).result()

def transformar_y_cargar_campaigns(bq_client, gcs_client=None, usar_gcs=False):
    fecha = date.today().strftime('%Y-%m-%d')
    platforms = bq_client.query("""
        SELECT DISTINCT platform FROM pipeline_management.account_catalog WHERE activo = TRUE AND platform = 'Meta'
    """).result()
    
    for pl in platforms:
        platform = pl['platform']
        archivos = listar_archivos_del_dia(
            source        = platform.lower(),
            business_line = "campaigns",
            fecha         = fecha,
            usar_gcs      = usar_gcs,
            gcs_client    = gcs_client
        )
        
        for path in archivos:
            ya_cargado = bq_client.query(f"""
                SELECT COUNT(*) as total FROM pipeline_management.load_control
                WHERE gcs_path = '{path}' AND status = 'completado' AND fecha_archivo = '{fecha}'
            """).result()
            
            if list(ya_cargado)[0]['total'] > 0:
                print(f"Archivo {path} ya cargado a Silver, omitiendo")
                continue
            
            account_id = path.split('/')[3]
            registrar_carga(bq_client, path, "campaigns", account_id, fecha)
            
            try:
                if usar_gcs:
                    bucket = gcs_client.bucket(NOMBRE_BUCKET)
                    blob = bucket.blob(path)
                    json_string = blob.download_as_text()
                    raw_data = json.loads(json_string)
                else:
                    with open(path, 'r', encoding="utf-8") as f:
                        raw_data = json.load(f)
                
                campaigns, errors = transformar_campaigns(raw_data, platform, account_id)
                
                if campaigns:
                    cargar_a_silver_campaigns(bq_client, campaigns, account_id)
                
                actualizar_carga(bq_client, path, 'completado')
                print(f"✓ Catálogo de campañas actualizado exitosamente en Silver para cuenta: {account_id}")
                
            except Exception as e:
                actualizar_carga(bq_client, path, 'fallido', str(e))
                registrar_error(
                    bq_client    = bq_client,
                    proceso      = "upload_campaigns_meta",
                    platform     = "meta",
                    parent_id    = account_id,
                    parent_name  = "ad_account",
                    entity_id    = account_id,
                    entity_name  = "ad_account",
                    error        = e
                )
                continue

# ==============================================================================
# 5. ORQUESTADOR MAESTRO (ENTRY POINT HTTP PARA CLOUD RUN)
# ==============================================================================
@functions_framework.http
def meta_campaigns_handler(request):
    """
    Punto de entrada que Cloud Run ejecutará al recibir el estímulo del cron job.
    """
    print("Despertando Pipeline de Campañas de Meta...")
    try:
        bq_client = bigquery.Client()
        storage_client = storage.Client()
        
        print("\n--- INICIANDO EXTRACCIÓN (API -> GCS RAW) ---")
        extraer_campanas_meta(bq_client, gcs_client=storage_client)
        
        print("\n--- INICIANDO TRANSFORMACIÓN (GCS RAW -> SILVER BQ) ---")
        transformar_y_cargar_campaigns(bq_client, gcs_client=storage_client, usar_gcs=True)
        
        return "Pipeline de Catálogo de Campañas de Meta ejecutado con éxito.", 200
    except Exception as e:
        print(f"Error crítico en el orquestador general de campañas: {str(e)}")
        return f"Error crítico: {str(e)}", 500
