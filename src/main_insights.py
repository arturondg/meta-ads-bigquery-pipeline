import json 
import requests
import pandas as pd

from datetime import datetime, date, timedelta, timezone
import os
from pathlib import Path
import hashlib
import time
from collections import defaultdict

from google.cloud import bigquery
from google.oauth2 import service_account


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9"
}

ef extraer_insights_meta(bq_client, gcs_client=None):
    print("Iniciando extracción de insights de Meta...")
    
    # 1. Ejecutar tu Query de BigQuery
    campaigns_rows = bq_client.query("""
        SELECT DISTINCT
            s.campaign_id,
            c.account_id,
            ec.last_insight_date,
            COALESCE(ec.extraction_status, 'pendiente') AS extraction_status
        FROM `datawarehouseevents-demo.silver_layer.subitems_digital` s
        INNER JOIN `datawarehouseevents-demo.silver_layer.campaigns` c 
            ON s.campaign_id = c.campaign_id
        LEFT JOIN `datawarehouseevents-demo.pipeline_management.campaigns_extraction_control` ec 
            ON s.campaign_id = ec.campaign_id
        WHERE DATE(c.start_time) <= CURRENT_DATE() 
          -- AQUI AÑADIMOS EL FALLIDO
          AND COALESCE(ec.extraction_status, 'pendiente') IN ('pendiente', 'extraccion_hecha', 'fallido')
          AND c.platform = 'Meta'
    """).result()
    
    # 2. AGRUPACIÓN INTELIGENTE (Por date_preset en lugar de status)
    estructura_cuentas = defaultdict(lambda: defaultdict(list))
    
    for row in campaigns_rows:
        acc_id = row['account_id']
        
        # MAGIA AQUI: Si nunca ha bajado datos, pide todo. Si ya tiene historial, pide 14 días.
        rango_fecha = "maximum" if row['last_insight_date'] is None else "last_14d"
        
        estructura_cuentas[acc_id][rango_fecha].append(row['campaign_id'])
        
    if not estructura_cuentas:
        print("No hay campañas pendientes de insights para el día de hoy.")
        return

    # 3. PROCESAMIENTO POR CUENTA
    for account_id, estados in estructura_cuentas.items():
        print(f"\nProcesando Cuenta Publicitaria: act_{account_id}")
        
        # Lista maestra donde acumularemos los insights de TODAS las campañas de esta cuenta
        todos_los_insights_cuenta = []
        campanas_exitosas_cuenta = []
        campanas_fallidas_cuenta = []
        
        token = access_token # Tu variable de acceso a la API
        
        # Procesamos de forma separada según el estado para aplicar el date_preset correcto
        for rango_fecha_lote, lista_campanas in estados.items():
            # Truco de la tabla de presets:
            
            # SEGMENTACIÓN: Dividimos las campañas de este estado en lotes de 10
            for lote in chunk_list(lista_campanas, 10):
                url, params = get_facebook_data("campaigns_insights", cuenta=account_id, campaigns=lote, date_preset=rango_fecha_lote)
                
                # Consumimos la API con control de paginación interno
                data_lote, error = run_meta_insights_paginated(url, params, token)
                
                if error:
                    print(f"  X Error en lote {lote}: {error}")
                    campanas_fallidas_cuenta.extend(lote)
                    
                    # Registramos el error de este lote específico
                    registrar_error(
                        bq_client=bq_client, proceso="get_insights_meta", platform="meta", error=Exception(error),
                        entity_id=account_id, entity_name="ad_account", parent_id=account_id, parent_name="ad_account"
                    )
                else:
                    # Acumulamos en la bolsa única de la cuenta
                    todos_los_insights_cuenta.extend(data_lote)
                    campanas_exitosas_cuenta.extend(lote)
                    print(f"  ✓ Lote de {len(lote)} campañas extraído con éxito ({len(data_lote)} filas diarias).")
                
                # Pausa de cortesía para cuidar el Rate Limit de Meta
                time.sleep(0.5)
                
        # 4. GUARDADO ÚNICO POR CUENTA
        # Solo guardamos el archivo si al menos un lote tuvo éxito y trajo información
        if todos_los_insights_cuenta:
            path = name_path("meta", "insights", account_id)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            with open(path, "w", encoding='utf-8') as f:
                json.dump(todos_los_insights_cuenta, f, indent=2)
            print(f"➔ ARCHIVO GUARDADO: {path} con {len(todos_los_insights_cuenta)} registros totales.")
            
        # 5. ACTUALIZACIÓN DE ESTADOS EN BATCH
        # Pasamos a 'extraccion_hecha' las que funcionaron (así mañana entran en modo incremental/last_14d)
        if campanas_exitosas_cuenta:
            actualizar_status_campanas_batch(bq_client, campanas_exitosas_cuenta, account_id, 'extraccion_hecha')
            
        # Si un lote falló por timeout o token, las dejamos en su estado anterior o 'fallido' para reintento
        if campanas_fallidas_cuenta:
            actualizar_status_campanas_batch(bq_client, campanas_fallidas_cuenta, account_id, 'fallido')

    print("\nProceso de extracción de insights finalizado.")
    
    
def actualizar_status_campanas_batch(bq_client, campaign_ids, account_id, status_intencion):
    """
    Actualiza el estado de control. 
    Si status_intencion es 'exito', el SQL decidirá si pasa a 'extraccion_hecha' o a 'completo'.
    Si es 'fallido', se queda en 'fallido'.
    """
    if not campaign_ids:
        return
        
    query = """
        MERGE pipeline_management.campaigns_extraction_control AS target
        USING (
            -- Extraemos los IDs de la lista de Python y cruzamos con Silver para traer el stop_time
            SELECT 
                id AS campaign_id,  -- ← CAMBIO 1
                @account_id AS account_id, 
                'Meta' AS platform,
                c.stop_time
            FROM UNNEST(@campaign_ids) AS id  -- ← CAMBIO 2 (Sin paréntesis)
            LEFT JOIN `datawarehouseevents-demo.silver_layer.campaigns` c
                ON id = c.campaign_id  -- ← CAMBIO 3
        ) AS source ON target.campaign_id = source.campaign_id
        
        WHEN MATCHED THEN UPDATE SET 
            last_insight_date = CURRENT_DATE(),
            ultima_extraccion = CURRENT_TIMESTAMP(),
            extraction_status = CASE 
                WHEN @status_intencion = 'fallido' THEN 'fallido'
                -- AQUI ENTRA TU LOGICA DE STOP TIME + 7 DIAS
                WHEN source.stop_time IS NOT NULL AND CURRENT_DATE() >= DATE_ADD(DATE(source.stop_time), INTERVAL 7 DAY) THEN 'completo'
                ELSE 'extraccion_hecha'
            END
            
        WHEN NOT MATCHED THEN INSERT (campaign_id, account_id, platform, last_insight_date, extraction_status, ultima_extraccion)
            VALUES (
                source.campaign_id, 
                source.account_id, 
                source.platform, 
                CURRENT_DATE(), 
                -- Si es nueva, evaluamos de una vez si ya nació 'completa' por ser muy vieja
                CASE 
                    WHEN @status_intencion = 'fallido' THEN 'fallido'
                    WHEN source.stop_time IS NOT NULL AND CURRENT_DATE() >= DATE_ADD(DATE(source.stop_time), INTERVAL 7 DAY) THEN 'completo'
                    ELSE 'extraccion_hecha'
                END,
                CURRENT_TIMESTAMP()
            )
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("campaign_ids", "STRING", list(campaign_ids)),
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
            bigquery.ScalarQueryParameter("status_intencion", "STRING", status_intencion)
        ]
    )
    bq_client.query(query, job_config=job_config).result()
    
    
# import time
# from collections import defaultdict

def run_meta_insights_paginated(url, params, token):
    """Ejecuta la query de Insights en Meta y sigue los cursores de paginación."""
    actual_params = params.copy()
    actual_params["access_token"] = token
    current_url = url
    results = []
    
    try:
        while current_url:
            response = requests.get(current_url, params=actual_params, headers=HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if "error" in data:
                msg = data["error"].get("message", "Error desconocido")
                return None, f"Meta API Error: {msg}"
                
            results.extend(data.get("data", []))
            
            # Meta devuelve la URL completa para la siguiente página en 'paging.next'
            current_url = data.get("paging", {}).get("next")
            actual_params = None # Limpiamos params porque la URL 'next' ya los incluye
            
        return results, None
    except Exception as e:
        return None, str(e)

def chunk_list(lst, n):
    """Divide una lista en lotes de tamaño n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
        

# funcion para extraccion geneneral, obtener datos


def name_path(source, tipo, id_elemento):
    
    path=f"raw/{source}/{tipo}/{id_elemento}/{date.today()}/data.json"
    return path


def get_facebook_data(proceso, cuenta=None, campaigns=None, date_preset=None):
    
    BASE = "https://graph.facebook.com/v25.0"
    url = ""
    params_campaings = {}

    if proceso == "campaigns_general":
        url = f"{BASE}/act_{cuenta}/campaigns"
        params_campaings = {
            "fields": "id,name,status,objective,daily_budget,lifetime_budget,start_time,stop_time,created_time, updated_time",
            "limit": 1000,
        }
        
    elif proceso == "campaigns_insights":
        url = f"{BASE}/act_{cuenta}/insights"
        
        # 1. Armamos el objeto filtro limpio
        # Nota: asegúrate de que al llamar la función, 'campaigns' ya sea una lista: ["123", "456"]
        filtro = [
            {
                "field": "campaign.id",
                "operator": "IN",
                "value": campaigns  
            },
            {
                "field": "action_type",
                "operator": "IN",
                "value": ["purchase"]
            }
        ]
        
        # 2. Armamos los parámetros
        params_campaings = {
            "fields": "campaign_id,campaign_name,impressions,clicks,spend,actions,action_values",
            "level": "campaign",
            "date_preset": date_preset, # Sin las llaves de diccionario
            "time_increment": 1,        # Coma agregada
            "limit": 100,
            "filtering": json.dumps(filtro) # Serializamos a texto para que la URL se arme bien
        }
        
    return url, params_campaings


def registrar_error(bq_client, proceso, entity_id, entity_name, error, intentos=1,
                    platform=None, parent_id=None, parent_name=None,
                    child_id=None,  child_name=None):
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
    
    
def listar_archivos_del_dia(source,business_line, fecha=None, usar_gcs=False, gcs_client=None):
    if not fecha:
        fecha = date.today().strftime('%Y-%m-%d')
    
    prefix = f"raw/{source}/{business_line}"
    
    if usar_gcs:
        # Lista desde GCS
        blobs = gcs_client.list_blobs(prefix=prefix)
        return [b.name for b in blobs if fecha in b.name]
    else:
        # Lista desde local
        archivos = []
        base_path = Path(prefix)
        # base_path = prefix
        if base_path.exists():
            archivos = [
                str(p).replace('\\', '/') for p in base_path.rglob("data.json")
                if fecha in str(p)
            ]
        return archivos
    
def transformar_y_cargar_insights(bq_client, gcs_client=None, usar_gcs=False):
    
    fecha = date.today().strftime('%Y-%m-%d')
    
    # Lista business lines activas
    platforms = bq_client.query("""
        SELECT DISTINCT platform
        FROM pipeline_management.account_catalog
        WHERE activo = TRUE
    """).result()
    
    for pl in platforms:
        platform = pl['platform']                    # ← corregido

        archivos = listar_archivos_del_dia(
            source        = platform,                # ← agregado
            business_line = "insights",
            fecha         = fecha,
            usar_gcs      = usar_gcs,
            gcs_client    = gcs_client
        )
        
        for path in archivos:
            
            # Verificar si ya fue cargado hoy
            ya_cargado = bq_client.query(f"""
                SELECT COUNT(*) as total
                FROM pipeline_management.load_control
                WHERE gcs_path = '{path}'
                AND status = 'completado'
                AND fecha_archivo = '{fecha}'
            """).result()
            
            if list(ya_cargado)[0]['total'] > 0:
                print(f"Archivo {path} ya cargado, omitiendo")
                continue
            
            # Registrar intento
            account_id = path.split('/')[3]
            registrar_carga(bq_client, path, platform, account_id, fecha)
            
            try:
                # Leer archivo
                if usar_gcs:
                    raw_data = gcs_client.leer(path)
                else:
                    with open(path, 'r') as f:
                        raw_data = json.load(f)
                
                # Transformar según business line
                campaigns, errors = transformar_insights(raw_data, platform, account_id)
                
                
                if campaigns:
                # Cargar a BigQuery
                    cargar_a_silver_insights(bq_client, campaigns, account_id)
                
                # Marcar completado
                actualizar_carga(bq_client, path, 'completado')
                
            except Exception as e:
                actualizar_carga(bq_client, path, 'fallido', str(e))
                registrar_error(
                    bq_client    = bq_client,
                    proceso      = "upload_insights_meta",
                    platform     = "meta",
                    parent_id    = account_id,
                    parent_name  = "ad_account",
                    entity_id    = account_id,
                    entity_name  = "ad_account",
                    error        = e
                )
                continue

#Funcion para registrar la carga en bigquery, load_control
def registrar_carga(bq_client, path, business_line, entity_id, fecha):
    query = """
        MERGE pipeline_management.load_control AS target
        USING (
            SELECT 
                @path AS gcs_path, 
                @business_line AS business_line, 
                @entity_id AS entity_id, 
                CAST(@fecha AS DATE) AS fecha_archivo  -- ← EL FIX ESTÁ AQUÍ
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
        # Se sigue mandando como STRING desde Python, pero BigQuery ya lo convertirá a DATE
        bigquery.ScalarQueryParameter("fecha", "STRING", fecha),
    ])
    
    bq_client.query(query, job_config=job_config).result()

def actualizar_carga(bq_client, path, status, error=None):
    # Base del query usando variables de BigQuery (@status, @path, etc.)
    if error:
        query = """
            UPDATE pipeline_management.load_control
            SET status = @status, error_mensaje = @error
            WHERE gcs_path = @path
            AND status = 'en_proceso'
        """
        # Configuramos los parámetros seguros
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("error", "STRING", str(error)),
                bigquery.ScalarQueryParameter("path", "STRING", path),
            ]
        )
    else:
        query = """
            UPDATE pipeline_management.load_control
            SET status = @status
            WHERE gcs_path = @path
            AND status = 'en_proceso'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("error", "STRING", str(error)),
                bigquery.ScalarQueryParameter("path", "STRING", path),
            ]
        )
        
    # Ejecutamos de forma 100% segura
    bq_client.query(query, job_config=job_config).result()
    

def transformar_insights(raw_data, platform, account):
    
    if platform == "Meta":
        campaigns, errors = transformar_insights_meta(raw_data, platform, account)
    elif platform == "Tiktok":
        campaigns, errors = transformar_insights_tiktok(raw_data, platform, account)
    else:
        raise Exception("No platform")
        
    return campaigns, errors



def generate_hash(row):
    # El orden de las llaves importa para el hash, sort_keys=True lo asegura
    row_str = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(row_str.encode()).hexdigest()


def get_action_value(item, key_name, action_type='purchase'):
    """Busca el valor numérico de un tipo de acción dentro de los arreglos de Meta."""
    actions = item.get(key_name, [])
    for action in actions:
        if action.get('action_type') == action_type:
            # Forzamos a float/int dependiendo de si es valor monetario o conteo
            return float(action.get('value', 0)) if key_name == 'action_values' else int(action.get('value', 0))
    return 0


def transformar_insights_meta(raw_data, platform, account):
    campaings_insights = []
    errors = []

    for item in raw_data:
        try:
            item_row = {
                'account_id':        str(account),
                'campaign_id':       item.get('campaign_id'),
                'impressions':       int(item.get('impressions', 0)),
                'clicks':            int(item.get('clicks', 0)),
                'spend':             float(item.get('spend', 0.0)), # Formato moneda real directo de Meta
                'date_information':  item.get('date_stop'),         # Llave del día
                'purchases':         get_action_value(item, 'actions', 'purchase'),
                'value_purchases':   get_action_value(item, 'action_values', 'purchase'),
                'platform':          platform
            }

            # Generamos el row_hash incluyendo las métricas para detectar variaciones retroactivas
            item_row['row_hash'] = generate_hash(item_row)
            
            campaings_insights.append(item_row)

        except Exception as e:
            errors.append({
                'nivel':   'item',
                'item_id': item.get('campaign_id', 'Desconocido'),
                'error':   str(e)
            })
            continue
            
    return campaings_insights, errors


def cargar_a_silver_insights(bq_client, insights, entity_id):
    fecha     = date.today().strftime('%Y%m%d')
    temp_base = f"datawarehouseevents-demo.silver_layer.temp_insights_{entity_id}_{fecha}"
    
    try:
        # Paso 1: carga a temporales
        _cargar_temporal(bq_client, insights, f"{temp_base}_insights")
        
        # Paso 2: MERGE a la tabla de Hechos de Silver
        _merge_insights(bq_client, f"{temp_base}_insights")

    finally:
        # Paso 3: elimina temporales siempre
        bq_client.delete_table(f"{temp_base}_insights", not_found_ok=True)
        
        
def _cargar_temporal(bq_client, datos, tabla):
    job_config = bigquery.LoadJobConfig(
        write_disposition  = "WRITE_TRUNCATE",
        create_disposition = "CREATE_IF_NEEDED"
    )
    bq_client.load_table_from_json(
        datos, tabla, job_config=job_config
    ).result()
        
        
def _merge_insights(bq_client, temp_table):
    bq_client.query(f"""
        MERGE `datawarehouseevents-demo.silver_layer.campaign_insights` AS target
        USING (
            SELECT
                CAST(account_id       AS STRING)  AS account_id,
                CAST(campaign_id      AS STRING)  AS campaign_id,
                CAST(date_information AS DATE)    AS date_information,
                CAST(impressions      AS INT64)   AS impressions,
                CAST(clicks           AS INT64)   AS clicks,
                CAST(spend            AS FLOAT64) AS spend,
                CAST(purchases        AS INT64)   AS purchases,
                CAST(value_purchases  AS FLOAT64) AS value_purchases,
                CAST(platform         AS STRING)  AS platform,
                CAST(row_hash         AS STRING)  AS row_hash
            FROM `{temp_table}`
        ) AS source
        -- CRÍTICO: El cruce se hace por campaña Y por fecha del insight diario
        ON target.campaign_id = source.campaign_id 
       AND target.date_information = source.date_information
        
        -- Si el día ya existe pero el Hash cambió (ej: Meta actualizó conversiones retroactivas)
        WHEN MATCHED AND target.row_hash != source.row_hash THEN UPDATE SET
            impressions     = source.impressions,
            clicks          = source.clicks,
            spend           = source.spend,
            purchases       = source.purchases,
            value_purchases = source.value_purchases,
            row_hash        = source.row_hash,
            updated_at      = CURRENT_TIMESTAMP()
            
        -- Si es un día nuevo para esa campaña
        WHEN NOT MATCHED THEN 
            INSERT (
                account_id, campaign_id, date_information, impressions, clicks, 
                spend, purchases, value_purchases, platform, row_hash, updated_at
            )
            VALUES (
                source.account_id, source.campaign_id, source.date_information, source.impressions, source.clicks, 
                source.spend, source.purchases, source.value_purchases, source.platform, source.row_hash, CURRENT_TIMESTAMP()
            )
    """).result()
