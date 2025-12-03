import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta, timezone
import pytz 
import os 
from typing import Dict, Any, List
from math import radians, cos, sin, asin, sqrt
import sqlite3 # <- AGREGADO: Importa la biblioteca SQLite

hide_st_page_style = """
<style>
/* Oculta la navegación multipágina en la barra lateral */
div[data-testid="stSidebarNav"] {
    display: none;
}
</style>
"""

# Aplica el CSS
st.markdown(hide_st_page_style, unsafe_allow_html=True)

# ====================================================
# 0. CONSTANTES GLOBALES Y CONFIGURACIÓN CRÍTICA
# ====================================================

# --- Rutas de Configuración de Flotas ---
CONFIG_DIR = "configuracion_flotas"
MAPPING_FILE = "flotas_codigos.json" 
FLOTA_PLACEHOLDER = "--- Seleccione una flota ---"
DB_FILE_PATH = "gps.db" # <- AGREGADO: Ruta al archivo de base de datos

# --- ZONAS HORARIAS ---
VENEZUELA_TZ = timezone(timedelta(hours=-4))
VENEZUELA_PYTZ = pytz.timezone('America/Caracas')

# --- API ---
REPORT_ENDPOINT = "https://flexapi.foresightgps.com/ForesightFlexAPI.ashx" 
# Cadena de autenticación Base64 
BASIC_AUTH_HEADER = "Basic dGVycGVsOmZzZ3BzVGVycGVs" 
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": BASIC_AUTH_HEADER 
}

# Parámetros fijos de conexión 
USER_ID = "82825"
COMPANY_ID = "5809"

# 🚨 PARÁMETROS DE FILTRADO 🚨
REPORT_ID_PARADAS = 5      
VEHICLE_ID_CHUNK_SIZE = 5  
MIN_DURATION_DEFAULT = 5    # Mínimo 5 minutos
EXCLUSION_RADIUS_METERS = 120 # 120 metros

PLACEHOLDER = "--- Seleccione una opción ---"

# Palabras clave de Ubicación (filtro de respaldo)
EXCLUSION_KEYWORDS = ['SEDE', 'RESGUARDO', 'ESTACIONAMIENTO', 'PARQUEADERO'] 

# Columnas finales solicitadas
DISPLAY_COLUMN_NAMES = [
    'Unit', 'Conductor', 'Start', 'End', 'T.Total', 'Ubicacion', 
    'Status', 'Longitude', 'Latitude', 'Mensaje Parada Larga' 
]



# ====================================================
# 2. FUNCIONES DE DISTANCIA, CARGA DE FLOTAS Y UTILIDAD
# ====================================================

def haversine(lon1, lat1, lon2, lat2):
    """Calcula la distancia Haversine en metros entre dos puntos (lat/lon)."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000 # Radio de la Tierra en metros
    return c * r

# --------------------------------------------------------------------------
# --- LÓGICA DE CONEXIÓN A BASE DE DATOS (CORREGIDA) ---
# --------------------------------------------------------------------------

@st.cache_resource(show_spinner="Conectando a la base de datos de asignación...")
def get_db_connection(db_file=DB_FILE_PATH):
    """Crea una conexión de base de datos SQLite cacheada."""
    try:
        # Usamos check_same_thread=False para compatibilidad con Streamlit
        conn = sqlite3.connect(db_file, check_same_thread=False) 
        return conn
    except sqlite3.Error as e:
        st.error(f"❌ Error al conectar con la DB '{db_file}': {e}")
        return None

def get_driver_name_for_unit(conn: sqlite3.Connection, unit_id: str) -> str:
    """
    Busca el nombre del conductor para una unidad dada en la tabla 'asignacion',
    usando el campo conductor_ficha y buscando por unidad.
    
    🚨 FIX CLAVE: Usa TRIM() en la columna 'unidad' de la DB para manejar
    los espacios en blanco que causan el ERROR_DB.
    """
    if conn is None:
        return "ERROR_DB_CONEXIÓN"
    
    # 1. Limpiamos el ID de la unidad de entrada de espacios externos
    unit_id_clean = unit_id.strip()

    # 2. Usamos TRIM(unidad) en la consulta para asegurar que el dato en la DB esté limpio.
    sql = "SELECT conductor_ficha FROM asignacion WHERE TRIM(unidad) = ?" 
    cur = conn.cursor()
    try:
        # Ejecutamos la consulta con el ID de unidad limpio
        cur.execute(sql, (unit_id_clean,))
        row = cur.fetchone()
        if row:
            # Devuelve el nombre del conductor (también limpiado)
            return str(row[0]).strip()
        else:
            return "NO ASIGNADO"
    except sqlite3.Error as e:
        # En caso de error de SQL (ej: tabla/columna incorrecta)
        print(f"[ERROR DB] Al buscar conductor para unidad {unit_id}: {e}")
        return "ERROR_DB"

# --------------------------------------------------------------------------
# --- FIN LÓGICA DE CONEXIÓN A BASE DE DATOS ---
# --------------------------------------------------------------------------


@st.cache_data(show_spinner="Cargando configuración de flotas...")
def load_all_fleets_config() -> Dict[str, Dict[str, Any]]:
    """
    Carga la configuración de flotas, manejando tanto el formato de Diccionario 
    como el formato de Lista para las coordenadas.
    """
    flotas_config = {}
    mapping_filepath = os.path.join(CONFIG_DIR, MAPPING_FILE)

    if not os.path.exists(mapping_filepath):
        print(f"Error: Archivo maestro '{MAPPING_FILE}' no encontrado en '{CONFIG_DIR}'.")
        return {}

    try:
        with open(mapping_filepath, 'r', encoding='utf-8') as f:
            flotas_map = json.load(f)
    except Exception as e:
        print(f"Error al cargar '{MAPPING_FILE}'. Error: {e}")
        return {}

    for nombre_flota, map_data in flotas_map.items():
        if not isinstance(map_data, dict) or 'codigo_db' not in map_data or 'archivo_flota' not in map_data:
            continue
            
        archivo_json_nombre = map_data['archivo_flota']
        filepath = os.path.join(CONFIG_DIR, archivo_json_nombre)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if "ids" in data and isinstance(data["ids"], str): 
                    
                    sede_coords = data.get("sede_coords", [])
                    resguardo_coords = data.get("resguardo_secundario_coords", [])
                    
                    exclusion_zones = []
                    
                    # LÓGICA DE CARGA MEJORADA (SOPORTE LISTA Y DICCIONARIO)
                    for item in sede_coords + resguardo_coords:
                        coord = None
                        if isinstance(item, dict) and 'lat' in item and 'lon' in item:
                            # Formato Diccionario: {"lat": N, "lon": N}
                            coord = item
                        elif isinstance(item, list) and len(item) == 2:
                            # Formato Lista: [lat, lon]
                            coord = {"lat": item[0], "lon": item[1]}

                        if coord:
                            try:
                                final_coord = {
                                    "lat": float(coord['lat']),
                                    "lon": float(coord['lon'])
                                }
                                exclusion_zones.append(final_coord)
                            except (TypeError, ValueError):
                                continue # Ignorar coordenadas no numéricas
                    # FIN LÓGICA DE CARGA
                    
                    flotas_config[nombre_flota] = {
                        "VEHICLE_IDS_FULL_STRING": data["ids"],
                        "SUBFLEET_ID": map_data["codigo_db"],       
                        "EXCLUSION_ZONES": exclusion_zones  
                    }
                else:
                    print(f"[ADVERTENCIA] Archivo '{archivo_json_nombre}' omitido: falta la clave 'ids' o no es una cadena de texto.")

        except FileNotFoundError:
            print(f"[ADVERTENCIA] Archivo de detalles '{archivo_json_nombre}' referenciado no encontrado.")
        except Exception as e:
            print(f"Error al cargar '{archivo_json_nombre}': {e}")

    return flotas_config

# Carga y definición global de la configuración
FLOTAS_CONFIG = load_all_fleets_config()

# Función para dividir la cadena de IDs en chunks
def chunk_vehicle_ids(id_list_string: str, size: int) -> List[str]:
    """Divide una cadena de IDs separada por comas en fragmentos (chunks) de un tamaño dado."""
    ids = [id.strip() for id in id_list_string.split(',') if id.strip()]
    return [','.join(ids[i:i + size]) for i in range(0, len(ids), size)]

def format_duration_for_narrative(total_minutes: int) -> str:
    """
    Convierte la duración total en minutos a un formato de horas y minutos 
    si es >= 60 min, o solo minutos si es < 60 min.
    Ej: 82 minutos -> '1 hora, 22 minutos'.
    """
    total_minutes = int(total_minutes) 
    
    if total_minutes >= 60:
        hours = total_minutes // 60
        minutes_remaining = total_minutes % 60
        
        hour_label = "hora" if hours == 1 else "horas"
        
        if minutes_remaining > 0:
            return f"{hours} {hour_label}, {minutes_remaining} minutos"
        else:
            return f"{hours} {hour_label}"
    else:
        return f"{total_minutes} minutos"
        
# FUNCIÓN UTILIZADA PARA GENERAR EL TXT/CSV DELIMITADO POR COMAS
def convert_df_to_csv_text(df_source: pd.DataFrame) -> str:
    """
    Convierte el DataFrame filtrado de paradas a un string CSV/TXT, 
    limpiando la columna Ubicacion y seleccionando las columnas clave.
    """
    # Seleccionar las columnas clave para el archivo de texto
    # Se incluyen las nuevas columnas: Conductor y Mensaje Parada Larga
    download_cols = ['Start', 'Unit', 'Conductor', 'T.Total', 'Ubicacion', 'Mensaje Parada Larga'] 
    download_df = df_source[download_cols].copy()
    
    # Renombrar columnas para la salida
    download_df.rename(columns={
        'Start': 'Hora',
        'Unit': 'Unidad',
        'Conductor': 'Conductor',
        'T.Total': 'Duracion (min)',
        'Ubicacion': 'Ubicacion',
        'Mensaje Parada Larga': 'Mensaje'
    }, inplace=True)
    
    # Limpieza: Asegurarse de que no hay saltos de línea ni comillas en la ubicación
    download_df['Ubicacion'] = (
        download_df['Ubicacion'].astype(str)
        .str.replace('"', '')
        .str.replace('\n', ' ')
        .str.replace(',', ';') 
    )
    
    # Generar el string delimitado por comas (sep=',')
    csv_text = download_df.to_csv(index=False, sep=',')
    return csv_text


# --------------------------------------------------------------------------
# --- FUNCIÓN DE ENRIQUECIMIENTO CON CONDUCTOR Y MENSAJE ---
# --------------------------------------------------------------------------

def enrich_data_with_driver_and_message(df_source: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Añade la columna 'Conductor' y 'Mensaje Parada Larga' al DataFrame, 
    usando la tabla 'asignacion' de la DB.
    """
    df = df_source.copy()
    
    # 1. Obtener el Conductor para cada Unidad
    # Aplicamos la función de consulta por cada unidad
    df['Conductor'] = df['Unit'].apply(lambda uid: get_driver_name_for_unit(conn, uid))
    
    # 2. Construir el Mensaje de Parada Larga
    def build_long_stop_message(row):
        driver = row['Conductor']
        unit = row['Unit']
        start_time = row['Start']
        duration = format_duration_for_narrative(row['T.Total'])
        # Usar la Ubicacion original de la API
        location = row['Ubicacion'].replace(';', ',') 
        
        # CONSTRUCCIÓN DEL MENSAJE SOLICITADO
        return (
            f"⚠️ PARADA LARGA DETECTADA\n"
            f"Conductor: {driver}\n" 
            f"Unidad: {unit}\n"
            f"Inicio: {start_time} (Hora VET)\n"
            f"Duración Total: {duration}\n"
            f"Ubicación: {location}"
        )

    df['Mensaje Parada Larga'] = df.apply(build_long_stop_message, axis=1)
    
    return df

# --------------------------------------------------------------------------
# --- FIN FUNCIÓN DE ENRIQUECIMIENTO ---
# --------------------------------------------------------------------------


# ====================================================
# 3. FUNCIONES DE EJECUCIÓN DE API Y PROCESAMIENTO
# ====================================================

def ejecutar_reporte_paradas(current_vehicle_ids_chunk, fecha_inicio_iso, fecha_fin_iso, subfleet_id, min_duration_minutes):
    """Ejecuta una solicitud de reporte para un fragmento de IDs (máx. 5)."""
    
    # Parámetros para REPORT_ID 5
    VALUE_STRING = (
        f"{USER_ID}|{current_vehicle_ids_chunk}|{fecha_inicio_iso}|{fecha_fin_iso}|"       
        f"{COMPANY_ID}|{subfleet_id}|0|{min_duration_minutes}"
    )

    PAYLOAD = {
        "method": "REPORT_EXECUTE",
        "conncode": "SATEQSA",
        "reportid": REPORT_ID_PARADAS, 
        "userid": USER_ID,
        "prefix": True,
        "parameter": "@USERID|@LIST_VEHICLE_IDS|@STARTDATEANDTIME|@ENDDATEANDTIME|@IsCompany|@IsSubfleet|@IsGroup|@STOPMINDURATION", 
        "value": VALUE_STRING
    }

    try:
        response = requests.post(REPORT_ENDPOINT, headers=HEADERS, json=PAYLOAD, timeout=30)
        response.raise_for_status()
        
        if response.headers.get("Content-Type", "").startswith("application/json"):
            reporte_data = response.json()
            return reporte_data.get("ForesightFlexAPI", {}).get("DATA1", []), reporte_data
        
        return [], {"ERROR": "Respuesta no JSON"}

    except requests.exceptions.RequestException as e:
        return [], {"ERROR": f"Fallo en la conexión para el chunk {current_vehicle_ids_chunk}: {e}"}

def convert_duration_to_minutes(duration_str):
    """Convierte cadenas de duración en formatos 'H:M:S' o 'H:M' a minutos (entero)."""
    try:
        parts = str(duration_str).split(':')
        
        if len(parts) == 3:
            h = int(parts[0])
            m = int(parts[1])
            s = int(parts[2])
        elif len(parts) == 2:
            h = int(parts[0])
            m = int(parts[1])
            s = 0
        else:
            return 0.0

        return (h * 60) + m + (s / 60)
    except:
        return 0.0

@st.cache_data(show_spinner="Cargando reporte de paradas (procesando por lotes de 5)...")
def get_report_data_paradas(vehicle_ids_full_string: str, fecha_inicio_iso: str, fecha_fin_iso: str, subfleet_id: str, min_duration_minutes: int, exclusion_zones: List[Dict[str, Any]]):
    """Ejecuta el reporte y aplica el filtro de duración, ubicación de texto y coordenadas de sede."""
    
    vehicle_chunks = chunk_vehicle_ids(vehicle_ids_full_string, VEHICLE_ID_CHUNK_SIZE)
    all_results = []
    last_debug_output = {}
    
    # Usamos DISPLAY_COLUMN_NAMES como referencia para un DataFrame vacío
    empty_df_cols = [c for c in DISPLAY_COLUMN_NAMES if c not in ('Conductor', 'Mensaje Parada Larga')] + ['Start_Time_UTC', 'End_Time_UTC']

    if not vehicle_chunks:
        return pd.DataFrame(columns=empty_df_cols), {}

    total_ids = sum(len(c.split(',')) for c in vehicle_chunks)
    progress_bar = st.progress(0, text=f"Iniciando procesamiento de {total_ids} unidades en {len(vehicle_chunks)} lotes...")

    for i, chunk in enumerate(vehicle_chunks):
        
        resultados_chunk, debug_output = ejecutar_reporte_paradas(
            chunk, fecha_inicio_iso, fecha_fin_iso, subfleet_id, min_duration_minutes
        )
        all_results.extend(resultados_chunk)
        last_debug_output = debug_output 
        
        progress_value = (i + 1) / len(vehicle_chunks)
        progress_bar.progress(progress_value, text=f"Procesando lote {i+1} de {len(vehicle_chunks)}...")

    progress_bar.empty()
    
    if not all_results:
        return pd.DataFrame(columns=empty_df_cols), last_debug_output 

    df = pd.DataFrame(all_results)
    
    # Mapeo de columnas internas de la API (Report ID 5)
    df = df.rename(columns={
        'Unit': 'Unit', 'Start': 'Start_Time_UTC', 'End': 'End_Time_UTC',                  
        'Duration': 'Duration_Str', 'Location': 'Ubicacion', 'Status': 'Status',                     
        'Longitude': 'Longitude', 'Latitude': 'Latitude'                  
    })
    
    required_cols = ['Unit', 'Duration_Str', 'Ubicacion', 'Start_Time_UTC', 'End_Time_UTC', 'Status', 'Longitude', 'Latitude']
    if not all(col in df.columns for col in required_cols):
         missing_cols = [col for col in required_cols if col not in df.columns]
         st.error(f"Error: La API no devolvió todas las claves esperadas. Faltan: {missing_cols}. Revise el ID de reporte y el mapeo de columnas.")
         return pd.DataFrame(columns=empty_df_cols), last_debug_output

    # Conversión y Cálculo de Duración y FILTRADO INICIAL
    df['T.Total'] = df['Duration_Str'].apply(convert_duration_to_minutes).round(0).astype(int) 
    df.dropna(subset=['T.Total'], inplace=True)
    df = df[df['T.Total'] >= min_duration_minutes].copy() 
    
    if df.empty:
        return pd.DataFrame(columns=empty_df_cols), last_debug_output

    # --- 🚨 FILTRADO DE PARADAS NO DESEADAS (DOBLE FILTRO) 🚨 ---
    
    # 1. FILTRO DE TEXTO (Respaldo)
    pattern = '|'.join(EXCLUSION_KEYWORDS)
    is_text_excluded = df['Ubicacion'].astype(str).str.contains(pattern, case=False, na=False)
    
    # 2. FILTRO GEOGRÁFICO (Principal, 120m)
    
    if exclusion_zones:
        # Función para verificar la proximidad y devolver el estado de exclusión
        def check_proximity_status(row, zones, radius):
            """Chequea si la parada está dentro del radio de exclusión de cualquier zona conocida."""
            lat, lon = pd.to_numeric(row['Latitude'], errors='coerce'), pd.to_numeric(row['Longitude'], errors='coerce')
            if pd.isna(lat) or pd.isna(lon):
                return False 

            for zone in zones:
                zone_lat = pd.to_numeric(zone.get('lat'), errors='coerce') 
                zone_lon = pd.to_numeric(zone.get('lon'), errors='coerce')
                
                if not pd.isna(zone_lat) and not pd.isna(zone_lon):
                    distance = haversine(lon, lat, zone_lon, zone_lat)
                    if distance <= radius:
                        return True # Excluido
                        
            return False # No excluido


        # Aplicar la lógica de proximidad a cada fila
        is_coord_excluded = df.apply(lambda row: check_proximity_status(row, exclusion_zones, EXCLUSION_RADIUS_METERS), axis=1)
        
    else:
        # Si no hay zonas de exclusión definidas
        is_coord_excluded = pd.Series([False] * len(df), index=df.index)
        st.caption("Advertencia: No se encontraron zonas de exclusión por coordenadas válidas en el JSON de la flota. Solo se aplicará el filtro por texto.")

    # Combinar ambos filtros: Excluir si está excluido por texto O por coordenadas
    is_total_excluded = is_text_excluded | is_coord_excluded
    
    # Aplicar el filtro: conservar solo las que NO están excluidas (~is_total_excluded)
    df_filtered = df[~is_total_excluded].copy() 
    
    # FIN: LÓGICA DE FILTRADO
    
    if df_filtered.empty:
        return pd.DataFrame(columns=empty_df_cols), last_debug_output


    # --- Conversión de Hora (UTC -> VET) y Limpieza Final ---
    try:
        df_filtered['Start'] = pd.to_datetime(df_filtered['Start_Time_UTC'], utc=True).dt.tz_convert(VENEZUELA_TZ).dt.strftime('%H:%M:%S')
        df_filtered['End'] = pd.to_datetime(df_filtered['End_Time_UTC'], utc=True).dt.tz_convert(VENEZUELA_TZ).dt.strftime('%H:%M:%S')
    except Exception as e:
        df_filtered['Start'] = df_filtered['Start_Time_UTC']
        df_filtered['End'] = df_filtered['End_Time_UTC']

    # Limpieza de Ubicación y Coordenadas
    df_filtered['Ubicacion'] = df_filtered['Ubicacion'].astype(str).str.replace('\n', ' ').str.strip() 
    df_filtered['Longitude'] = pd.to_numeric(df_filtered['Longitude'], errors='coerce').round(6)
    df_filtered['Latitude'] = pd.to_numeric(df_filtered['Latitude'], errors='coerce').round(6)

    # Nota: No se hace la selección final de columnas aquí, se hace después del enriquecimiento.
    
    return df_filtered, last_debug_output

def local_css():
    """Inyecta CSS personalizado para cambiar el tamaño de la fuente y la estructura."""
    st.markdown(f"""
        <style>
        /* Aumenta el tamaño de fuente base */
        html, body, [class*="stText"], [data-testid="stSidebar"], [data-testid="stMetric"], [data-testid="stCaption"], [data-testid="stInfo"] {{
            font-size: 1.15em !important; 
        }}
        
        /* Ajuste específico para la tabla (st.dataframe) */
        .dataframe {{
            font-size: 1.15em !important;
        }}

        /* Regla para reducir el espacio superior de TODA la página */
        .block-container {{
            padding-top: 2rem; 
            padding-bottom: 0rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }}
        
        /* Modifica el tamaño de todos los st.header() (h1) */
        h1 {{
            font-size: 2.5rem; 
            font-weight: 700; 
        }}
        
        /* Estilo para los mensajes de parada larga */
        .long-stop-message {{
            white-space: pre-wrap; /* Respeta los saltos de línea \n */
            font-size: 1.05em;
            background-color: #f0f2f6;
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 10px;
            border-left: 5px solid #ff4b4b; /* Rojo */
        }}
        
        </style>
        """, unsafe_allow_html=True)


# ====================================================
# 4. FUNCIÓN PRINCIPAL DE LA PÁGINA
# ====================================================

def show_reporte_paradas_page():
    
    st.set_page_config(
        page_title=f"Reporte Detallado de Paradas (ID {REPORT_ID_PARADAS})",
        layout="wide"
    )
    
    # Llamada a la función para ocultar el sidebar
    local_css() 
    
    st.title(f"🚫 Paradas Largas")
    st.caption(f"El reporte se genera haciendo solicitudes por lotes de **{VEHICLE_ID_CHUNK_SIZE} unidades** a la API.")
    
    fleet_map = FLOTAS_CONFIG
    fleet_keys = [FLOTA_PLACEHOLDER] + list(fleet_map.keys())
    
    current_date = datetime.now(VENEZUELA_PYTZ).date() 
    yesterday = current_date - timedelta(days=1)
    
    if 'paradas_submitted' not in st.session_state:
        st.session_state.paradas_submitted = False
        st.session_state.min_duration = MIN_DURATION_DEFAULT 
        st.session_state.date_option_selector_paradas_key = "Ayer" 
        st.session_state.selected_fleet_key = FLOTA_PLACEHOLDER
        st.session_state.dynamic_params = {} 

    # --- SECCIÓN DE CONFIGURACIÓN EN SIDEBAR (AHORA INCLUYE EL FORMULARIO) ---
    with st.sidebar:
        # --- Botón Home con Limpieza de Caché (CORREGIDO) ---
        if st.button("🏡 Home", use_container_width=True):
            st.cache_data.clear()
            st.switch_page("home.py") 
            # 🚨 CORRECCIÓN CLAVE: Esto fuerza el cambio de página después de limpiar la caché.
            # Asegúrate de que tienes un archivo 'home.py' en tu configuración Streamlit.
            # st.switch_page("home.py") 
            # Si no tienes multipagina, solo recarga
            st.rerun() 

         # Bloque de Logo y CSS de la navegación ya existente
        st.markdown(
            '<p style="text-align: center; margin-bottom: 5px; margin-top: 5px;">'
            '<img src="https://fospuca.com/wp-content/uploads/2018/08/fospuca.png" alt="Logo Fospuca" style="width: 100%; max-width: 120px; height: auto; display: block; margin-left: auto; margin-right: auto;">'
            '</p>',
         unsafe_allow_html=True
        )
        
           
        # 🚨 FORMULARIO MOVIDO DENTRO DEL SIDEBAR 🚨
        with st.form("paradas_report_form"):
            st.subheader("Configuración del Reporte")
            
            selected_fleet_key = st.selectbox(
                "Seleccione la Flota:", options=fleet_keys, key="selected_fleet_key"
            )
            
            date_options = (PLACEHOLDER, "Ayer", "Hoy", "Día Específico")
            date_option_input = st.selectbox(
                "Seleccione el Rango de Fecha:", options=date_options, key="date_option_selector_paradas_key"
            )
            
            # Mínimo 5 minutos, paso de 5 en 5
            min_duration_input = st.number_input(
                "Duración Mínima (minutos):", min_value=5, max_value=1440, value=MIN_DURATION_DEFAULT, step=5
            )

            start_date_calc = yesterday 
            end_date_calc = yesterday
            
            if date_option_input == "Día Específico":
                selected_day = st.date_input("Seleccione el Día:", value=yesterday)
                start_date_calc = selected_day
                end_date_calc = selected_day
            elif date_option_input == "Hoy":
                start_date_calc = current_date
                end_date_calc = current_date
            
            submitted = st.form_submit_button(
                "Generar Reporte",
                disabled=False,
                use_container_width=True
            )
            
            # LÓGICA DE VALIDACIÓN (DESPUÉS DEL SUBMIT)
            if submitted:
                st.session_state.paradas_submitted = False 

                if selected_fleet_key == FLOTA_PLACEHOLDER or date_option_input == PLACEHOLDER:
                    st.error("❌ **ERROR:** Debe seleccionar **Flota** y **Rango de Fecha**.")
                    pass 

                elif selected_fleet_key in FLOTAS_CONFIG:
                    st.session_state.dynamic_params = FLOTAS_CONFIG[selected_fleet_key]
                    st.session_state.paradas_submitted = True
                    
                    st.session_state.min_duration = int(min_duration_input)
                    
                    # Generación de fechas UTC para la API
                    start_dt_local = VENEZUELA_PYTZ.localize(datetime.combine(start_date_calc, datetime.min.time()))
                    start_dt_utc = start_dt_local.astimezone(pytz.utc)
                    st.session_state.fecha_inicio_api = start_dt_utc.strftime('%Y-%m-%d %H:%M:%S') + '.217'

                    end_dt_local = VENEZUELA_PYTZ.localize(datetime.combine(end_date_calc, datetime.max.time().replace(microsecond=0)))
                    end_dt_utc = end_dt_local.astimezone(pytz.utc)
                    st.session_state.fecha_fin_api = end_dt_utc.strftime('%Y-%m-%d %H:%M:%S') + '.999'

                else:
                    st.error(f"❌ **ERROR DE CONFIGURACIÓN:** La flota '{selected_fleet_key}' no se encontró.")
                    pass
        
        st.warning("En Fase de Desarrollo: puede presentar inconsistencias") # Mensaje de advertencia para el final del sidebar
    # --- FIN DE LA BARRA LATERAL (SIDEBAR) ---


    if st.session_state.paradas_submitted:
        
        vehicle_ids_string = st.session_state.dynamic_params.get("VEHICLE_IDS_FULL_STRING", "")
        subfleet_id = st.session_state.dynamic_params.get("SUBFLEET_ID", "")
        exclusion_zones = st.session_state.dynamic_params.get("EXCLUSION_ZONES", []) 


        if not vehicle_ids_string or not subfleet_id:
            st.error(f"La flota **{st.session_state.selected_fleet_key}** no tiene IDs de vehículos o SUBFLEET_ID.")
            return

        total_units = len(vehicle_ids_string.split(','))
        st.markdown("---")
        st.info(f"Reporte generado para **{st.session_state.selected_fleet_key}** ({total_units} unidades) | Código DB: **{subfleet_id}**")
        
        # --- Obtención de la Fecha para Display ---
        try:
            # Intenta obtener la fecha del string de la API
            start_date_used = datetime.strptime(st.session_state.fecha_inicio_api[:10], '%Y-%m-%d').date()
        except Exception:
            # Fallback seguro: Si el parseo falla, usa la fecha actual como objeto date
            start_date_used = datetime.now(VENEZUELA_PYTZ).date()

        st.caption(f"Día (Hora de Venezuela): **{start_date_used.isoformat()}**") 
        # ---------------------------------------------------------------
        
        st.caption(f"Se excluyen paradas dentro de **{EXCLUSION_RADIUS_METERS} metros** de **{len(exclusion_zones)}** zonas definidas por coordenadas (sede/resguardo), y por las palabras clave: **{', '.join(EXCLUSION_KEYWORDS)}**.")

        # Obtener los datos filtrados
        data_df, debug_output = get_report_data_paradas(
            vehicle_ids_string, 
            st.session_state.fecha_inicio_api,
            st.session_state.fecha_fin_api,
            subfleet_id,        
            st.session_state.min_duration,
            exclusion_zones 
        )

        if not data_df.empty:
            
            # --------------------------------------------------------------------------
            # LÓGICA DE CONEXIÓN Y ENRIQUECIMIENTO
            db_conn = get_db_connection()
            if db_conn:
                data_df = enrich_data_with_driver_and_message(data_df, db_conn)
            # Aplicar la selección de columnas finales, incluyendo las nuevas
            data_df = data_df[DISPLAY_COLUMN_NAMES].sort_values(by=['Unit', 'Start']).reset_index(drop=True)
            # --------------------------------------------------------------------------
            
            st.subheader(f"Resultados Consolidados (Duración ≥ {st.session_state.min_duration} min)")
            
            # ======================================================
            # CÁLCULO DE MÉTRICAS 
            # ======================================================
            total_duration_minutes = data_df['T.Total'].sum()
            total_events = len(data_df)
            
            # Calcular el promedio de parada larga
            average_stop_duration_minutes = total_duration_minutes / total_events
            
            # Formatear el promedio a 'X hora(s), Y minutos'
            formatted_average_duration = format_duration_for_narrative(int(round(average_stop_duration_minutes)))

            
            col1, col2, col3 = st.columns(3)
            col1.metric("Unidades con Paradas", f"{data_df['Unit'].nunique()}")
            col2.metric("Total de Eventos", f"{total_events}")
            col3.metric("Promedio de Parada Larga", formatted_average_duration) 
            # ======================================================
            
            st.markdown("---")
            
            st.subheader("Tabla de Eventos Detallados (Incluye Conductor y Mensaje)")
            st.dataframe(data_df, use_container_width=True)

            # ======================================================
            # SECCIÓN DE MENSAJES DE PARADA LARGA Y DESCARGA 
            # ======================================================
            st.markdown("---")
            st.subheader("📝 Mensajes de Parada Larga (Para Consolidado)")
            
            for _, row in data_df.iterrows():
                # Mostrar el mensaje con estilo CSS (ver función local_css)
                st.markdown(
                    f'<div class="long-stop-message">{row["Mensaje Parada Larga"]}</div>', 
                    unsafe_allow_html=True
                )
                
            # Generar el contenido del CSV/TXT
            csv_content = convert_df_to_csv_text(data_df)

            # Botón de descarga para TXT delimitado por comas
            st.download_button(
                label="📥 Descargar Reporte Completo (CSV)",
                data=csv_content,
                # El archivo se nombra automáticamente con la fecha
                file_name=f"Reporte_Paradas_Largas_{start_date_used}.csv", 
                mime="text/csv", 
                key='download_paradas_csv'
            )
            # ======================================================
            
            st.markdown("---")

        elif st.session_state.paradas_submitted:
             st.markdown("---")
             st.warning(f"No se encontraron registros de paradas operativas que cumplieran el filtro (≥ {st.session_state.min_duration} min) para ninguna de las {total_units} unidades después del doble filtrado.")
             
             st.subheader("🚨 Último Debug de Respuesta de API:")
             if debug_output:
                st.code(json.dumps(debug_output, indent=2), language='json')


    else:
        st.info("Seleccione la **Flota**, la **Fecha** y el **Límite de Duración** y luego haga clic en 'Generar Reporte Detallado'.")

# ====================================================
# 5. PUNTO DE ENTRADA PARA EJECUCIÓN DIRECTA
# ====================================================
if __name__ == '__main__':
    show_reporte_paradas_page()
