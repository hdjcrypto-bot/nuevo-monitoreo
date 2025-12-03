import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import pytz 
import os 
from typing import Dict, Any 
import sqlite3 
# import altair as alt # Gráfico de línea no solicitado
import plotly.express as px 
import plotly.graph_objects as go 

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
# 0. CONFIGURACIÓN INICIAL Y FUNCIONES DE ESTILO
# ====================================================

# Definición de la Zona Horaria de Venezuela (VET)
VENEZUELA_TZ = pytz.timezone('America/Caracas')
DATABASE_PATH = "gps.db" # Ruta de la BD

def local_css():
    """Inyecta CSS personalizado para cambiar el tamaño de la fuente, centrar y ajustar anchos de columnas."""
    st.markdown(f"""
        <style>
        /* Aumenta el tamaño de fuente base en 15% para el cuerpo y otros elementos clave */
        html, body, [class*="stText"], [data-testid="stSidebar"], [data-testid="stMetric"], [data-testid="stCaption"], [data-testid="stInfo"] {{
            font-size: 1.15em !important; 
        }}
        
        /* Ajuste específico para la tabla (st.dataframe) */
        .dataframe {{
            font-size: 1.15em !important;
        }}

        /* ============== CENTRADO DE COLUMNAS ============== */
        /* Indices: 6: Tiempo (min), 7: N° Reg., 8: V. máx (km/h), 9: V. prom (km/h) */
        
        /* Centrar ENCABEZADOS */
        div[data-testid="stDataFrame"] th:nth-child(6), 
        div[data-testid="stDataFrame"] th:nth-child(7), 
        div[data-testid="stDataFrame"] th:nth-child(8), 
        div[data-testid="stDataFrame"] th:nth-child(9) {{
            text-align: center !important;
        }}
        
        /* Centrar CONTENIDO de las celdas */
        div[data-testid="stDataFrame"] tbody tr td:nth-child(6), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(7), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(8), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(9) {{
            text-align: center !important;
        }}
        /* ============== FIN: CENTRADO DE COLUMNAS ============== */
        
        /* ============== AJUSTE DE ANCHOS POR CSS ============== */
        
        /* Und., Tiempo, N° Reg. - SMALL (~70px) */
        div[data-testid="stDataFrame"] th:nth-child(2), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(2),
        div[data-testid="stDataFrame"] th:nth-child(6), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(6),
        div[data-testid="stDataFrame"] th:nth-child(7), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(7) {{
            width: 70px !important;
            min-width: 70px !important;
            max-width: 70px !important;
        }}
        
        /* Exceso, Inicio, Fin, V. máx, V. prom - MEDIUM (~100px) */
        div[data-testid="stDataFrame"] th:nth-child(3), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(3),
        div[data-testid="stDataFrame"] th:nth-child(4), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(4),
        div[data-testid="stDataFrame"] th:nth-child(5), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(5),
        div[data-testid="stDataFrame"] th:nth-child(8), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(8),
        div[data-testid="stDataFrame"] th:nth-child(9), 
        div[data-testid="stDataFrame"] tbody tr td:nth-child(9) {{
            width: 100px !important;
            min-width: 100px !important;
            max-width: 100px !important;
        }}
        
        /* UBICACIÓN INICIO - FLEX (Ocupar el resto) */
        div[data-testid="stDataFrame"] th:nth-child(10),
        div[data-testid="stDataFrame"] tbody tr td:nth-child(10) {{
             width: auto !important; 
             min-width: 200px !important; 
        }}
        /* ============== FIN: AJUSTE DE ANCHOS POR CSS ============== */
        
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
        
        </style>
        """, unsafe_allow_html=True)
    
# --- CONFIGURACIÓN DINÁMICA DE FLOTAS ---
CONFIG_DIR = "configuracion_flotas"
MAPPING_FILE = "flotas_codigos.json"
FLOTA_PLACEHOLDER = "--- Seleccione la Flota ---"
PLACEHOLDER = "--- Seleccione una opción ---" # Definición de placeholder para fecha

# Función de carga de configuración de flotas... 
@st.cache_data(ttl=None)
def load_all_fleets_config() -> Dict[str, Dict[str, Any]]:
    """
    Carga la configuración maestra desde flotas_codigos.json y 
    los detalles de cada flota desde su archivo JSON asociado.
    """
    flotas_config = {}
    mapping_filepath = os.path.join(CONFIG_DIR, MAPPING_FILE)

    if not os.path.exists(mapping_filepath):
        print(f"Error: Archivo maestro '{MAPPING_FILE}' no encontrado en '{CONFIG_DIR}'.")
        return {}

    # 1. CARGAR EL ARCHIVO MAESTRO DE MAPEO
    try:
        with open(mapping_filepath, 'r', encoding='utf-8') as f:
            flotas_map = json.load(f)
    except Exception as e:
        print(f"Error al cargar '{MAPPING_FILE}'. Revise el formato JSON. Error: {e}")
        return {}

    # 2. ITERAR Y CARGAR LOS DETALLES INDIVIDUALES
    for nombre_flota, map_data in flotas_map.items():
        if not isinstance(map_data, dict) or 'codigo_db' not in map_data or 'archivo_flota' not in map_data:
            print(f"[ADVERTENCIA] Flota '{nombre_flota}' omitida: faltan claves (codigo_db o archivo_flota) en {MAPPING_FILE} o el formato es incorrecto.")
            continue
            
        archivo_json_nombre = map_data['archivo_flota']
        filepath = os.path.join(CONFIG_DIR, archivo_json_nombre)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if "ids" in data and isinstance(data["ids"], str): 
                    flotas_config[nombre_flota] = {
                        "SUBFLEET_ID": map_data["codigo_db"],       
                        "VEHICLE_IDS_FULL": data["ids"]             
                    }
                else:
                    print(f"[ADVERTENCIA] Archivo '{archivo_json_nombre}' omitido: falta la clave 'ids' o no es una cadena de texto (string).")

        except FileNotFoundError:
            print(f"[ADVERTENCIA] Archivo de detalles '{archivo_json_nombre}' referenciado no encontrado en {CONFIG_DIR}.")
        except Exception as e:
            print(f"Error al cargar '{archivo_json_nombre}': {e}")

    return flotas_config

FLOTAS_CONFIG = load_all_fleets_config()
# --- FIN: CONFIGURACIÓN DINÁMICA DE FLOTAS ---


# --- Funciones de Búsqueda Dinámica en DB ---

def get_driver_full_name_by_ficha(ficha: str) -> str:
    if not ficha or ficha.startswith('ERROR') or 'Desconocido' in ficha:
        return ficha 
        
    sql_query = """
        SELECT 
            nombre, apellido
        FROM 
            conductores
        WHERE 
            ficha_empleado = ?  
        LIMIT 1;
    """
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute(sql_query, (ficha.strip(),))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            nombre_completo = result[0].strip() if result[0] else ""
            apellido_completo = result[1].strip() if result[1] else ""
            
            primer_nombre = nombre_completo.split(' ')[0] if nombre_completo else ""
            primer_apellido = apellido_completo.split(' ')[0] if apellido_completo else ""
            
            short_name = f"{primer_nombre} {primer_apellido}".strip()
            
            return short_name if short_name else f"FICHA: {ficha.strip()}"
        
        return f"FICHA: {ficha.strip()}" 
        
    except sqlite3.Error as e:
        print(f"Error al buscar nombre en tabla conductores: {e}")
        return f"ERROR DB (Conductores): {ficha.strip()}"
    except Exception:
        return f"FICHA: {ficha.strip()}" 


def get_driver_ficha_only_by_unit(unidad: str) -> str:
    sql_query = """
        SELECT 
            conductor_ficha  
        FROM 
            asignacion
        WHERE 
            unidad = ? 
        ORDER BY 
            fecha DESC
        LIMIT 1;
    """
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute(sql_query, (unidad,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0].strip()
        return "Conductor Desconocido"

    except Exception:
        return "Conductor Desconocido"

def get_driver_ficha_for_unit(unidad: str, fecha_str: str, flota_name: str) -> str:
    sql_query = """
        SELECT 
            conductor_ficha  
        FROM 
            asignacion
        WHERE 
            unidad = ? 
            AND flota = ? 
            AND fecha = ? 
        ORDER BY 
            fecha DESC
        LIMIT 1;
    """
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute(sql_query, (unidad, flota_name, fecha_str))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0].strip()
        
        return get_driver_ficha_only_by_unit(unidad)

    except sqlite3.Error as e:
        print(f"Error crítico en la consulta de asignación dinámica: {e}")
        return f"ERROR DB (Asignación): {e}"
    except Exception as e:
        print(f"Error inesperado al buscar ficha: {e}")
        return "Error Desconocido"

def get_driver_info_for_unit(unidad: str, fecha_str: str, flota_name: str) -> str:
    ficha_result = get_driver_ficha_for_unit(unidad, fecha_str, flota_name)
    
    if 'ERROR' in ficha_result or 'Desconocido' in ficha_result:
        return ficha_result
    
    full_name_result = get_driver_full_name_by_ficha(ficha_result)
    
    return full_name_result

# --- FIN: Funciones de Búsqueda Dinámica en DB ---


# ====================================================
# 1. CONSTANTES DE LA API
# ====================================================

REPORT_ENDPOINT = "https://flexapi.foresightgps.com/ForesightFlexAPI.ashx" 

# --- CONFIGURACIÓN DE LA API Y SEGURIDAD (st.secrets) ---
try:
    # Como el archivo original no tenía secrets.toml, se mantiene la variable estática
    BASIC_AUTH_HEADER = "dGVycGVsOmZzZ3BzVGVycGVs"
except KeyError:
    st.error("ERROR CRÍTICO: No se pudo encontrar la clave 'basic_auth_header' en st.secrets.")
    st.info("Asegúrese de configurar el archivo '.streamlit/secrets.toml' o la configuración de 'Secrets' en la nube.")
    st.stop()

# Encabezados (HEADERS) utilizando la clave SEGURA
HEADERS = {
    "Content-Type": "application/json",
    "Authorization":f"Basic {BASIC_AUTH_HEADER}"   
}

# Parámetros fijos
USER_ID = "82825"
COMPANY_ID = "5809"

# Parámetro clave para el DOBLE FILTRO
VELOCITY_MAX_API = "1" 
CHUNK_SIZE = 5 
THRESHOLD_SECONDS = 60  

# Columna crítica de la API
API_SPEED_COLUMN = 'Speed_dUnit'
API_TIME_COLUMN = 'Report Time'

# ====================================================
# ENCABEZADOS DE COLUMNA
# ====================================================

# Nombre interno para la velocidad mínima (solo se usa en la narrativa)
NARRATIVE_MIN_SPEED_COL = 'V. min (km/h)' 

# Estructura de la tabla consolidada (Nombres de columna internos)
CONSOLIDATED_COLUMN_NAMES = [
    'Und.',                       
    'Exceso',                     
    'Inicio',                     
    'Fin',                        
    'Tiempo (min)',               
    'N° Reg.',                    
    'V. máx (km/h)',              
    'V. prom (km/h)',             
    'UBICACIÓN INICIO',
]

# Columas a mostrar en la tabla de Streamlit 
DISPLAY_COLUMN_NAMES = [
    'Und.', 
    'Exceso', 
    'Inicio', 
    'Fin', 
    'Tiempo (min)', 
    'N° Reg.', 
    'V. máx (km/h)', 
    'V. prom (km/h)',
    'UBICACIÓN INICIO',
]

# Mapeo de nombres originales usados en la lógica interna a los nuevos nombres
RENAME_MAP = {
    'UNIDAD': 'Und.',
    'TIPO DE EXCESO': 'Exceso',
    'HORA INICIO': 'Inicio',
    'HORA FIN': 'Fin',
    'DURACIÓN (min)': 'Tiempo (min)',
    '# REGISTROS': 'N° Reg.',
    'VELOCIDAD MAX (km/h)': 'V. máx (km/h)',
    'VELOCIDAD PROMEDIO (km/h)': 'V. prom (km/h)',
    'VELOCIDAD MIN (km/h)': NARRATIVE_MIN_SPEED_COL,
    # UBICACIÓN INICIO se mantiene igual
}


# ====================================================
# 2. FUNCIONES DE LÓGICA Y API 
# ====================================================

def chunk_ids(full_id_string, size):
    """Divide la cadena de IDs en trozos de tamaño 'size'."""
    ids_list = full_id_string.split(',')
    chunks = [ids_list[i:i + size] for i in range(0, len(ids_list), size)]
    return [','.join(chunk) for chunk in chunks]

def ejecutar_reporte(current_vehicle_ids, subfleet_id, fecha_inicio_iso, fecha_fin_iso): 
    """Ejecuta una sola solicitud de reporte para un grupo de IDs con fechas dinámicas."""
    
    VALUE_STRING = (
        f"{USER_ID}|{current_vehicle_ids}|{fecha_inicio_iso}|{fecha_fin_iso}|{VELOCITY_MAX_API}|"       
        f"{COMPANY_ID}|{subfleet_id}|0" 
    )

    PAYLOAD = {
        "method": "REPORT_EXECUTE",
        "conncode": "SATEQSA",
        "reportid": 115, 
        "userid": USER_ID,
        "prefix": True,
        "parameter": "@USERID|@LIST_VEHICLE_IDS|@STARTDATEANDTIME|@ENDDATEANDTIME|@VELOCITY_MAX|@IsCompany|@IsSubfleet|@IsGroup",
        "value": VALUE_STRING
    }

    try:
        response = requests.post(REPORT_ENDPOINT, headers=HEADERS, json=PAYLOAD)
        response.raise_for_status()
        
        if response.headers.get("Content-Type", "").startswith("application/json"):
            reporte_data = response.json()
            return reporte_data.get("ForesightFlexAPI", {}).get("DATA1", [])
        
        return []

    except requests.exceptions.RequestException as e:
        return []

def adjust_time_for_display(time_str):
    """
    Ajusta la hora restando 1 hora para corregir inconsistencias de zona horaria.
    """
    try:
        time_obj = datetime.strptime(time_str, '%H:%M:%S')
        adjusted_time = time_obj - timedelta(hours=1)
        return adjusted_time.strftime('%H:%M:%S')
    except:
        return time_str

@st.cache_data(show_spinner="Cargando, consolidando y filtrando reportes...")
def get_report_data(min_speed, vehicle_ids, subfleet_id, fecha_inicio_iso, fecha_fin_iso): 
    """
    Ejecuta todos los reportes, consolida los datos, aplica el filtro de velocidad
    y realiza el agrupamiento de excesos 'Sostenidos'.
    
    RETORNA:
    - df_final: DataFrame consolidado de eventos (Tabla)
    - df: DataFrame detallado (Base para el gráfico)
    """
    
    id_chunks = chunk_ids(vehicle_ids, CHUNK_SIZE) 
    resultados_finales = []
    
    for chunk in id_chunks:
        resultados_chunk = ejecutar_reporte(chunk, subfleet_id, fecha_inicio_iso, fecha_fin_iso) 
        resultados_finales.extend(resultados_chunk)

    # ----------------------------------------------------------------------
    # Preparación de DataFrame Detallado Vacío para retorno dual
    empty_consolidated_df = pd.DataFrame(columns=CONSOLIDATED_COLUMN_NAMES + [NARRATIVE_MIN_SPEED_COL])
    empty_detailed_df = pd.DataFrame(columns=['UNIDAD', 'HORA_VZLA', 'VELOCIDAD (km/h)']) 
    # ----------------------------------------------------------------------

    if not resultados_finales:
        return empty_consolidated_df, empty_detailed_df 

    df = pd.DataFrame(resultados_finales)
    
    if API_SPEED_COLUMN not in df.columns:
        st.warning(f"La respuesta de la API no contiene la columna '{API_SPEED_COLUMN}' (Velocidad). No se encontraron datos de movimiento o hubo un error de formato. Intente otra fecha o verifique la conexión.")
        return empty_consolidated_df, empty_detailed_df
    
    # 1. Limpieza, preparación y filtrado
    df = df.rename(columns={
        API_TIME_COLUMN: 'Report_Time_Str',
        'Unit': 'UNIDAD',
        API_SPEED_COLUMN: 'VELOCIDAD (km/h)', 
        'Latitude': 'LATITUD',
        'Longitude': 'LONGITUD',
        'Location': 'UBICACIÓN'
    })
    
    df['VELOCIDAD (km/h)'] = pd.to_numeric(df['VELOCIDAD (km/h)'], errors='coerce')
    df['Full_Time'] = pd.to_datetime(
    df['Report_Time_Str'], 
    format='ISO8601', 
    errors='coerce'
    ).dt.tz_convert(VENEZUELA_TZ)
    df['UBICACIÓN'] = df['UBICACIÓN'].str.replace('\n', ' ').str.strip() 

    # 2. FILTRADO INICIAL (SOLO EXCESOS)
    df = df[df['VELOCIDAD (km/h)'] > min_speed].copy() # <--- USA min_speed (N+1)
    
    # Columna de hora ajustada en Venezuela (string) para tooltips y narrativas
    df['HORA_VZLA'] = df['Full_Time'].dt.strftime('%H:%M:%S').apply(adjust_time_for_display)

    if df.empty:
        return empty_consolidated_df, empty_detailed_df 

    df = df.sort_values(by=['UNIDAD', 'Full_Time']).reset_index(drop=True)
    
    # 3. LÓGICA DE ESTADO (Pico vs. Sostenido)
    df['Time_Diff_Prev'] = df.groupby('UNIDAD')['Full_Time'].diff().dt.total_seconds()
    df['Time_Diff_Next'] = df.groupby('UNIDAD')['Full_Time'].diff().dt.total_seconds().shift(-1)
    
    # LÓGICA CLAVE DE CLASIFICACIÓN (60 segundos)
    is_sustained = (df['Time_Diff_Prev'].fillna(THRESHOLD_SECONDS + 1) <= THRESHOLD_SECONDS) | \
                   (df['Time_Diff_Next'].fillna(THRESHOLD_SECONDS + 1) <= THRESHOLD_SECONDS)

    df['TIPO DE EXCESO'] = 'Pico'
    df.loc[is_sustained, 'TIPO DE EXCESO'] = 'Sostenido'
    
    # 4. CREAR ID DE GRUPO PARA EXCESOS SOSTENIDOS CONSECUTIVOS
    df['new_group'] = (df['TIPO DE EXCESO'] == 'Sostenido') & (df['TIPO DE EXCESO'].shift(1) != 'Sostenido')
    df['Excess_Group'] = df.groupby('UNIDAD')['new_group'].cumsum().mask(df['TIPO DE EXCESO'] == 'Pico', 0)
    
    # 5. CONSOLIDACIÓN DE DATOS
    
    # Agregación para Excesos Sostenidos (Group ID > 0)
    df_sostenido = df[df['Excess_Group'] > 0].groupby(['UNIDAD', 'Excess_Group']).agg(
        TIPO_DE_EXCESO=('TIPO DE EXCESO', 'first'), 
        HORA_INICIO=('Full_Time', 'min'), 
        HORA_FIN=('Full_Time', 'max'),     
        REGISTROS=('UNIDAD', 'count'), 
        VELOCIDAD_MAX=('VELOCIDAD (km/h)', 'max'),
        VELOCIDAD_MIN=('VELOCIDAD (km/h)', 'min'), 
        VELOCIDAD_PROMEDIO=('VELOCIDAD (km/h)', 'mean'),                      
        UBICACION_INICIO=('UBICACIÓN', 'first')
    ).reset_index()
    
    # Aplicar nombres de columna originales a df_sostenido antes de calcular la duración
    df_sostenido.rename(columns={
        'TIPO_DE_EXCESO': 'TIPO DE EXCESO',
        'REGISTROS': '# REGISTROS', 
        'VELOCIDAD_MAX': 'VELOCIDAD MAX (km/h)', 
        'VELOCIDAD_MIN': 'VELOCIDAD MIN (km/h)',
        'VELOCIDAD_PROMEDIO': 'VELOCIDAD PROMEDIO (km/h)', 
        'UBICACION_INICIO': 'UBICACIÓN INICIO'
    }, inplace=True)

    # Calcular Duración
    df_sostenido['DURACIÓN (min)'] = (df_sostenido['HORA_FIN'] - df_sostenido['HORA_INICIO']).dt.total_seconds() / 60
    
    # Convertir a string y crear las columnas finales HORA INICIO/HORA FIN (Hora VZLA)
    df_sostenido['HORA FIN'] = df_sostenido['HORA_FIN'].dt.strftime('%H:%M:%S').apply(adjust_time_for_display)
    df_sostenido['HORA INICIO'] = df_sostenido['HORA_INICIO'].dt.strftime('%H:%M:%S').apply(adjust_time_for_display)

    # Limpiar columnas temporales de datetime
    df_sostenido = df_sostenido.drop(columns=['HORA_INICIO', 'HORA_FIN'])
    
    # Formateo de decimales
    df_sostenido['DURACIÓN (min)'] = df_sostenido['DURACIÓN (min)'].round(1)
    df_sostenido['VELOCIDAD PROMEDIO (km/h)'] = df_sostenido['VELOCIDAD PROMEDIO (km/h)'].round(1)
    
    # Aplicar el mapeo de nombres de columnas FINAL antes de concatenar
    df_sostenido.rename(columns=RENAME_MAP, inplace=True)
    
    # Añadir y formatear la columna de velocidad mínima para la narrativa (no se muestra en tabla)
    df_sostenido[NARRATIVE_MIN_SPEED_COL] = df_sostenido.pop(NARRATIVE_MIN_SPEED_COL).round(0).astype(int).astype(str)

    # Seleccionar las columnas finales para el DataFrame Sostenido
    df_sostenido = df_sostenido[CONSOLIDATED_COLUMN_NAMES + [NARRATIVE_MIN_SPEED_COL]]

    # Formateo para Excesos Pico
    df_pico = df[df['TIPO DE EXCESO'] == 'Pico'].copy()
    
    # Crear las columnas con nombres originales
    df_pico['HORA INICIO'] = df_pico['Full_Time'].dt.strftime('%H:%M:%S').apply(adjust_time_for_display)
    df_pico['HORA FIN'] = df_pico['HORA INICIO']
    df_pico['DURACIÓN (min)'] = 0.0
    df_pico['# REGISTROS'] = 1
    df_pico['VELOCIDAD MAX (km/h)'] = df_pico['VELOCIDAD (km/h)']
    df_pico['VELOCIDAD PROMEDIO (km/h)'] = df_pico['VELOCIDAD (km/h)']
    df_pico['UBICACIÓN INICIO'] = df_pico['UBICACIÓN']
    df_pico['VELOCIDAD PROMEDIO (km/h)'] = df_pico['VELOCIDAD PROMEDIO (km/h)'].round(1)
    
    # Para Pico, la velocidad mínima es la velocidad máxima
    df_pico['VELOCIDAD MIN (km/h)'] = df_pico['VELOCIDAD (km/h)'] 

    # Aplicar el mapeo de nombres de columnas FINAL antes de concatenar
    df_pico.rename(columns=RENAME_MAP, inplace=True)
    
    # Añadir y formatear la columna de velocidad mínima para la narrativa (no se muestra en tabla)
    df_pico[NARRATIVE_MIN_SPEED_COL] = df_pico.pop(NARRATIVE_MIN_SPEED_COL).round(0).astype(int).astype(str)

    # 6. UNIFICAR Y RETORNAR
    df_pico = df_pico[CONSOLIDATED_COLUMN_NAMES + [NARRATIVE_MIN_SPEED_COL]]

    df_final = pd.concat([df_sostenido, df_pico], ignore_index=True)
    df_final = df_final.sort_values(by=['Und.', 'Inicio']).reset_index(drop=True)
    
    # RETORNO DUAL: Consolidado (Tabla) y Detallado (Gráfico)
    return df_final, df # df es el DataFrame detallado filtrado (>= min_speed)

# ====================================================
# 4. FUNCIÓN PARA GENERAR TXT 
# ====================================================

def generate_txt_narrative(data_df_full, fecha_str: str, flota_name: str): 
    
    df_sostenido = data_df_full[data_df_full['Exceso'] == 'Sostenido'].copy()
    
    if df_sostenido.empty:
        return ""
    
    narrative_lines = []
    
    unidad_conductor_cache = {} 
    
    for index, row in df_sostenido.iterrows():
        unidad = row['Und.']
        h_inicio = row['Inicio'][:-3] 
        h_fin = row['Fin'][:-3]
        
        min_speed = row[NARRATIVE_MIN_SPEED_COL] 
        max_speed = int(row['V. máx (km/h)'])
        promedio = row['V. prom (km/h)']
        
        if unidad not in unidad_conductor_cache:
            conductor_info = get_driver_info_for_unit(unidad, fecha_str, flota_name) 
            unidad_conductor_cache[unidad] = conductor_info
        else:
            conductor_info = unidad_conductor_cache[unidad]
            
        if 'FICHA:' in conductor_info or 'Desconocido' in conductor_info or 'ERROR' in conductor_info:
            conductor_text = ""
        else:
            conductor_text = f" (Conductor: {conductor_info})" 
            
        
        line = (
            f"{fecha_str} La Unidad {unidad}{conductor_text}, registró un exceso de velocidad sostenido "
            f"entre {h_inicio} y {h_fin} Hrs, con velocidad de {min_speed} a {max_speed} km/h "
            f"y con una velocidad promedio de {promedio:.1f} km/h"
        )
        
        narrative_lines.append(line)
    
    return '\n'.join(narrative_lines)

# ====================================================
# 3. ESTRUCTURA DE LA APLICACIÓN STREAMLIT (DASHBOARD)
# ====================================================

st.set_page_config(
    page_title="Reporte de Excesos de Velocidad",
    layout="wide"
)

local_css()

st.title("🚦 Reporte Excesos de Velocidad")

# --- Lógica de Fechas de Referencia ---
current_date = datetime.now(VENEZUELA_TZ).date() 
yesterday = current_date - timedelta(days=1)


# Inicialización de variables de estado
# Se inicializa solo si las claves NO existen (primera carga)
if 'selected_speed' not in st.session_state:
    st.session_state.selected_speed = 70 
    st.session_state.selected_date_option = PLACEHOLDER 
    st.session_state.selected_flota_key = FLOTA_PLACEHOLDER 
    st.session_state.report_submitted = False
    # Inicialización de variables que serán calculadas al hacer submit
    st.session_state.search_speed_threshold = 71 
    st.session_state.fecha_inicio_api = ""
    st.session_state.fecha_fin_api = ""
   
# ------------------------------------------------------------------
# SECCIÓN CLAVE: FORMULARIO EN EL SIDEBAR CON BOTÓN SIEMPRE HABILITADO
# ------------------------------------------------------------------
with st.sidebar:
    
    # --- Botón Home con Limpieza de Caché y REINICIO COMPLETO ---
    if st.button("🏡 Home", use_container_width=True):
        st.cache_data.clear()
        
        # REINICIO COMPLETO DEL ESTADO DE SESIÓN (SOLO AQUÍ)
        st.session_state.selected_speed = 70 
        st.session_state.selected_date_option = PLACEHOLDER 
        st.session_state.selected_flota_key = FLOTA_PLACEHOLDER 
        st.session_state.report_submitted = False
        st.session_state.search_speed_threshold = 71
        st.session_state.fecha_inicio_api = ""
        st.session_state.fecha_fin_api = ""
        
        # Si 'home.py' existe en el mismo directorio, esto cambiará de página.
        try:
             st.switch_page("home.py") 
        except FileNotFoundError:
             st.info("Página 'home.py' no encontrada. Limpieza de caché realizada.")
        
        pass 
        
    st.markdown(
        '<p style="text-align: center; margin-bottom: 5px; margin-top: 5px;">'
        '<img src="https://fospuca.com/wp-content/uploads/2018/08/fospuca.png" alt="Logo Fospuca" style="width: 100%; max-width: 120px; height: auto; display: block; margin-left: auto; margin-right: auto;">'
        '</p>',
         unsafe_allow_html=True
    )
    
    # --- Formulario de Input ---
    with st.form("speed_report_form"):
        st.subheader("Configuración del Reporte")

        # Selector de Flota DINÁMICO
        flotas_list = sorted(FLOTAS_CONFIG.keys())
        flota_options = [FLOTA_PLACEHOLDER] + flotas_list

        # Usar el valor persistente de la sesión
        flota_input = st.selectbox(
            "Seleccione la Flota:",
            flota_options,
            key="flota_selector",
            index=flota_options.index(st.session_state.selected_flota_key) if st.session_state.selected_flota_key in flota_options else 0
        )
        
        # Selector de Fecha
        date_option_options = (PLACEHOLDER, "Ayer", "Hoy", "Día Específico")
        
        # Usar el valor persistente de la sesión
        date_option_input = st.selectbox(
            "Seleccione Fecha:",
            date_option_options,
            key="date_option_selector",
            index=date_option_options.index(st.session_state.selected_date_option) if st.session_state.selected_date_option in date_option_options else 0
        )

        # El speed_limit se mantiene al último valor usado
        speed_limit = st.number_input(
            "Límite de Velocidad (km/h):",
            min_value=1,
            max_value=200,
            value=st.session_state.selected_speed, 
            step=5
        )

        # LÓGICA DE SELECCIÓN DE FECHA DINÁMICA 
        if date_option_input == "Día Específico":
            selected_day = st.date_input("Seleccione el Día:", value=yesterday)
            start_date_calc = selected_day
            end_date_calc = selected_day
        elif date_option_input == "Ayer":
            start_date_calc = yesterday
            end_date_calc = yesterday
        elif date_option_input == "Hoy":
            start_date_calc = current_date
            end_date_calc = current_date
        else: # PLACEHOLDER o no seleccionado
            # Se usa el día de ayer por defecto para el cálculo interno si no se ha seleccionado nada.
            start_date_calc = yesterday 
            end_date_calc = yesterday
        
        st.markdown("---")
        
        submitted = st.form_submit_button(
            "Generar Reporte", 
            use_container_width=True
        )
        
    st.warning("En Fase de Desarrollo: puede presentar inconsistencias")    
# ------------------------------------------------------------------


# --- Lógica de Manejo del Submit (Fuera del sidebar) ---
if submitted:
    
    # 1. Validación de Flota y Fecha
    if flota_input == FLOTA_PLACEHOLDER or date_option_input == PLACEHOLDER:
        st.error("Por favor, seleccione una **Flota válida** y la **Fecha** para generar el reporte.")
        # No se marca como enviado para evitar que el código de renderizado se ejecute
        st.session_state.report_submitted = False
    else:
        # 2. Si la validación pasa, se activa la visualización
        st.session_state.report_submitted = True 
        
        # Guardar las opciones del formulario al hacer submit 
        st.session_state.selected_date_option = date_option_input 
        st.session_state.selected_flota_key = flota_input 
        st.session_state.selected_speed = int(speed_limit) 
        st.session_state.search_speed_threshold = int(speed_limit) + 1 # Velocidad para el filtro (N+1) 

        # LÓGICA DE ZONA HORARIA Y CONVERSIÓN A UTC PARA LA API
        start_dt_local = VENEZUELA_TZ.localize(datetime.combine(start_date_calc, datetime.min.time()))
        start_dt_utc = start_dt_local.astimezone(pytz.utc)
        st.session_state.fecha_inicio_api = start_dt_utc.strftime('%Y-%m-%d %H:%M:%S') + '.217'

        end_dt_local = VENEZUELA_TZ.localize(datetime.combine(end_date_calc, datetime.max.time().replace(microsecond=0)))
        end_dt_utc = end_dt_local.astimezone(pytz.utc)
        st.session_state.fecha_fin_api = end_dt_utc.strftime('%Y-%m-%d %H:%M:%S') + '.999'
    

# --- Sección de Output (Condicional) ---

if st.session_state.report_submitted and st.session_state.selected_flota_key != FLOTA_PLACEHOLDER and st.session_state.selected_date_option != PLACEHOLDER:
    
    # --- Recalcular la fecha de visualización de forma robusta ---
    try:
        api_start_dt = datetime.strptime(st.session_state.fecha_inicio_api[:-4], '%Y-%m-%d %H:%M:%S')
        start_date_used = pytz.utc.localize(api_start_dt).astimezone(VENEZUELA_TZ).date()
    except:
        start_date_used = "Error de Fecha"
        
    # ----------------------------------------------------
    # OBTENER PARÁMETROS DINÁMICOS DE LA FLOTA SELECCIONADA
    # ----------------------------------------------------
    flota_key = st.session_state.selected_flota_key
    
    if flota_key in FLOTAS_CONFIG:
        flota_params = FLOTAS_CONFIG[flota_key]
        dynamic_vehicle_ids = flota_params["VEHICLE_IDS_FULL"] 
        dynamic_subfleet_id = flota_params["SUBFLEET_ID"]     
        
        st.caption(f"Flota Seleccionada: **{flota_key}** | Código DB (SUBFLEET_ID): **{dynamic_subfleet_id}**") 
        
    else:
        st.error(f"Error: La configuración para la flota '{flota_key}' no es válida.")
        st.session_state.report_submitted = False
        st.stop()
    
    st.markdown("---")
    # Título Muestra el límite del usuario (N)
    st.subheader(f"Resultados Consolidados de Excesos > {st.session_state.selected_speed} km/h")  
    
    caption_text = f"Día (Hora de Venezuela): **{start_date_used.isoformat()}**"
        
    st.caption(caption_text)
    
    # LLAMADA A LA FUNCIÓN CON PARÁMETROS DINÁMICOS (RECIBE 2 DF)
    data_df, detailed_df = get_report_data(
        st.session_state.search_speed_threshold, 
        dynamic_vehicle_ids, 
        dynamic_subfleet_id, 
        st.session_state.fecha_inicio_api,
        st.session_state.fecha_fin_api
    )

    if not data_df.empty:
        
        # ----------------------------------------------------
        # Sección de Análisis (KPIs) 
        # ----------------------------------------------------
        
        if 'N° Reg.' in data_df.columns:
            total_registros_bruto = data_df['N° Reg.'].sum() 
            unidades_con_excesos = data_df['Und.'].nunique()
            velocidad_maxima = data_df['V. máx (km/h)'].max()
            
            unidad_max_velocidad = data_df.loc[data_df['V. máx (km/h)'] == velocidad_maxima, 'Und.'].iloc[0] if not data_df.empty else "N/A"
            total_eventos_consolidados = len(data_df)

            col1, col2, col3, col4, col5 = st.columns(5)
            
            col1.metric("Unidades Afectadas", f"{unidades_con_excesos}") 
            col2.metric("Eventos Consolidados", f"{total_eventos_consolidados}")
            col3.metric("Total Registros Brutos", f"{total_registros_bruto}")
            col4.metric("Velocidad Máxima", f"{velocidad_maxima} km/h")
            col5.metric("Unidad Máxima Velocidad", f"{unidad_max_velocidad}")
                 
        # ----------------------------------------------------
        # Sección de Tabla y Botón de Descarga TXT
        # ----------------------------------------------------
        
        st.subheader("Consolidados de Excesos de Velocidad")
        
        txt_content = generate_txt_narrative(
            data_df, 
            start_date_used.isoformat(), 
            flota_key
        ) 
        
        # Botón de Descarga TXT
        if txt_content:
            st.download_button(
                label="Descargar Resumen TXT",
                data=txt_content.encode('utf-8'),
                file_name=f'excesos_sostenidos_{flota_key}_{start_date_used.isoformat()}.txt',
                mime='text/plain',
                key="download_txt_button" 
            )
        else:
            st.info("No hay excesos 'Sostenidos' para generar el resumen TXT.")

        # Renderizar la tabla (sin botones ni lógica de detalle)
        st.dataframe(
            data_df[DISPLAY_COLUMN_NAMES], # Se usan solo las columnas a mostrar 
            use_container_width=True
        )
        
        # ----------------------------------------------------
        # SECCIÓN NUEVA: Selector de Unidad y Gráfico
        # ----------------------------------------------------
        st.markdown("---")
        st.subheader("📊 Análisis Detallado por Unidad")
        
        # Obtener la lista de unidades con excesos del DF CONSOLIDADO
        unidades_con_excesos = sorted(data_df['Und.'].unique().tolist())
        
        if unidades_con_excesos:
            # No se usa la clave de sesión, Streamlit maneja automáticamente el valor del widget
            unidad_seleccionada = st.selectbox(
                "Seleccione la Unidad para el Gráfico de Velocidad:",
                options=unidades_con_excesos,
                key="unidad_grafico_selector" 
            )
            
            if unidad_seleccionada:
                # Filtrar el DataFrame detallado (detailed_df) por la unidad seleccionada
                df_unidad_detallado = detailed_df[detailed_df['UNIDAD'] == unidad_seleccionada].copy()
                
                # Añadir información del conductor (opcional, para el título del gráfico)
                conductor_info = get_driver_info_for_unit(unidad_seleccionada, start_date_used.isoformat(), flota_key)
                
                driver_label = f" (Conductor: {conductor_info})" if not ('FICHA:' in conductor_info or 'Desconocido' in conductor_info or 'ERROR' in conductor_info) else ""
                
                st.caption(f"Comportamiento de la Unidad **{unidad_seleccionada}**{driver_label} a lo largo del día. Límite: **{st.session_state.selected_speed} km/h**")

                # Generar el gráfico de línea (CÓDIGO PLOTLY)
                if not df_unidad_detallado.empty:
                    
                    # Prepara el DataFrame para Plotly (df_chart)
                    # Mantener 'Full_Time' como el eje X principal (datetime object) para Plotly
                    df_chart = df_unidad_detallado[['Full_Time', 'HORA_VZLA', 'VELOCIDAD (km/h)', 'UBICACIÓN']].copy()
                    
                    limite_velocidad = st.session_state.selected_speed
                    
                    # --------------------------------------------------------------------------
                    # 1. Definición del Gráfico Base (Línea con Plotly Express)
                    # --------------------------------------------------------------------------
                    
                    # Usa Full_Time para el eje X. Plotly maneja automáticamente el formato temporal.
                    fig = px.line(
                        df_chart, 
                        x='Full_Time', 
                        y='VELOCIDAD (km/h)',
                        title=f"Velocidad ({unidad_seleccionada}) - Límite: {limite_velocidad} km/h",
                        color_discrete_sequence=['#A52A2A'], # Marrón/Rojo Oscuro para la línea base
                        labels={
                            'Full_Time': 'Hora (VZLA)',
                            'VELOCIDAD (km/h)': 'Velocidad (km/h)'
                        }
                    )
                    
                    # Asegurarse de que la línea base sea gruesa y tenga marcadores con forma de rombo
                    fig.update_traces(
                        line=dict(width=3.5), 
                        mode='lines+markers', 
                        marker=dict(
                            symbol='diamond', 
                            size=8, 
                            line=dict(width=0.5, color='black')
                        )
                    )
                    
                    # --------------------------------------------------------------------------
                    # 2. CAPA DE LÍMITE DE VELOCIDAD (Regla Punteada Roja)
                    # --------------------------------------------------------------------------
                    
                    fig.add_hline(
                        y=limite_velocidad, 
                        line_dash="dot", 
                        line_color="red",
                        annotation_text=f"Límite: {limite_velocidad} km/h",
                        annotation_position="bottom right"
                    )
                    
                    # --------------------------------------------------------------------------
                    # 3. CAPA DE PUNTOS DE INTERÉS (Excesos > Límite)
                    # --------------------------------------------------------------------------
                    df_picos_interes = df_chart[df_chart['VELOCIDAD (km/h)'] > limite_velocidad].copy()
                    
                    if not df_picos_interes.empty:
                        # Añadir los puntos de exceso como un scatter plot separado
                        picos_scatter = go.Scatter(
                            x=df_picos_interes['Full_Time'],
                            y=df_picos_interes['VELOCIDAD (km/h)'],
                            mode='markers',
                            name='Excesos',
                            marker=dict(
                                symbol='diamond', # Forma de rombo
                                size=10, 
                                color='red', 
                                line=dict(width=1, color='black')
                            ),
                            hoverinfo='text',
                            text=[
                                f"Hora: {r['HORA_VZLA']}<br>Velocidad: {r['VELOCIDAD (km/h)']:.1f} km/h<br>Ubicación: {r['UBICACIÓN']}"
                                for _, r in df_picos_interes.iterrows()
                            ]
                        )
                        fig.add_trace(picos_scatter)

                    # --------------------------------------------------------------------------
                    # 4. CAPA DE PICO MÁXIMO ABSOLUTO (AMARILLO)
                    # --------------------------------------------------------------------------
                    max_speed_record = df_chart.loc[df_chart['VELOCIDAD (km/h)'].idxmax()]
                    
                    max_point_scatter = go.Scatter(
                        x=[max_speed_record['Full_Time']],
                        y=[max_speed_record['VELOCIDAD (km/h)']],
                        mode='markers',
                        name='Pico Máximo',
                        marker=dict(
                            symbol='diamond', # Forma de rombo
                            size=12, 
                            color='yellow', 
                            line=dict(width=2, color='black')
                        ),
                        hoverinfo='text',
                        text=[
                            f"Hora: {max_speed_record['HORA_VZLA']}<br>Pico Máximo: {max_speed_record['VELOCIDAD (km/h)']:.1f} km/h<br>Ubicación: {max_speed_record['UBICACIÓN']}"
                        ]
                    )
                    fig.add_trace(max_point_scatter)
                    
                    # --------------------------------------------------------------------------
                    # 5. Configuración y Renderizado
                    # --------------------------------------------------------------------------
                    
                    # Ocultar la leyenda y cambiar el fondo a blanco
                    fig.update_layout(
                        showlegend=False,
                        # Fondo del área de trazado (el gráfico en sí)
                        plot_bgcolor='white', 
                        # Fondo del papel/lienzo (alrededor del gráfico)
                        paper_bgcolor='white',
                    )

                    # Mejorar el formato del eje X, la densidad de ticks y añadir el Range Slider
                    fig.update_xaxes(
                        # 1. Nuevo Formato: HH:MM
                        tickformat="%H:%M", 
                        # 2. Nueva Densidad de Ticks: 30 minutos (1800000 milisegundos)
                        dtick=1800000, 
                        rangeslider_visible=True, # Slider para facilitar el zoom/pan
                        title_font=dict(size=18),
                        tickfont=dict(size=14),
                        # Configuración de las líneas de cuadrícula del Eje X (Horizontales)
                        gridcolor='#CCCCCC',    # Color Gris
                        gridwidth=1,            
                        griddash='dot'          # Estilo Punteado
                    )
                    
                    # Mejorar el formato del eje Y
                    fig.update_yaxes(
                        title_font=dict(size=18),
                        tickfont=dict(size=14),
                        tick0=0, # Comienza el eje en 0
                        dtick=2, # Marcas de 2 en 2 km/h (ej: 0, 2, 4, 6...)
                        # Configuración de las líneas de cuadrícula del Eje Y (Verticales)
                        gridcolor='#CCCCCC',    # Color Gris
                        gridwidth=1,            
                        griddash='dot'          # Estilo Punteado
                    )
                    
                    # Renderizar el gráfico de Plotly en Streamlit
                    st.plotly_chart(fig, use_container_width=True)

                else:
                    st.error(f"Error interno: No se encontraron registros detallados para la unidad {unidad_seleccionada}.")
        
        # ----------------------------------------------------
        # Sección de Resumen Narrativo (Eventos Resaltantes) 
        # ----------------------------------------------------
        
        st.markdown("---")
        df_sostenido_narrative = data_df[data_df['Exceso'] == 'Sostenido'] 
        
        if not df_sostenido_narrative.empty:
            st.markdown("## 📜 Excesos Resaltantes") 
            
            narratives_markdown = []
            
            unidad_conductor_cache = {}
            
            for index, row in df_sostenido_narrative.iterrows():
                unidad = row['Und.']
                h_inicio = row['Inicio'][:-3] 
                h_fin = row['Fin'][:-3]
                
                min_speed = row[NARRATIVE_MIN_SPEED_COL] 
                max_speed = int(row['V. máx (km/h)'])
                promedio = row['V. prom (km/h)']
                
                if unidad not in unidad_conductor_cache:
                    conductor_info = get_driver_info_for_unit(unidad, start_date_used.isoformat(), flota_key)
                    unidad_conductor_cache[unidad] = conductor_info
                else:
                    conductor_info = unidad_conductor_cache[unidad]
                
                if 'FICHA:' in conductor_info or 'Desconocido' in conductor_info or 'ERROR' in conductor_info:
                    conductor_text = ""
                else:
                    conductor_text = f" (**Conductor: {conductor_info}**)" 
                
                narrative = (
                    f"{start_date_used.isoformat()} "
                    f" La Unidad {unidad}{conductor_text}, registró un exceso de velocidad sostenido "
                    f" entre {h_inicio} y {h_fin} Hrs, con velocidad de {min_speed} a {max_speed} km/h "
                    f" y con una velocidad promedio de {promedio:.1f} km/h"
                )
                
                narratives_markdown.append(f"• {narrative}")

            for narrative_line in narratives_markdown:
                st.write(narrative_line)
            
        st.caption(f"Nota: Los excesos **'Sostenido'** agrupan múltiples registros consecutivos con **menos de {THRESHOLD_SECONDS // 60} minutos** de diferencia entre ellos. Los excesos **'Pico'** son eventos individuales.")

    else:
        # El mensaje de advertencia usa la velocidad de entrada del usuario (N), que es la referencia.
        st.warning(f"No se encontraron registros de exceso de velocidad (> {st.session_state.selected_speed} km/h) para el período seleccionado: {caption_text} (Hora de Venezuela).")

# Mensaje inicial cuando no se ha enviado el reporte o faltan selecciones
else:
    st.info("Por favor, configure los parámetros en la **barra lateral** y haga clic en 'Generar Reporte' para cargar los datos.")
