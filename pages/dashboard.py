# IMPORTACIONES
import streamlit as st
import requests
import json
import pandas as pd
import pydeck as pdk
import time
import numpy as np
from typing import List, Dict, Any
import base64
import os
import glob
import sqlite3 
import re
from datetime import datetime, timedelta, timezone, date
from shapely.geometry import Polygon, Point


@st.cache_data(ttl=60) # Cache de 1 minuto
def get_current_unit_assignment(flota: str, unidad: str) -> dict:
    """
    Busca la asignación para la fecha de hoy, la unidad y la flota.
    Retorna un diccionario con los detalles de la asignación.
    """
    conn = get_db_connection()
    if conn is None: return {}
    

    today_str = str(date.today()) # Fecha actual
    
    try:
        cursor = conn.cursor()
        
        # Buscamos la asignación para la fecha de hoy, la unidad y la flota.
        # Ordenamos por ID para obtener la última asignación creada hoy.

        sql = """
            SELECT 
                conductor_ficha, 
                telefono, 
                ruta_nombre,
                hora_salida,
                hora_entrada
            FROM asignacion 
            WHERE flota = ? AND unidad = ? AND fecha = ?
            ORDER BY id DESC
            LIMIT 1
        """
        cursor.execute(sql, (flota, unidad, today_str))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        
        return {}
        # Retorna vacío si no hay asignación
        
    except Exception as e:
        print(f"Error al buscar asignación de unidad {unidad}: {e}")
        return {}
        
    finally:
        if conn: conn.close()

def get_driver_name_for_unit(unidad: str, flota: str) -> str:
    """Busca la asignación del día y retorna el nombre completo del conductor."""
    
    conductores_db = get_all_conductores_db(flota) 
    assignment = get_current_unit_assignment(flota, unidad)
        
    if assignment:
        ficha = assignment.get('conductor_ficha')
        driver_info = conductores_db.get(ficha, {})
        
        nombre_completo= driver_info.get('nombre', '').strip()
        primer_nombre = nombre_completo.split()[0] if nombre_completo else ''
        
        apellido_completo = driver_info.get('apellido', '').strip()
        primer_apellido = apellido_completo.split()[0] if apellido_completo else ''
        
        # Combina nombre y apellido, o usa la ficha si no hay nombre
        
        nombre_corto = f"{primer_nombre} {primer_apellido}".strip()
        return nombre_corto if nombre_corto else f"Ficha: {ficha}"
        
    return "Sin Conductor Asignado"


# LÓGICA DE NAVEGACIÓN Y ESTADO DE SESIÓN

# Inicialización de estado de sesión

if 'current_logistica_view' not in st.session_state:
    st.session_state.current_logistica_view = 'menu'
    
if 'flota_seleccionada' not in st.session_state:
    st.session_state.flota_seleccionada = 'Flota Principal' 

def set_logistica_view(view):
    """Cambia la vista actual del módulo Logística."""
    st.session_state.current_logistica_view = view
    
# CONFIGURACIÓN DE BASE DE DATOS SQLITE

DB_NAME = 'gps.db' 

def get_db_connection():
    """Establece y retorna la conexión a la base de datos SQLite."""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row 
        return conn
    except Exception as e:
        st.error(f"Error de conexión a la BD: {e}") 
        return None

def initialize_db():
    """Crea las tablas 'unidades', 'conductores', 'rutas' y 'asignacion' si no existen, asegurando el aislamiento por FLOTA."""
    conn = get_db_connection()
    if conn is None: return
    try:
        cursor = conn.cursor()
        
# 1. TABLA UNIDADES

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS unidades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flota TEXT NOT NULL,
                unidad TEXT NOT NULL,
                placa TEXT NOT NULL,
                tipo_gps TEXT,
                modelo TEXT,
                numero_telefonico TEXT,
                UNIQUE(flota, unidad),
                UNIQUE(flota, placa)
            )
        """)
        
#2. TABLA CONDUCTORES
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS conductores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flota TEXT NOT NULL,
                nombre TEXT NOT NULL,
                apellido TEXT NOT NULL,
                telefono1 TEXT,
                telefono2 TEXT,
                cedula TEXT NOT NULL,
                ficha_empleado TEXT NOT NULL,
                UNIQUE(flota, cedula),
                UNIQUE(flota, ficha_empleado)
            )
        """)
        
# 3. TABLA RUTAS
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rutas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flota TEXT NOT NULL,
                nombre TEXT NOT NULL,
                descripcion TEXT,
                UNIQUE(flota, nombre)
            )
        """)
        
# 4. TABLA ASIGNACION
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS asignacion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flota TEXT NOT NULL,
                fecha DATE NOT NULL,
                unidad TEXT NOT NULL,
                conductor_ficha TEXT NOT NULL,
                telefono TEXT,
                ruta_nombre TEXT,
                hora_salida TEXT,
                hora_entrada TEXT,
                observaciones TEXT
            )
        """)
        conn.commit()
    except Exception as e:
        print(f"Error al inicializar la BD: {e}")
    finally:
        if conn: conn.close()

# Inicialización de base de datos
initialize_db()


#--FUNCIONES CRUD POR FLOTA--


# UNIDADES CRUD

@st.cache_data(ttl=600, show_spinner=False)
def get_all_units_db(flota: str):
    """Obtiene todas las unidades para la flota especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    units = {}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT unidad, placa, tipo_gps, modelo, numero_telefonico FROM unidades WHERE flota = ?", (flota,))
        for row in cursor.fetchall():
            units[row['unidad']] = dict(row)
        return units
    finally:
        if conn: conn.close()

def create_unit_db(flota, unidad, placa, tipo_gps, modelo, num_telefono):
    """Guarda una nueva unidad bajo la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO unidades (flota, unidad, placa, tipo_gps, modelo, numero_telefonico) VALUES (?, ?, ?, ?, ?, ?)"
        cursor.execute(sql, (flota, unidad, placa, tipo_gps, modelo, num_telefono))
        conn.commit()
        get_all_units_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error("Error de Integridad: La Unidad o Placa ya existe en esta flota.")
        return False
    except Exception as e:
        st.error(f"Error al crear unidad: {e}")
        return False
    finally:
        if conn: conn.close()

def update_unit_db(flota, unidad_original, placa, tipo_gps, modelo, num_telefono):
    """Actualiza una unidad existente en la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "UPDATE unidades SET placa = ?, tipo_gps = ?, modelo = ?, numero_telefonico = ? WHERE flota = ? AND unidad = ?"
        cursor.execute(sql, (placa, tipo_gps, modelo, num_telefono, flota, unidad_original))
        conn.commit()
        get_all_units_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error("Error de Integridad: La Placa ya está asignada a otra unidad en esta flota.")
        return False
    except Exception as e:
        st.error(f"Error al actualizar unidad: {e}")
        return False
    finally:
        if conn: conn.close()

def delete_unit_db(flota, unidad):
    """Elimina una unidad de la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM unidades WHERE flota = ? AND unidad = ?", (flota, unidad))
        conn.commit()
        get_all_units_db.clear()
        return True
    except Exception as e:
        st.error(f"Error al eliminar unidad: {e}")
        return False
    finally:
        if conn: conn.close()


# CONDUCTORES CRUD

@st.cache_data(ttl=600, show_spinner=False)
def get_all_conductores_db(flota: str):
    """Obtiene todos los conductores para la flota especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    conductores = {}
    try:
        cursor = conn.cursor()
        sql = "SELECT nombre,ficha_empleado,  apellido, telefono1, telefono2, cedula FROM conductores WHERE flota = ?"
        cursor.execute(sql, (flota,))
        for row in cursor.fetchall():
            conductores[row['nombre']] = dict(row)
        return conductores
    finally:
        if conn: conn.close()

def create_conductor_db(flota, nombre, apellido, tel1, tel2, cedula, ficha):
    """Guarda un nuevo conductor bajo la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO conductores (flota, nombre, apellido, telefono1, telefono2, cedula, ficha_empleado) VALUES (?, ?, ?, ?, ?, ?, ?)"
        cursor.execute(sql, (flota, nombre, apellido, tel1, tel2, cedula, ficha))
        conn.commit()
        get_all_conductores_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error("Error de Integridad: La Cédula o Ficha de Empleado ya existe en esta flota.")
        return False
    except Exception as e:
        st.error(f"Error al crear conductor: {e}")
        return False
    finally:
        if conn: conn.close()

def update_conductor_db(flota, ficha_original, nombre, apellido, tel1, tel2, cedula):
    """Actualiza un conductor existente en la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "UPDATE conductores SET nombre = ?, apellido = ?, telefono1 = ?, telefono2 = ?, cedula = ? WHERE flota = ? AND ficha_empleado = ?"
        cursor.execute(sql, (nombre, apellido, tel1, tel2, cedula, flota, ficha_original))
        conn.commit()
        get_all_conductores_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error("Error de Integridad: La Cédula ya está asignada a otro empleado en esta flota.")
        return False
    except Exception as e:
        st.error(f"Error al actualizar conductor: {e}")
        return False
    finally:
        if conn: conn.close()

def delete_conductor_db(flota, ficha):
    """Elimina un conductor de la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM conductores WHERE flota = ? AND ficha_empleado = ?", (flota, ficha))
        conn.commit()
        get_all_conductores_db.clear()
        return True
    except Exception as e:
        st.error(f"Error al eliminar conductor: {e}")
        return False
    finally:
        if conn: conn.close()

# RUTAS CRUD

@st.cache_data(ttl=600, show_spinner=False)
def get_all_rutas_db(flota: str):
    """Obtiene todas las rutas para la flota especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    rutas = {}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT nombre, descripcion FROM rutas WHERE flota = ?", (flota,))
        for row in cursor.fetchall():
            rutas[row['nombre']] = dict(row)
        return rutas
    finally:
        if conn: conn.close()

def create_ruta_db(flota, nombre, descripcion=""):
    """Guarda una nueva ruta bajo la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO rutas (flota, nombre, descripcion) VALUES (?, ?, ?)"
        cursor.execute(sql, (flota, nombre, descripcion))
        conn.commit()
        get_all_rutas_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error(f"Error de Integridad: La ruta '{nombre}' ya existe en esta flota.")
        return False
    except Exception as e:
        st.error(f"Error al crear ruta: {e}")
        return False
    finally:
        if conn: conn.close()

def update_ruta_db(flota, nombre_original, nombre_nuevo, descripcion):
    """Actualiza una ruta existente en la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "UPDATE rutas SET nombre = ?, descripcion = ? WHERE flota = ? AND nombre = ?"
        cursor.execute(sql, (nombre_nuevo, descripcion, flota, nombre_original))
        conn.commit()
        get_all_rutas_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error("Error de Integridad: El nuevo nombre de ruta ya está en uso en esta flota.")
        return False
    except Exception as e:
        st.error(f"Error al actualizar ruta: {e}")
        return False
    finally:
        if conn: conn.close()

def delete_ruta_db(flota, nombre):
    """Elimina una ruta de la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM rutas WHERE flota = ? AND nombre = ?", (flota, nombre))
        conn.commit()
        get_all_rutas_db.clear()
        return True
    except Exception as e:
        st.error(f"Error al eliminar ruta: {e}")
        return False
    finally:
        if conn: conn.close()

# ASIGNACION CRUD

@st.cache_data(ttl=600, show_spinner=False)
def get_all_asignaciones_db(flota: str):
    """Obtiene todas las asignaciones para la flota especificada."""
    conn = get_db_connection()
    if conn is None: return []
    asignaciones = []
    try:
        cursor = conn.cursor()
        sql = "SELECT id, fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada FROM asignacion WHERE flota = ? ORDER BY fecha DESC"
        cursor.execute(sql, (flota,))
        for row in cursor.fetchall():
            asignaciones.append(dict(row))
        return asignaciones
    finally:
        if conn: conn.close()

def create_asignacion_db(flota, fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada, observaciones=None):
    """Guarda una nueva asignación bajo la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO asignacion (flota, fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada, observaciones) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(sql, (flota, fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada, observaciones))
        conn.commit()
        get_all_asignaciones_db.clear()
        return True
    except Exception as e:
        st.error(f"Error al crear asignación: {e}")
        return False
    finally:
        if conn: conn.close()

def update_asignacion_db(asignacion_id, flota, fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada, observaciones=None):
    """Actualiza una asignación existente por ID."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = """
            UPDATE asignacion 
            SET fecha = ?, unidad = ?, conductor_ficha = ?, telefono = ?, ruta_nombre = ?, hora_salida = ?, hora_entrada = ?, observaciones = ?
            WHERE id = ? AND flota = ?
        """
        cursor.execute(sql, (fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada, observaciones, asignacion_id, flota))
        conn.commit()
        get_all_asignaciones_db.clear()
        return True
    except Exception as e:
        st.error(f"Error al actualizar asignación: {e}")
        return False
    finally:
        if conn: conn.close()

def delete_asignacion_db(asignacion_id: int, flota: str) -> bool:
    """Elimina una asignación por ID y flota."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "DELETE FROM asignacion WHERE id = ? AND flota = ?"
        cursor.execute(sql, (asignacion_id, flota))
        conn.commit()
        get_all_asignaciones_db.clear()
        return True
    except Exception as e:
        st.error(f"Error al eliminar asignación: {e}")
        return False
    finally:
        if conn: conn.close()

# NUEVA FUNCIÓN: Crear datos de prueba
def create_sample_data(flota: str):
    """Crea datos de prueba para demostración."""
    conn = get_db_connection()
    if conn is None: return False
    
    try:
        cursor = conn.cursor()
        
        # Verificar si ya existen datos de prueba
        cursor.execute("SELECT COUNT(*) FROM asignacion WHERE flota = ?", (flota,))
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            st.info(f"Ya existen {existing_count} asignaciones para la flota {flota}")
            return True
        
        # Crear algunas unidades de prueba si no existen
        test_units = [
            ('7001', 'ABC123', 'Concox', 'Mercedes', '0414-1234567'),
            ('7002', 'DEF456', 'Teltonika', 'Volvo', '0414-7654321'),
            ('7003', 'GHI789', 'Ruptela', 'Kenworth', '0414-1111111')
        ]
        
        for unidad, placa, gps, modelo, telefono in test_units:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO unidades (flota, unidad, placa, tipo_gps, modelo, numero_telefonico) 
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (flota, unidad, placa, gps, modelo, telefono))
            except:
                pass
        
        # Crear algunos conductores de prueba si no existen
        test_conductores = [
            ('Juan', 'Pérez', '0414-1111111', '0424-1111111', 'V12345678', 'F001'),
            ('María', 'González', '0414-2222222', '0424-2222222', 'V87654321', 'F002'),
            ('Carlos', 'Rodríguez', '0414-3333333', '0424-3333333', 'V11223344', 'F003')
        ]
        
        for nombre, apellido, tel1, tel2, cedula, ficha in test_conductores:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO conductores (flota, nombre, apellido, telefono1, telefono2, cedula, ficha_empleado) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (flota, nombre, apellido, tel1, tel2, cedula, ficha))
            except:
                pass
        
        # Crear algunas rutas de prueba si no existen
        test_rutas = [
            ('Ruta Centro', 'Ruta hacia el centro de la ciudad'),
            ('Ruta Norte', 'Ruta hacia la zona norte'),
            ('Ruta Sur', 'Ruta hacia la zona sur')
        ]
        
        for nombre, descripcion in test_rutas:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO rutas (flota, nombre, descripcion) 
                    VALUES (?, ?, ?)
                """, (flota, nombre, descripcion))
            except:
                pass
        
        # Crear asignaciones de prueba
        today = str(date.today())
        test_assignments = [
            (today, '7001', 'F001', '0414-1111111', 'Ruta Centro', '08:00', '18:00'),
            (today, '7002', 'F002', '0414-2222222', 'Ruta Norte', '07:30', '17:30'),
            (today, '7003', 'F003', '0414-3333333', 'Ruta Sur', '09:00', '19:00'),
            (str(date.today() + timedelta(days=1)), '7001', 'F002', '0414-2222222', 'Ruta Norte', '08:00', '18:00')
        ]
        
        for fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada in test_assignments:
            cursor.execute("""
                INSERT INTO asignacion (flota, fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (flota, fecha, unidad, conductor_ficha, telefono, ruta_nombre, hora_salida, hora_entrada))
        
        conn.commit()
        
        # Limpiar cache
        get_all_asignaciones_db.clear()
        get_all_units_db.clear()
        get_all_conductores_db.clear()
        get_all_rutas_db.clear()
        get_available_units_db.clear()
        get_available_conductors_db.clear()
        
        st.success("✅ Datos de prueba creados exitosamente")
        return True
        
    except Exception as e:
        st.error(f"Error al crear datos de prueba: {e}")
        return False
    finally:
        if conn: conn.close()

# NUEVA FUNCIÓN: Verificar datos en BD

def check_database_data(flota: str):
    """Verifica qué datos existen en la base de datos."""
    conn = get_db_connection()
    if conn is None: return {}
    
    try:
        cursor = conn.cursor()
        
        # Contar datos por tabla
        data_summary = {}
        
        cursor.execute("SELECT COUNT(*) FROM unidades WHERE flota = ?", (flota,))
        data_summary['unidades'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM conductores WHERE flota = ?", (flota,))
        data_summary['conductores'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM rutas WHERE flota = ?", (flota,))
        data_summary['rutas'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM asignacion WHERE flota = ?", (flota,))
        data_summary['asignaciones'] = cursor.fetchone()[0]
        
        return data_summary
        
    except Exception as e:
        st.error(f"Error al verificar datos: {e}")
        return {}
    finally:
        if conn: conn.close()

@st.cache_data(ttl=600, show_spinner=False)
def get_available_units_db(flota: str, fecha: str) -> dict:
    """Obtiene solo las unidades NO asignadas para la flota y fecha especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    
    try:
        # Obtener todas las unidades de la flota
        cursor = conn.cursor()
        cursor.execute("SELECT unidad, placa, tipo_gps, modelo, numero_telefonico FROM unidades WHERE flota = ?", (flota,))
        todas_las_unidades = {row['unidad']: dict(row) for row in cursor.fetchall()}
        
        # Obtener unidades ya asignadas en la fecha especificada
        cursor.execute("SELECT unidad FROM asignacion WHERE flota = ? AND fecha = ?", (flota, fecha))
        unidades_asignadas = {row['unidad'] for row in cursor.fetchall()}
        
        # Filtrar solo las unidades NO asignadas
        unidades_disponibles = {unidad: datos for unidad, datos in todas_las_unidades.items() 
                              if unidad not in unidades_asignadas}
        
        return unidades_disponibles
    finally:
        if conn: conn.close()

@st.cache_data(ttl=600, show_spinner=False)

def get_available_conductors_db(flota: str, fecha: str) -> dict:
    """Obtiene solo los conductores NO asignados para la flota y fecha especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    
    try:
        # Obtener todos los conductores de la flota
        cursor = conn.cursor()
        cursor.execute("SELECT nombre, ficha_empleado, apellido, telefono1, telefono2, cedula FROM conductores WHERE flota = ?", (flota,))
        todos_los_conductores = {row['ficha_empleado']: {
            'nombre': row['nombre'],
            'apellido': row['apellido'],
            'telefono1': row['telefono1'],
            'telefono2': row['telefono2'],
            'cedula': row['cedula']
        } for row in cursor.fetchall()}
        
        # Obtener conductores ya asignados en la fecha especificada
        cursor.execute("SELECT conductor_ficha FROM asignacion WHERE flota = ? AND fecha = ?", (flota, fecha))
        conductores_asignados = {row['conductor_ficha'] for row in cursor.fetchall()}
        
        # Filtrar solo los conductores NO asignados
        conductores_disponibles = {ficha: datos for ficha, datos in todos_los_conductores.items() 
                                 if ficha not in conductores_asignados}
        
        return conductores_disponibles
    finally:
        if conn: conn.close()


# IMPORTANTE: Una sola inicialización de estado de sesión
    
# NUEVA FUNCIÓN: Limpiar unidad seleccionada
def clear_unit_to_locate():
    """Limpia la selección de unidad para volver a la vista de lista."""
    if 'unit_to_locate_id' in st.session_state:
        del st.session_state['unit_to_locate_id']

# NUEVA FUNCIÓN: Establecer unidad a ubicar
def set_unit_to_locate(unit_id):
    """Establece la unidad seleccionada para mostrar en el mapa."""
    st.session_state['unit_to_locate_id'] = unit_id


def display_asignacion_create_only():
    """Muestra SOLO el formulario para Crear Nueva Asignación, filtrado por FLOTA."""
    
    flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')
    st.header(f"📝 Crear Nueva Asignación (Flota: {flota_a_usar})")
    
    st.button("⬅️ Volver", on_click=set_logistica_view, args=('menu',), key="volver_menu_asignacion_create")
    st.markdown("---")

    # Operación fija en "Crear Nueva Asignación"
    operation = 'Crear Nueva Asignación'
    
    # Obtener todas las asignaciones
    all_assignments = get_all_asignaciones_db(flota_a_usar)
    
    # Variables para el formulario - solo creación
    current_assignment_data = None
    selected_assignment_key = None
    form_key = "asignacion_form_create"
    is_disabled_form = False
    
    # Obtener todas las asignaciones para mostrar contexto
    all_assignments = get_all_asignaciones_db(flota_a_usar)
    
    # DATOS DE REFERENCIA PARA CREACIÓN
    fecha_seleccionada_default = str(date.today())
    
    # Para crear, obtener unidades y conductores disponibles
    available_units = get_available_units_db(flota_a_usar, fecha_seleccionada_default)
    available_conductores = get_available_conductors_db(flota_a_usar, fecha_seleccionada_default)
    
    # Preparar opciones de conductores
    conductor_map = {}
    for ficha, data in available_conductores.items():
        nombre_completo = f"{data['nombre']} {data['apellido']}"
        telefono = data.get('telefono1', 'Sin Teléfono')
        display_name = f"{nombre_completo} | {telefono}"
        conductor_map[display_name] = {'ficha': ficha, 'telefono': telefono}
    
    # Opciones de rutas
    available_rutas = sorted(get_all_rutas_db(flota_a_usar).keys())
    
    # Formulario de Gestión - Solo para creación
    with st.form(form_key, clear_on_submit=True):
        st.subheader("Datos de la Nueva Asignación")
        
        # Valores por defecto para creación
        fecha_value = date.today()
        
        # Campos del formulario
        col1, col2 = st.columns(2)
        with col1:
            fecha_asignacion = st.date_input(
                "Fecha", 
                value=fecha_value, 
                disabled=True,
                help="La fecha se establece automáticamente como la fecha actual",
                key="asignacion_fecha_field"
            )
        
        with col2:
            # Para crear, mostrar solo unidades disponibles
            unit_options = ["Seleccione Unidad..."] + sorted(available_units.keys())
            unidad_seleccionada = st.selectbox(
                "Unidad Disponible", 
                unit_options, 
                key="asignacion_unidad_field"
            )
        
        # Conductor - Para crear, mostrar selectbox de conductores disponibles
        conductor_options = ["Seleccione Chofer..."] + sorted(conductor_map.keys())
        conductor_display = st.selectbox(
            "Chofer y Teléfono", 
            conductor_options, 
            key="asignacion_conductor_field"
        )
        
        # Advertencias de disponibilidad
        if len(available_units) == 0:
            st.warning("⚠️ **No hay unidades disponibles** para la fecha seleccionada.")
        if len(conductor_map) == 0:
            st.warning("⚠️ **No hay conductores disponibles** para la fecha seleccionada.")
        
        # Ruta
        route_options = ["(Opcional) Seleccione Ruta..."] + available_rutas
        ruta_seleccionada = st.selectbox(
            "Ruta a Cumplir", 
            route_options, 
            key="asignacion_ruta_field"
        )
        
        # Horarios
        col3, col4 = st.columns(2)
        with col3:
            hora_salida = st.text_input(
                "Hora de Salida (Ej: 08:00) *", 
                value="",
                max_chars=5, 
                help="Campo obligatorio. Formato: HH:MM",
                key="asignacion_h_salida_field",
                placeholder="Ej: 08:00"
            )
        with col4:
            hora_entrada = st.text_input(
                "Hora de Entrada (Ej: 18:30)", 
                value="",
                max_chars=5, 
                disabled=True,
                help="La hora de entrada se registrará cuando la unidad regrese",
                key="asignacion_h_entrada_field",
                placeholder="Se completará al regreso"
            )
        
        # Campo observaciones (opcional)
        observaciones = st.text_area(
            "Observaciones (Opcional)", 
            value="",
            key="asignacion_observaciones_field",
            placeholder="Ej: Vehículo en buenas condiciones, conductor nuevo, ruta alternativa..."
        )
        
        # Botón de submit
        submitted = st.form_submit_button("Crear Nueva Asignación")
        
        if submitted:
            # Validación para creación
            if unidad_seleccionada == "Seleccione Unidad..." or conductor_display == "Seleccione Chofer...":
                st.error("⚠️ Debe seleccionar una Unidad y un Chofer para crear la asignación.")
            elif not hora_salida.strip():
                st.error("⚠️ La hora de salida es obligatoria para crear la asignación.")
            elif not re.match(r'^\d{2}:\d{2}$', hora_salida.strip()):
                st.error("⚠️ El formato de hora de salida debe ser HH:MM (ejemplo: 08:00)")
            elif hora_entrada.strip() and not re.match(r'^\d{2}:\d{2}$', hora_entrada.strip()):
                st.error("⚠️ El formato de hora de entrada debe ser HH:MM (ejemplo: 18:30) - se permite dejar vacío")
            else:
                # Obtener datos del chofer
                conductor_data = conductor_map.get(conductor_display)
                
                if not conductor_data:
                    st.error("Error interno: Selección de chofer no válida.")
                else:
                    ficha = conductor_data['ficha']
                    telefono = conductor_data['telefono']
                    ruta = ruta_seleccionada if ruta_seleccionada != "(Opcional) Seleccione Ruta..." else ""
                    
                    # Crear la asignación
                    if create_asignacion_db(
                        flota_a_usar,
                        str(date.today()),  # Usar fecha actual automáticamente
                        unidad_seleccionada,
                        ficha,
                        telefono,
                        ruta,
                        hora_salida.strip(),
                        "",  # La hora de entrada estará vacía (se completará al ingreso)
                        observaciones.strip() if observaciones.strip() else None
                    ):
                        st.success(f"✅ Asignación de {unidad_seleccionada} a {conductor_display.split(' | ')[0]} registrada con éxito en {flota_a_usar}.")
                        st.cache_data.clear()
                        st.rerun()
    
    # MOSTRAR ASIGNACIONES ACTUALES PARA CONTEXTO
    st.markdown("---")
    st.markdown("**📋 Asignaciones actuales para hoy:**")
    
    # Obtener asignaciones del día actual
    today_assignments = [a for a in all_assignments if a['fecha'] == str(date.today())]
    
    if today_assignments:
        # Crear tabla para mostrar asignaciones actuales
        assignment_data = []
        conductores_db = get_all_conductores_db(flota_a_usar)
        
        for assignment in today_assignments:
            # Obtener nombre del conductor
            ficha = assignment['conductor_ficha']
            conductor_info = conductores_db.get(ficha, {})
            nombre_conductor = f"{conductor_info.get('nombre', '')} {conductor_info.get('apellido', '')}".strip() or f"Ficha: {ficha}"
            
            assignment_data.append({
                'Unidad': assignment['unidad'],
                'Conductor': nombre_conductor,
                'Ruta': assignment['ruta_nombre'] or 'Sin ruta',
                'Hora Salida': assignment['hora_salida'] or 'N/A'
            })
        
        df_current = pd.DataFrame(assignment_data)
        st.dataframe(df_current, use_container_width=True, hide_index=True)
    else:
        st.info("ℹ️ No hay asignaciones registradas para hoy.")


def display_asignacion_edit():
    """Muestra el formulario para Editar Asignaciones (solo del día actual)."""
    
    flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')
    fecha_actual = date.today()
    
    st.header(f"✏️ Editar Asignación (Flota: {flota_a_usar})")
    st.write(f"**Día:** {fecha_actual}")
    
    st.button("⬅️ Volver", on_click=set_logistica_view, args=('menu',), key="volver_menu_asignacion_edit")
    st.markdown("---")
    
    # Obtener asignaciones del día actual
    all_assignments = get_all_asignaciones_db(flota_a_usar)
    today_assignments = [a for a in all_assignments if a['fecha'] == str(fecha_actual)]
    
    if not today_assignments:
        st.warning("📅 No hay asignaciones registradas para el día de hoy.")
        return
    
    # Obtener conductores para el mapa
    conductores_db = get_all_conductores_db(flota_a_usar)
    unidades_db = get_all_units_db(flota_a_usar)
    rutas_db = get_all_rutas_db(flota_a_usar)
    
    # Preparar opciones para selectbox
    assignment_options = []
    assignment_map = {}
    
    for assignment in today_assignments:
        ficha = assignment['conductor_ficha']
        conductor_info = conductores_db.get(ficha, {})
        nombre_conductor = f"{conductor_info.get('nombre', '')} {conductor_info.get('apellido', '')}".strip() or f"Ficha: {ficha}"
        
        display_name = f"{assignment['unidad']} - {nombre_conductor} - {assignment.get('ruta_nombre', 'Sin ruta')}"
        assignment_options.append(display_name)
        assignment_map[display_name] = assignment
    
    if not assignment_options:
        st.warning("📅 No hay asignaciones disponibles para editar.")
        return
    
    # Selector de asignación
    selected_assignment = st.selectbox(
        "Seleccione la asignación a editar:",
        options=["Seleccione asignación..."] + assignment_options,
        key="asignacion_edit_selector"
    )
    
    if selected_assignment != "Seleccione asignación...":
        assignment_data = assignment_map[selected_assignment]
        
        with st.form("asignacion_edit_form", clear_on_submit=False):
            st.subheader("Editar Asignación")
            
            # Todos los campos son editables excepto la fecha que debe mantenerse
            fecha_value = datetime.strptime(assignment_data['fecha'], '%Y-%m-%d').date()
            fecha_asignacion = st.date_input("Fecha", value=fecha_value, disabled=True, help="La fecha no se puede modificar", key="edit_fecha_field")
            
            col1, col2 = st.columns(2)
            with col1:
                unidad_nueva = st.text_input("Unidad", value=assignment_data['unidad'], key="edit_unidad_field")
            
            with col2:
                telefono_nuevo = st.text_input("Teléfono", value=assignment_data.get('telefono', ''), key="edit_telefono_field")
            
            conductor_nuevo = st.text_input("Ficha Conductor", value=assignment_data['conductor_ficha'], key="edit_conductor_field")
            
            ruta_nueva = st.text_input("Ruta", value=assignment_data.get('ruta_nombre', ''), key="edit_ruta_field")
            
            col3, col4 = st.columns(2)
            with col3:
                hora_salida_nueva = st.text_input("Hora Salida", value=assignment_data.get('hora_salida', ''), key="edit_h_salida_field")
            with col4:
                hora_entrada_nueva = st.text_input("Hora Entrada", value=assignment_data.get('hora_entrada', ''), key="edit_h_entrada_field")
            
            # Campo observaciones editable
            observaciones_nuevas = st.text_area(
                "Observaciones (Editable)", 
                value=assignment_data.get('observaciones', '') or '',
                key="edit_observaciones_field",
                placeholder="Ej: Vehículo en buenas condiciones, conductor nuevo, ruta alternativa..."
            )
            
            submitted = st.form_submit_button("Actualizar Asignación")
            
            if submitted:
                # Validación básica
                if not unidad_nuevo.strip() or not conductor_nuevo.strip():
                    st.error("⚠️ Los campos Unidad y Ficha Conductor son obligatorios.")
                elif hora_salida_nueva.strip() and not re.match(r'^\d{2}:\d{2}$', hora_salida_nueva.strip()):
                    st.error("⚠️ El formato de hora de salida debe ser HH:MM (ejemplo: 08:00)")
                elif hora_entrada_nueva.strip() and not re.match(r'^\d{2}:\d{2}$', hora_entrada_nueva.strip()):
                    st.error("⚠️ El formato de hora de entrada debe ser HH:MM (ejemplo: 18:30)")
                else:
                    # Actualizar en la base de datos
                    if update_asignacion_db(
                        assignment_data['id'],
                        flota_a_usar,
                        str(fecha_asignacion),
                        unidad_nuevo.strip(),
                        conductor_nuevo.strip(),
                        telefono_nuevo.strip() if telefono_nuevo.strip() else None,
                        ruta_nueva.strip() if ruta_nueva.strip() else None,
                        hora_salida_nueva.strip() if hora_salida_nueva.strip() else None,
                        hora_entrada_nueva.strip() if hora_entrada_nueva.strip() else None,
                        observaciones_nuevas.strip() if observaciones_nuevas.strip() else None
                    ):
                        st.success(f"✅ Asignación actualizada exitosamente.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ Error al actualizar la asignación.")
        
        # Mostrar información actual
        st.markdown("---")
        st.subheader("Información Actual de la Asignación:")
        
        assignment_info = {
            'ID': assignment_data['id'],
            'Unidad': assignment_data['unidad'],
            'Conductor (Ficha)': assignment_data['conductor_ficha'],
            'Teléfono': assignment_data.get('telefono', 'N/A'),
            'Ruta': assignment_data.get('ruta_nombre', 'N/A'),
            'Hora Salida': assignment_data.get('hora_salida', 'N/A'),
            'Hora Entrada': assignment_data.get('hora_entrada', 'N/A'),
            'Observaciones': assignment_data.get('observaciones', 'N/A')
        }
        
        for key, value in assignment_info.items():
            st.write(f"**{key}:** {value}")


def display_asignacion_delete():
    """Muestra el formulario para Eliminar Asignaciones (solo del día actual)."""
    
    flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')
    fecha_actual = date.today()
    
    st.header(f"🗑️ Eliminar Asignación (Flota: {flota_a_usar})")
    st.write(f"**Día:** {fecha_actual}")
    
    st.button("⬅️ Volver", on_click=set_logistica_view, args=('menu',), key="volver_menu_asignacion_delete")
    st.markdown("---")
    
    # Obtener asignaciones del día actual
    all_assignments = get_all_asignaciones_db(flota_a_usar)
    today_assignments = [a for a in all_assignments if a['fecha'] == str(fecha_actual)]
    
    if not today_assignments:
        st.warning("📅 No hay asignaciones registradas para el día de hoy.")
        return
    
    # Obtener conductores para el mapa
    conductores_db = get_all_conductores_db(flota_a_usar)
    
    # Preparar opciones para selectbox
    assignment_options = []
    assignment_map = {}
    
    for assignment in today_assignments:
        ficha = assignment['conductor_ficha']
        conductor_info = conductores_db.get(ficha, {})
        nombre_conductor = f"{conductor_info.get('nombre', '')} {conductor_info.get('apellido', '')}".strip() or f"Ficha: {ficha}"
        
        display_name = f"{assignment['unidad']} - {nombre_conductor} - {assignment.get('ruta_nombre', 'Sin ruta')}"
        assignment_options.append(display_name)
        assignment_map[display_name] = assignment
    
    if not assignment_options:
        st.warning("📅 No hay asignaciones disponibles para eliminar.")
        return
    
    # Selector de asignación
    selected_assignment = st.selectbox(
        "Seleccione la asignación a eliminar:",
        options=["Seleccione asignación..."] + assignment_options,
        key="asignacion_delete_selector"
    )
    
    if selected_assignment != "Seleccione asignación...":
        assignment_data = assignment_map[selected_assignment]
        
        # Mostrar información de la asignación a eliminar
        st.subheader("📋 Asignación a Eliminar:")
        
        st.markdown(f"""
        **Unidad:** {assignment_data['unidad']}  
        **Conductor:** {assignment_data['conductor_ficha']}  
        **Ruta:** {assignment_data.get('ruta_nombre', 'N/A')}  
        **Hora Salida:** {assignment_data.get('hora_salida', 'N/A')}  
        **Hora Entrada:** {assignment_data.get('hora_entrada', 'N/A')}  
        **Observaciones:** {assignment_data.get('observaciones', 'N/A')}
        """)
        
        # Confirmación de eliminación
        confirm_text = f"ELIMINAR-{assignment_data['unidad']}-{assignment_data['conductor_ficha']}"
        confirmacion = st.text_input(
            f"Para confirmar la eliminación, escriba exactamente: {confirm_text}",
            key="delete_confirmation"
        )
        
        if st.button("🗑️ Eliminar Asignación", type="primary"):
            if confirmacion == confirm_text:
                if delete_asignacion_db(assignment_data['id'], flota_a_usar):
                    st.success(f"✅ Asignación de {assignment_data['unidad']} eliminada exitosamente.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("❌ Error al eliminar la asignación.")
            else:
                st.error("⚠️ Texto de confirmación incorrecto. Verifique que haya escrito exactamente como se indica.")


def display_asignacion_ingreso():
    """Muestra el formulario para Ingreso de unidades (solo hora_entrada editable, campo observaciones bloqueado)."""
    
    flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')
    fecha_actual = date.today()
    
    st.header(f"🚪 Ingreso de Unidades (Flota: {flota_a_usar})")
    st.write(f"**Día:** {fecha_actual}")
    
    st.button("⬅️ Volver", on_click=set_logistica_view, args=('menu',), key="volver_menu_asignacion_ingreso")
    st.markdown("---")
    
    # Obtener asignaciones del día actual
    all_assignments = get_all_asignaciones_db(flota_a_usar)
    today_assignments = [a for a in all_assignments if a['fecha'] == str(fecha_actual)]
    
    if not today_assignments:
        st.warning("📅 No hay asignaciones registradas para el día de hoy.")
        return
    
    # Obtener conductores para el mapa
    conductores_db = get_all_conductores_db(flota_a_usar)
    
    # Preparar opciones para selectbox
    assignment_options = []
    assignment_map = {}
    
    for assignment in today_assignments:
        ficha = assignment['conductor_ficha']
        conductor_info = conductores_db.get(ficha, {})
        nombre_conductor = f"{conductor_info.get('nombre', '')} {conductor_info.get('apellido', '')}".strip() or f"Ficha: {ficha}"
        
        display_name = f"{assignment['unidad']} - {nombre_conductor} - {assignment.get('ruta_nombre', 'Sin ruta')}"
        assignment_options.append(display_name)
        assignment_map[display_name] = assignment
    
    if not assignment_options:
        st.warning("📅 No hay asignaciones disponibles para registrar ingreso.")
        return
    
    # Selector de asignación
    selected_assignment = st.selectbox(
        "Seleccione la unidad que está ingresando:",
        options=["Seleccione unidad..."] + assignment_options,
        key="asignacion_ingreso_selector"
    )
    
    if selected_assignment != "Seleccione unidad...":
        assignment_data = assignment_map[selected_assignment]
        
        with st.form("asignacion_ingreso_form", clear_on_submit=False):
            st.subheader("Ingreso de Unidad")
            
            # Todos los campos bloqueados excepto hora_entrada y observaciones (solo lectura)
            fecha_value = datetime.strptime(assignment_data['fecha'], '%Y-%m-%d').date()
            
            col1, col2 = st.columns(2)
            with col1:
                fecha_bloqueada = st.date_input("Fecha", value=fecha_value, disabled=True, help="La fecha de la asignación", key="ingreso_fecha_field")
            
            with col2:
                unidad_bloqueada = st.text_input("Unidad", value=assignment_data['unidad'], disabled=True, help="Unidad asignada", key="ingreso_unidad_field")
            
            conductor_bloqueado = st.text_input("Ficha Conductor", value=assignment_data['conductor_ficha'], disabled=True, help="Conductor asignado", key="ingreso_conductor_field")
            
            telefono_bloqueado = st.text_input("Teléfono", value=assignment_data.get('telefono', ''), disabled=True, help="Teléfono del conductor", key="ingreso_telefono_field")
            
            ruta_bloqueada = st.text_input("Ruta", value=assignment_data.get('ruta_nombre', ''), disabled=True, help="Ruta asignada", key="ingreso_ruta_field")
            
            col3, col4 = st.columns(2)
            with col3:
                hora_salida_bloqueada = st.text_input("Hora Salida", value=assignment_data.get('hora_salida', ''), disabled=True, help="Hora de salida registrada", key="ingreso_h_salida_field")
            with col4:
                hora_entrada = st.text_input("Hora Entrada (Editable) *", value=assignment_data.get('hora_entrada', ''), help="Campo obligatorio para registrar el ingreso", key="ingreso_h_entrada_field", placeholder="Ej: 18:30")
            
            # Campo observaciones solo lectura
            observaciones_bloqueadas = st.text_area(
                "Observaciones (Solo lectura)", 
                value=assignment_data.get('observaciones', '') or 'Sin observaciones',
                disabled=True,
                key="ingreso_observaciones_field"
            )
            
            submitted = st.form_submit_button("Registrar Ingreso")
            
            if submitted:
                # Validación
                if not hora_entrada.strip():
                    st.error("⚠️ La hora de entrada es obligatoria.")
                elif not re.match(r'^\d{2}:\d{2}$', hora_entrada.strip()):
                    st.error("⚠️ El formato de hora de entrada debe ser HH:MM (ejemplo: 18:30)")
                else:
                    # Actualizar solo la hora_entrada y observaciones (mantener observaciones igual)
                    if update_asignacion_db(
                        assignment_data['id'],
                        flota_a_usar,
                        assignment_data['fecha'],
                        assignment_data['unidad'],
                        assignment_data['conductor_ficha'],
                        assignment_data.get('telefono'),
                        assignment_data.get('ruta_nombre'),
                        assignment_data.get('hora_salida'),
                        hora_entrada.strip(),
                        assignment_data.get('observaciones')  # Mantener observaciones igual
                    ):
                        st.success(f"✅ Ingreso de {assignment_data['unidad']} registrado exitosamente.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ Error al registrar el ingreso.")
        
        # Mostrar información actual
        st.markdown("---")
        st.subheader("Información de la Asignación:")
        
        assignment_info = {
            'Unidad': assignment_data['unidad'],
            'Conductor (Ficha)': assignment_data['conductor_ficha'],
            'Teléfono': assignment_data.get('telefono', 'N/A'),
            'Ruta': assignment_data.get('ruta_nombre', 'N/A'),
            'Hora Salida': assignment_data.get('hora_salida', 'N/A'),
            'Hora Entrada Actual': assignment_data.get('hora_entrada', 'N/A'),
            'Observaciones': assignment_data.get('observaciones', 'N/A')
        }
        
        for key, value in assignment_info.items():
            st.write(f"**{key}:** {value}")


def display_rutas_crud():
    """Muestra el formulario para Crear/Modificar/Eliminar Rutas, filtrado por FLOTA."""
    
    # 🚨 OBTENER LA FLOTA SELECCIONADA
    flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')
    
    st.header(f"🗺️Gestión de Rutas  (Flota: {flota_a_usar})")
    
    st.button("⬅️ Volver al Menú ", on_click=set_logistica_view, args=('menu',), key="volver_menu_rutas_main")
    st.markdown("---")

    # Selector de Operación
    operation = st.radio(
        "¿Qué desea hacer?",
        ('Crear Nueva Ruta', 'Modificar Ruta Existente', 'Eliminar Ruta'),
        horizontal=True,
        key="ruta_crud_operation_radio"
    )
    
    # Obtener datos de rutas
    available_rutas = get_all_rutas_db(flota_a_usar) 
    
    current_ruta_data = None
    selected_ruta_key = None
    form_key = "ruta_form_create" 
    is_disabled_form = False 
    
    if operation != 'Crear Nueva Ruta':
        ruta_keys = ["Seleccione una Ruta..."] + sorted(available_rutas.keys())
        selected_ruta_key = st.selectbox(
            f"Seleccionar Ruta a {operation.split()[0].lower()}:",
            ruta_keys,
            key="ruta_selector"
        )
        
        if selected_ruta_key != "Seleccione una Ruta...":
            current_ruta_data = available_rutas.get(selected_ruta_key)
            is_disabled_form = (operation == 'Eliminar Ruta') 
            form_key = f"ruta_form_mod_{selected_ruta_key}"
        else:
            is_disabled_form = True 
            form_key = "ruta_form_empty"
            
    # Formulario de Gestión
    with st.form(form_key, clear_on_submit=True):
        st.subheader(f"Datos de la Ruta ({operation})")
        
        nombre_value = selected_ruta_key if current_ruta_data else ""
        descripcion_value = current_ruta_data.get('descripcion', '') if current_ruta_data else ""

        nombre = st.text_input(
            "Nombre de la Ruta", 
            value=nombre_value, 
            max_chars=100, 
            key="ruta_nombre_field", 
            disabled=is_disabled_form
        )
        
        descripcion = st.text_area(
            "Descripción (Opcional)", 
            value=descripcion_value,
            height=100,
            key="ruta_descripcion_field",
            disabled=is_disabled_form
        )
        
        enable_submit = operation == 'Crear Nueva Ruta' or (current_ruta_data is not None)
        submitted = st.form_submit_button(f"{operation}", disabled=not enable_submit)


        if submitted:
            if not nombre:
                st.error("⚠️ El campo 'Nombre de la Ruta' es obligatorio para esta operación.")
                
            elif operation == 'Crear Nueva Ruta':
                if create_ruta_db(flota_a_usar, nombre, descripcion):
                    st.success(f"✅ ¡Ruta **{nombre}** creada con éxito en la flota {flota_a_usar}!")
                    st.cache_data.clear()
                    st.rerun() 

            elif operation == 'Modificar Ruta Existente':
                # Nota: Esta función asume que tienes la función update_ruta_db
                # que maneja el cambio de nombre. Si no la tienes, te la puedo proporcionar.
                if update_ruta_db(flota_a_usar, selected_ruta_key, nombre, descripcion):
                    st.success(f"✅ ¡Ruta **{nombre}** modificada con éxito!")
                    st.cache_data.clear()
                    st.rerun()
                
            elif operation == 'Eliminar Ruta':
                # Nota: Esta función asume que tienes la función delete_ruta_db
                if delete_ruta_db(flota_a_usar, selected_ruta_key):
                    st.success(f"✅ ¡Ruta **{selected_ruta_key}** eliminada con éxito!")
                    set_logistica_view('menu') 
                    st.cache_data.clear()
                    st.rerun() 
        

def display_conductores_crud():
    """Muestra el formulario para Crear/Modificar/Eliminar Conductores, filtrado por FLOTA."""
    
    # 🚨 OBTENER LA FLOTA SELECCIONADA
    flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')
    
    st.header(f"Gestión de Conductores (Flota: {flota_a_usar})")
    
    st.button(
        "⬅️ Volver al Menú",
        on_click=set_logistica_view,
        args=('menu',),
        key="volver_menu_conductores_main"
        )
    st.markdown("---")

    # Selector de Operación
    operation = st.radio(
        "¿Qué desea hacer?",
        ('Crear Nuevo Conductor', 'Modificar Conductor Existente', 'Eliminar Conductor'),
        horizontal=True,
        key="conductor_crud_radio_main"
    )
    
    # Obtener datos de conductores filtrados por flota
    available_conductores = get_all_conductores_db(flota_a_usar) 
    
    current_conductor_data = None
    selected_ficha_key = None
    form_key = "conductor_form_create" 
    is_disabled_form = False 
    
    if operation != 'Crear Nuevo Conductor':
        # La clave es la ficha de empleado
        conductor_keys = ["Seleccione un Conductor..."] + sorted(available_conductores.keys())
        selected_ficha_key = st.selectbox(
            f"Seleccionar Ficha de Empleado a {operation.split()[0].lower()}:",
            conductor_keys,
            key="conductor_selector"
        )
        
        if selected_ficha_key != "Seleccione un Conductor...":
            current_conductor_data = available_conductores.get(selected_ficha_key)
            is_disabled_form = (operation == 'Eliminar Conductor') 
            form_key = f"conductor_form_mod_{selected_ficha_key}"
        else:
            is_disabled_form = True 
            form_key = "conductor_form_empty"
            
    # Formulario de Gestión
    with st.form(form_key, clear_on_submit=True):
        st.subheader(f"Datos del Conductor ({operation})")
        
        # Recuperar valores para Modificar/Eliminar
        nombre_value = current_conductor_data.get('nombre', '') if current_conductor_data else ""
        apellido_value = current_conductor_data.get('apellido', '') if current_conductor_data else ""
        tel1_value = current_conductor_data.get('telefono1', '') if current_conductor_data else ""
        tel2_value = current_conductor_data.get('telefono2', '') if current_conductor_data else ""
        cedula_value = current_conductor_data.get('cedula', '') if current_conductor_data else ""
        ficha_value = selected_ficha_key if current_conductor_data else "" # Ficha ya está seleccionada

        col_nom, col_ape = st.columns(2)
        with col_nom:
            nombre = st.text_input("Nombre", value=nombre_value, max_chars=100, key="cond_nombre", disabled=is_disabled_form)
        with col_ape:
            apellido = st.text_input("Apellido", value=apellido_value, max_chars=100, key="cond_apellido", disabled=is_disabled_form)

        col_tel1, col_tel2 = st.columns(2)
        with col_tel1:
            tel1 = st.text_input("Teléfono 1", value=tel1_value, max_chars=20, key="cond_tel1", disabled=is_disabled_form)
        with col_tel2:
            tel2 = st.text_input("Teléfono 2 (Opcional)", value=tel2_value, max_chars=20, key="cond_tel2", disabled=is_disabled_form)
            
        col_ced, col_fic = st.columns(2)
        with col_ced:
            cedula = st.text_input("Cédula", value=cedula_value, max_chars=20, key="cond_cedula", disabled=is_disabled_form)
        with col_fic:
            # La Ficha solo se puede ingresar en Creación
            ficha = st.text_input(
                "Ficha Empleado", 
                value=ficha_value, 
                max_chars=20, 
                key="cond_ficha",
                disabled=is_disabled_form or (operation != 'Crear Nuevo Conductor') 
            )

        enable_submit = operation == 'Crear Nuevo Conductor' or (current_conductor_data is not None)
        submitted = st.form_submit_button(f"{operation}", disabled=not enable_submit)


        if submitted:
            if not nombre or not apellido or not cedula or not ficha:
                st.error("⚠️ Los campos Nombre, Apellido, Cédula y Ficha Empleado son obligatorios.")
                
            elif operation == 'Crear Nuevo Conductor':
                if create_conductor_db(flota_a_usar, nombre, apellido, tel1, tel2, cedula, ficha):
                    st.success(f"✅ ¡Conductor **{nombre} {apellido}** creado con éxito en la flota {flota_a_usar}!")
                    st.cache_data.clear()
                    st.rerun() 

            elif operation == 'Modificar Conductor Existente':
                # Asume que tienes la función update_conductor_db
                if update_conductor_db(flota_a_usar, selected_ficha_key, nombre, apellido, tel1, tel2, cedula):
                    st.success(f"✅ ¡Conductor **{selected_ficha_key}** modificado con éxito!")
                    st.cache_data.clear()
                    st.rerun()
                
            elif operation == 'Eliminar Conductor':
                # Asume que tienes la función delete_conductor_db
                if delete_conductor_db(flota_a_usar, selected_ficha_key):
                    st.success(f"✅ ¡Conductor **{selected_ficha_key}** eliminado con éxito!")
                    set_logistica_view('menu')
                    st.cache_data.clear()
                    st.rerun()
        
def display_unidades_crud():
    """Muestra el formulario para Crear/Modificar/Eliminar Unidades, filtrado por FLOTA."""
    
    # 🚨 OBTENER LA FLOTA SELECCIONADA
    flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')
    
    st.header(f"🚛 Gestión de Unidades (Flota: {flota_a_usar})")
    
    st.button("⬅️ Volver al Menú", on_click=set_logistica_view, args=('menu',), key="volver_menu_unidades_main")
    st.markdown("---")

    # Selector de Operación
    operation = st.radio(
        "¿Qué desea hacer?",
        ('Crear Nueva Unidad', 'Modificar Unidad Existente', 'Eliminar Unidad'),
        horizontal=True,
        key="crud_operation_radio_main"
    )
    
    available_units = get_all_units_db(flota_a_usar) 
    
    current_unit_data = None
    selected_unit_key = None
    form_key = "unidad_form_create" 
    is_disabled_form = False 
    
    if operation != 'Crear Nueva Unidad':
        unit_keys = ["Seleccione una Unidad..."] + sorted(available_units.keys())
        selected_unit_key = st.selectbox(
            f"Seleccionar Unidad a {operation.split()[0].lower()}:",
            unit_keys,
            key="unit_selector"
        )
        
        if selected_unit_key != "Seleccione una Unidad...":
            current_unit_data = available_units.get(selected_unit_key)
            is_disabled_form = (operation == 'Eliminar Unidad') 
            form_key = f"unidad_form_mod_{selected_unit_key}" 
        else:
            is_disabled_form = True 
            form_key = "unidad_form_empty"
            
    # Formulario de Gestión
    with st.form(form_key, clear_on_submit=True):
        st.subheader(f"Datos de la Unidad ({operation})")
        
        unidad_value = selected_unit_key if current_unit_data else ""
        placa_value = current_unit_data.get('placa', '') if current_unit_data else ""
        gps_value = current_unit_data.get('tipo_gps', 'Sin GPS') if current_unit_data else 'Sin GPS'
        modelo_value = current_unit_data.get('modelo', '') if current_unit_data else ""
        telefono_value = current_unit_data.get('numero_telefonico', '') if current_unit_data else ""

        col_id, col_placa = st.columns(2)
        with col_id:
            unidad = st.text_input(
                "Identificador de Unidad", 
                value=unidad_value, 
                max_chars=50, 
                key="unidad_id_field",
                disabled=is_disabled_form or (operation != 'Crear Nueva Unidad') 
            )
        with col_placa:
            placa = st.text_input(
                "Placa/Matrícula", 
                value=placa_value, 
                max_chars=20, 
                key="unidad_placa_field",
                disabled=is_disabled_form
            )
        
        col_gps, col_modelo = st.columns(2)
        with col_gps:
            gps_options = ['Sin GPS', 'Concox', 'Teltonika','Ruptela', 'Otros']
            default_index = gps_options.index(gps_value) if gps_value in gps_options else 0
            tipo_gps = st.selectbox(
                "Tipo de GPS", 
                gps_options, 
                index=default_index,
                key="unidad_gps_field",
                disabled=is_disabled_form
            )
        with col_modelo:
            modelo = st.text_input(
                "Modelo de Vehículo", 
                value=modelo_value, 
                max_chars=100, 
                key="unidad_modelo_field",
                disabled=is_disabled_form
            )
            
        num_telefono = st.text_input(
            "Número Telefónico (Asociado al GPS)", 
            value=telefono_value, 
            max_chars=20, 
            key="unidad_telefono_field",
            disabled=is_disabled_form
        )
        
        enable_submit = operation == 'Crear Nueva Unidad' or (current_unit_data is not None)
        submitted = st.form_submit_button(f"{operation}", disabled=not enable_submit)


        if submitted:
            if not unidad or not placa:
                st.error("⚠️ Los campos 'Identificador de Unidad' y 'Placa' son obligatorios para esta operación.")
                
            elif operation == 'Crear Nueva Unidad':
                if create_unit_db(flota_a_usar, unidad, placa, tipo_gps, modelo, num_telefono):
                    st.success(f"✅ ¡Unidad **{unidad}** creada con éxito en la flota {flota_a_usar}!")
                    st.cache_data.clear()
                    st.rerun() 

            elif operation == 'Modificar Unidad Existente':
                if update_unit_db(flota_a_usar, selected_unit_key, placa, tipo_gps, modelo, num_telefono):
                    st.success(f"✅ ¡Unidad **{selected_unit_key}** modificada con éxito!")
                    st.cache_data.clear()
                    st.rerun()
                
            elif operation == 'Eliminar Unidad':
                if delete_unit_db(flota_a_usar, selected_unit_key):
                    st.success(f"✅ ¡Unidad **{selected_unit_key}** eliminada con éxito!")
                    set_logistica_view('menu')
                    st.cache_data.clear()
                    st.rerun()

# ====================================================================
#                      FUNCIONES CRUD POR FLOTA
# ====================================================================

# UNIDADES CRUD

@st.cache_data(ttl=600, show_spinner=False)
def get_all_units_db(flota: str):
    """Obtiene todas las unidades para la flota especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    units = {}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT unidad, placa, tipo_gps, modelo, numero_telefonico FROM unidades WHERE flota = ?", (flota,))
        for row in cursor.fetchall():
            units[row['unidad']] = dict(row)
        return units
    finally:
        if conn: conn.close()

def create_unit_db(flota, unidad, placa, tipo_gps, modelo, num_telefono):
    """Guarda una nueva unidad bajo la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO unidades (flota, unidad, placa, tipo_gps, modelo, numero_telefonico) VALUES (?, ?, ?, ?, ?, ?)"
        cursor.execute(sql, (flota, unidad, placa, tipo_gps, modelo, num_telefono))
        conn.commit()
        get_all_units_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error("Error de Integridad: La Unidad o Placa ya existe en esta flota.")
        return False
    finally:
        if conn: conn.close()

# (Otras funciones de UPDATE y DELETE de unidades no incluidas aquí por espacio, 
# pero deben pasar la variable 'flota' en la cláusula WHERE)

# CONDUCTORES CRUD

@st.cache_data(ttl=600, show_spinner=False)
def get_all_conductores_db(flota: str):
    """Obtiene todos los conductores para la flota especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    conductores = {}
    try:
        cursor = conn.cursor()
        sql = "SELECT ficha_empleado, nombre, apellido, telefono1, telefono2, cedula FROM conductores WHERE flota = ?"
        cursor.execute(sql, (flota,))
        for row in cursor.fetchall():
            conductores[row['ficha_empleado']] = dict(row)
        return conductores
    finally:
        if conn: conn.close()

def create_conductor_db(flota, nombre, apellido, tel1, tel2, cedula, ficha):
    """Guarda un nuevo conductor bajo la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO conductores (flota, nombre, apellido, telefono1, telefono2, cedula, ficha_empleado) VALUES (?, ?, ?, ?, ?, ?, ?)"
        cursor.execute(sql, (flota, nombre, apellido, tel1, tel2, cedula, ficha))
        conn.commit()
        get_all_conductores_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error("Error de Integridad: La Cédula o Ficha de Empleado ya existe en esta flota.")
        return False
    finally:
        if conn: conn.close()

# (Otras funciones de UPDATE y DELETE de conductores deben pasar 'flota')

# RUTAS CRUD

@st.cache_data(ttl=600, show_spinner=False)
def get_all_rutas_db(flota: str):
    """Obtiene todas las rutas para la flota especificada."""
    conn = get_db_connection()
    if conn is None: return {}
    rutas = {}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT nombre, descripcion FROM rutas WHERE flota = ?", (flota,))
        for row in cursor.fetchall():
            rutas[row['nombre']] = dict(row)
        return rutas
    finally:
        if conn: conn.close()

def create_ruta_db(flota, nombre, descripcion=""):
    """Guarda una nueva ruta bajo la flota especificada."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO rutas (flota, nombre, descripcion) VALUES (?, ?, ?)"
        cursor.execute(sql, (flota, nombre, descripcion))
        conn.commit()
        get_all_rutas_db.clear()
        return True
    except sqlite3.IntegrityError:
        st.error(f"Error de Integridad: La ruta '{nombre}' ya existe en esta flota.")
        return False
    finally:
        if conn: conn.close()


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

# CONFIGURACIÓN DE ZONA HORARIA Y LÓGICA DE TIEMPO

# Definir la zona horaria de Venezuela (VET = UTC-4)
VENEZUELA_TZ = timezone(timedelta(hours=-4))
# Formato de fecha y hora requerido para parsear 'LastReportTime': 'Sep 30 2025 12:57PM'
TIME_FORMAT = '%b %d %Y %I:%M%p'


# FUNCIONES DE PERÍMETROS
@st.cache_data(ttl=None) # 🚨 OPTIMIZACIÓN: Cargar perímetros una sola vez por selección de flota
def cargar_perimetros(perimetros_dir: str = "perimetros") -> Dict[str, Dict[str, Any]]:
    """
    Carga y procesa todos los perímetros (GeoJSON Polygon/LineString) 
    de la carpeta 'perimetros/' en objetos Shapely Polygon.
    """
    if not os.path.exists(perimetros_dir):
        os.makedirs(perimetros_dir)
        print(f"Directorio de perímetros '{perimetros_dir}' creado. ¡Agrega tus archivos JSON!")
        return {}
    
    perimetros_cargados = {}
    archivos_json = glob.glob(os.path.join(perimetros_dir, "*.json"))
    
    if not archivos_json:
        return {}

    for i, file_path in enumerate(archivos_json):
        nombre_perimetro = os.path.basename(file_path).replace('.json', '')
        coords_lon_lat = None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                geojson_data = json.load(f)
            
            # Navegación GeoJSON: FeatureCollection > Feature > Geometry
            feature = geojson_data.get('features', [{}])[0]
            geometry = feature.get('geometry', {})
            properties = feature.get('properties', {})
            geom_type = geometry.get('type')
            
            if geom_type == 'Polygon':
                # Coordenadas de Polygon: un nivel extra de anidamiento
                poligon_rings = geometry.get('coordinates', [[]])
                coords_lon_lat = poligon_rings[0]
            
            elif geom_type == 'LineString':
                # Coordenadas de LineString: lista simple de puntos
                coords_lon_lat = geometry.get('coordinates', [])
            
            else:
                 print(f"El archivo {nombre_perimetro}.json tiene un tipo de geometría ('{geom_type}') no soportado y fue omitido.")
                 continue

            if not coords_lon_lat:
                 print(f"El archivo {nombre_perimetro}.json no contiene coordenadas válidas y fue omitido.")
                 continue

            # Crear el objeto Polygon de Shapely (usa [lon, lat])
            poligono = Polygon(coords_lon_lat)
            
            # Extraer color de las propiedades (por defecto azul)
            color_perimetro = properties.get('color', '#9CF527')
            color_relleno = properties.get('fill', '#9CF527')

            
            # Almacenar la información
            perimetros_cargados[nombre_perimetro] = {
                "poligono_shapely": poligono,
                "coords_lon_lat": coords_lon_lat,
                "archivo_path": file_path,
                "geometria": {
                    "type": geom_type,
                    "coordinates": coords_lon_lat
                },
                "nombre": properties.get('name', nombre_perimetro),
                "color_perimetro": color_perimetro,
                "color_relleno": color_relleno,
                "properties": properties
            }
        
        except json.JSONDecodeError:
            print(f"El archivo {nombre_perimetro}.json no es un GeoJSON válido y fue omitido.")
        except Exception as e:
            print(f"Error al procesar el archivo {nombre_perimetro}.json: {e}. Revise la estructura de coordenadas.")
            
    return perimetros_cargados

def verificar_coordenada_en_perimetro(latitud, longitud, perimetros_cargados):
    """Verifica si una coordenada está dentro de los polígonos cargados usando Shapely."""
    # Shapely usa (Longitud, Latitud)
    punto = Point(longitud, latitud)
    perimetros_encontrados = {}
    
    for nombre, data in perimetros_cargados.items():
        if data["poligono_shapely"].contains(punto):
            perimetros_encontrados[nombre] = data
            
    return perimetros_encontrados

# 🔒 CONSTANTE DE CONTRASEÑA 🔒
CONFIG_PASSWORD = "admin" # <-- ¡CÁMBIALA AQUÍ!
# -------------------------------------

# 🚨 NUEVAS CONSTANTES DE COLOR PARA UBICACIONES DINÁMICAS 🚨
PROXIMIDAD_KM_S = 0.1 # Distancia de la sede (PARA ASUMIR EN SEDE/VERTEDERO)
PROXIMIDAD_KM_V = 0.30
PROXIMIDAD_KM_R = 0.05


COLOR_RESGUARDO_SECUNDARIO = "#191452"
COLOR_VERTEDERO = "#FCC6BB" 
COLOR_FUERA_DE_PERIMETRO = "#7627F5" 
COLOR_FALLA_GPS = "#AAAAAA" 


# 🚨 CONSTANTES DE ALARMAS DE VELOCIDAD 🚨
# Velocidad mínima para activar alerta visual (color naranja) - configurable
# Velocidad crítica para activar alarma sonora (color rojo) - fija en 75 km/h
VELOCIDAD_CRITICA_AUDIO = 75  # Km/h - La alarma sonora se activa a partir de esta velocidad
# ----------------------------------------------------------------------

# 🚨 CONSTANTES DE ALARMAS DE PERÍMETRO 🚨
TIEMPO_REPETICION_AUDIO_PERIMETRO = 20  # Segundos - Repetición del audio si no es aceptado
TIEMPO_SILENCIO_PERIMETRO = 15  # Minutos - Tiempo de silencio después de aceptar alarma
# ----------------------------------------------------------------------

# CONSTANTES DEL TESTIGO DE ACTUALIZACIÓN (INSERTE AQUÍ)
COLOR_LIGHT_ON = "#4CAF50"  # Verde para "Dashboard Renderizado"
COLOR_LIGHT_OFF = "#606060" # Gris para "En Espera/Solicitando Data"
LIGHT_SIZE_PX = "15px"
# ----------------------------------------------------------------------

def obtener_hora_venezuela() -> datetime:
    """Retorna el objeto datetime con la hora actual en la Zona Horaria de Venezuela (VET)."""
    return datetime.now(VENEZUELA_TZ)
def scroll_to_top_callback():
    """
    Fuerza el scroll de la ventana al inicio (0, 0)
    y limpia el estado de la unidad seleccionada.
    """
    # 1. Fuerza el scroll al inicio (0, 0) usando JavaScript
    st.markdown(
        """
        <script>
            window.scrollTo(0, 0);
        </script>
        """,
        unsafe_allow_html=True,
    )
    # 2. Resetea el estado de la unidad seleccionada para mostrar la lista completa
    if 'selected_unit_id' in st.session_state:
        st.session_state.selected_unit_id = None
        
# Conversion de grados a Sentido Cardinal        
def grados_a_direccion(grados):
    # Asegura que el valor esté entre 0 y 360 grados
    grados = grados % 360
    
    # Lista de direcciones cardinales (16 puntos)
    direcciones = [
        "Norte", "Nor-Noreste", "Noreste", "Este-Noreste", 
        "Este", "Este-Sureste", "Sureste", "Sur-Sureste", 
        "Sur", "Sur-Suroeste", "Suroeste", "Oeste-Suroeste", 
        "Oeste", "Oeste-Noroeste", "Noroeste", "Nor-Noroeste"
    ]
    
    # Desplazamiento inicial de la primera dirección a 11.25 grados
    # para centrar las direcciones cardinales en sus rangos.
    # Cada sector es de 22.5 grados, 11.25 es la mitad.
    indice = round(grados / 22.5) % 16
    
    return direcciones[indice]        

def construir_status_con_emojis(row_data, is_en_perimetro, estado_display=None):
    """
    Construye el status con emojis coloridos según el estado de la unidad.
    """
    if is_en_perimetro:
        return "En Perímetro"
    
    # Si está fuera de perímetro, construir el estado con emojis coloridos
    if estado_display is None:
        # Construir estado si no se proporciona
        estado_ignicion = row_data['IGNICION']
        velocidad = row_data['VELOCIDAD']
        stop_duration = row_data['STOP_DURATION_MINUTES']
        
        # Lógica similar a las tarjetas
        is_out_of_hq_status = not (row_data['EN_SEDE_FLAG'] or row_data['EN_RESGUARDO_SECUNDARIO_FLAG'] or row_data['EN_VERTEDERO_FLAG'] or row_data['ES_FALLA_GPS_FLAG'])
        
        if row_data['ES_FALLA_GPS_FLAG']:
            return '<span style="color: gray;">🛑</span> Falla GPS'
        elif stop_duration > STOP_THRESHOLD_MINUTES and velocidad < 1.0 and is_out_of_hq_status:
            return f'<span style="color: #FFD700;">🛑</span> Parada Larga: {stop_duration:.0f} min'
        elif velocidad >= VELOCIDAD_CRITICA_AUDIO:
            return '<span style="color: red;">🛑</span> EXCESO VELOCIDAD CRÍTICO 🚨'
        elif velocidad >= SPEED_THRESHOLD_KPH:
            return '<span style="color: orange;">🛑</span> Alerta Velocidad ⚠️'
        else:
            if "Encendida" in estado_ignicion or estado_ignicion == "Encendida":
                return '<span style="color: green;">🛑</span> Encendida'
            elif "Apagada" in estado_ignicion or estado_ignicion == "Apagada":
                return '<span style="color: red;">🛑</span> Apagada'
            else:
                return f'<span style="color: black;">🛑</span> {estado_ignicion}'
    else:
        # Usar el estado_display proporcionado y agregar emojis con colores
        if "Parada Larga" in estado_display:
            return f'<span style="color: #FFD700;">🛑</span> {estado_display}'
        elif "EXCESO VELOCIDAD CRÍTICO" in estado_display:
            return '<span style="color: red;">🛑</span> EXCESO VELOCIDAD CRÍTICO 🚨'
        elif "Alerta Velocidad" in estado_display:
            return '<span style="color: orange;">🛑</span> Alerta Velocidad ⚠️'
        elif "Falla GPS" in estado_display:
            return '<span style="color: gray;">🛑</span> Falla GPS'
        elif "Encendida" in estado_display:
            return '<span style="color: green;">🛑</span> Encendida'
        elif "Apagada" in estado_display:
            return '<span style="color: red;">🛑</span> Apagada'
        else:
            return f'<span style="color: black;">🛑</span> {estado_display}'

# 🚨 FUNCIÓN OPTIMIZADA 🚨
def verificar_falla_gps(unidad_data: Dict[str, Any], hora_venezuela: datetime,
                        minutos_encendida: int, minutos_apagada: int) -> Dict[str, Any]:
    """
    Evalúa si la unidad debe cambiar a estado 'Falla GPS' y actualiza el diccionario de datos.
    """
    last_report_str = unidad_data.get('LastReportTime')
    ignicion_raw = unidad_data.get("ignition", "false").lower()
    estado_ignicion = ignicion_raw == "true"

    if not last_report_str:
        return unidad_data

    try:
        last_report_dt = datetime.strptime(last_report_str, TIME_FORMAT).replace(tzinfo=VENEZUELA_TZ)
    except ValueError:
        return unidad_data

    # 1. Calcular la diferencia de tiempo
    diferencia_tiempo: timedelta = hora_venezuela - last_report_dt
    minutos_sin_reportar = diferencia_tiempo.total_seconds() / 60.0

    # 2. Definir los umbrales de tiempo
    UMBRAL_ENCENDIDA = timedelta(minutes=minutos_encendida)
    UMBRAL_APAGADA = timedelta(minutes=minutos_apagada)

    es_falla_gps = False
    motivo_falla = ""

    if estado_ignicion:
        if diferencia_tiempo > UMBRAL_ENCENDIDA:
            es_falla_gps = True
            motivo_falla = f"Encendida **{minutos_sin_reportar:.0f} minutos** sin reportar (Umbral {minutos_encendida} min)."
    else:
        if diferencia_tiempo > UMBRAL_APAGADA:
            es_falla_gps = True
            # Display simplificado a minutos totales (o horas si es mucho)
            if minutos_sin_reportar >= 60:
                 tiempo_display = f"{(minutos_sin_reportar / 60.0):.1f} horas"
            else:
                 tiempo_display = f"{minutos_sin_reportar:.0f} minutos"

            umbral_display = f"{minutos_apagada // 60}h {minutos_apagada % 60}min" if minutos_apagada >= 60 else f"{minutos_apagada}min"

            motivo_falla = f"Apagada **{tiempo_display}** sin reportar (Umbral {umbral_display})."

# 3. Aplicar el estado y estilo si es Falla GPS
    if es_falla_gps:
        unidad_data['Estado_Falla_GPS'] = True
        unidad_data['FALLA_GPS_MOTIVO'] = motivo_falla
        unidad_data['LAST_REPORT_TIME_FOR_DETAIL'] = last_report_str
        unidad_data['IGNICION_OVERRIDE'] = "Falla GPS 🛠"
        # Usamos el f-string para el estilo
        unidad_data['CARD_STYLE_OVERRIDE'] = f"background-color: {COLOR_FALLA_GPS}; padding: 15px; border-radius: 5px; color: black; margin-bottom: 0px;"

    return unidad_data

# 🚨 CONFIGURACIÓN DE AUDIO Y BASE64 (EJECUCIÓN ÚNICA AL INICIO) 🚨

# @st.cache_resource para asegurar que se ejecuta una sola vez y no se recalcula en cada rerun.
@st.cache_resource(ttl=None)
def obtener_audio_base64(audio_path):
    """Codifica el archivo de audio en una cadena Base64 al inicio."""
    if not os.path.exists(audio_path):
        # En una app en producción, es mejor solo logear el error que detener la app
        print(f"Error Crítico: No se encontró el archivo de audio '{audio_path}'.")
        return None
    try:
        with open(audio_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    except Exception as e:
        print(f"Error al codificar el audio: {e}")
        return None

# 🚨 EJECUCIÓN DEL BASE64 UNA SOLA VEZ AL INICIO 🚨
# NOTA: ASUMO que los archivos 'parada.mp3', 'velocidad.mp3' y 'perimetro.mp3' existen en el mismo directorio.
# Si no existen, las alertas de audio no funcionarán.
AUDIO_BASE64_PARADA = obtener_audio_base64("parada.mp3")
AUDIO_BASE64_VELOCIDAD = obtener_audio_base64("velocidad.mp3")
AUDIO_BASE64_PERIMETRO = obtener_audio_base64("perimetro.mp3")  # 🆕 NUEVO AUDIO PARA PERÍMETROS
AUDIO_BASE64_ENCENDIDO = obtener_audio_base64("encendido.mp3")  # 🔊 NUEVO AUDIO PARA CAMBIO A ENCENDIDO

def reproducir_alerta_sonido(base64_str):
    """
    Inyecta el script HTML con el audio Base64 usando st.markdown.
    """
    if not base64_str:
        return

    unique_id = int(time.time() * 1000)
    audio_html = f"""
    <audio controls autoplay style="display:none" id="alerta_audio_tag_{unique_id}">
        <source src="data:audio/mp3;base64,{base64_str}" type="audio/mp3">
    </audio>
    <script>
        // Lógica de carga y reproducción para evitar bloqueo de autoplay en algunos navegadores
        const audio = document.getElementById('alerta_audio_tag_{unique_id}');
        if (audio) {{
            audio.volume = 1.0;
            audio.load();
            audio.play().catch(error => console.warn('Bloqueo de Autoplay: ', error));
        }}
        // OPCIONAL: Eliminar el elemento después de la reproducción para limpiar el DOM
        audio.onended = function() {{
            this.remove();
        }};
    </script>
    """
    st.markdown(audio_html, unsafe_allow_html=True)
# CONFIGURACIÓN DE PÁGINA
st.set_page_config(
    page_title="Monitoreo GPS - FOSPUCA",
    layout="wide",
    initial_sidebar_state="expanded"
)
# INYECCIÓN DE CSS (Optimizado para solo lo necesario)
st.markdown("""
    <style>
    /* 1. Regla para reducir el espacio superior de TODA la página */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 0rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }

    /* 2. Regla para apuntar al título "Monitoreo En Tiempo Real" */
    #main-title {
        margin-top: -30px;
    }

    /* 3. Estilos para las tarjetas de estado */

    .stButton > button {
        border-radius: 10px;
        font-weight: bold;
        font-color: bold;
        font-size: #1e88e5;
        border: 1px solid #e0e0e0;
        padding: 5px 20px;
        transition: all 0.3s ease;
    }

    .stButton > button[kind="secondary"]:hover {
        background-color: #434B4D;
        color: white;
        border-color: #434B4D;
    }

    .st.expander> :hover {
        background-color: #434B4D;
        color: white;
        border-color: #434B4D;
     }
     

    /* Modifica el tamaño de todos los st.header() (h1) */
    h1 {
        font-size: 2.5rem;
        font-weight: 700;
    }


    /* Estilo para los botones en la barra lateral para ocupar todo el ancho */
    .stButton>button {
    width: 100%; /* Hace que el botón ocupe todo el ancho de su contenedor */
    }

    /* Estilo para centrar el texto dentro de los botones */
    .stButton>button p {
    text-align: center; /* Centra el texto del párrafo (p) dentro del botón */
    }
    

    /* ✨ REGLA PARA ALINEAR EL TESTIGO A LA DERECHA ✨ */
    .update-align {
        display: flex; /* Habilita el manejo de alineación flexible */
        justify-content: flex-end; /* Empuja el contenido a la derecha */
        width: 100%; /* Ocupa todo el ancho disponible */
    }
    </style>
""", unsafe_allow_html=True)
# CONFIGURACIÓN DE LA API Y SEGURIDAD (st.secrets)
API_URL = "https://flexapi.foresightgps.com/ForesightFlexAPI.ashx"

try:
# 🔑 La clave se carga de forma SEGURA desde st.secrets
    BASIC_AUTH_HEADER = st.secrets["api"]["basic_auth_header"]
except KeyError:

# Este error detiene la aplicación si no hay clave, lo cual es correcto
    st.error("ERROR CRÍTICO: No se pudo encontrar la clave 'basic_auth_header' en st.secrets.")
    st.info("Asegúrese de configurar el archivo '.streamlit/secrets.toml' o la configuración de 'Secrets' en la nube.")
    st.stop()

# CONFIGURACIÓN DINÁMICA DE CARGA

# Nombre de la carpeta que contendrá los archivos JSON de las flotas

CONFIG_DIR = "configuracion_flotas"
DATA_DIR = "data"
PERIMETROS_DIR = "perimetros" # ¡NUEVA CARPETA PARA ARCHIVOS DE PERÍMETRO!

# CARGAR PERÍMETROS AL INICIO
PERIMETROS_CARGADOS = cargar_perimetros(PERIMETROS_DIR)

@st.cache_data(ttl=None) # La configuración solo se carga una vez
def cargar_configuracion_flotas(config_dir: str = CONFIG_DIR) -> Dict[str, Dict[str, Any]]:
    """Carga dinámicamente la configuración de las flotas desde archivos JSON."""
    flotas_config = {}

    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        print(f"Directorio de configuración '{config_dir}' creado. ¡Agrega tus archivos JSON!")
        return {}

    for filename in os.listdir(config_dir):
        if filename.endswith(".json"):
            nombre_flota_file = os.path.splitext(filename)[0]
            nombre_flota = nombre_flota_file.replace("_", " ")

            filepath = os.path.join(config_dir, filename)

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                    # 🚨 MODIFICACIÓN CLAVE: Ahora se valida la existencia de 'sede_coords'.
                    if all(key in data for key in ["ids", "sede_coords"]):

                        # Hacemos que 'resguardo_secundario_coords' sea opcional.
                        if "resguardo_secundario_coords" not in data:
                            data["resguardo_secundario_coords"] = []

                        # 🚨 ¡NUEVO! Hacemos que 'vertedero_coords' sea opcional.
                        if "vertedero_coords" not in data:
                            data["vertedero_coords"] = []
                            
                        data['ids_exep'] = [str(id).strip() for id in data.get('ids_exep', [])]
                        


                        flotas_config[nombre_flota] = data
                    else:
                        print(f" [ADVERTENCIA] Archivo '{filename}' omitido: faltan claves obligatorias (ids, sede_coords).")

            except json.JSONDecodeError:
                print(f" [ERROR] No se pudo parsear el archivo JSON: {filename}. Revisa su formato.")
            except Exception as e:
                print(f" [ERROR] Ocurrió un error al cargar {filename}: {e}")

    return flotas_config
@st.cache_data(ttl=10) # Cachea los datos de los conductores por 10 Seg
def cargar_datos_flota_conductor(nombre_flota: str, data_dir: str = DATA_DIR) -> Dict[str, Dict[str, str]]:
    """Carga los datos de conductor, ruta y teléfono para la flota seleccionada."""
    
    if not nombre_flota:
        return {}

# El nombre del archivo es el nombre de la flota (ej: FlotaNorte.json)
    filename = f"{nombre_flota}.json"
    filepath = os.path.join(data_dir, filename)

    if not os.path.exists(filepath):
        # Si el archivo no existe, retorna un diccionario vacío
        # Puedes descomentar la línea de abajo si necesitas debuggear
        # print(f" [ADVERTENCIA] Archivo de datos de conductor no encontrado: {filepath}") 
        return {}

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Retorna el diccionario de unidades (ej: {"7001": {...}})
            return data
    except json.JSONDecodeError:
        print(f" [ERROR] No se pudo parsear el archivo JSON de conductor: {filename}. Revisa su formato.")
        return {}
    except Exception as e:
        print(f" [ERROR] Ocurrió un error al cargar los datos del conductor {filename}: {e}")
        return {}


# CONFIGURACIÓN MULTI-FLOTA (Se carga dinámicamente)
FLOTAS_CONFIG = cargar_configuracion_flotas()

# VERIFICACIÓN DE CARGA DE CONFIGURACIÓN
if not FLOTAS_CONFIG:
    
    # Esta verificación asegura que si falla la carga al inicio, la app muestra un mensaje útil.
    
    st.set_page_config(page_title="Error de Configuración", layout="wide")
    st.error("❌ **ERROR CRÍTICO DE CARGA DE FLOTAS** ❌")
    st.markdown("---")
    st.warning("No se pudieron cargar flotas. Por favor, verifica lo siguiente:")
    st.markdown("""
        1.  Asegúrate de tener la carpeta **`configuracion_flotas`** al lado de `dashboard.py`.
        2.  Revisa que tus archivos JSON estén dentro de esa carpeta.
        3.  Verifica que **TODOS** los archivos JSON utilicen el formato con **`"ids"`** y **`"sede_coords"`** (ej: `"sede_coords": [[10.456, -66.123]]`).
    """)
    st.stop()

# -----------------------------------------------------------

# ENCABEZADO DE AUTENTICACION

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": BASIC_AUTH_HEADER
}

# CALCULO DE DISTANCIA (FUNCIÓN HAVERSINE)
def haversine(lat1, lon1, lat2, lon2):
    """Calcula la distancia Haversine entre dos puntos en la Tierra (en km). OPTIMIZADA con numpy."""
    R = 6371
    # Vectorización con numpy
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

# FUNCIÓN DE RESPALDO PARA VERIFICACIÓN SIN SHAPELY
def es_punto_dentro_perimetro(lat, lon, coordenadas_perimetro):
    """
    Verifica si un punto (lat, lon) está dentro del polígono definido por las coordenadas del perímetro.
    Usa el algoritmo de ray casting para punto en polígono (función de respaldo).
    
    Args:
        lat (float): Latitud del punto
        lon (float): Longitud del punto  
        coordenadas_perimetro (list): Lista de coordenadas [lon, lat] que forman el polígono
    
    Returns:
        bool: True si el punto está dentro del polígono, False en caso contrario
    """
    if not coordenadas_perimetro or len(coordenadas_perimetro) < 3:
        return False
    
    try:
        # Convertir coordenadas del perímetro (que están en formato [lon, lat]) a formato estándar
        polygon = [(coord[1], coord[0]) for coord in coordenadas_perimetro]  # [lat, lon]
        point = (lat, lon)
        
        # Algoritmo de ray casting para punto en polígono
        x, y = point
        n = len(polygon)
        inside = False
        
        p1x, p1y = polygon[0]
        for i in range(1, n + 1):
            p2x, p2y = polygon[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        
        return inside
    except Exception as e:
        print(f"❌ Error en cálculo de punto en polígono: {e}")
        return False

# FUNCIÓN AUXILIAR PARA ESTILOS (Sigue existiendo para la Leyenda)
def get_card_style(ignicion_status, speed):
    
    """Determina el estilo de la tarjeta basado en el estado de ignición y velocidad."""

    # 1. ESTADO POR DEFECTO: Encendida en Ruta (Verde)
    bg_color = "#4CAF50" 
    text_color = "white"

    if "Vertedero" in ignicion_status:
        bg_color = "#FCC6BB"
        text_color = "white"

    elif "Resguardo (Sede)" in ignicion_status:
        bg_color = "#337ab7"
        text_color = "white"

    elif "Encendida (Sede)" in ignicion_status:
        bg_color = "#B37305"
        text_color = "white"

    elif "Apagada" in ignicion_status:
        bg_color = "#D32F2F"
        text_color = "white"

    elif "Resguardo (Fuera de Sede)" in ignicion_status:
        bg_color = "#191452"
        text_color = "white"

    elif "Falla GPS" in ignicion_status:
        bg_color = "#AAAAAA"
        text_color = "white"

    elif "Fuera de Perímetro" in ignicion_status: 
        bg_color = "#7627F5"
        text_color = "white"

    style = (
        f"background-color: {bg_color}; "
        f"padding: 15px; "
        f"border-radius: 5px; "
        f"color: {text_color}; "
        f"margin-bottom: 0px;"
    )
    return style

# CALLBACK MODIFICADO PARA DESCARTE
def descartar_alerta_stop(unidad_id_a_descartar):
    """Marca la alerta de Parada Larga como 'descartada' y DESACTIVA la bandera de audio."""
    st.session_state['alertas_descartadas'][unidad_id_a_descartar] = True
    st.session_state['reproducir_audio_alerta'] = False
    st.cache_data.clear()
    st.session_state['scroll_to_top_flag'] = True

# DESCARTAR EXCESO DE VELOCIDAD
def descartar_alerta_velocidad(unidad_id_a_descartar):
    """Marca la alerta de Exceso de Velocidad como 'descartada' y DESACTIVA la bandera de audio."""
    st.session_state['alertas_velocidad_descartadas'][unidad_id_a_descartar] = True
    st.session_state['reproducir_audio_velocidad'] = False
    st.cache_data.clear()
    st.session_state['scroll_to_top_flag'] = True

# DATOS DE RESPALDO (FALLBACK)
def get_fallback_data(error_type="Conexión Fallida"):
    """Genera una estructura de datos de una sola fila para señalizar el error en el main loop."""

    # Aseguramos que la estructura del DataFrame sea completa para evitar errores en el bucle
    return pd.DataFrame([{
        "UNIDAD": "FALLBACK",
        "UNIT_ID": "FALLBACK_ID",
        "IGNICION": "N/A",
        "VELOCIDAD": 0.0,
        "LATITUD": 0.0,
        "LONGITUD": 0.0,
        "UBICACION_TEXTO": f"FALLBACK - {error_type}",
        "CARD_STYLE": "background-color: #D32F2F; padding: 15px; border-radius: 5px; color: white; margin-bottom: 0px;",
        "FALLA_GPS_MOTIVO": None,
        "LAST_REPORT_TIME_DISPLAY": None,
        "STOP_DURATION_MINUTES": 0.0, # Añadido para consistencia
        "STOP_DURATION_TIMEDELTA": timedelta(seconds=0), # Añadido para consistencia
        "EN_SEDE_FLAG": False, # Añadido para consistencia
        "EN_RESGUARDO_SECUNDARIO_FLAG": False, # Añadido para consistencia
        "EN_VERTEDERO_FLAG": False, # NUEVO FLAG
        "EN_FUERA_PERIMETRO_FLAG": False, # NUEVO FLAG
        "ES_FALLA_GPS_FLAG": False # Añadido para consistencia
    }])

# FUNCIÓN DE OBTENCIÓN Y FILTRADO DE DATOS DINÁMICA (TTL de 5 segundos)
# Se usan los argumentos gps_min_encendida y gps_min_apagada para que la función sepa cuándo refrescar el caché.
@st.cache_data(ttl=5)
def obtener_datos_unidades(nombre_flota: str, config: Dict[str, Any], gps_min_encendida: int, gps_min_apagada: int):
    """Obtiene y limpia los datos de la API, aplicando la lógica de color por estado/sede, incluyendo Falla GPS."""

    flota_data = config.get(nombre_flota)
    if not flota_data:
        # Esto no debería pasar si la lógica de selección en el sidebar es correcta
        return get_fallback_data("Configuración de Flota No Encontrada")

    # 🚨 OBTENCIÓN DE COORDENADAS DE UBICACIONES DINÁMICAS DESDE EL JSON 🚨
    # Todas son listas de listas de [lat, lon]
    SEDE_COORDS = flota_data.get("sede_coords", [])
    COORDENADAS_RESGUARDO_SECUNDARIO = flota_data.get("resguardo_secundario_coords", [])
    COORDENADAS_VERTEDERO = flota_data.get("vertedero_coords", [])
    IDS_EXEP: List[str] = flota_data.get("ids_exep", [])
    

    
    # 🚨 CARGA DE PERÍMETROS DESDE ARCHIVOS JSON (OPCIONAL) 🚨
    # Verificar si existe un archivo .json en la carpeta perimetros con el nombre de la flota
    coordenadas_perimetro = []
    tiene_perimetro = False
    archivo_perimetro_path = os.path.join(PERIMETROS_DIR, f"{nombre_flota}.json")
    
    # Intentar cargar perímetros desde diferentes fuentes
    if nombre_flota in PERIMETROS_CARGADOS:
        coordenadas_perimetro = PERIMETROS_CARGADOS[nombre_flota]["coords_lon_lat"]
        tiene_perimetro = True
        print(f"✅ Perímetro cargado para {nombre_flota}: {len(coordenadas_perimetro)} puntos")
    elif os.path.exists(archivo_perimetro_path):
        # Fallback: intentar cargar directamente
        try:
            with open(archivo_perimetro_path, 'r', encoding='utf-8') as f:
                perimetro_data = json.load(f)
                # Extraer coordenadas del GeoJSON
                if 'features' in perimetro_data and len(perimetro_data['features']) > 0:
                    geometry = perimetro_data['features'][0].get('geometry', {})
                    if geometry.get('type') == 'LineString':
                        coordenadas_perimetro = geometry.get('coordinates', [])
                        tiene_perimetro = True
                        print(f"✅ Perímetro cargado para {nombre_flota}: {len(coordenadas_perimetro)} puntos")
                    else:
                        print(f"ℹ️ Geometría del perímetro no válida en {nombre_flota}, continuando sin perímetro")
        except Exception as e:
            print(f"⚠️ Error al cargar perímetro de {nombre_flota}: {e}, continuando sin verificación de perímetro")
    else:
        print(f"ℹ️ No se encontró archivo de perímetro para {nombre_flota}, verificacion de perímetro DESHABILITADA")
    
    # Información del estado del perímetro
    if tiene_perimetro:
        print(f"🔍 Verificación de perímetro HABILITADA para {nombre_flota}")
    else:
        print(f"🔍 Verificación de perímetro DESHABILITADA para {nombre_flota} (funcionamiento normal)")

    if not SEDE_COORDS:
        return get_fallback_data("Error de Configuración: 'sede_coords' vacía.")

    # Aseguramos un tamaño de página suficiente para todos los IDs
    payload = {
        "userid": "86946",
        "requesttype": 0,
        "isdeleted": 0,
        "pageindex": 1,
        "orderby": "name",
        "orderdirection": "ASC",
        "conncode": "SATEQSA",
        "elements": 1,
        "ids": flota_data["ids"],
        "method": "usersearchplatform",
        "pagesize": len(flota_data["ids"].split(',')) + 5,
        "prefix": True
    }

    try:
        response = requests.post(API_URL, json=payload, headers=HEADERS, timeout=5)
        response.raise_for_status()
        data = response.json()

        lista_unidades = data.get("ForesightFlexAPI", {}).get("DATA", [])

        if not lista_unidades:
            return get_fallback_data("Lista de Unidades Vacía (Revisa IDs)")

        hora_actual_ve = obtener_hora_venezuela()

        # PROCESAMIENTO DE DATOS REALES
        datos_filtrados = []
        for unidad in lista_unidades:

            # 1. APLICAR LÓGICA DE FALLA GPS CON PARÁMETROS DINÁMICOS
            unidad_con_falla_check = verificar_falla_gps(unidad, hora_actual_ve, gps_min_encendida, gps_min_apagada)

            es_falla_gps = unidad_con_falla_check.get('Estado_Falla_GPS', False)

            # Extracción y limpieza de datos
            ignicion_raw = unidad_con_falla_check.get("ignition", "false").lower()
            # Uso de float() con valor por defecto seguro
            velocidad = float(unidad_con_falla_check.get("speed_dunit", 0.0))
            lat = float(unidad_con_falla_check.get("ylat", 0.0))
            lon = float(unidad_con_falla_check.get("xlong", 0.0))
            sentido = float(unidad_con_falla_check.get("heading", 0.0))
            # unit_id debe ser único, usamos unitid o name como fallback
            unit_id = unidad_con_falla_check.get("unitid", unidad_con_falla_check.get("name", "N/A_ID_FALLBACK"))

            ignicion_estado = ignicion_raw == "true"

            falla_gps_motivo = None
            last_report_time_display = unidad_con_falla_check.get('LastReportTime', 'N/A')

            if es_falla_gps:
                # Caso Falla GPS: Sobrescribe el estado y estilo
                estado_final_display = unidad_con_falla_check['IGNICION_OVERRIDE']
                card_style = unidad_con_falla_check['CARD_STYLE_OVERRIDE']
                falla_gps_motivo = unidad_con_falla_check.get('FALLA_GPS_MOTIVO')
                last_report_time_display = unidad_con_falla_check.get('LAST_REPORT_TIME_FOR_DETAIL', last_report_time_display)

                # Para fines de métricas, marcamos el tipo de resguardo como NINGUNO
                en_sede = False
                en_resguardo_secundario = False
                en_vertedero = False # ¡NUEVO FLAG!

            else:
                # CÁLCULO DE DISTANCIA A UBICACIONES DINÁMICAS

                # 1. ¿Está en el VERTEDERO? (Máxima Prioridad Operacional)
                en_vertedero = False
                for vertedero_coords in COORDENADAS_VERTEDERO:
                    v_lat, v_lon = vertedero_coords
                    distancia = haversine(lat, lon, v_lat, v_lon)
                    if distancia <= PROXIMIDAD_KM_V:
                        en_vertedero = True
                        break

                # 2. ¿Está en la SEDE? (Revisar solo si NO está en Vertedero)
                en_sede = False
                if not en_vertedero:
                    for sede_coords in SEDE_COORDS:
                        lat_sede, lon_sede = sede_coords
                        distancia = haversine(lat, lon, lat_sede, lon_sede)
                        if distancia <= PROXIMIDAD_KM_S:
                            en_sede = True
                            break

                # 3. ¿Está en Resguardo Secundario? (Revisar solo si NO está en Vertedero ni Sede)
                en_resguardo_secundario = False
                if not en_vertedero and not en_sede and COORDENADAS_RESGUARDO_SECUNDARIO:
                    for resguardo_coords in COORDENADAS_RESGUARDO_SECUNDARIO:
                        lat_res, lon_res = resguardo_coords
                        distancia_resguardo = haversine(lat, lon, lat_res, lon_res)
                        if distancia_resguardo <= PROXIMIDAD_KM_R:
                            en_resguardo_secundario = True
                            break

                # LÓGICA DE ESTADO FINAL

                estado_final_display = "Apagada ❄️"
                color_fondo = "#D32F2F"
                color_texto = "white"
                en_fuera_perimetro = False  # Inicializar flag de fuera de perímetro

                if en_vertedero:
                    estado_final_display = "Vertedero 🚛";
                    color_fondo = "#FCC6BB"
                    color_texto = "white"

                elif ignicion_estado:
                    if en_sede:
                        estado_final_display = "Encendida (Sede) 🔥";
                        color_fondo = "#B37305"
                        color_texto = "white"
                        
                    else:
                        estado_final_display = "Encendida 🔥";
                        color_fondo = "#4CAF50"
                        color_texto = "white"

                else: # Apagada
                    if en_sede:
                        estado_final_display = "Resguardo (Sede) 🛡️";
                        color_fondo = "#337ab7"
                        color_texto = "white"
                        
                    elif en_resguardo_secundario:
                        estado_final_display = "Resguardo (Fuera de Sede) 🛡️";
                        color_fondo = "#191452"
                    
                # 4. ¿Está FUERA DE PERÍMETRO? 
                # Solo verificar si:
                # - NO está en la lista de excepciones (ids_exep)
                # - NO está en estado de resguardo (en sede o fuera de sede)  
                # - NO está en otras ubicaciones críticas como vertedero/sede/resguardo secundario
                # - Hay perímetro disponible

                en_fuera_perimetro = False
                perimetros_encontrados = False
                
                unit_id = unidad_con_falla_check.get("unitid", unidad_con_falla_check.get("name", "N/A_ID_FALLBACK"))
                
                # EXCEPCIÓN PARA UNIDADES EN ids_exep
                unit_id_str = str(unit_id).strip()
                
                # Verificar si la unidad está en la lista de excepciones
                # Esto funciona para IDs numéricos, alfanuméricos, con espacios, letras, etc.
                is_exception = False
                for id_exep in IDS_EXEP:
                    id_exep_str = str(id_exep).strip()
                    # Comparación directa de strings (maneja cualquier tipo de ID)
                    if unit_id_str == id_exep_str:
                        is_exception = True
                        break
                    
                    # Comparación adicional para IDs numéricos (por compatibilidad)
                    if unit_id_str.isdigit() and id_exep_str.isdigit():
                        if int(unit_id_str) == int(id_exep_str):
                            is_exception = True
                            break
                
                # VERIFICACIÓN DE FUERA DE PERÍMETRO
                # Solo verificar si:
                # 1. Hay perímetro disponible 
                # 2. La unidad NO está en la lista de excepciones
                # 3. La unidad NO está en estado de resguardo (en sede o fuera de sede)
                en_fuera_perimetro = False
                
                if tiene_perimetro and not is_exception and not (en_sede or en_resguardo_secundario):
                    # Verificar si está dentro del perímetro principal
                    if coordenadas_perimetro and len(coordenadas_perimetro) >= 3:
                        if es_punto_dentro_perimetro(lat, lon, coordenadas_perimetro):
                            en_fuera_perimetro = False
                        else:
                            en_fuera_perimetro = True
                    
                    # También verificar perímetros secundarios (si la unidad está en sede/vertedero/resguardo)
                    if (en_sede or en_vertedero or en_resguardo_secundario) and nombre_flota in PERIMETROS_CARGADOS:
                        perimetros_encontrados = verificar_coordenada_en_perimetro(lat, lon, {nombre_flota: PERIMETROS_CARGADOS[nombre_flota]})
                        if perimetros_encontrados:
                            en_fuera_perimetro = False
                
                # SI NO hay perímetro o es excepción, nunca se marca como fuera de perímetro
                if not tiene_perimetro or is_exception:
                    en_fuera_perimetro = False
                                
                if en_fuera_perimetro:
                    # Solo se marca como fuera de perímetro si:
                    # 1) Hay perímetro disponible 
                    # 2) La unidad no está en la lista de excepciones
                    # 3) La verificación confirma que está fuera del perímetro
                    print(f"⚠️ FUERA DE PERÍMETRO - Unidad: {unidad_con_falla_check.get('name', 'N/A')} | unit_id: {unit_id} | confirmado fuera del perímetro")
                    estado_final_display = "Fuera de Perímetro 🌐"
                    color_fondo = "#7627F5"
                    color_texto = "white"
                #else:
                    #print(f"✅ DENTRO DE PERÍMETRO - Unidad: {unidad_con_falla_check.get('name', 'N/A')} | unit_id: {unit_id} | dentro del perímetro o sin verificación")

                # 🔊 DETECCIÓN DE CAMBIO DE ESTADO A ENCENDIDO 🔊
                # Solo procesar si no es Falla GPS y tiene unit_id
                if not es_falla_gps and unit_id:
                    estado_anterior = st.session_state['unidades_estado_anterior'].get(unit_id, "")
                    
                    # Verificar si hay cambio de resguardo externo a encendido
                    if detectar_cambio_a_encendido(unit_id, estado_final_display, estado_anterior):
                        st.session_state['reproducir_audio_encendido'] = True
                        print(f"🔊 Cambio detectado: Unidad {unit_id} cambió de resguardo externo a encendido")
                    
                    # Actualizar el estado anterior para la próxima verificación
                    st.session_state['unidades_estado_anterior'][unit_id] = estado_final_display

                card_style = f"background-color: {color_fondo}; padding: 15px; border-radius: 5px; color: {color_texto}; margin-bottom: 0px;"

            datos_filtrados.append({
                "UNIDAD": unidad_con_falla_check.get("name", "N/A"),
                "UNIT_ID": unit_id,
                "IGNICION": estado_final_display,
                "VELOCIDAD": velocidad,
                "LATITUD": lat,
                "LONGITUD": lon,
                "SENTIDO": sentido,
                "UBICACION_TEXTO": unidad_con_falla_check.get("location", "Dirección no disponible"),
                "CARD_STYLE": card_style,
                "FALLA_GPS_MOTIVO": falla_gps_motivo,
                "LAST_REPORT_TIME_DISPLAY": last_report_time_display,
                "STOP_DURATION_MINUTES": 0.0, # Inicializado para el DataFrame
                "STOP_DURATION_TIMEDELTA": timedelta(seconds=0), # Inicializado para el DataFrame
                # NUEVAS COLUMNAS PARA MÉTRICAS (incluido Vertedero)
                "EN_SEDE_FLAG": en_sede,
                "EN_RESGUARDO_SECUNDARIO_FLAG": en_resguardo_secundario,
                "EN_VERTEDERO_FLAG": en_vertedero, # ¡NUEVO FLAG!
                "EN_FUERA_PERIMETRO_FLAG": en_fuera_perimetro if 'en_fuera_perimetro' in locals() else False, # ¡NUEVO FLAG!
                "ES_FALLA_GPS_FLAG": es_falla_gps
            })

        # El DataFrame se devuelve con las columnas inicializadas
        return pd.DataFrame(datos_filtrados)

    except requests.exceptions.RequestException as e:
        #error_msg = f"API Error: {e}" if not hasattr(e, 'response') else f"HTTP Error: {e.response.status_code}"
        error_msg = f"API Error: {e}" if not (hasattr(e, 'response') and e.response is not None) else f"HTTP Error: {e.response.status_code}"
        print(f"❌ Error de Conexión/API: {error_msg}")
        return get_fallback_data("Error de Conexión/API")

# FUNCIÓN PARA MOSTRAR LA LEYENDA DE COLORES EN EL SIDEBAR
def display_color_legend():
    """Muestra la leyenda de colores de las tarjetas de estado de forma compacta."""

    # 🚨 LEYENDA ACTUALIZADA CON EL NUEVO COLOR 🚨
    COLOR_MAP = {
        "#4CAF50": "Encendida en Ruta",
        "#D32F2F": "Apagada",
        "#337ab7": "Resguardo (Sede)",
        "#191452": "Resguardo (F. Sede)",
        "#FCC6BB": "En Vertedero", # ¡NUEVO COLOR!
        "#7627F5": "Fuera de Perímetro", # ¡NUEVO COLOR!
        "#B37305": "Encendida (Sede)",
        "#FFC107": "Parada Larga",
        "#AAAAAA": "Falla GPS",
    }

    cols_legend = st.columns(2)
    col_index = 0

    for color, description in COLOR_MAP.items():
        # Determina si el texto debe ser negro para fondos claros
        text_color = "white"

        with cols_legend[col_index % 2]:
            legend_html = f"""
            <div style="display: flex; align-items: center; margin-bottom: 3px;">
                <div style="width: 14px; height: 14px; background-color: {color}; border-radius: 3px; margin-right: 5px; border: 1px solid #ddd;"></div>
                <span style="font-size: 0.85em; color: {text_color};">{description}</span>
            </div>
            """
            st.markdown(legend_html, unsafe_allow_html=True)
        col_index += 1
# -------------------------------------------------------------------------------------

# CALLBACKS DE AUTENTICACIÓN Y GUARDADO

def check_password(password_key="config_password_input"):
    """Callback para verificar la contraseña e iniciar la sesión de configuración."""
    if st.session_state.get(password_key) == CONFIG_PASSWORD:
        st.session_state['authenticated'] = True
        st.session_state[password_key] = "" # Limpiar el campo
    else:
        st.session_state['authenticated'] = False

def save_dynamic_config():
    """Guarda los valores actuales de los inputs del sidebar en el estado de sesión persistente."""
    # Los valores de los inputs se almacenan en st.session_state con sus keys temporales
    st.session_state['config_params']['TIME_SLEEP'] = st.session_state['input_time_sleep_temp']
    st.session_state['config_params']['STOP_THRESHOLD_MINUTES'] = st.session_state['input_stop_threshold_temp']
    st.session_state['config_params']['SPEED_THRESHOLD_KPH'] = st.session_state['input_speed_threshold_temp']
    st.session_state['config_params']['GPS_MIN_ENCENDIDA'] = st.session_state['input_gps_min_on_temp']
    st.session_state['config_params']['GPS_MIN_APAGADA'] = st.session_state['input_gps_min_off_temp']
    # Limpiar la caché para que la próxima llamada a la API use los nuevos parámetros
    st.cache_data.clear()

    st.toast("✅ Configuración guardada y aplicada!", icon='💾')

# CALLBACK PARA SELECCIONAR LA UNIDAD A UBICAR (NUEVO)
def set_unit_to_locate(unit_id):
    """
    Callback para establecer la UNIT_ID en el estado de sesión.
    Si se hace clic en la misma unidad, la deselecciona (toggle).
    """
    current_selected = st.session_state.get('unit_to_locate_id')
    if current_selected == unit_id:
        st.session_state['unit_to_locate_id'] = None
        st.cache_data.clear()
    else:
        st.session_state['unit_to_locate_id'] = unit_id
    # Es crucial limpiar la caché para forzar el re-renderizado
        st.cache_data.clear()

# CALLBACK PARA DESELECCIONAR LA UNIDAD A UBICAR (NUEVO)
def clear_unit_to_locate():
    """
    Callback para deseleccionar la unidad, limpiar el caché 
    y establecer un indicador para forzar el scroll al inicio.
    """
    # 1. Lógica de limpieza de estado y caché
    st.session_state['unit_to_locate_id'] = None
    st.cache_data.clear()
        
    # 2. ✅ NUEVA ACCIÓN: Establecer el indicador de scroll
    st.session_state['scroll_to_top_flag'] = True

# 🚨 NUEVAS FUNCIONES PARA CONTROL AVANZADO DE AUDIO DE PERÍMETRO 🚨
def limpiar_alarmas_perimetro_expiradas():
    """
    Limpia las alarmas de perímetro que han expirado (más de 15 minutos desde la aceptación).
    """
    if not st.session_state.get('perimetro_unidades_aceptadas'):
        return
    
    hora_actual = obtener_hora_venezuela()
    unidades_para_limpiar = []
    
    for unidad_id, tiempo_aceptacion in st.session_state['perimetro_unidades_aceptadas'].items():
        # Verificar si han pasado más de 15 minutos (TIEMPO_SILENCIO_PERIMETRO)
        if isinstance(tiempo_aceptacion, datetime):
            diferencia = hora_actual - tiempo_aceptacion
            if diferencia.total_seconds() >= (TIEMPO_SILENCIO_PERIMETRO * 60):  # Convertir minutos a segundos
                unidades_para_limpiar.append(unidad_id)
                # También remover de alertas descartadas para permitir nueva alarma
                if unidad_id in st.session_state['alertas_perimetro_descartadas']:
                    del st.session_state['alertas_perimetro_descartadas'][unidad_id]
    
    # Limpiar las expiradas
    for unidad_id in unidades_para_limpiar:
        del st.session_state['perimetro_unidades_aceptadas'][unidad_id]
        print(f"🧹 Alarma de perímetro expirada para unidad {unidad_id}")

def aceptar_alarma_perimetro(unidad_id):
    """
    Marca una alarma de perímetro como aceptada y registra el tiempo para el silencio de 15 minutos.
    """
    st.session_state['alertas_perimetro_descartadas'][unidad_id] = True
    st.session_state['perimetro_unidades_aceptadas'][unidad_id] = obtener_hora_venezuela()
    st.cache_data.clear()
    print(f"✅ Alarma de perímetro aceptada para unidad {unidad_id} - silencio por 15 minutos")
    
def aceptar_todas_alarmas_perimetro(unidades_ids):
    """
    Marca todas las alarmas de perímetro como aceptadas.
    """
    hora_actual = obtener_hora_venezuela()
    for unidad_id in unidades_ids:
        st.session_state['alertas_perimetro_descartadas'][unidad_id] = True
        st.session_state['perimetro_unidades_aceptadas'][unidad_id] = hora_actual
    st.session_state['reproducir_audio_perimetro'] = False
    st.cache_data.clear()
    print(f"✅ {len(unidades_ids)} alarmas de perímetro aceptadas - silencio por 15 minutos")

# 🔊 FUNCIÓN PARA DETECTAR CAMBIO DE ESTADO A ENCENDIDO 🔊
def detectar_cambio_a_encendido(unit_id, estado_actual, estado_anterior):
    """
    Detecta si una unidad cambió de resguardo externo a encendido.
    Retorna True si debe reproducirse el audio de encendido.
    """
    # Verificar si el estado actual es "Encendida" o "Encendida 🔥"
    estado_actual_encendido = ("Encendida" in estado_actual and "Vertedero" not in estado_actual and "Sede" not in estado_actual)
    
    # Verificar si el estado anterior era resguardo externo
    estado_anterior_resguardo = "Resguardo (Fuera de Sede)" in estado_anterior
    
    # Si hay un cambio de resguardo externo a encendido, retornar True
    return estado_actual_encendido and estado_anterior_resguardo and estado_anterior != "" 
    

# INICIALIZACIÓN DEL ESTADO DE SESIÓN

if 'flota_seleccionada' not in st.session_state:
    st.session_state['flota_seleccionada'] = None
if 'ultima_flota_procesada' not in st.session_state:  # 🆕 NUEVO ESTADO PARA DETECTAR CAMBIOS DE FLOTA
    st.session_state['ultima_flota_procesada'] = None
if 'perimetro_check_counter' not in st.session_state:  # 🆕 NUEVO ESTADO PARA CONTROL DE VERIFICACIÓN CADA 10 SEGUNDOS
    st.session_state['perimetro_check_counter'] = 0
if 'filtro_en_ruta' not in st.session_state:
    st.session_state['filtro_en_ruta'] = False
if 'filtro_estado_especifico' not in st.session_state:
    st.session_state['filtro_estado_especifico'] = "Mostrar Todos"
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'config_params' not in st.session_state:
    # Parámetros por defecto activos
    st.session_state['config_params'] = {
        'STOP_THRESHOLD_MINUTES': 10,
        'SPEED_THRESHOLD_KPH': 70,
        'GPS_MIN_ENCENDIDA': 5,
        'GPS_MIN_APAGADA': 70,
        'TIME_SLEEP': 3
    }
# 🚨 NUEVO ESTADO DE SESIÓN PARA LA UNIDAD A UBICAR 🚨
if 'unit_to_locate_id' not in st.session_state:
    st.session_state['unit_to_locate_id'] = None
# ------------------------------------------------------------------------------------

# 🚨 FUNCIONES COMPARTIDAS: Almacena el estado globalmente (Shared State) 🚨
@st.cache_resource(ttl=None)
def get_global_stop_state() -> Dict[str, Any]:
    """Retorna un diccionario de estado que es único y compartido por todos los usuarios (Global State)."""
    # Usaremos una simple clave para almacenar el estado de cada unidad
    return {}

@st.cache_resource(ttl=None)
def get_global_coordinate_state() -> Dict[str, Any]:
    """Retorna un diccionario de estado para tracking de coordenadas por unidad."""
    return {}

@st.cache_resource(ttl=None)
def get_global_velocity_state() -> Dict[str, Any]:
    """Retorna un diccionario de estado para tracking de velocidad por unidad."""
    return {}

# Inicializar y obtener la referencia al estado global (se ejecuta una sola vez)
current_stop_state = get_global_stop_state()
current_coordinate_state = get_global_coordinate_state()
current_velocity_state = get_global_velocity_state()
# ------------------------------------------------------------------------------------

# El resto de variables deben seguir usando st.session_state ya que son locales a cada usuario.
if 'alertas_descartadas' not in st.session_state:
    st.session_state['alertas_descartadas'] = {}
if 'alertas_velocidad_descartadas' not in st.session_state:
    st.session_state['alertas_velocidad_descartadas'] = {}
if 'reproducir_audio_alerta' not in st.session_state:
    st.session_state['reproducir_audio_alerta'] = False
if 'reproducir_audio_velocidad' not in st.session_state:
    st.session_state['reproducir_audio_velocidad'] = False
if 'reproducir_audio_perimetro' not in st.session_state:  # 🆕 NUEVO ESTADO PARA AUDIO DE PERÍMETRO
    st.session_state['reproducir_audio_perimetro'] = False
if 'alertas_perimetro_descartadas' not in st.session_state:  # 🆕 NUEVO ESTADO PARA ALERTAS DE PERÍMETRO DESCARTADAS
    st.session_state['alertas_perimetro_descartadas'] = {}
# 🚨 NUEVOS ESTADOS PARA CONTROL AVANZADO DE AUDIO DE PERÍMETRO 🚨
if 'perimetro_audio_last_play' not in st.session_state:  # Último tiempo de reproducción de audio
    st.session_state['perimetro_audio_last_play'] = None
if 'perimetro_unidades_aceptadas' not in st.session_state:  # Unidades con alarma aceptada y tiempo de silencio
    st.session_state['perimetro_unidades_aceptadas'] = {}
# 🔊 NUEVOS ESTADOS PARA DETECCIÓN DE CAMBIO A ENCENDIDO 🔊
if 'unidades_estado_anterior' not in st.session_state:  # Estado anterior de cada unidad para detectar cambios
    st.session_state['unidades_estado_anterior'] = {}
if 'reproducir_audio_encendido' not in st.session_state:  # Bandera para reproducir audio de encendido
    st.session_state['reproducir_audio_encendido'] = False
    st.session_state['perimetro_unidades_aceptadas'] = {}  # {unidad_id: tiempo_aceptacion}
if 'perimetro_ultimas_unidades_fuera' not in st.session_state:  # Tracking de unidades fuera de perímetro en ciclo anterior
    st.session_state['perimetro_ultimas_unidades_fuera'] = set()

# CONFIGURACION DEL SIDEBAR

def actualizar_dashboard():
    """Función de callback para re-ejecutar el script al cambiar el filtro o flota."""
    # 🆕 LIMPIAR CACHE CUANDO SE CAMBIE LA FLOTA
    flota_actual = st.session_state.get('flota_selector')
    if flota_actual and flota_actual != "-- Seleccione una Flota --":
        if st.session_state.get('ultima_flota_procesada') != flota_actual:
            # Detectar cambio de flota y limpiar cache
            st.session_state['ultima_flota_procesada'] = flota_actual
            st.session_state['perimetro_check_counter'] = 0  # 🆕 RESETEAR CONTADOR DE VERIFICACIÓN
            st.cache_data.clear()
            print(f"🔄 Cambio de flota detectado: limpieza de cache para '{flota_actual}'")
    st.cache_data.clear()
    pass

with st.sidebar:
    
     if st.button("🏡 Home", use_container_width=True):
        st.cache_data.clear()
        # 🚨 CORRECCIÓN CLAVE: Esto fuerza el cambio de página después de limpiar la caché.
        st.switch_page("home.py") 
   
     st.markdown(
        '<p style="text-align: center; margin-bottom: 5px; margin-top: 5px;">'
        '<img src="https://fospuca.com/wp-content/uploads/2018/08/fospuca.png" alt="Logo Fospuca" style="width: 100%; max-width: 120px; height: auto; display: block; margin-left: auto; margin-right: auto;">'
        '</p>',
        unsafe_allow_html=True
     )

     # 1. SELECCIÓN DE FLOTA
     st.markdown(
        '<p style="font-size: 30px; font-weight: bold; color: white; margin-bottom: 0px;margin-top: 20px;text-align: center;">Selección de Flota</p>',
        unsafe_allow_html=True
     )

     flota_keys = ["-- Seleccione una Flota --"] + list(FLOTAS_CONFIG.keys())

     current_flota = st.session_state.get('flota_seleccionada')
     try:
        current_index = flota_keys.index(current_flota) if current_flota else 0
     except ValueError:
        current_index = 0

     flota_actual = st.selectbox(
        "Seleccione la Flota a Monitorear:",
        options=flota_keys,
        index=current_index,
        key="flota_selector",
        on_change=actualizar_dashboard,
        label_visibility="collapsed"
    )

     if flota_actual == flota_keys[0]:
        st.session_state['flota_seleccionada'] = None
     else:
        st.session_state['flota_seleccionada'] = flota_actual

    # NUEVA SECCIÓN: CONFIGURACIÓN DINÁMICA DE PARÁMETROS
     with st.expander("⚙️ **Configuración Dinámica**", expanded=st.session_state['authenticated']):

        if st.session_state['authenticated']:
            # Inicializar los valores temporales con los activos al abrir el expander
            for key, default_key in [
                ('input_time_sleep_temp', 'TIME_SLEEP'),
                ('input_stop_threshold_temp', 'STOP_THRESHOLD_MINUTES'),
                ('input_speed_threshold_temp', 'SPEED_THRESHOLD_KPH'),
                ('input_gps_min_on_temp', 'GPS_MIN_ENCENDIDA'),
                ('input_gps_min_off_temp', 'GPS_MIN_APAGADA')
            ]:
                if key not in st.session_state:
                    st.session_state[key] = st.session_state['config_params'][default_key]

            st.markdown("##### Frecuencia y Tiempos de App")

            st.slider(
                "Pausa del Ciclo (Segundos)", min_value=1, max_value=10,
                value=st.session_state['config_params']['TIME_SLEEP'],
                step=1,
                key="input_time_sleep_temp",
                help="Tiempo de espera entre actualizaciones completas del Dashboard (tiempo.sleep)."
            )

            st.caption(f"TTL de Datos (API): **5 segundos** (fijo en el código, pero se limpia con cada cambio de parámetro).")

            st.markdown("##### Umbrales de Alerta")

            st.number_input(
                "Parada Larga (minutos)", min_value=1, max_value=120,
                value=st.session_state['config_params']['STOP_THRESHOLD_MINUTES'],
                step=1,
                key="input_stop_threshold_temp",
                help="Tiempo inactivo fuera de sede para activar la alerta de parada larga."
            )

            st.number_input(
                "Umbral de Alerta Velocidad (Km/h)", min_value=10, max_value=120,
                value=st.session_state['config_params']['SPEED_THRESHOLD_KPH'],
                step=5,
                key="input_speed_threshold_temp",
                help="Velocidad mínima para activar la alerta visual (color naranja). La alarma sonora se activa a los 75 km/h."
            )

            st.markdown("##### Falla GPS (Minutos sin Reportar)")

            st.number_input(
                "Falla GPS (Motor Encendido)", min_value=1, max_value=60,
                value=st.session_state['config_params']['GPS_MIN_ENCENDIDA'],
                step=1,
                key="input_gps_min_on_temp",
                help="Umbral de minutos sin reporte con ignición en True."
            )

            st.number_input(
                "Falla GPS (Motor Apagado)", min_value=30, max_value=180,
                value=st.session_state['config_params']['GPS_MIN_APAGADA'],
                step=5,
                key="input_gps_min_off_temp",
                help="Umbral de minutos sin reporte con ignición en False (70 min = 1h 10min)."
            )

            st.markdown("---")

            st.button(
                "💾 Guardar Cambios y Aplicar",
                on_click=save_dynamic_config,
                type="secondary",
                use_container_width=True,
                key="btn_save_config"
            )

        else:
            # USUARIO NO AUTENTICADO: SOLICITAR CONTRASEÑA
            st.markdown("🔒 Introduce la contraseña para acceder a la configuración dinámica.")

            st.text_input(
                "Contraseña",
                type="password",
                key="config_password_input",
                on_change=check_password,
                label_visibility="collapsed"
            )

            if 'config_password_input' in st.session_state and st.session_state['config_password_input'] and st.session_state['config_password_input'] != CONFIG_PASSWORD:
                st.error("Contraseña incorrecta.")

            st.caption("Contraseña de acceso: `admin`")

    # 3. LEYENDA DE COLORES
     with st.expander("🎨 Leyenda de Estados ", expanded=False):
        display_color_legend()

    # INICIO DEL EXPANDER DE FILTROS
     if st.session_state['flota_seleccionada']:

        with st.expander("🔎 Filtros de Estado", expanded=False):

            st.checkbox(
                "**Unidades en Ruta** (Excluir Resguardo y Fallas)",
                key="filtro_en_ruta",
                on_change=actualizar_dashboard
            )

            # 🚨 FILTRO ACTUALIZADO CON VERTEDERO Y FUERA DE PERÍMETRO 🚨
            filtro_estado_options = [
                "Mostrar Todos",
                "Vertedero 🚛", # ¡NUEVO FILTRO!
                "Fuera de Perímetro 🌐", # ¡NUEVO FILTRO!
                "Falla GPS 🛠",
                "Apagadas ❄️",
                "Paradas Largas 🛑",
                "Resguardo (Sede) 🛡️",
                "Resguardo (Fuera de Sede) 🛡️"
            ]

            st.session_state['filtro_estado_especifico'] = st.radio(
                "O estados específicos:",
                options=filtro_estado_options,
                key="filtro_radio",
                index=filtro_estado_options.index(st.session_state['filtro_estado_especifico']),
                on_change=actualizar_dashboard,
                label_visibility="collapsed"
            )
            # ---# --- FIN DEL EXPANDER DE FILTROS ---


        with st.expander("🚚 Asignacion Unidades", expanded=False):

            st.button(
                "Salida de unidad",
                use_container_width=True, 
                on_click=set_logistica_view,
                disabled=(st.session_state.current_logistica_view != 'menu'),
                args=('asignacion_create_only',),
                key="btn_asignacion_salida_sidebar"
            )

            st.button(
                "Ingreso de unidad",
                use_container_width=True, 
                on_click=set_logistica_view,
                disabled=(st.session_state.current_logistica_view != 'menu'),
                args=('asignacion_ingreso',),
                key="btn_asignacion_ingreso_sidebar"
            )
            
            st.button(
                "Editar Asignación",
                use_container_width=True, 
                on_click=set_logistica_view,
                disabled=(st.session_state.current_logistica_view != 'menu'),
                args=('asignacion_edit',),
                key="btn_asignacion_edit_sidebar"
            )
            
            st.button(
                "Eliminar Asignación",
                use_container_width=True, 
                on_click=set_logistica_view,
                disabled=(st.session_state.current_logistica_view != 'menu'),
                args=('asignacion_delete',),
                key="btn_asignacion_delete_sidebar"
            )
            

            
        with st.sidebar.expander("📝 Administracion", expanded=False):
    # EXPANDER ANIDADO: Data

            if st.session_state.current_logistica_view == 'menu':
                st.button(
                    "Unidades",
                    use_container_width=True,
                    on_click=set_logistica_view,
                    args=('unidades_crud',),
                    key="btn_unidades_sidebar"
                )
                st.button(
                    "Conductores",
                    use_container_width=True,
                    on_click=set_logistica_view,
                    args=('conductores_crud',),
                    key="btn_conductores_sidebar"
                )
                st.button(
                    "Rutas",
                    use_container_width=True,
                    on_click=set_logistica_view,
                    args=('rutas_crud',),
                    key="btn_rutas_sidebar"
                )
      
     
     # PLACEHOLDERS EN EL SIDEBAR (Declaración única)
     metricas_placeholder = st.empty() # Este es el placeholder que contendrá el expander de estadísticas.
     #st.markdown("---")
     audio_stop_placeholder = st.empty()
     alerta_stop_placeholder = st.empty()
     #st.markdown("---")
     audio_velocidad_placeholder = st.empty()
     alerta_velocidad_placeholder = st.empty()
     #st.markdown("---")
     audio_perimetro_placeholder = st.empty()  # 🆕 NUEVO PLACEHOLDER PARA AUDIO DE PERÍMETRO
     audio_encendido_placeholder = st.empty()  # 🔊 NUEVO PLACEHOLDER PARA AUDIO DE ENCENDIDO
     alerta_perimetro_placeholder = st.empty()  # 🆕 NUEVO PLACEHOLDER PARA ALERTAS DE PERÍMETRO
     #st.markdown("---")

     debug_status_placeholder = st.empty()

# Función para generar la línea de métrica con estilo (Fuera del sidebar para uso en el loop)
def format_metric_line(label, value=None, value_size="1.5rem", is_header=False, is_section_title=False):
    """Genera el HTML para las métricas con estilo unificado: Etiqueta a la izquierda, Valor a la derecha."""

    text_style = "color: white; font-family: 'Consolas', 'Courier New', monospace; font-size: 1rem;"

    if is_header:
        label_html = f'<p style="font-size: 1.2rem; font-weight: bold; margin-bottom: 0px;">{label}</p>'
        return f'<div style="border-bottom: 1px solid #444444; margin: 10px 0 10px 0;">{label_html}</div>'

    if is_section_title:
        return f'<p style="font-size: 1.1rem; font-weight: bold; margin-bottom: 5px; color: white;">{label}</p>'

    value_html = f'<span style="font-size: {value_size}; font-weight: bold; color: white;">{value}</span>'

    html_content = f"""
    <p style="{text_style} display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px;">
        <span style="white-space: nowrap;">{label}:</span>
        <span style="display: flex; align-items: baseline;">{value_html}</span>
    </p>
     """
    return html_content


# RENDERIZACIÓN CONDICIONAL EN EL CUERPO PRINCIPAL

flota_a_mostrar = st.session_state.get('flota_seleccionada', 'Flota No Seleccionada')

if st.session_state.current_logistica_view == 'unidades_crud':
    st.cache_data.clear()
    display_unidades_crud()
    st.stop()

elif st.session_state.current_logistica_view == 'conductores_crud':
    st.cache_data.clear()
    display_conductores_crud()
    st.stop()

elif st.session_state.current_logistica_view == 'rutas_crud':
    st.cache_data.clear()
    display_rutas_crud()
    st.stop()

# 🚨 NUEVO: Vista de Asignación de Unidades

elif st.session_state.current_logistica_view == 'asignacion_crud':
    st.cache_data.clear()
    display_asignacion_create_only()
    st.stop()
    
elif st.session_state.current_logistica_view == 'asignacion_create_only':
    st.cache_data.clear()
    display_asignacion_create_only()
    st.stop()
    
elif st.session_state.current_logistica_view == 'asignacion_edit':
    st.cache_data.clear()
    display_asignacion_edit()
    st.stop()
    
elif st.session_state.current_logistica_view == 'asignacion_delete':
    st.cache_data.clear()
    display_asignacion_delete()
    st.stop()
    
elif st.session_state.current_logistica_view == 'asignacion_ingreso':
    st.cache_data.clear()
    display_asignacion_ingreso()
    st.stop()
#else:
    
    #st.title(f"Dashboard (Flota: {flota_a_mostrar})")
   
    # ...

# 🚨 PLACEHOLDER PARA EL TESTIGO DE ESTADO (Fuera del Sidebar) 🚨
placeholder_status_light = st.empty()

# Placeholder para el contenido principal (Tarjetas)
placeholder_main_content = st.empty()

# Placeholder para el contenido principal (Tarjetas)
placeholder_main_content = st.empty()

# Usamos la referencia al estado global obtenida antes del bucle
# current_stop_state = get_global_stop_state()

# =========================================================================
# INICIO DEL BUCLE PRINCIPAL (while True) con Testigo
# =========================================================================

while True:
    
    if st.session_state.current_logistica_view != 'menu':
        # Si estamos en una vista CRUD, pausar un poco y verificar de nuevo
        time.sleep(0.1)
        st.rerun()
        continue


    # 🚨 PUNTO A: ESTADO GRIS (SOLICITANDO Y PROCESANDO DATOS) 🚨
    # El testigo se pone en GRIS para indicar que el sistema está ocupado.
    with placeholder_status_light.container():
        st.markdown(
            f"""
            <div class="update-align">
                <div>
                    <span style='color: #DDDDDD; font-weight: bold;'>🟡 Procesando...</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    # LECTURA DE FLOTA Y PARÁMETROS (SU CÓDIGO)
    flota_a_usar = st.session_state['flota_seleccionada']
    config = st.session_state['config_params']
    STOP_THRESHOLD_MINUTES = config['STOP_THRESHOLD_MINUTES']
    SPEED_THRESHOLD_KPH = config['SPEED_THRESHOLD_KPH']
    GPS_MIN_ENCENDIDA = config['GPS_MIN_ENCENDIDA']
    GPS_MIN_APAGADA = config['GPS_MIN_APAGADA']
    TIME_SLEEP = config['TIME_SLEEP']
    datos_flota_conductor = cargar_datos_flota_conductor(flota_a_usar)
    # 🚨 Definición del ID de tiempo para claves únicas 🚨
    unique_time_id = int(time.time() * 1000)

    # CONDICIÓN CRÍTICA: NO EJECUTAR SI NO HAY FLOTA SELECCIONADA
    if not flota_a_usar:
        with placeholder_main_content.container():
            st.markdown(
                f"<h2 id='main-title'>Rastreo GPS - Monitoreo GPS - FOSPUCA</h2>",
                unsafe_allow_html=True
            )
            st.markdown("---")
            st.info("**seleccione una Flota** en el panel lateral para comenzar el monitoreo en tiempo real.")

        # Limpiar Placeholders (reutilizamos la referencia del sidebar)
        try:
            audio_stop_placeholder.empty()
            audio_velocidad_placeholder.empty()
            alerta_velocidad_placeholder.empty()
            alerta_stop_placeholder.empty()
            metricas_placeholder.empty()
            debug_status_placeholder.empty()
        except NameError:
             pass

        # 🚨 PUNTO B: ESTADO GRIS EN 'NO FLOTA SELECCIONADA' (INACTIVO) 🚨
        # Mantenemos el testigo en GRIS pero con mensaje de inactividad
        with placeholder_status_light.container():
             st.markdown(
                f"""
                <div class="update-align">
                    <div>
                        <span style='color: #DDDDDD; font-weight: bold;'>😴 Inactivo (Esperando seleccion)</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        time.sleep(1)
        continue
    # --------------------------------------------------------------------------
    # Obtener datos
    df_data_original = obtener_datos_unidades(flota_a_usar, FLOTAS_CONFIG, GPS_MIN_ENCENDIDA, GPS_MIN_APAGADA)

    is_fallback = "FALLBACK" in df_data_original["UNIDAD"].iloc[0]

    # -- LÓGICA DE DETECCIÓN DE PARADAS LARGAS Y EXCESO DE VELOCIDAD --

    now = pd.Timestamp.now(tz='America/Caracas')

    if not is_fallback:
        # 🚨 DOBLE VERIFICACIÓN: Iterar sobre las filas y actualizar el estado
        for index, row in df_data_original.iterrows():
            unit_id_api = row['UNIT_ID']
            velocidad = row['VELOCIDAD']
            lat = row['LATITUD']
            lon = row['LONGITUD']

            # Inicialización de estado completo para cada unidad
            if unit_id_api not in current_stop_state:
                current_stop_state[unit_id_api] = {
                    'last_move_time': now,
                    'alerted_stop_minutes': None,
                    'speed_alert_start_time': None,
                    'last_recorded_speed': 0.0
                }

            if unit_id_api not in current_coordinate_state:
                current_coordinate_state[unit_id_api] = {
                    'stable_time': now,
                    'last_coordinate': None,
                    'coordinate_duration': 0.0
                }

            if unit_id_api not in current_velocity_state:
                current_velocity_state[unit_id_api] = {
                    'zero_velocity_time': now,
                    'velocity_duration': 0.0
                }

            # Obtener referencias a los estados
            last_state = current_stop_state[unit_id_api]
            coord_state = current_coordinate_state[unit_id_api]
            vel_state = current_velocity_state[unit_id_api]

            is_moving = velocidad > 1.0

            # Determinar si la unidad NO está en ninguna zona de resguardo/sede/vertedero
            is_out_of_hq = not (row['EN_SEDE_FLAG'] or row['EN_RESGUARDO_SECUNDARIO_FLAG'] or row['EN_VERTEDERO_FLAG'] or row['ES_FALLA_GPS_FLAG'])

            is_speeding = velocidad >= SPEED_THRESHOLD_KPH

            # 🚨 LÓGICA DE DOBLE VERIFICACIÓN PARA PARADAS 🚨
            
            # Verificar coordenadas actuales (redondeadas para evitar variaciones GPS)
            coordenadas_actuales = (round(lat, 6), round(lon, 6))
            
            if coord_state['last_coordinate'] is None:
                
                # Primera vez que vemos esta unidad
                coord_state['last_coordinate'] = coordenadas_actuales
                coord_state['stable_time'] = now
            else:
                # Verificar si las coordenadas cambiaron
                if coordenadas_actuales != coord_state['last_coordinate']:
                    # Coordenadas cambiaron - reiniciar contador
                    coord_state['last_coordinate'] = coordenadas_actuales
                    coord_state['stable_time'] = now
                    coord_state['coordinate_duration'] = 0.0
                else:
                    # Coordenadas iguales - incrementar duración
                    coord_state['coordinate_duration'] = (now - coord_state['stable_time']).total_seconds() / 60.0

            # Verificar velocidad cero
            if velocidad < 1.0:
                # Velocidad cero - incrementar duración
                vel_state['velocity_duration'] = (now - vel_state['zero_velocity_time']).total_seconds() / 60.0
            else:
                # Velocidad > 0 - reiniciar contador
                vel_state['zero_velocity_time'] = now
                vel_state['velocity_duration'] = 0.0

            # LÓGICA DE EXCESO DE VELOCIDAD (START/UPDATE)
            if is_speeding and is_out_of_hq:
                if last_state['speed_alert_start_time'] is None:
                    last_state['speed_alert_start_time'] = now

                if velocidad > last_state['last_recorded_speed']:
                    last_state['last_recorded_speed'] = velocidad

            # LÓGICA DE EXCESO DE VELOCIDAD (END/LOG)
            elif not is_speeding and last_state['speed_alert_start_time'] is not None:
                start_time = last_state['speed_alert_start_time']
                duration_timedelta = now - start_time
                duration_minutes = duration_timedelta.total_seconds() / 60.0

                # Registra solo si el exceso duró más de 10 segundos (~0.166 min)
                if duration_minutes >= 0.166:
                    hora_log = now.strftime('%H:%M:%S')
                    nombre_unidad_display = row['UNIDAD'].split('-')[0] if '-' in row['UNIDAD'] else row['UNIDAD']
                    max_speed_recorded = last_state['last_recorded_speed']

                    log_message = (
                        f"**🟡 {hora_log}** | Unidad: **{nombre_unidad_display}** "
                        f"| Exceso de Velocidad Máx: **{max_speed_recorded:.1f} Km/h** "
                        f"| por: **{duration_minutes:.1f} min** "
                        f"| en Dirección: {row['UBICACION_TEXTO']}"
                    )

                # RESET
                last_state['speed_alert_start_time'] = None
                last_state['last_recorded_speed'] = 0.0

            # LÓGICA DE PARADA LARGA DOBLE VERIFICACIÓN
            if not is_moving and (vel_state['velocity_duration'] > 0 and coord_state['coordinate_duration'] > 0):
                # Unidad detenida con coordenadas estables - actualizar duración
                # Usar el máximo entre ambos contadores para la duración mostrada
                max_duration = max(vel_state['velocity_duration'], coord_state['coordinate_duration'])
                
                # Actualizar el DataFrame
                df_data_original.loc[index, 'STOP_DURATION_MINUTES'] = max_duration
                df_data_original.loc[index, 'STOP_DURATION_TIMEDELTA'] = timedelta(seconds=max_duration * 60)

                # 🚨 DOBLE VERIFICACIÓN PARA ALERTA: Ambas condiciones deben cumplirse
                parada_doble_verificacion = (
                    velocidad < 1.0 and 
                    vel_state['velocity_duration'] > STOP_THRESHOLD_MINUTES and
                    coord_state['coordinate_duration'] > STOP_THRESHOLD_MINUTES and
                    is_out_of_hq
                )

                if parada_doble_verificacion:
                    last_state['alerted_stop_minutes'] = max_duration

            elif is_moving:
                # Unidad en movimiento - resetear todos los contadores y finalizar alerta activa
                
                # Actualizar el DataFrame
                df_data_original.loc[index, 'STOP_DURATION_MINUTES'] = 0.0
                df_data_original.loc[index, 'STOP_DURATION_TIMEDELTA'] = timedelta(seconds=0)

                if last_state.get('alerted_stop_minutes'):
                    hora_log = now.strftime('%H:%M:%S')
                    duracion_log = f"{last_state['alerted_stop_minutes']:.1f}"
                    nombre_unidad_display = row['UNIDAD'].split('-')[0] if '-' in row['UNIDAD'] else row['UNIDAD']

                    log_message = (
                        f"**🟢 {hora_log}** | Unidad: **{nombre_unidad_display}** "
                        f"| FIN de Parada Larga (Doble Verificación), por: **{duracion_log} min** "
                        f"| Ubicación: {row['UBICACION_TEXTO']}"
                    )

                    last_state['alerted_stop_minutes'] = None

                # Resetear el tiempo de último movimiento
                last_state['last_move_time'] = now

                # Reinicio de estados de alerta al moverse
                if row['UNIDAD'] in st.session_state['alertas_descartadas']:
                    del st.session_state['alertas_descartadas'][row['UNIDAD']]
                if row['UNIDAD'] in st.session_state['alertas_velocidad_descartadas']:
                    del st.session_state['alertas_velocidad_descartadas'][row['UNIDAD']]

                # Desactivamos las banderas de reproducción si la unidad se mueve
                st.session_state['reproducir_audio_alerta'] = False
                st.session_state['reproducir_audio_velocidad'] = False

            else:
                # Unidad detenida pero aún no cumple doble verificación
                # Actualizar el DataFrame con la duración máxima disponible
                max_duration = max(vel_state['velocity_duration'], coord_state['coordinate_duration'])
                df_data_original.loc[index, 'STOP_DURATION_MINUTES'] = max_duration
                df_data_original.loc[index, 'STOP_DURATION_TIMEDELTA'] = timedelta(seconds=max_duration * 60)

    # Lógica de Filtrado Condicional (Mejorada la lógica de Parada Larga)
    df_data_mostrada = df_data_original.copy() # Usamos una copia de la original para aplicar filtros

    filtro_en_ruta_activo = st.session_state.get("filtro_en_ruta", False)
    filtro_estado_activo = st.session_state.get('filtro_estado_especifico', "Mostrar Todos")

    filtro_descripcion = "Todas las Unidades"

    if not is_fallback:

        # 1. Aplicar filtro de ESTADO ESPECÍFICO
        if filtro_estado_activo != "Mostrar Todos":

            if "Vertedero" in filtro_estado_activo: # ¡NUEVO FILTRO!
                df_data_mostrada = df_data_original[df_data_original["EN_VERTEDERO_FLAG"] == True]

            elif "Fuera de Perímetro" in filtro_estado_activo: # ¡NUEVO FILTRO!
                df_data_mostrada = df_data_original[df_data_original["EN_FUERA_PERIMETRO_FLAG"] == True]

            elif "Falla GPS" in filtro_estado_activo:
                df_data_mostrada = df_data_original[df_data_original["IGNICION"].str.contains("Falla GPS")]

            elif "Apagadas" in filtro_estado_activo:
                df_data_mostrada = df_data_original[df_data_original["IGNICION"].str.contains("Apagada ❄️")]

            elif "Resguardo (Sede)" in filtro_estado_activo:
                # Usa el flag para ser más preciso
                df_data_mostrada = df_data_original[df_data_original['EN_SEDE_FLAG'] == True]

            elif "Resguardo (Fuera de Sede)" in filtro_estado_activo:
                # Usa el flag para ser más preciso
                df_data_mostrada = df_data_original[df_data_original['EN_RESGUARDO_SECUNDARIO_FLAG'] == True]

            elif "Paradas Largas" in filtro_estado_activo:
                is_out_of_hq_status = ~df_data_original["IGNICION"].str.contains("(Sede)|Resguardo|Falla GPS|Vertedero|Fuera de Perímetro") # ACTUALIZADO

                df_data_mostrada = df_data_original[
                    (df_data_original['STOP_DURATION_MINUTES'] > STOP_THRESHOLD_MINUTES) &
                    (df_data_original['VELOCIDAD'] < 1.0) &
                    is_out_of_hq_status
                ].copy()

            filtro_descripcion = filtro_estado_activo

        # 2. Aplicar filtro "Unidades en Ruta"
        elif filtro_en_ruta_activo:
            # En ruta significa: NO está en sede, NO en resguardo secundario, NO en vertedero, NO en perímetro, NO es Falla GPS
            is_en_ruta = ~(df_data_original['EN_SEDE_FLAG'] | df_data_original['EN_RESGUARDO_SECUNDARIO_FLAG'] | df_data_original['EN_VERTEDERO_FLAG'] | df_data_original['EN_FUERA_PERIMETRO_FLAG'] | df_data_original['ES_FALLA_GPS_FLAG'])
            df_data_mostrada = df_data_original[is_en_ruta].copy()
            filtro_descripcion = "Unidades Fuera de Sede 🛣️"

        df_data_mostrada = df_data_mostrada.reset_index(drop=True)
    # FIN DE LA LÓGICA DE FILTRADO

    # Lógica de Detección y Construcción de Alerta de Parada Larga (Alertas Visibles)
    unidades_en_alerta_stop = pd.DataFrame()
    mensaje_alerta_stop = ""

    if not is_fallback:
        
        # La condición de parada larga incluye ahora NO estar en Vertedero ni Fuera de Perímetro
        todas_las_alertas_stop = df_data_original[
            (df_data_original['STOP_DURATION_MINUTES'] > STOP_THRESHOLD_MINUTES) &
            (~(df_data_original['EN_SEDE_FLAG'] | df_data_original['EN_RESGUARDO_SECUNDARIO_FLAG'] | df_data_original['EN_VERTEDERO_FLAG'] | df_data_original['EN_FUERA_PERIMETRO_FLAG'] | df_data_original['ES_FALLA_GPS_FLAG']))
        ].copy()

        unidades_pendientes_stop = [
            uid for uid in todas_las_alertas_stop['UNIDAD']
            if st.session_state['alertas_descartadas'].get(uid) != True
        ]

        unidades_en_alerta_stop = todas_las_alertas_stop[
            todas_las_alertas_stop['UNIDAD'].isin(unidades_pendientes_stop)
        ].sort_values(by='STOP_DURATION_MINUTES', ascending=False)

        # CONTROL DEL AUDIO PARADA
        if not unidades_en_alerta_stop.empty:
            if st.session_state.get('reproducir_audio_alerta') == False:
                 st.session_state['reproducir_audio_alerta'] = True

            total_alertas = len(unidades_en_alerta_stop)
            mensaje_alerta_stop += f"**{total_alertas} PARADA LARGA(S) PENDIENTE(S) 🚨**\n\n"

            for _, row in unidades_en_alerta_stop.head(5).iterrows():
                nombre_unidad = row['UNIDAD'].split('-')[0]
                total_segundos = row['STOP_DURATION_TIMEDELTA'].total_seconds()
                tiempo_parado = f"{int(total_segundos // 60)}min {int(total_segundos % 60):02}seg"
                #mensaje_alerta_stop += (f"**{nombre_unidad}** ({tiempo_parado}):\n---\n")
                ubicacion_texto = row['UBICACION_TEXTO']
                nombre_conductor = get_driver_name_for_unit(row['UNIDAD'], flota_a_usar)
                linea1 = f"**{nombre_unidad}** PARADA LARGA  ({tiempo_parado})"
                linea2 = f"Conductor: {nombre_conductor}"
                linea3 = f"{ubicacion_texto}"
                mensaje_alerta_stop += (
                        f"{linea1}\n\n"   
                        f"{linea2}\n\n"   
                        f"{linea3}\n"   
                        f"---------\n"      
                    )
        else:
             st.session_state['reproducir_audio_alerta'] = False
             st.session_state['reproducir_audio_encendido'] = False  # 🔊 Desactivar audio de encendido
                                                
    # Lógica de Detección de Alerta de Velocidad
    unidades_en_alerta_speed = pd.DataFrame()
    mensaje_alerta_speed = ""

    if not is_fallback:
        # La condición de exceso de velocidad incluye ahora NO estar en Vertedero ni Fuera de Perímetro
        todas_las_alertas_speed = df_data_original[
            (df_data_original['VELOCIDAD'] >= SPEED_THRESHOLD_KPH) &
            (~(df_data_original['EN_SEDE_FLAG'] | df_data_original['EN_RESGUARDO_SECUNDARIO_FLAG'] | df_data_original['EN_VERTEDERO_FLAG'] | df_data_original['EN_FUERA_PERIMETRO_FLAG'] | df_data_original['ES_FALLA_GPS_FLAG']))
        ].copy()
        
        # Alertas críticas (velocidad >= 75 km/h) para activar alarma sonora
        todas_las_alertas_criticas = df_data_original[
            (df_data_original['VELOCIDAD'] >= VELOCIDAD_CRITICA_AUDIO) &
            (~(df_data_original['EN_SEDE_FLAG'] | df_data_original['EN_RESGUARDO_SECUNDARIO_FLAG'] | df_data_original['EN_VERTEDERO_FLAG'] | df_data_original['EN_FUERA_PERIMETRO_FLAG'] | df_data_original['ES_FALLA_GPS_FLAG']))
        ].copy()

        unidades_pendientes_speed = [
            uid for uid in todas_las_alertas_speed['UNIDAD']
            if st.session_state['alertas_velocidad_descartadas'].get(uid) != True
        ]

        unidades_en_alerta_speed = todas_las_alertas_speed[
            todas_las_alertas_speed['UNIDAD'].isin(unidades_pendientes_speed)
        ].sort_values(by='VELOCIDAD', ascending=False)

        # CONTROL DEL AUDIO VELOCIDAD - Solo para alertas críticas (>= 75 km/h)
        unidades_pendientes_criticas = [
            uid for uid in todas_las_alertas_criticas['UNIDAD']
            if st.session_state['alertas_velocidad_descartadas'].get(uid) != True
        ]
        
        unidades_en_alerta_critica = todas_las_alertas_criticas[
            todas_las_alertas_criticas['UNIDAD'].isin(unidades_pendientes_criticas)
        ].sort_values(by='VELOCIDAD', ascending=False)
        
        if not unidades_en_alerta_critica.empty:
            if st.session_state.get('reproducir_audio_velocidad') == False:
                 st.session_state['reproducir_audio_velocidad'] = True

            total_alertas_criticas = len(unidades_en_alerta_critica)
            mensaje_alerta_speed += f"**{total_alertas_criticas} EXCESO CRÍTICO DE VELOCIDAD PENDIENTE(S) 🚨**\n\n"

            for _, row in unidades_en_alerta_critica.head(5).iterrows():
                nombre_unidad = row['UNIDAD'].split('-')[0]
                velocidad_formateada = f"{row['VELOCIDAD']:.1f} Km/h"
                ubicacion_texto = row['UBICACION_TEXTO']
                nombre_conductor = get_driver_name_for_unit(row['UNIDAD'], flota_a_usar)
                # mensaje_alerta_speed += (f"**{nombre_unidad}** (CRÍTICO a {velocidad_formateada}):\n---\n")
                linea1 = f"**{nombre_unidad}** (CRÍTICO a {velocidad_formateada})"
                linea2 = f"Conductor: {nombre_conductor}"
                linea3 = f"{ubicacion_texto}"
                mensaje_alerta_speed += (
                    f"{linea1}\n\n"   
                    f"{linea2}\n\n"   
                    f"{linea3}\n"   
                    f"---------\n"      
                )
                
        else:
            st.session_state['reproducir_audio_velocidad'] = False
            st.session_state['reproducir_audio_encendido'] = False  # 🔊 Desactivar audio de encendido

           
        # Agregar alertas visuales (70-74 km/h) sin audio
        if not unidades_en_alerta_speed.empty:
            alertas_visuales = unidades_en_alerta_speed[unidades_en_alerta_speed['VELOCIDAD'] < VELOCIDAD_CRITICA_AUDIO]
            if not alertas_visuales.empty:
                total_visuales = len(alertas_visuales)
                if mensaje_alerta_speed:
                    mensaje_alerta_speed += f"\n**{total_visuales} ALERTA(S) DE VELOCIDAD PENDIENTE(S) ⚠️**\n\n"
                else:
                    mensaje_alerta_speed += f"**{total_visuales} ALERTA(S) DE VELOCIDAD PENDIENTE(S) ⚠️**\n\n"

                for _, row in alertas_visuales.head(5).iterrows():
                    nombre_unidad = row['UNIDAD'].split('-')[0]
                    velocidad_formateada = f"{row['VELOCIDAD']:.1f} Km/h"
                    ubicacion_texto = row['UBICACION_TEXTO']
                    nombre_conductor = get_driver_name_for_unit(row['UNIDAD'], flota_a_usar)

                    linea1 = f"**{nombre_unidad}** (ALERTA a {velocidad_formateada})"
                    linea2 = f"Conductor: {nombre_conductor}"
                    linea3 = f"{ubicacion_texto}"           

                    mensaje_alerta_speed += (
                        f"{linea1}\n\n"   
                        f"{linea2}\n\n"   
                        f"{linea3}\n"   
                        f"---------\n"      
                    )
                
              
    # 🆕 LÓGICA DE DETECCIÓN DE ALERTAS DE PERÍMETRO (ACTUALIZADA CON CONTROL AVANZADO)
    unidades_en_alerta_perimetro = pd.DataFrame()
    mensaje_alerta_perimetro = ""

    if not is_fallback:
        # 1. Limpiar alarmas expiradas al inicio del ciclo
        limpiar_alarmas_perimetro_expiradas()
        
        # Verificar si la flota actual tiene perímetro configurado
        tiene_perimetro_configurado = flota_a_usar in PERIMETROS_CARGADOS
        
        if tiene_perimetro_configurado:
            # Verificar unidades Fuera de Perímetro
            todas_las_unidades_fuera_perimetro = df_data_original[df_data_original['EN_FUERA_PERIMETRO_FLAG'] == True].copy()
            
            # 2. Detectar si hay nuevas unidades fuera de perímetro (que no estaban en el ciclo anterior)
            unidades_fuera_actuales = set(todas_las_unidades_fuera_perimetro['UNIDAD'].tolist())
            unidades_fuera_anteriores = st.session_state.get('perimetro_ultimas_unidades_fuera', set())
            nuevas_unidades_fuera = unidades_fuera_actuales - unidades_fuera_anteriores
            
            # Actualizar el tracking de unidades fuera para el próximo ciclo
            st.session_state['perimetro_ultimas_unidades_fuera'] = unidades_fuera_actuales
            
            # 3. Filtrar unidades que NO tienen alarma aceptada Y cuyo silencio de 15 min no ha expirado
            unidades_pendientes_perimetro = []
            hora_actual = obtener_hora_venezuela()
            
            for uid in todas_las_unidades_fuera_perimetro['UNIDAD']:
                # Verificar si la unidad NO está en alertas descartadas
                if st.session_state['alertas_perimetro_descartadas'].get(uid) != True:
                    unidades_pendientes_perimetro.append(uid)
                else:
                    # Verificar si el tiempo de silencio de 15 minutos ha expirado
                    tiempo_aceptacion = st.session_state['perimetro_unidades_aceptadas'].get(uid)
                    if tiempo_aceptacion:
                        diferencia = hora_actual - tiempo_aceptacion
                        if diferencia.total_seconds() >= (TIEMPO_SILENCIO_PERIMETRO * 60):  # Usar constante
                            # El tiempo de silencio ha expirado, permitir nueva alarma
                            if uid in st.session_state['alertas_perimetro_descartadas']:
                                del st.session_state['alertas_perimetro_descartadas'][uid]
                            if uid in st.session_state['perimetro_unidades_aceptadas']:
                                del st.session_state['perimetro_unidades_aceptadas'][uid]
                            unidades_pendientes_perimetro.append(uid)

            unidades_en_alerta_perimetro = todas_las_unidades_fuera_perimetro[
                todas_las_unidades_fuera_perimetro['UNIDAD'].isin(unidades_pendientes_perimetro)
            ].sort_values(by='UNIDAD', ascending=True)

            # 4. CONTROL DEL AUDIO PERÍMETRO CON REPETICIÓN CADA 20 SEGUNDOS
            audio_debe_reproducirse = False
            
            if not unidades_en_alerta_perimetro.empty:
                # Hay unidades pendientes de aceptar
                ultimo_play = st.session_state.get('perimetro_audio_last_play')
                
                # Verificar si hay nuevas unidades fuera de perímetro (forzar reproducción)
                if nuevas_unidades_fuera:
                    audio_debe_reproducirse = True
                    print(f"🔊 Nueva(s) unidad(es) fuera de perímetro detectada(s): {nuevas_unidades_fuera}")
                
                # O si han pasado más de TIEMPO_REPETICION_AUDIO_PERIMETRO segundos desde la última reproducción
                elif ultimo_play is None:
                    audio_debe_reproducirse = True
                else:
                    tiempo_desde_ultimo_play = (hora_actual - ultimo_play).total_seconds()
                    if tiempo_desde_ultimo_play >= TIEMPO_REPETICION_AUDIO_PERIMETRO:  # Usar constante
                        audio_debe_reproducirse = True
                        print(f"⏰ Reproductando audio de perímetro después de {tiempo_desde_ultimo_play:.1f} segundos")
            
            # Actualizar estado de reproducción
            if audio_debe_reproducirse:
                st.session_state['reproducir_audio_perimetro'] = True
                st.session_state['perimetro_audio_last_play'] = hora_actual
                print(f"🔊 Audio de perímetro activado para {len(unidades_en_alerta_perimetro)} unidad(es)")
            else:
                st.session_state['reproducir_audio_perimetro'] = False

            # 5. Generar mensaje de alerta
            if not unidades_en_alerta_perimetro.empty:
                total_alertas_perimetro = len(unidades_en_alerta_perimetro)
                mensaje_alerta_perimetro += f"**{total_alertas_perimetro} UNIDAD(ES) FUERA DE PERÍMETRO** 🌐\n\n"
                
                for _, row in unidades_en_alerta_perimetro.head(5).iterrows():
                    nombre_unidad = row['UNIDAD'].split('-')[0] if '-' in row['UNIDAD'] else row['UNIDAD']
                    ubicacion_texto = row['UBICACION_TEXTO']
                    nombre_conductor = get_driver_name_for_unit(row['UNIDAD'], flota_a_usar)
                    
                    linea1 = f"**{nombre_unidad}** FUERA DE PERÍMETRO"
                    linea2 = f"Conductor: {nombre_conductor}"
                    linea3 = f"Ubicación: {ubicacion_texto}"
                    
                    mensaje_alerta_perimetro += (
                        f"{linea1}\n\n"   
                        f"{linea2}\n\n"   
                        f"{linea3}\n"   
                        f"---------\n"      
                    )
                    
                # Mostrar información sobre nuevas unidades si las hay
                if nuevas_unidades_fuera:
                    mensaje_alerta_perimetro += f"\n**🆕 NUEVAS UNIDADES DETECTADAS:** {len(nuevas_unidades_fuera)}\n"
                    df_nuevas_alertas = unidades_en_alerta_perimetro[
                        unidades_en_alerta_perimetro['UNIDAD'].isin(nuevas_unidades_fuera)
                    ]
                    for _, row in df_nuevas_alertas.iterrows():
                        nombre_unidad = row['UNIDAD'].split('-')[0] if '-' in row['UNIDAD'] else row['UNIDAD']
                        ubicacion_texto = row['UBICACION_TEXTO']
                        nombre_conductor = get_driver_name_for_unit(row['UNIDAD'], flota_a_usar)

                        linea1 = f"**{nombre_unidad}** FUERA DE PERÍMETRO (NUEVA)"
                        linea2 = f"Conductor: {nombre_conductor}"
                        linea3 = f"Ubicación: {ubicacion_texto}"

                        mensaje_alerta_perimetro += (
                            f"{linea1}\n\n"   
                            f"{linea2}\n\n"   
                            f"{linea3}\n"   
                            f"---------\n"     
                        )
                   
        else:
            
            # Si no hay alarmas de perímetro, desactivar la bandera de reproducción
            st.session_state['reproducir_audio_perimetro'] = False
            st.session_state['reproducir_audio_encendido'] = False  # 🔊 Desactivar audio de encendido
            print(f"ℹ️ Flota '{flota_a_usar}' no tiene perímetro configurado, continuando con verificaciones normales")

    # =========================================================================
    # RENDERIZADO DE ALERTAS EN EL SIDEBAR
    # =========================================================================
    # El unique_time_id se define al inicio del bucle para que sea estable en todo el ciclo.

    # AUDIO PARADA
    with audio_stop_placeholder.container():
        if st.session_state.get('reproducir_audio_alerta'):
             reproducir_alerta_sonido(AUDIO_BASE64_PARADA)
        else:
             audio_stop_placeholder.empty()

    # ALERTA PARADA
    with alerta_stop_placeholder.container():
        if not unidades_en_alerta_stop.empty:
            total_alertas_pendientes = len(unidades_en_alerta_stop)
            st.markdown(f"#### 🚨 Alerta de Parada Larga ({total_alertas_pendientes})")
           
            st.warning(mensaje_alerta_stop)


            def aceptar_todas_paradas():
                for uid in unidades_en_alerta_stop['UNIDAD']:
                    st.session_state['alertas_descartadas'][uid] = True
                st.session_state['reproducir_audio_alerta'] = False
                st.cache_data.clear()
                
            st.button(
                "✅ Aceptar y Silenciar TODAS las Paradas",
                key=f"descartar_all_stops_{unique_time_id}",
                on_click=aceptar_todas_paradas,
                type="secondary",
                use_container_width=True
            )
        else:
            alerta_stop_placeholder.empty()

    # AUDIO VELOCIDAD
    with audio_velocidad_placeholder.container():
        if st.session_state.get('reproducir_audio_velocidad'):
             reproducir_alerta_sonido(AUDIO_BASE64_VELOCIDAD)
        else:
             audio_velocidad_placeholder.empty()

    # ALERTA VELOCIDAD
    with alerta_velocidad_placeholder.container():
        if not unidades_en_alerta_speed.empty:
            total_alertas_pendientes_speed = len(unidades_en_alerta_speed)
            st.markdown(f"#### ⚠️ Exceso de Velocidad ({total_alertas_pendientes_speed})")
           
            st.error(mensaje_alerta_speed)

            def aceptar_todas_velocidades():
                for uid in unidades_en_alerta_speed['UNIDAD']:
                    st.session_state['alertas_velocidad_descartadas'][uid] = True
                st.session_state['reproducir_audio_velocidad'] = False
                st.cache_data.clear()
                
            st.button(
                "✅ Aceptar y Silenciar TODOS los Excesos",
                key=f"descartar_all_speed_{unique_time_id}",
                on_click=aceptar_todas_velocidades,
                type="secondary",
                use_container_width=True
            )
        else:
            alerta_velocidad_placeholder.empty()

    # 🆕 AUDIO PERÍMETRO
    with audio_perimetro_placeholder.container():
        if st.session_state.get('reproducir_audio_perimetro'):
            reproducir_alerta_sonido(AUDIO_BASE64_PERIMETRO)
        else:
            audio_perimetro_placeholder.empty()
    
    # 🔊 AUDIO ENCENDIDO (CAMBIO DE RESGUARDO EXTERNO A ENCENDIDO)
    with audio_encendido_placeholder.container():
        if st.session_state.get('reproducir_audio_encendido'):
            reproducir_alerta_sonido(AUDIO_BASE64_ENCENDIDO)
        else:
            audio_encendido_placeholder.empty()

    # 🆕 ALERTA PERÍMETRO (ACTUALIZADA CON CONTROL INDIVIDUAL)
    with alerta_perimetro_placeholder.container():
        if not unidades_en_alerta_perimetro.empty:
            total_alertas_pendientes_perimetro = len(unidades_en_alerta_perimetro)
            st.markdown(f"#### 🌐 Fuera de Perímetro ({total_alertas_pendientes_perimetro})")
           
            st.info(mensaje_alerta_perimetro)

                # Botón para aceptar TODAS las alarmas
            st.button(
                "✅ Aceptar TODAS (Silencio 15 min)",
                key=f"descartar_all_perimeter_{unique_time_id}",
                on_click=aceptar_todas_alarmas_perimetro,
                args=(unidades_en_alerta_perimetro['UNIDAD'].tolist(),),
                type="secondary",
                use_container_width=True
            )
        else:
            alerta_perimetro_placeholder.empty()

    # 3. Actualización de Métricas del Sidebar (AHORA EN UN EXPANDER)
    with metricas_placeholder.container():
        if not is_fallback:

            # Lógica para calcular métricas
            total_unidades = len(df_data_original)

            # Unidades encendidas (Incluye Encendida en Sede y Encendida en Ruta)
            unidades_encendidas = len(df_data_original[df_data_original["IGNICION"].str.contains("Encendida")])

            # Unidades apagadas (Solo Apagada ❄️)
            unidades_apagadas = len(df_data_original[df_data_original["IGNICION"].str.contains("Apagada ❄️")])

            # Unidades en Resguardo/Encendida en Sede (Usa el nuevo flag EN_SEDE_FLAG)
            unidades_en_sede = df_data_original['EN_SEDE_FLAG'].sum()

            # Unidades en Resguardo (Fuera de Sede) (Usa el nuevo flag EN_RESGUARDO_SECUNDARIO_FLAG)
            unidades_resguardo_fuera_sede = df_data_original['EN_RESGUARDO_SECUNDARIO_FLAG'].sum()

            # Unidades en Vertedero (¡NUEVO!)
            unidades_en_vertedero = df_data_original['EN_VERTEDERO_FLAG'].sum()

            # Unidades Fuera de Perímetro (¡NUEVO!)
            unidades_fuera_perimetro = df_data_original['EN_FUERA_PERIMETRO_FLAG'].sum()

            # Unidades Falla GPS (Usa el nuevo flag ES_FALLA_GPS_FLAG)
            unidades_falla_gps = df_data_original['ES_FALLA_GPS_FLAG'].sum()

            # INICIO DEL DESPLEGABLE DE ESTADÍSTICAS
            with st.expander("📊 **Estadísticas de la Flota**", expanded=False):
                # Renderizado (usando la función definida fuera del loop)
                st.markdown(format_metric_line("Total Flota", total_unidades), unsafe_allow_html=True)
                st.markdown("---")
                st.markdown(format_metric_line("Estado Operacional", is_section_title=True), unsafe_allow_html=True)
                st.markdown(format_metric_line("Encendidas", unidades_encendidas), unsafe_allow_html=True)
                st.markdown(format_metric_line("Apagadas (Ruta)", unidades_apagadas), unsafe_allow_html=True)
                st.markdown("---")
                st.markdown(format_metric_line("Ubicación Crítica", is_section_title=True), unsafe_allow_html=True)

                # METRICA 3: EN VERTEDERO
                st.markdown(format_metric_line("En Vertedero", unidades_en_vertedero), unsafe_allow_html=True)

                # METRICA 4: FUERA DE PERÍMETRO
                st.markdown(format_metric_line("Fuera de Perímetro", unidades_fuera_perimetro), unsafe_allow_html=True)

                st.markdown("---")
                st.markdown(format_metric_line("Resguardo y Fallas", is_section_title=True), unsafe_allow_html=True)

                # METRICA 1: EN SEDE
                st.markdown(format_metric_line("En Sede", unidades_en_sede), unsafe_allow_html=True)

                # METRICA 2: RESGUARDO FUERA DE SEDE
                st.markdown(format_metric_line("Resguardo (F. Sede)", unidades_resguardo_fuera_sede), unsafe_allow_html=True)

                # METRICA 4: FALLA GPS
                st.markdown(format_metric_line("Falla GPS", unidades_falla_gps), unsafe_allow_html=True)
            # FIN DEL DESPLEGABLE

            # RENDERIZADO DE DEBUG Y HORA
            with debug_status_placeholder.container():
                hora_actual = obtener_hora_venezuela().strftime('%Y-%m-%d %H:%M:%S')

                st.markdown(
                    f'<div style="text-align: center; color: #888888; margin-top: 15px; font-size: 0.8em;">'
                    f'Última actualización:<br><strong>{hora_actual} VET</strong>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        else:
             metricas_placeholder.empty()
             debug_status_placeholder.empty()

    with placeholder_main_content.container():

        # 🟢 PUNTO C: ESTADO VERDE (MOSTRANDO/RENDERIZANDO DATA) 🟢
        # El testigo se pone en VERDE, indicando que la data fue recibida y se está mostrando.
        with placeholder_status_light.container():
            st.markdown(
                f"""

                <div class="update-align">
                    <div>
                        <span style='color: white; font-weight: bold;'>🟢 Actualizado</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown(
            f"<h2 id='main-title'>Rastreo GPS - Flota {flota_a_usar}</h2>",
            unsafe_allow_html=True
        )
     
        # -----------------------------------------------------------------------------------
        # 🚨 LÓGICA DE RENDERIZADO CONDICIONAL PARA MAPA (NUEVO) 🚨
        # -----------------------------------------------------------------------------------

        unidad_a_ubicar_id = st.session_state.get('unit_to_locate_id')
        df_data_final_render = pd.DataFrame() # DataFrame que se usará para el renderizado de tarjetas
        filtro_descripcion_final = filtro_descripcion

        # Si hay una unidad seleccionada Y no hay fallback de API
        if unidad_a_ubicar_id and not is_fallback:
            # 1. Intentar encontrar la unidad seleccionada en los datos filtrados (df_data_mostrada)
            df_unidad_seleccionada = df_data_mostrada[df_data_mostrada['UNIT_ID'] == unidad_a_ubicar_id]

            if not df_unidad_seleccionada.empty:
                # ===========================================================
                # A. RENDERIZAR VISTA DE MAPA Y TARJETA INDIVIDUAL (Parte Superior)
                # ===========================================================
                selected_row = df_unidad_seleccionada.iloc[0]
                
                lat = selected_row['LATITUD']
                lon = selected_row['LONGITUD']
                nombre_unidad = selected_row['UNIDAD'].split('-')[0]
                
                # >>> INICIO DE MODIFICACIÓN SOLICITADA: CÁLCULO DEL PULSO <<<
                PULSE_FREQUENCY = 5 
                pulsing_factor = (np.sin(time.time() * PULSE_FREQUENCY) + 1) / 10.0 

                BASE_RADIUS_M = 2        
                PULSE_AMPLITUDE_M = 15
                current_radius = BASE_RADIUS_M + (pulsing_factor * PULSE_AMPLITUDE_M)
                # Opacidad varía de 0.5 (127) a 1.0 (255)
                current_opacity_int = int((0.5 + (pulsing_factor * 0.5)) * 255) 
                # >>> FIN DE MODIFICACIÓN SOLICITADA: CÁLCULO DEL PULSO <<<

                st.subheader(f"🌐 Ubicación en Mapa de la Unidad **{nombre_unidad}**")


                # Estructura de dos columnas: Mapa (izq) y Tarjeta (der)
                map_col, card_col = st.columns([3, 1])

              
                with map_col:
                # ... definition of map_data and view_state ...
                    map_data = pd.DataFrame({
                        'lat': [lat],
                        'lon': [lon],
                    })

                    view_state = pdk.ViewState(
                        latitude=lat, 
                        longitude=lon,
                        zoom=18,
                        pitch=0,
                    )
    
                    # >>> INICIO DE MODIFICACIÓN SOLICITADA: CAPAS DEL PULSO <<<
                    # La capa estática original es reemplazada por dos capas dinámicas.
                    
                    # 1. Capa de Fondo Fijo (Base)
                    layer_unidad_fija = pdk.Layer(
                        "ScatterplotLayer",
                        data=map_data, 
                        get_position=["lon", "lat"], 
                        get_color="[255, 0, 0, 255]",      # Rojo Sólido (255 de opacidad)
                        get_radius=BASE_RADIUS_M,          # Radio fijo (100m)
                        radius_min_pixels=3,
                        pickable=True,
                        auto_highlight=True,
                        id="unidad_base_pulso",
                    )

                    # 2. Capa de Pulso (Superposición Dinámica)
                    layer_unidad_pulso = pdk.Layer(
                        "ScatterplotLayer",
                        data=map_data, 
                        get_position=["lon", "lat"],
                        # Color rojo con la opacidad dinámica calculada:
                        get_color=f"[255, 0, 0, {current_opacity_int}]", 
                        get_radius=current_radius, # <--- ¡Radio dinámico!
                        radius_min_pixels=3,
                        pickable=False, 
                        id="unidad_pulso_dinamico",
                    )

                    # >>> NUEVA SECCIÓN: CAPAS DE PERÍMETROS <<<
                    perimetros_layers = []
                    
                    # Procesar cada perímetro cargado
                    for nombre_perimetro, datos_perimetro in PERIMETROS_CARGADOS.items():
                        if 'geometria' in datos_perimetro:
                            geometria = datos_perimetro['geometria']
                            color_perimetro = datos_perimetro.get('color_perimetro', '#42F527') 
                            color_relleno_hex = datos_perimetro.get('color_relleno', '#9CF527')
                                                        
                            # Convertir color hex a RGB
                            # Convertir color de Borde (color_perimetro) a RGB
                            color_borde_hex = color_perimetro.replace('#', '')
                            r_borde = int(color_borde_hex[0:2], 16)
                            g_borde = int(color_borde_hex[2:4], 16)
                            b_borde = int(color_borde_hex[4:6], 16)
                            
                            # Convertir color de Relleno (color_relleno_hex) a RGB
                            color_relleno_sin_hash = color_relleno_hex.replace('#', '')
                            r_relleno = int(color_relleno_sin_hash[0:2], 16)
                            g_relleno = int(color_relleno_sin_hash[2:4], 16)
                            b_relleno = int(color_relleno_sin_hash[4:6], 16)
                            
                            if geometria['type'] == 'Polygon':
                                # Para Polígonos, crear una capa PolygonLayer
                                polygon_coords = geometria['coordinates'][0]  # Primer anillo de coordenadas
                                
                                # Crear DataFrame para el polígono
                                polygon_data = pd.DataFrame({
                                'polygon': [polygon_coords],
                                'nombre': [nombre_perimetro],
                                # Usamos las variables de relleno (r_relleno, g_relleno, b_relleno) para la columna 'fill_color'. 
                                # Mantenemos la opacidad en 50 (muy transparente)
                                'fill_color': [[r_relleno, g_relleno, b_relleno, 255]],
                                # Usamos las variables de borde para la columna 'line_color'
                                'line_color': [[r_borde, g_borde, b_borde, 255]] # Opacidad 255 (sólido) para el borde
                                })
                                
                                layer_perimetro = pdk.Layer(
                                    "PolygonLayer",
                                    data=polygon_data,
                                    get_polygon="polygon",
                                    get_fill_color="fill_color",
                                    get_line_color="[r, g, b, 50]",
                                    get_line_width=2,
                                    pickable=True,
                                    stroked=True,
                                    filled=True,
                                    auto_highlight=True,
                                    id=f"perimetro_{nombre_perimetro}",
                                )
                                perimetros_layers.append(layer_perimetro)
                                
                            elif geometria['type'] == 'LineString':
                                # Para LineStrings, crear una capa PathLayer
                                line_coords = geometria['coordinates']
                                
                                # Crear DataFrame para la línea
                                line_data = pd.DataFrame({
                                    'path': [line_coords],
                                    'nombre': [nombre_perimetro],
                                    # USAR r_borde, g_borde, b_borde
                                    # Opacidad (el último número): 255 es sólido, 50 es muy transparente.
                                    'color': [[r_borde, g_borde, b_borde, 255]] 
                                })
                                
                                layer_perimetro = pdk.Layer(
                                    "PathLayer",
                                    data=line_data,
                                    get_path="path",
                                    get_color="color",
                                    get_width=3,
                                    width_min_pixels=2,
                                    pickable=True,
                                    auto_highlight=True,
                                    id=f"perimetro_{nombre_perimetro}",
                                )
                                perimetros_layers.append(layer_perimetro)
                                
                                layer_perimetro = pdk.Layer(
                                    "PathLayer",
                                    data=line_data,
                                    get_path="path",
                                    get_color="color",
                                    get_width=3,
                                    width_min_pixels=2,
                                    pickable=True,
                                    auto_highlight=True,
                                    id=f"perimetro_{nombre_perimetro}",
                                )
                                perimetros_layers.append(layer_perimetro)
                    
                    # Combinar todas las capas: perímetros + unidad
                    all_layers = perimetros_layers + [layer_unidad_fija, layer_unidad_pulso]

                    # 🚨 Asegúrese de que el renderizado use pydeck:
                    st.pydeck_chart(pdk.Deck(
                        map_style='light',
                        initial_view_state=view_state,
                        layers=all_layers, # Incluye perímetros + capas de unidad
                        parameters={
                            'tooltip': {
                                'html': '<b>{nombre}</b><br/>Perímetro de {nombre}',
                                'style': {'backgroundColor': 'steelblue', 'color': 'white'}
                            }
                        }
                    ), use_container_width=True)
                    # >>> FIN DE MODIFICACIÓN SOLICITADA: CAPAS DEL PULSO <<<

                with card_col:
                    # 🖼️ RENDERIZAR TARJETA DE LA UNIDAD SELECCIONADA (usamos la lógica de tarjeta del loop de abajo)

                    # Definición de variables para el renderizado
                    nombre_unidad_display = selected_row['UNIDAD'].split('-')[0] if '-' in selected_row['UNIDAD'] else selected_row['UNIDAD']
                    velocidad_formateada = f"{selected_row['VELOCIDAD']:.0f}"
                    card_style = selected_row['CARD_STYLE']
                    estado_ignicion = selected_row['IGNICION']
                    velocidad_float = selected_row['VELOCIDAD']
                    stop_duration = selected_row['STOP_DURATION_MINUTES']
                    estado_display = estado_ignicion
                    color_velocidad = "white"

                    is_out_of_hq_status = not (selected_row['EN_SEDE_FLAG'] or selected_row['EN_RESGUARDO_SECUNDARIO_FLAG'] or selected_row['EN_VERTEDERO_FLAG'] or selected_row['EN_FUERA_PERIMETRO_FLAG'] or selected_row['ES_FALLA_GPS_FLAG'])

                    if selected_row['ES_FALLA_GPS_FLAG']:
                        color_velocidad = "black"
                    else:
                        if stop_duration > STOP_THRESHOLD_MINUTES and velocidad_float < 1.0 and is_out_of_hq_status:
                            parada_display = f"Parada Larga 🛑: {stop_duration:.0f} min"
                            card_style = "background-color: #FFC107; padding: 15px; border-radius: 5px; color: black; margin-bottom: 0px;"
                            estado_display = parada_display
                            color_velocidad = "black"
                        elif velocidad_float >= VELOCIDAD_CRITICA_AUDIO:
                            color_velocidad = "#D32F2F" # ROJO (Crítico)
                            estado_display = "EXCESO VELOCIDAD CRÍTICO 🚨"
                        elif velocidad_float >= SPEED_THRESHOLD_KPH:
                            color_velocidad = "#FF9800" # NARANJA (Alerta)
                            estado_display = "Alerta Velocidad ⚠️"

                    final_text_color = "black" if COLOR_VERTEDERO == "#FCC6BB" and selected_row['EN_VERTEDERO_FLAG'] else color_velocidad
                    if selected_row['ES_FALLA_GPS_FLAG']:
                        final_text_color = "black"

                    # Estructura del card HTML (usando el estilo unificado)
                    st.markdown(f'<div style="{card_style}">', unsafe_allow_html=True)
                    st.markdown(
                        f'<p style="text-align: center; margin-bottom: 10px; margin-top: 0px;">'
                        f'<span style="background-color: rgba(0,0,0,0.3); padding: 5px 10px; border-radius: 5px; font-size: 1.5em; font-weight: 900;">'
                        f'{nombre_unidad_display}'
                        f'</span>'
                        f'</p>',
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f'<p style="display: flex; align-items: center; justify-content: center; font-size: 1.9em; font-weight: 900; margin-top: 0px;">'
                        f'📍 <span style="margin-left: 8px; color: {final_text_color};">{velocidad_formateada} Km</span>'
                        f'</p>',
                        unsafe_allow_html=True
                    )
                    st.markdown(f'<p style="font-size: 1.0em; margin-top: 0px; opacity: 1.1; text-align: center; margin-bottom: 0px;">{estado_display}</p>', unsafe_allow_html=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                    # Detalles de la tarjeta (simplificados para esta vista)
                    with card_col:
                        # 1. Definir la unidad y la flota a usar
                        unidad_actual = selected_row['UNIDAD']
                        flota_a_usar = st.session_state.get('flota_seleccionada', 'Flota Principal')

                    # 2. Obtener la asignación actual usando las variables dinámicas
                    #    (¡Esta es la corrección clave!)
                        try:
                            assignment_data = get_current_unit_assignment(flota_a_usar, unidad_actual)
                        except Exception as e:
                    # Manejo de error si la función falla (ej. error de conexión a DB)
                            st.error(f"Error al obtener asignación para {unidad_actual}: {e}")
                            assignment_data = None # Asegurar que assignment_data sea None para el caso de no asignación

                    # Lógica de Presentación
                        with card_col:
                    # 3. Determinar el estado de la asignación y el conductor
                            if assignment_data:
                    # Asignación Encontrada
                                ficha = assignment_data.get('conductor_ficha', 'Sin Ficha')
        
                    # Obtener el mapa de conductores para buscar el nombre completo
                                conductores_map = get_all_conductores_db(flota_a_usar) 
                                conductor_info = conductores_map.get(ficha)

                                if conductor_info:
                                # Conductor encontrado en la base de datos de Conductores
                                    # Extraer el nombre y el apellido
                                    nombre_completo = conductor_info.get('nombre', '').strip()
                                    apellido_completo = conductor_info.get('apellido', '').strip()
        
                                # 🌟 LÓGICA CLAVE PARA TOMAR SOLO EL PRIMER NOMBRE Y PRIMER APELLIDO
        
                                # Divide el nombre por espacios y toma el primer elemento
                                    primer_nombre = nombre_completo.split(' ')[0] if nombre_completo else ''
        
                                # Divide el apellido por espacios y toma el primer elemento
                                    primer_apellido = apellido_completo.split(' ')[0] if apellido_completo else ''
                                    if primer_nombre and primer_apellido:
                                        conductor_display = f"{primer_nombre} {primer_apellido}"
                                    else:
                                # Si solo se pudo obtener la ficha o solo una parte del nombre/apellido
                                        conductor_display = f"**Conductor Asignado (Parcial):** Ficha {ficha}"                                    
                                else:
                    # Conductor no encontrado en la base de datos de Conductores (pero sí hay una ficha asignada)
                                    conductor_display = f"**Conductor Asignado (Ficha):** Ficha {ficha} (Conductor no encontrado en BD)"

                                ruta_display = assignment_data.get('ruta_nombre', 'Sin Ruta')
                                telefono_display = assignment_data.get('telefono', 'N/A')
        
                    # Horarios (la lógica de horarios se mantiene como la tenías)
                                hora_salida = assignment_data.get('hora_salida', '').strip()
                                hora_entrada = assignment_data.get('hora_entrada', '').strip()
                                horas_display = f"Salida: {hora_salida or 'N/A'} / Entrada: {hora_entrada or 'N/A'}"

                            else:
                    # Caso sin asignación o error en la obtención
                    # (Esto responde directamente a tu requisito de "no hay conductor asignado")
                                conductor_display = "**Sin Conductor Asignado** 🚫"
                                ruta_display = 'N/A'
                                telefono_display = 'N/A'
                                horas_display = 'N/A'
                         

                    st.caption(f"**Ruta:** {ruta_display}")
                    st.caption(f"**Conductor:** {conductor_display}")
                    st.caption(f"**Teléfono:** {telefono_display}")
                    st.caption(f"Dirección: **{selected_row['UBICACION_TEXTO']}**")
                    st.caption(f"Último Reporte: **{selected_row['LAST_REPORT_TIME_DISPLAY']}**")

                    # 🆕 STATUS CON EMOJIS COLORIDOS

                    status_display = construir_status_con_emojis(selected_row, not selected_row['EN_FUERA_PERIMETRO_FLAG'])
                    st.markdown(f"Status: **{status_display}**", unsafe_allow_html=True)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.caption(f"Coord: ({selected_row['LONGITUD']:.4f}, {selected_row['LATITUD']:.4f})\r\n")
                    # Detalle de Sentido en el que se Encuentra la Unidad.
                    with col2:
                        grados = selected_row['SENTIDO']
                        direccion_cardinal = grados_a_direccion(grados)
                        st.caption(f"Sentido: {direccion_cardinal} ({grados}°)")


                    # Botón de Deselección (opcional en la tarjeta)
                    st.button(
                        "Volver a la Lista",
                        on_click=clear_unit_to_locate,
                        key=f"btn_clear_unit_card_map_{unique_time_id}", # Clave única
                        use_container_width=True,
                        type="secondary"
                    )

                st.markdown("---")
                # B. Filtrar el DataFrame para mostrar solo las unidades NO seleccionadas
                df_data_final_render = df_data_mostrada[df_data_mostrada['UNIT_ID'] != unidad_a_ubicar_id]
                df_data_final_render = df_data_final_render.reset_index(drop=True)

                # C. Actualizar descripción para la sección inferior
                filtro_descripcion_final = f"Otras Unidades de la Flota ({filtro_descripcion})"

            else:
                # Si la unidad seleccionada no está en el dataframe (ej. filtrada por estado), volvemos a la vista normal
                st.session_state['unit_to_locate_id'] = None
                df_data_final_render = df_data_mostrada
                filtro_descripcion_final = filtro_descripcion

        # Vista por defecto (No hay unidad seleccionada O Fallback)
        else:
            df_data_final_render = df_data_mostrada
            filtro_descripcion_final = filtro_descripcion


        # -----------------------------------------------------------------------------------
        # 🚨 INICIO DEL RENDERIZADO DE TARJETAS (Común a ambas vistas) 🚨
        # -----------------------------------------------------------------------------------

        st.subheader(f"{filtro_descripcion_final} - ({len(df_data_final_render)})")

        if is_fallback:
            causa_display = df_data_original['UBICACION_TEXTO'].iloc[0].split(' - ')[1]
            st.error(f"🚨 **ERROR CRÍTICO DE CONEXIÓN/DATOS** 🚨")
            st.warning(f"La API de Foresight GPS no devolvió datos. Razón: **{causa_display}**.")

        elif df_data_final_render.empty:
             st.info(f"No hay unidades que cumplan el filtro **'{filtro_descripcion_final}'** para la flota **{flota_a_usar}** en este momento.")

        else:

            COLUMNS_PER_ROW = 5
            rows = [df_data_final_render[i:i + COLUMNS_PER_ROW] for i in range(0, len(df_data_final_render), COLUMNS_PER_ROW)]

            for row_index, row_data in enumerate(rows):
                cols = st.columns(COLUMNS_PER_ROW)

                for col_index, row_tuple in enumerate(row_data.iterrows()):
                    with cols[col_index]:

                        row = row_tuple[1]
                        unit_id_current = row['UNIT_ID'] # ID de la unidad actual

                        nombre_unidad_display = row['UNIDAD'].split('-')[0] if '-' in row['UNIDAD'] else row['UNIDAD']
                        velocidad_formateada = f"{row['VELOCIDAD']:.0f}"
                        card_style = row['CARD_STYLE']
                        estado_ignicion = row['IGNICION']
                        velocidad_float = row['VELOCIDAD']
                        stop_duration = row['STOP_DURATION_MINUTES']

                        estado_display = estado_ignicion
                        color_velocidad = "white"

                        # Determinar si la unidad NO está en ninguna zona crítica
                        is_out_of_hq_status = not (row['EN_SEDE_FLAG'] or row['EN_RESGUARDO_SECUNDARIO_FLAG'] or row['EN_VERTEDERO_FLAG'] or row['EN_FUERA_PERIMETRO_FLAG'] or row['ES_FALLA_GPS_FLAG'])

                        # Lógica de Precedencia: Falla GPS > Parada Larga > Exceso de Velocidad

                        if row['ES_FALLA_GPS_FLAG']:
                            color_velocidad = "black"

                        else:
                            # Resaltado visual para Parada Larga
                            if stop_duration > STOP_THRESHOLD_MINUTES and velocidad_float < 1.0 and is_out_of_hq_status:
                                parada_display = f"Parada Larga 🛑: {stop_duration:.0f} min"
                                card_style = "background-color: #FFC107; padding: 15px; border-radius: 5px; color: black; margin-bottom: 0px;"
                                estado_display = parada_display
                                color_velocidad = "black"

                            # Resaltado visual para Exceso de Velocidad
                            elif velocidad_float >= VELOCIDAD_CRITICA_AUDIO:
                                color_velocidad = "#D32F2F" # ROJO (Crítico)
                                estado_display = "EXCESO VELOCIDAD CRÍTICO 🚨"
                            elif velocidad_float >= SPEED_THRESHOLD_KPH:
                                color_velocidad = "#FF9800" # NARANJA (Alerta)
                                estado_display = "Alerta Velocidad ⚠️"

                        # Estructura del card HTML
                        st.markdown(f'<div style="{card_style}">', unsafe_allow_html=True)
                        st.markdown(
                            f'<p style="text-align: center; margin-bottom: 10px; margin-top: 0px;">'
                            f'<span style="background-color: rgba(0,0,0,0.3); padding: 5px 10px; border-radius: 5px; font-size: 1.5em; font-weight: 900;">'
                            f'{nombre_unidad_display}'
                            f'</span>'
                            f'</p>',
                            unsafe_allow_html=True
                        )
                        
                        final_text_color = "black" if COLOR_VERTEDERO == "#FCC6BB" and row['EN_VERTEDERO_FLAG'] else color_velocidad
                        if row['ES_FALLA_GPS_FLAG']:
                            final_text_color = "black"

                        st.markdown(
                            f'<p style="display: flex; align-items: center; justify-content: center; font-size: 1.9em; font-weight: 900; margin-top: 0px;">'
                            f'📍 <span style="margin-left: 8px; color: {final_text_color};">{velocidad_formateada} Km</span>'
                            f'</p>',
                            unsafe_allow_html=True
                        )
                        st.markdown(f'<p style="font-size: 1.0em; margin-top: 0px; opacity: 1.1; text-align: center; margin-bottom: 0px;">{estado_display}</p>', unsafe_allow_html=True)
                        st.markdown('</div>', unsafe_allow_html=True)

                        # Botón de Mapa
   
                        st.button(
                                "🗺️ Ubicacion",
                                # SE AÑADE unique_time_id PARA EVITAR DUPLICIDAD
                                key=f"map_btn_{unit_id_current}_{row_index}_{col_index}_{unique_time_id}",
                                on_click=set_unit_to_locate,
                                args=(unit_id_current,), # Pasa el UNIT_ID al callback
                                use_container_width=True,
                                type="secondary"
                        )
                       
                        with st.expander("Detalles ℹ️", expanded=False):
                            stop_timedelta_card = row['STOP_DURATION_TIMEDELTA']
                            tiempo_parado_display = f"{int(stop_timedelta_card.total_seconds() // 60)} min {int(stop_timedelta_card.total_seconds() % 60):02} seg"

                            st.caption(f"Tiempo Parado: **{tiempo_parado_display}**")

                            falla_motivo = row.get('FALLA_GPS_MOTIVO')
                            last_report_display = row.get('LAST_REPORT_TIME_DISPLAY')

                            if falla_motivo:
                                st.error(
                                    f"🛠 **Motivo Falla GPS:** {falla_motivo}\n\n"
                                    f"🕒 **Último Reporte:** {last_report_display}"
                                )

                            st.caption(f"Dirección: **{row['UBICACION_TEXTO']}**")
                            st.caption(f"Sentido: **{row['SENTIDO']:.0f}°** (Grados)")
                            
                            # 🆕 STATUS CON EMOJIS COLORIDOS
                            status_display = construir_status_con_emojis(row, not row['EN_FUERA_PERIMETRO_FLAG'], estado_display)
                            st.markdown(f"Status: **{status_display}**", unsafe_allow_html=True)
                            
                            if not falla_motivo:
                                st.caption(f"Último Reporte: **{last_report_display}**")

                            st.caption(f"Coordenadas: ({row['LONGITUD']:.4f}, {row['LATITUD']:.4f})\r\n")
                    
                st.markdown("<div style='height: 15px;'></div>", unsafe_allow_html=True)

    #st.markdown("---")

    # 🆕 VERIFICACIÓN DE PERÍMETROS CADA 10 SEGUNDOS
    st.session_state['perimetro_check_counter'] += TIME_SLEEP
    
    # Si han pasado 10 segundos o más, forzar una nueva verificación de datos
    if st.session_state['perimetro_check_counter'] >= 10:
        st.session_state['perimetro_check_counter'] = 0
        print(f"🔄 Verificación de perímetros ejecutada después de 10 segundos para flota: {flota_a_usar}")
        # Limpiar cache para forzar nueva carga de datos con verificación de perímetro
        st.cache_data.clear()
        # En Streamlit, no se puede usar continue - la página se re-renderiza automáticamente
    
    # PARÁMETRO DINÁMICO DE PAUSA
    # testigo permanece VERDE durante el time.sleep
    time.sleep(TIME_SLEEP)

    # script al final de cada renderización para forzar el inicio de la página.
    if st.session_state.get('scroll_to_top_flag', False):
        
    # borra el flag si ya se usó, para que no se ejecute continuamente
        del st.session_state['scroll_to_top_flag']

    # script para desplazar la página.
        st.markdown(
            """
            <script>
                window.scrollTo(0, 0);
            </script>
        """,
        unsafe_allow_html=True,
    )
