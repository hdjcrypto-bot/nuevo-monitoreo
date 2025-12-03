import streamlit as st
import sqlite3
import os
from datetime import datetime

# ===================================
# === CONFIGURACIÓN DE PÁGINA ===
# ===================================
st.set_page_config(
    page_title="Gestión de Roles - GPS System",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===================================
# === CONFIGURACIÓN DE BASE DE DATOS ===
# ===================================
# Ruta a la base de datos GPS existente
GPS_DB_PATH = "gps.db"

# ===================================
# === FUNCIONES DE BASE DE DATOS ===
# ===================================
def init_db():
    """
    Verifica la conexión a la base de datos GPS existente
    No crea nuevas tablas, solo verifica que existan
    """
    try:
        conn = sqlite3.connect(GPS_DB_PATH)
        cursor = conn.cursor()
        
        # Verificar que la tabla 'roles' existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='roles'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            st.error("❌ Error: La tabla 'roles' no existe en la base de datos GPS.")
            st.info("💡 Por favor, verifica que la base de datos GPS tenga la tabla 'roles' configurada.")
            conn.close()
            return False
        
        # Verificar estructura de la tabla
        cursor.execute("PRAGMA table_info(roles)")
        columns = cursor.fetchall()
        
        expected_columns = ['id', 'nombre', 'permisos_acceso']
        existing_columns = [col[1] for col in columns]
        
        for expected_col in expected_columns:
            if expected_col not in existing_columns:
                st.error(f"❌ Error: La columna '{expected_col}' no existe en la tabla 'roles'.")
                st.info(f"📋 Columnas encontradas: {', '.join(existing_columns)}")
                conn.close()
                return False
        
        conn.close()
        return True
        
    except sqlite3.Error as e:
        st.error(f"❌ Error de base de datos: {str(e)}")
        return False
    except Exception as e:
        st.error(f"❌ Error al conectar con la base de datos: {str(e)}")
        return False

def create_role(name, permissions):
    """Crea un nuevo rol en la tabla roles de la base de datos GPS"""
    try:
        conn = sqlite3.connect(GPS_DB_PATH)
        cursor = conn.cursor()
        
        permissions_str = ','.join(permissions)
        cursor.execute(
            "INSERT INTO roles (nombre, permisos_acceso) VALUES (?, ?)",
            (name, permissions_str)
        )
        
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        st.error("Error: Ya existe un rol con ese nombre.")
        return False
    except Exception as e:
        st.error(f"Error al crear el rol: {str(e)}")
        return False

def get_roles():
    """Obtiene todos los roles de la tabla roles en la base de datos GPS"""
    try:
        conn = sqlite3.connect(GPS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, nombre, permisos_acceso FROM roles ORDER BY id")
        roles = cursor.fetchall()
        
        conn.close()
        return roles
    except Exception as e:
        st.error(f"Error al obtener roles: {str(e)}")
        return []

def update_role(role_id, new_name, new_permissions):
    """Actualiza un rol existente en la tabla roles"""
    try:
        conn = sqlite3.connect(GPS_DB_PATH)
        cursor = conn.cursor()
        
        permissions_str = ','.join(new_permissions)
        cursor.execute(
            "UPDATE roles SET nombre = ?, permisos_acceso = ? WHERE id = ?",
            (new_name, permissions_str, role_id)
        )
        
        if cursor.rowcount > 0:
            conn.commit()
            conn.close()
            return True
        else:
            st.error("Error: No se encontró el rol a actualizar.")
            return False
    except sqlite3.IntegrityError:
        st.error("Error: Ya existe un rol con ese nombre.")
        return False
    except Exception as e:
        st.error(f"Error al actualizar el rol: {str(e)}")
        return False

def delete_role(role_id):
    """Elimina un rol de la tabla roles en la base de datos GPS"""
    try:
        conn = sqlite3.connect(GPS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM roles WHERE id = ?", (role_id,))
        
        if cursor.rowcount > 0:
            conn.commit()
            conn.close()
            return True
        else:
            st.error("Error: No se encontró el rol a eliminar.")
            return False
    except Exception as e:
        st.error(f"Error al eliminar el rol: {str(e)}")
        return False

def get_role_by_id(role_id):
    """Obtiene un rol específico por ID"""
    try:
        conn = sqlite3.connect(GPS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, nombre, permisos_acceso FROM roles WHERE id = ?", (role_id,))
        role = cursor.fetchone()
        
        conn.close()
        return role
    except Exception as e:
        st.error(f"Error al obtener el rol: {str(e)}")
        return None

# ===================================
# === FUNCIONES DE DETECCIÓN DE PÁGINAS ===
# ===================================
def get_available_pages():
    """
    Detecta automáticamente las páginas disponibles en las carpetas 'page' y 'pages'
    Retorna una lista de nombres de páginas en mayúsculas
    """
    page_folders = ['page', 'pages']
    detected_pages = set()
    
    for folder in page_folders:
        if os.path.exists(folder):
            try:
                files = os.listdir(folder)
                for file in files:
                    if file.endswith('.py') and file not in ['roles.py', 'app.py', 'main.py', 'home.py', '__init__.py']:
                        # Remover extensión .py y convertir a mayúsculas
                        page_name = file.replace('.py', '').upper()
                        detected_pages.add(page_name)
            except Exception as e:
                st.warning(f"Error al leer la carpeta '{folder}': {str(e)}")
    
    return sorted(list(detected_pages))

def ensure_sample_data():
    """Asegura que haya datos de ejemplo en la tabla roles (solo si está vacía)"""
    roles = get_roles()
    if not roles:
        # Datos de ejemplo basados en páginas detectadas
        pages = get_available_pages()
        
        # Si no se detectan páginas, usar ejemplos genéricos para GPS
        if not pages:
            pages = ['DASHBOARD', 'UNIDADES', 'CONDUCTORES', 'RUTAS', 'REPORTES']
        
        sample_roles = [
            ("ADMINISTRADOR", pages[:3] if len(pages) >= 3 else pages),
            ("OPERADOR", pages[:2] if len(pages) >= 2 else pages),
            ("SUPERVISOR", [pages[0]] if pages else [])
        ]
        
        for role_name, permissions in sample_roles:
            create_role(role_name, permissions)

# ===================================
# === OBTENER PÁGINAS DISPONIBLES ===
# ===================================
PERMISSIONS_OPTIONS = get_available_pages()

# ===================================
# === INICIALIZACIÓN ===
# ===================================
# Verificar conexión con la base de datos GPS
if init_db():
    # Asegurar datos de ejemplo si la tabla está vacía
    ensure_sample_data()
else:
    st.stop()  # Detener la aplicación si no hay conexión válida

# ===================================
# === ESTILOS CSS PERSONALIZADOS ===
# ===================================
# 1. CONFIGURACIÓN DE PÁGINA (Debe ser la primera instrucción)
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
    page_title="Recorrido" 
)
st.markdown("""
<style>
    /* Oculta la navegación multipágina en la barra lateral (IMPRESCINDIBLE) */
    div[data-testid="stSidebarNav"] {
    display: none;
    }

    /* MODIFICACIÓN: Centra el contenido (texto e ícono) de los botones */
    div.stButton > button {
    justify-content: center; /* Centra el contenido del botón Home */
    }

    /* Contenedor principal con gradiente */
    .main-container {
        background: linear-gradient(135deg, #2a2a2a 0%, #1a1a1a 100%);
        min-height: 100vh;
        padding: 20px;
    }
    
    /* Encabezados principales */
    .main-header {
        color: #f0f0f0;
        text-align: center;
        padding: 20px 0;
        margin-bottom: 30px;
        border-bottom: 2px solid #999999;
    }
    
    /* Tarjetas de información */
    .role-info {
        background: rgba(255, 255, 255, 0.1);
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #666666;
        margin: 10px 0;
    }
    
    /* Estilos para formularios */
    .stForm {
        background: rgba(255, 255, 255, 0.05);
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #666666;
        margin: 10px 0;
    }
    
    /* Botones personalizados */
    .stButton > button {
        border-radius: 10px;
        font-weight: bold;
        font-color: bold;
        font-size: #1e88e5;
        border: 1px solid #e0e0e0;
        padding: 5px 20px;
        transition: all 0.3s ease;
    }
    
    
    
    /* Selectbox personalizado */
    .stSelectbox > div > div {
        background: rgba(255, 255, 255, 0.1);
        border: 1px solid #666666;
        border-radius: 8px;
    }
    
    /* Multiselect personalizado */
    .stMultiSelect > div > div {
        background: rgba(255, 255, 255, 0.1);
        border: 1px solid #666666;
        border-radius: 8px;
    }
    
    /* Text input personalizado */
    .stTextInput > div > div {
        background: rgba(255, 255, 255, 0.1);
        border: 1px solid #666666;
        border-radius: 8px;
    }
    
    /* Información general */
    .info-box {
        background: rgba(153, 153, 153, 0.2);
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #999999;
        margin: 10px 0;
    }
    
    /* Estadísticas */
    .stats-container {
        display: flex;
        justify-content: space-around;
        flex-wrap: wrap;
        gap: 20px;
        margin: 20px 0;
    }
    
    .stat-card {
        background: rgba(255, 255, 255, 0.1);
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #666666;
        text-align: center;
        flex: 1;
        min-width: 150px;
    }
    
    .stat-number {
        font-size: 2em;
        font-weight: bold;
        color: #f0f0f0;
    }
    
    .stat-label {
        color: #999999;
        font-size: 0.9em;
        margin-top: 5px;
    }
</style>
""", unsafe_allow_html=True)

with st.sidebar:

    if st.button("🏡 Home", use_container_width=True):
        st.cache_data.clear()
        st.switch_page("home.py") 
    st.markdown(
        '<p style="text-align: center; margin-bottom: 5px; margin-top: 5px;">'
        '<img src="https://fospuca.com/wp-content/uploads/2018/08/fospuca.png" alt="Logo Fospuca" style="width: 100%; max-width: 120px; height: auto; display: block; margin-left: auto; margin-right: auto;">'
        '</p>',
        unsafe_allow_html=True
    )
    # Título del Sidebar
    st.markdown(
        '<p style="font-size: 24px; font-weight: bold; color: white; margin-bottom: 25px; margin-top: 20px; text-align: center;">Roles de Acceso</p>', 
        unsafe_allow_html=True
    )
    #st.title("PROXIMAMENTE")
    #st.subheader ("En Esta Seccion")
    #st.subheader ("creacion y administracion de Usuarios")
    
roles_list = get_roles()
total_roles = len(roles_list)
total_permissions = len(PERMISSIONS_OPTIONS)

# Estadísticas


# Información sobre la base de datos GPS
st.markdown(f"""
<div class="info-box">
    <h4>Gestion de Accesos</h4>
    {f'<p>{", ".join(PERMISSIONS_OPTIONS)}</p>' if PERMISSIONS_OPTIONS else ''}
    <p style="color: #999999; font-size: 0.9em;">
</div>
""", unsafe_allow_html=True)

# ===================================
# === PESTAÑAS PRINCIPALES ===
# ===================================
tab_create, tab_update, tab_delete = st.tabs([
    "➕ Crear Rol", 
    "✏️ Modificar Rol", 
    "🗑️ Eliminar Rol"
])

# ===================================
# === 1. Pestaña de CREAR ROL ===
# ===================================
with tab_create:
    st.subheader("Crear Nuevo Rol de Usuario")
    
    with st.form("create_role_form", clear_on_submit=True):
        # Nombre del Rol
        role_name = st.text_input(
            "📝 Nombre del Rol", 
            placeholder="Ej: Administrador GPS, Supervisor de Flota, Operador de Rutas...",
            max_chars=50,
            help="Máximo 50 caracteres. Nombre único en el sistema."
        )
        
        # Permisos de Acceso
        if PERMISSIONS_OPTIONS:
            selected_permissions = st.multiselect(
                "✅ Permisos de Acceso - Páginas GPS",
                options=PERMISSIONS_OPTIONS,
                help="Selecciona las páginas a las que este rol tendrá acceso"
            )
        else:
            selected_permissions = st.multiselect(
                "✅ Permisos de Acceso - Páginas GPS (Ejemplos)",
                options=['DASHBOARD', 'UNIDADES', 'CONDUCTORES', 'RUTAS', 'REPORTES'],
                help="Selecciona las páginas a las que este rol tendrá acceso"
            )
        
        submitted = st.form_submit_button("Crear Rol 🚀", use_container_width=True)
        
        if submitted:
            if role_name.strip() and selected_permissions:
                if create_role(role_name.strip(), selected_permissions):
                    st.success(f"✅ Rol '{role_name.strip()}' creado exitosamente con {len(selected_permissions)} permisos de acceso.")
                    st.rerun()
            else:
                st.error("❌ El nombre del rol y los permisos no pueden estar vacíos.")

# ===================================
# === 2. Pestaña de MODIFICAR ROL ===
# ===================================
with tab_update:
    st.subheader("Modificar Rol Existente")
    
    # Crear diccionario de roles para facilitar la selección
    role_names = {f"ID {r[0]} - {r[1]}": r[0] for r in roles_list}
    
    if not roles_list:
        st.info("No hay roles para modificar. Cree uno primero.")
    else:
        # 1. Selección del Rol a Modificar
        selected_role_option = st.selectbox(
            "🔍 Seleccione el Rol a Modificar",
            options=list(role_names.keys()),
            key="update_role_select",
            help="El formato es: ID - Nombre del Rol."
        )

        if selected_role_option:
            selected_role_id = role_names[selected_role_option]
            
            # Obtener los datos actuales del rol seleccionado
            current_role = get_role_by_id(selected_role_id)
            
            if current_role:
                current_name = current_role[1]
                # Convertir la cadena de permisos a una lista y filtrar permisos válidos
                current_permissions_raw = current_role[2].split(',') if current_role[2] else []
                
                # FILTRAR SOLO PERMISOS VÁLIDOS
                valid_permissions = [perm.strip() for perm in current_permissions_raw if perm.strip() in PERMISSIONS_OPTIONS]
                
                # Si no hay permisos válidos pero hay opciones disponibles, usar las primeras
                if not valid_permissions and PERMISSIONS_OPTIONS:
                    valid_permissions = [PERMISSIONS_OPTIONS[0]]  # Usar al menos un permiso para evitar error

                # Mostrar información del rol seleccionado
                st.markdown(f"""
                <div class="role-info">
                    <p><strong>ID del Rol:</strong> `{selected_role_id}`</p>
                    <p><strong>Nombre actual:</strong> `{current_name}`</p>
                    <p><strong>Permisos actuales:</strong> {len(current_permissions_raw)} total ({len(valid_permissions)} válidos)</p>
                    <p><strong>Permisos válidos:</strong> {', '.join(valid_permissions) if valid_permissions else 'Ninguno'}</p>
                </div>
                """, unsafe_allow_html=True)

                with st.form("update_role_form"):
                    st.markdown(f"**ID del Rol:** `{selected_role_id}`", unsafe_allow_html=True)
                    
                    # Nuevo Nombre
                    updated_role_name = st.text_input(
                        "📝 Nuevo Nombre del Rol", 
                        value=current_name, 
                        max_chars=50,
                        key="update_name_input"
                    )
                    
                    # OPCIONES DISPONIBLES PARA EL MULTISELECT
                    available_options = PERMISSIONS_OPTIONS if PERMISSIONS_OPTIONS else ['DASHBOARD', 'UNIDADES', 'CONDUCTORES']
                    
                    # Nuevos Permisos de Acceso
                    updated_role_permissions = st.multiselect(
                        "✅ Nuevos Permisos de Acceso - Páginas GPS", 
                        options=available_options,
                        default=valid_permissions if valid_permissions else [],
                        key="update_permissions_multiselect"
                    )

                    submitted_update = st.form_submit_button("Actualizar Rol 🔄")

                    if submitted_update:
                        if updated_role_name.strip() and updated_role_permissions:
                            if update_role(selected_role_id, updated_role_name.strip(), updated_role_permissions):
                                st.success(f"Rol ID {selected_role_id} actualizado con éxito a '{updated_role_name.strip()}'.")
                                st.rerun()
                        else:
                            st.error("El nombre del rol y los permisos no pueden estar vacíos.")

# ===================================
# === 3. Pestaña de ELIMINAR ROL ===
# ===================================
with tab_delete:
    st.subheader("Eliminar Rol de Usuario")

    if not roles_list:
        st.info("No hay roles para eliminar.")
    else:
        with st.form("delete_role_form"):
            # 1. Selección del Rol a Eliminar
            selected_role_option_delete = st.selectbox(
                "🔪 Seleccione el Rol a Eliminar",
                options=list(role_names.keys()),
                key="delete_role_select",
                help="¡Esta acción es irreversible!"
            )
            
            if selected_role_option_delete:
                selected_role_id_delete = role_names[selected_role_option_delete]
                
                # Mostrar información del rol a eliminar
                role_to_delete = get_role_by_id(selected_role_id_delete)
                if role_to_delete:
                    st.markdown(f"""
                    <div class="role-info" style="border: 2px solid #ff6666;">
                        <p><strong>⚠️ ADVERTENCIA: Esta acción no se puede deshacer</strong></p>
                        <p><strong>ID del Rol:</strong> `{selected_role_id_delete}`</p>
                        <p><strong>Nombre:</strong> `{role_to_delete[1]}`</p>
                        <p><strong>Permisos de Acceso:</strong> {role_to_delete[2]}</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            submitted_delete = st.form_submit_button("🗑️ Eliminar Rol Permanentemente", type="primary")
            
            if submitted_delete:
                if delete_role(selected_role_id_delete):
                    st.success(f"✅ Rol ID {selected_role_id_delete} eliminado exitosamente de la base de datos GPS.")
                    st.rerun()

# Mostrar tabla de roles actual
if roles_list:
    st.subheader("📋 Resumen de Roles en Base de Datos GPS")
    
    # Crear DataFrame para mostrar en tabla
    import pandas as pd
    
    data_for_table = []
    for role in roles_list:
        permissions_count = len(role[2].split(',')) if role[2] else 0
        permissions_list = role[2].split(',') if role[2] else []
        
        data_for_table.append({
            'ID': role[0],
            'Nombre': role[1],
            'Permisos': permissions_list,
            'Cantidad': permissions_count
        })
    
    if data_for_table:
        df = pd.DataFrame(data_for_table)
        
        # Mostrar tabla con formato personalizado
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True
        )
        
else:
    st.info("No hay roles configurados en la base de datos GPS.")
