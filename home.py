# --- IMPORTACIONES ---
import streamlit as st

# =========================================================
# === FUNCIONES Y CONFIGURACIÓN ===
# =========================================================

# 1. CONFIGURACIÓN DE PÁGINA (Debe ser la primera instrucción)
# Se usa st.set_page_config una sola vez al inicio del script.
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
    page_title="Monitoreo de Flota" 
)

# --- CSS INLINE (Mejoras: Consolidación y Nombres de Clase) ---

# Se usa una sola cadena f-string para el CSS, mejorando la legibilidad.
CSS_STYLE = f"""
<style>
/* Oculta la navegación multipágina en la barra lateral (IMPRESCINDIBLE) */
div[data-testid="stSidebarNav"] {{
    display: none;
}}

/* Alinea el contenido (texto e ícono) de los botones a la izquierda */
div.stButton > button {{
    justify-content: flex-start;
}}

/* REGLA CLAVE: IMAGEN DE FONDO A PANTALLA COMPLETA */
.main-page-background {{
    position: fixed; 
    top: 0;
    left: 0;
    width: 100vw; 
    height: 100vh; 
    background-image: url("https://fospuca.com/wp-content/uploads/2022/03/fospuca-san-diego-3.jpg");
    background-size: cover; 
    background-position: center; 
    background-repeat: no-repeat;
    z-index: 1000;
}}

/* REGLA CLAVE: IMPEDIR EL DESPLAZAMIENTO (SCROLL) */
body {{
    overflow: hidden !important; 
}}

/* Ajuste: Mantiene la barra lateral por encima del fondo */
.st-emotion-cache-1gto16h {{
    z-index: 1001; 
}}

/* Reglas de diseño (Mejorado: Uso de una única regla para h1) */
h1 {{ 
    font-size: 3rem; 
    font-weight: 800; 
    color: #4CAF50; 
    margin-bottom: 0px; 
}}

/* Ajustes menores de padding */
.block-container {{
    padding-top: 1.5rem !important;
    padding-bottom: 0rem;
    padding-left: 1rem;
    padding-right: 1rem;
}}

</style>
"""

# Aplica el CSS
st.markdown(CSS_STYLE, unsafe_allow_html=True)

# ⚠️ INYECTAMOS EL DIV DE FONDO PRIMERO (Para que esté debajo del contenido)
st.markdown('<div class="main-page-background"></div>', unsafe_allow_html=True)


# =========================================================
# === SIDEBAR (Menú de Navegación con Botones) ===
# =========================================================

# NOTA DE OPTIMIZACIÓN: Los botones están usando `st.switch_page("pages/home.py")` 
# lo cual refresca la página actual. Se deben corregir las rutas para apuntar
# a las páginas correctas (e.g., reporte_recorrido.py, inspeccion_vehicular.py).

with st.sidebar:
    # 1. Logo
    st.markdown(
        '<p style="text-align: center; margin-bottom: 5px; margin-top: 5px;">'
        '<img src="https://fospuca.com/wp-content/uploads/2018/08/fospuca.png" alt="Logo Fospuca" style="width: 100%; max-width: 120px; height: auto; display: block; margin-left: auto; margin-right: auto;">'
        '</p>',
        unsafe_allow_html=True
    )
    # 2. Título del Sidebar
    st.markdown(
        '<p style="font-size: 24px; font-weight: bold; color: white; margin-bottom: 25px; margin-top: 20px; text-align: center;">Panel de Control</p>', 
        unsafe_allow_html=True
    )
    
    # 3. Botones de Navegación
    
    # 🚛 Dashboard
    if st.button("🚛 Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")
        
    # ⚠️ Reporte de Excesos de Velocidad
    if st.button("⚠️ Reporte de Excesos de Velocidad", use_container_width=True):
        st.switch_page("pages/reporte_excesos.py")
        
    # 🛑 Reporte de Paradas Largas
    if st.button("🛑 Reporte de Paradas Largas", use_container_width=True):
        st.switch_page("pages/reporte_paradas_largas.py")
         
    # 🗺️ Reporte de Recorrido 
    # *Se asume que la página de reporte de recorrido es "reporte_recorrido.py"*
    if st.button("🗺️ Reporte de Recorrido", use_container_width=True):
        st.switch_page("pages/reporte_recorrido.py")

    # 📝 Inspección Vehicular 
    # *Se asume que la página de inspección vehicular es "inspeccion_vehicular.py"*
    if st.button("📝 Inspección Vehicular", use_container_width=True):
        st.switch_page("pages/inspeccion_vehicular.py")

    # 📄 Informes 
    # *Se asume que la página de informes es "informes.py"*
    if st.button("📄 Informes", use_container_width=True):
        st.switch_page("pages/informes.py")
        
    # 📄 Usuarios 
    # *Se asume que la página de informes es "informes.py"*
    if st.button("👤 Usuarios", use_container_width=True):
        st.switch_page("pages/usuarios.py")
        
    # 📄 Roles
    # *Se asume que la página de informes es "informes.py"*
    if st.button("⚙️ Roles", use_container_width=True):
        st.switch_page("pages/roles.py")

    # Eliminada la duplicación del botón "Inspección Vehicular"


# =========================================================
# === Contenido Principal de la Página ===
# =========================================================

# Título de Alto Impacto
st.title("Monitoreo de Flota")

# Subtítulo (El mensaje principal)
st.markdown(
    '<p style="font-size: 1.5rem; font-weight: 300; color: #CCCCCC; margin-top: 0px; margin-bottom: 20px; text-shadow: 1px 1px 2px #000;">'
    '<strong>Más versátil, rápido y eficiente.</strong>'
    '</p>',
    unsafe_allow_html=True
)

# Mensaje de Bienvenida / Llamada a la Acción
st.info("""
Acceda a **información crítica rápido y seguro**. Utilice el panel de la izquierda para una navegación rápida.
""")

# Imagen (Se comenta la imagen grande para priorizar la velocidad de carga en la página de inicio)
# *La imagen grande del fondo ya está cargada en el CSS. Evitar duplicar la carga de la misma imagen.*
# st.image(
#     "https://fospuca.com/wp-content/uploads/2022/03/fospuca-san-diego-3.jpg",
#     caption="Flota de Recolección de Fospuca",
#     use_container_width=True
# )
