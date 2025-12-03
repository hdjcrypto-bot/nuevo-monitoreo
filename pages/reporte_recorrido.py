# --- IMPORTACIONES ---
import streamlit as st

# =========================================================
# === CONFIGURACIÓN Y UI DE STREAMLIT ===
# =========================================================

# 1. CONFIGURACIÓN DE PÁGINA (Debe ser la primera instrucción)
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
    page_title="Recorrido" 
)

# --- INYECCIÓN DE CSS PARA ESTILO Y TAMAÑO DE FUENTE Y ALINEACIÓN ---
# Manteniendo la estructura de estilo de fondo, sidebar y botones.
CSS_STYLE = """
<style>
/* Oculta la navegación multipágina en la barra lateral (IMPRESCINDIBLE) */
div[data-testid="stSidebarNav"] {
    display: none;
}

/* MODIFICACIÓN: Centra el contenido (texto e ícono) de los botones */
div.stButton > button {
    justify-content: center; /* Centra el contenido del botón Home */
}

/* REGLA CLAVE: IMAGEN DE FONDO A PANTALLA COMPLETA (Usando la imagen de flota original) */
.main-page-background {
    position: fixed; 
    top: 0;
    left: 0;
    width: 100vw; 
    height: 100vh; 
    background-image: url("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQDS4XWcShq3GWROpvXSeMxSVGMQor4118Ieg&s");
    background-size: cover; 
    background-position: center; 
    background-repeat: no-repeat;
    z-index: 1000; /* Fondo */
    opacity: 0.1; /* Hacemos el fondo más sutil */
}
    .stButton > button {
        border-radius: 10px;
        font-weight: bold;
        font-color: bold;
        font-size: #1e88e5;
        border: 1px solid #e0e0e0;
        padding: 5px 20px;
        transition: all 0.3s ease;
    }

/* REGLA CLAVE: IMPEDIR EL DESPLAZAMIENTO (SCROLL) */
body {
    overflow: hidden !important; 
}

/* Ajuste: Mantiene la barra lateral por encima del fondo */
.st-emotion-cache-1gto16h {
    z-index: 1001; 
}

/* MODIFICACIÓN CLAVE: Estilo para centrar y ocupar todo el espacio restante */
.center-image {
    display: flex;
    justify-content: center; /* Centrado horizontal */
    align-items: center; /* Centrado vertical */
    /* Calcula la altura disponible: 100% del viewport menos el espacio ocupado por
       la cabecera/títulos de Streamlit (usamos un aproximado de 180px) */
    height: calc(100vh - 180px); /* Ajustado para abarcar el resto de la página */
}

/* Reglas de diseño */
.block-container {
    padding-top: 1.5rem !important;
    padding-bottom: 0rem;
    padding-left: 1rem;
    padding-right: 1rem;
}

h1 { font-size: 3rem; font-weight: 800; color: #4CAF50; margin-bottom: 0px; }
</style>
"""

# Aplica el CSS
st.markdown(CSS_STYLE, unsafe_allow_html=True)

# ⚠️ INYECTAMOS EL DIV DE FONDO
st.markdown('<div class="main-page-background"></div>', unsafe_allow_html=True)

# --- CONFIGURACION DEL SIDEBAR (Menú de Navegación con Botones) ---
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
        '<p style="font-size: 24px; font-weight: bold; color: white; margin-bottom: 25px; margin-top: 20px; text-align: center;">Recorrido</p>', 
        unsafe_allow_html=True
    )
    st.title("PROXIMAMENTE")
    st.subheader ("En Esta Seccion")
    st.subheader ("Reporte Recorrido Unidades")

# --- Contenido Principal de la Página (RECORRIDO) ---

# Título de la Página
st.title("Recorrido")

# Subtítulo (Mensaje para mantener el formato)
st.markdown(
    '<p style="font-size: 1.5rem; font-weight: 300; color: #CCCCCC; margin-top: 0px; margin-bottom: 20px; text-shadow: 1px 1px 2px #000;">'
    'Visualización de rutas y trazado geográfico de las Unidades .'
    '</p>',
    unsafe_allow_html=True
)

# ⚠️ CENTRADO DE LA IMAGEN DE CONSTRUCCIÓN
# El contenedor ahora tiene la altura calc(100vh - 180px)
with st.container():
    st.markdown('<div class="center-image">', unsafe_allow_html=True)
    st.image(
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcS5jFFm4xh2hEBJcN2WfO_W50Rziwa6SN5x4Q&s",
        caption="Página en Construcción",
        use_column_width=True # Responsivo: la imagen usará el ancho disponible
    )
    st.markdown('</div>', unsafe_allow_html=True)
