# =============================================================
# generar_reporte_pdf.py
# Genera reporte PDF profesional de Fase 1 del proyecto Leads WT25
# Paleta de colores: EGOS BI (https://egosbi.com/)
# Dependencias: fpdf2, plotly, kaleido
# Fecha: 2026-03-25 | Sesion 5
# =============================================================

import os
import tempfile
from fpdf import FPDF
import plotly.express as px
import plotly.graph_objects as go

# -- Paleta EGOS BI --
AZUL_OSCURO = (27, 58, 92)       # #1B3A5C - primario
AZUL_MEDIO = (41, 98, 155)       # #29629B - secundario
AZUL_CLARO = (86, 157, 214)      # #569DD6 - acento
GRIS_OSCURO = (51, 51, 51)       # #333333 - texto
GRIS_CLARO = (240, 242, 246)     # #F0F2F6 - fondo
BLANCO = (255, 255, 255)
VERDE = (46, 204, 113)           # #2ECC71 - exito
NARANJA = (243, 156, 18)         # #F39C12 - advertencia

# -- Datos del proyecto (extraidos de Snowflake) --
TOTAL_CUENTAS = 79
TOTAL_CONTACTOS = 93
CONTACTADOS = 0
CON_EMAIL = 93
CON_WHATSAPP = 24
INDUSTRIAS = 16
INTERACCIONES = 0

INDUSTRIA_DATA = [
    ("Fintech/Financial Services", 16),
    ("Technology", 16),
    ("Retail/Consumer Goods", 10),
    ("Manufacturing/Industrial", 9),
    ("Telecommunications", 6),
    ("Consulting/Prof. Services", 5),
    ("Food & Beverage", 4),
    ("Education/Research", 3),
    ("Government/Public Sector", 2),
    ("Automotive", 2),
    ("Media/Entertainment", 2),
    ("E-commerce", 1),
    ("Energy", 1),
    ("Healthcare/Pharma", 1),
    ("Logistics/Transportation", 1),
]

TAMANO_DATA = [
    ("Pequena (11-50)", 31),
    ("Enterprise (1000+)", 30),
    ("Grande (251-1000)", 13),
    ("Mediana (51-250)", 5),
]

PIPELINE_DATA = [
    ("PENDIENTE", 79),
]

# -- Directorio de salida --
OUTPUT_DIR = r"C:\Users\pedro\OneDrive - EGOS BI\Documentos\Snowflake\Leads_datos_SnowflakeWT25\output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


class ReportePDF(FPDF):
    """PDF personalizado con encabezado y pie de pagina EGOS BI."""

    def __init__(self):
        super().__init__()
        self._is_cover = True  # La primera pagina es portada (sin header/footer)

    def header(self):
        if self._is_cover:
            return
        # Barra superior azul oscuro
        self.set_fill_color(*AZUL_OSCURO)
        self.rect(0, 0, 210, 12, "F")
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*BLANCO)
        self.set_xy(10, 3)
        self.cell(0, 6, "EGOS BI  |  Leads Snowflake World Tour 2025  |  Fase 1", align="L")
        self.ln(15)

    def footer(self):
        if self._is_cover:
            return
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(128, 128, 128)
        pagina = self.page_no() - 1  # No contar portada
        self.cell(0, 10, f"EGOS BI - Reporte Confidencial  |  Pagina {pagina}", align="C")

    def titulo_seccion(self, texto, nivel=1):
        """Agrega un titulo de seccion con estilo EGOS BI."""
        if nivel == 1:
            self.set_font("Helvetica", "B", 16)
            self.set_text_color(*AZUL_OSCURO)
            self.cell(0, 10, texto, new_x="LMARGIN", new_y="NEXT")
            # Linea debajo
            self.set_draw_color(*AZUL_MEDIO)
            self.set_line_width(0.8)
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(4)
        elif nivel == 2:
            self.set_font("Helvetica", "B", 13)
            self.set_text_color(*AZUL_MEDIO)
            self.cell(0, 8, texto, new_x="LMARGIN", new_y="NEXT")
            self.ln(2)
        else:
            self.set_font("Helvetica", "B", 11)
            self.set_text_color(*GRIS_OSCURO)
            self.cell(0, 7, texto, new_x="LMARGIN", new_y="NEXT")
            self.ln(1)

    def texto(self, texto, bold=False):
        """Agrega un parrafo de texto."""
        style = "B" if bold else ""
        self.set_font("Helvetica", style, 10)
        self.set_text_color(*GRIS_OSCURO)
        self.multi_cell(0, 5, texto)
        self.ln(2)

    def kpi_box(self, x, y, w, h, label, valor, color=AZUL_OSCURO):
        """Dibuja una caja KPI con valor grande."""
        self.set_fill_color(*color)
        self.rect(x, y, w, h, "F")
        # Valor
        self.set_font("Helvetica", "B", 22)
        self.set_text_color(*BLANCO)
        self.set_xy(x, y + 4)
        self.cell(w, 12, str(valor), align="C")
        # Label
        self.set_font("Helvetica", "", 8)
        self.set_xy(x, y + 17)
        self.cell(w, 6, label, align="C")

    def tabla_simple(self, headers, data, col_widths=None):
        """Tabla con encabezado azul y filas alternadas."""
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)
        # Header
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(*AZUL_OSCURO)
        self.set_text_color(*BLANCO)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, align="C", fill=True)
        self.ln()
        # Datos
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*GRIS_OSCURO)
        for row_idx, row in enumerate(data):
            if row_idx % 2 == 0:
                self.set_fill_color(*GRIS_CLARO)
            else:
                self.set_fill_color(*BLANCO)
            for i, val in enumerate(row):
                align = "L" if i == 0 else "C"
                self.cell(col_widths[i], 6, str(val), border=1, align=align, fill=True)
            self.ln()

    def agregar_imagen(self, fig, ancho=180, alto=100):
        """Exporta un plotly figure como imagen y la agrega al PDF."""
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_path = tmp.name
        fig.write_image(tmp_path, width=ancho * 5, height=alto * 5, scale=2)
        # Verificar espacio en pagina
        if self.get_y() + alto + 10 > 270:
            self.add_page()
        self.image(tmp_path, x=15, w=ancho)
        self.ln(5)
        os.unlink(tmp_path)


def generar_grafica_industrias():
    """Grafica de barras horizontal: cuentas por industria."""
    nombres = [d[0] for d in reversed(INDUSTRIA_DATA)]
    valores = [d[1] for d in reversed(INDUSTRIA_DATA)]
    fig = px.bar(x=valores, y=nombres, orientation="h",
                 labels={"x": "Cuentas", "y": ""},
                 color_discrete_sequence=["#29629B"])
    fig.update_layout(
        title=None, margin=dict(l=10, r=10, t=10, b=30),
        font=dict(size=11), height=400, width=900,
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(gridcolor="#E0E0E0"),
    )
    fig.update_traces(texttemplate="%{x}", textposition="outside")
    return fig


def generar_grafica_tamano():
    """Grafica de pie: distribucion por tamano."""
    nombres = [d[0] for d in TAMANO_DATA]
    valores = [d[1] for d in TAMANO_DATA]
    fig = px.pie(values=valores, names=nombres,
                 color_discrete_sequence=["#1B3A5C", "#29629B", "#569DD6", "#93C5E8"],
                 hole=0.35)
    fig.update_layout(
        title=None, margin=dict(l=10, r=10, t=10, b=10),
        font=dict(size=12), height=350, width=500,
        paper_bgcolor="white",
    )
    fig.update_traces(textinfo="label+percent+value", textposition="outside")
    return fig


def generar_grafica_pipeline():
    """Grafica de barras: pipeline comercial."""
    status_labels = ["PENDIENTE", "CONTACTADO", "EN_SEGUIMIENTO", "CALIFICADO", "OPORTUNIDAD", "DESCARTADO"]
    status_vals = [79, 0, 0, 0, 0, 0]
    colors = ["#95a5a6", "#3498db", "#f39c12", "#2ecc71", "#9b59b6", "#e74c3c"]
    fig = go.Figure(data=[go.Bar(
        x=status_labels, y=status_vals,
        marker_color=colors,
        text=status_vals, textposition="outside"
    )])
    fig.update_layout(
        title=None, margin=dict(l=10, r=10, t=10, b=30),
        font=dict(size=11), height=300, width=700,
        plot_bgcolor="white", paper_bgcolor="white",
        yaxis=dict(gridcolor="#E0E0E0"),
    )
    return fig


def main():
    pdf = ReportePDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # =========================================================
    # PORTADA
    # =========================================================
    pdf.set_auto_page_break(auto=False)  # Sin salto automatico en portada
    pdf.add_page()
    # Rectangulo grande azul oscuro como fondo de portada
    pdf.set_fill_color(*AZUL_OSCURO)
    pdf.rect(0, 0, 210, 297, "F")

    # Titulo del proyecto
    pdf.set_font("Helvetica", "B", 32)
    pdf.set_text_color(*BLANCO)
    pdf.set_xy(15, 60)
    pdf.multi_cell(180, 14, "Leads Snowflake\nWorld Tour 2025")

    # Subtitulo
    pdf.set_font("Helvetica", "", 16)
    pdf.set_xy(15, 100)
    pdf.cell(180, 10, "Reporte de Proyecto - Fase 1")

    # Linea decorativa
    pdf.set_draw_color(*AZUL_CLARO)
    pdf.set_line_width(1.5)
    pdf.line(15, 118, 120, 118)

    # Detalles
    pdf.set_font("Helvetica", "", 12)
    pdf.set_xy(15, 130)
    detalles = [
        "Base de Datos: DB_LEADS_SNOWFLAKE_WT25",
        "Plataforma: Snowflake + Cortex AI",
        "Dashboard: Streamlit v3 (cloud-ready)",
        "79 cuentas | 93 contactos | 16 industrias",
        "",
        "Autor: Pedro Ulloa - EGOS BI",
        "Fecha: Marzo 2026",
        "Sesiones: 5 (23-25 marzo 2026)",
    ]
    for d in detalles:
        pdf.cell(180, 8, d)
        pdf.ln()

    # Footer de portada
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_xy(15, 250)
    pdf.cell(180, 8, "EGOS BI | Partner Oficial Snowflake en Mexico")
    pdf.set_xy(15, 258)
    pdf.cell(180, 8, "https://egosbi.com/")
    pdf.set_xy(15, 270)
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(180, 8, "Documento confidencial - Solo para uso interno EGOS BI")

    # =========================================================
    # PAGINA 2: RESUMEN EJECUTIVO
    # =========================================================
    pdf._is_cover = False  # A partir de aqui, header/footer normales
    pdf.set_auto_page_break(auto=True, margin=20)  # Restaurar salto automatico
    pdf.add_page()
    pdf.titulo_seccion("1. Resumen Ejecutivo")

    pdf.texto(
        "Este documento presenta los resultados de la Fase 1 del proyecto de gestion de leads "
        "del Snowflake World Tour 2025, celebrado en Ciudad de Mexico. El proyecto incluye la "
        "construccion de una base de datos dimensional en Snowflake, enriquecimiento de datos "
        "con Cortex AI (LLM llama3.1-8b), y un dashboard interactivo en Streamlit con "
        "generacion de pitch personalizado por cuenta."
    )

    pdf.texto(
        "La Fase 1 cubre: carga y limpieza de 100 registros originales del evento, "
        "deduplicacion (93 contactos unicos en 79 empresas), clasificacion de industria "
        "al 100% con IA, estimacion de tamano de empresa, generacion de insights por "
        "industria, y un dashboard funcional con CRUD completo y pitch IA."
    )

    # KPIs
    pdf.ln(3)
    pdf.titulo_seccion("KPIs Principales", nivel=2)
    y_kpi = pdf.get_y() + 2
    kpi_w = 25
    kpi_h = 28
    gap = 2.5
    start_x = 15

    kpis = [
        ("Cuentas", TOTAL_CUENTAS, AZUL_OSCURO),
        ("Contactos", TOTAL_CONTACTOS, AZUL_MEDIO),
        ("Con Email", CON_EMAIL, AZUL_CLARO),
        ("Con WhatsApp", CON_WHATSAPP, (41, 128, 185)),
        ("Industrias", INDUSTRIAS, AZUL_MEDIO),
        ("Contactados", CONTACTADOS, VERDE if CONTACTADOS > 0 else (149, 165, 166)),
        ("Interacciones", INTERACCIONES, NARANJA if INTERACCIONES > 0 else (149, 165, 166)),
    ]

    for i, (label, val, color) in enumerate(kpis):
        x = start_x + i * (kpi_w + gap)
        pdf.kpi_box(x, y_kpi, kpi_w, kpi_h, label, val, color)

    pdf.set_y(y_kpi + kpi_h + 8)

    # =========================================================
    # PAGINA 3: ARQUITECTURA
    # =========================================================
    pdf.titulo_seccion("2. Arquitectura de Datos")

    pdf.texto(
        "El proyecto sigue un modelo dimensional clasico con 4 capas en Snowflake:"
    )

    capas = [
        ("RAW", "Datos crudos del CSV original de ClickUp (100 filas). Landing zone sin transformaciones."),
        ("STAGING", "Datos limpios, deduplicados y normalizados. 93 contactos, 79 empresas unicas."),
        ("CORE", "Modelo dimensional: DIM_CUENTAS, DIM_CONTACTOS, DIM_INDUSTRIAS, DIM_CASOS_USO, "
                  "DIM_EJECUTIVOS, DIM_TERRITORIOS, REL_TERRITORIO_INDUSTRIA, FACT_INTERACCIONES."),
        ("ANALYTICS", "Vistas de consumo: VW_CUENTAS_CONTACTOS, VW_PITCH_PERSONALIZADO, VW_PIPELINE_COMERCIAL."),
    ]

    for esquema, desc in capas:
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(25, 6, esquema)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRIS_OSCURO)
        pdf.multi_cell(0, 5, desc)
        pdf.ln(1)

    pdf.ln(3)
    pdf.titulo_seccion("Tablas del Modelo", nivel=2)
    pdf.tabla_simple(
        ["Tabla", "Registros", "Descripcion"],
        [
            ("RAW.LEADS_RAW", "100", "Espejo del CSV original"),
            ("STAGING.CONTACTOS_STAGING", "93", "Contactos deduplicados y limpios"),
            ("STAGING.CUENTAS_STAGING", "79", "Empresas unicas extraidas"),
            ("CORE.DIM_CUENTAS", "79", "Dimension de cuentas (25 columnas)"),
            ("CORE.DIM_CONTACTOS", "93", "Dimension de contactos (21 columnas)"),
            ("CORE.DIM_INDUSTRIAS", "17", "Catalogo de industrias (16 activas + 1 default)"),
            ("CORE.DIM_CASOS_USO", "16", "Insights IA por industria"),
            ("CORE.DIM_EJECUTIVOS", "1", "Pedro Ulloa"),
            ("CORE.DIM_TERRITORIOS", "1", "Territorio general"),
            ("CORE.REL_TERRITORIO_INDUSTRIA", "15", "Asignacion territorio-industria"),
            ("CORE.FACT_INTERACCIONES", "0", "Historial de seguimiento (vacio)"),
        ],
        col_widths=[65, 20, 105]
    )

    # =========================================================
    # PAGINA: DISTRIBUCION POR INDUSTRIA
    # =========================================================
    pdf.add_page()
    pdf.titulo_seccion("3. Distribucion por Industria")

    pdf.texto(
        f"Las {TOTAL_CUENTAS} cuentas se distribuyeron en {INDUSTRIAS} industrias. "
        "La clasificacion se realizo en 3 pasos: 1) Cortex AI primera pasada (59/79), "
        "2) Cortex AI con contexto mexicano (10 mas), 3) clasificacion manual (10 restantes). "
        "Resultado: 100% de cuentas clasificadas."
    )

    fig_ind = generar_grafica_industrias()
    pdf.agregar_imagen(fig_ind, ancho=180, alto=80)

    pdf.ln(3)
    pdf.tabla_simple(
        ["Industria", "Cuentas", "% del Total"],
        [(n, str(v), f"{v/79*100:.1f}%") for n, v in INDUSTRIA_DATA],
        col_widths=[90, 30, 30]
    )

    # =========================================================
    # PAGINA: DISTRIBUCION POR TAMANO
    # =========================================================
    pdf.add_page()
    pdf.titulo_seccion("4. Distribucion por Tamano de Empresa")

    pdf.texto(
        "El tamano de empresa se estimo usando Cortex AI (llama3.1-8b) para 50 de 79 cuentas. "
        "Las 29 restantes recibieron un default de 'Pequena'. "
        "La distribucion muestra una mezcla equilibrada entre Enterprise y Pequena, "
        "con presencia significativa de empresas Grandes."
    )

    fig_tam = generar_grafica_tamano()
    pdf.agregar_imagen(fig_tam, ancho=120, alto=70)

    pdf.tabla_simple(
        ["Tamano", "Cuentas", "% del Total"],
        [(n, str(v), f"{v/79*100:.1f}%") for n, v in TAMANO_DATA],
        col_widths=[70, 30, 30]
    )

    # =========================================================
    # PAGINA: PIPELINE COMERCIAL
    # =========================================================
    pdf.ln(5)
    pdf.titulo_seccion("5. Pipeline Comercial")

    pdf.texto(
        "Al cierre de la Fase 1, las 79 cuentas se encuentran en estatus PENDIENTE. "
        "El pipeline esta listo para activarse con la Fase 2 de seguimiento comercial. "
        "El dashboard incluye herramientas para cambiar estatus, registrar interacciones "
        "y generar pitchs personalizados con IA."
    )

    fig_pipe = generar_grafica_pipeline()
    pdf.agregar_imagen(fig_pipe, ancho=160, alto=60)

    # =========================================================
    # PAGINA: DASHBOARD Y CORTEX AI
    # =========================================================
    pdf.add_page()
    pdf.titulo_seccion("6. Dashboard Streamlit + Cortex AI")

    pdf.titulo_seccion("Funcionalidades del Dashboard", nivel=2)

    funcionalidades = [
        "7 KPIs en tiempo real (cuentas, contactos, pendientes, WhatsApp, email, interacciones)",
        "5 graficas interactivas (industria, pipeline, tamano, geografia, heatmap)",
        "Tabla paginada de leads con empresa como link clickable",
        "Popup modal (@st.dialog) con detalle completo de cuenta",
        "6 acciones CRUD: marcar contactado, agregar contacto, cambiar principal, ubicacion, editar, registrar interaccion",
        "Generador de pitch personalizado con Cortex AI (llama3.1-8b)",
        "Siguiente accion sugerida por IA (1 oracion accionable)",
        "Envio de pitch por email (mailto link)",
        "Top 10 cuentas de mayor interes (score combinado datos + tamano)",
        "Top 5 por industria (grafico dinamico)",
        "Insights por industria con tabs (tendencias, retos, casos de uso, propuesta)",
        "Quick-action para marcar/desmarcar contactado",
        "Sidebar con filtros: estatus, contactado, industria, tamano, pais, busqueda",
    ]

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*GRIS_OSCURO)
    for f in funcionalidades:
        pdf.cell(5, 5, "")
        pdf.cell(0, 5, f"- {f}", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(3)
    pdf.titulo_seccion("Cortex AI - Pitch Personalizado", nivel=2)
    pdf.texto(
        "El pitch se genera con Snowflake Cortex COMPLETE (modelo llama3.1-8b). "
        "El prompt incluye: contexto de EGOS BI como consultora especializada en Modern Data Stack, "
        "referencia al Snowflake World Tour, reto relevante de la industria del lead, "
        "caso de uso de EGOS BI + Snowflake, y cierre casual proponiendo llamada de 20 minutos. "
        "Se prohiben menciones a demos y la frase 'potencialidad de Snowflake'."
    )

    pdf.titulo_seccion("Dependencias Tecnicas", nivel=2)
    deps = [
        ("Streamlit", "1.55.0", "Framework de dashboard"),
        ("Plotly", "6.6.0", "Graficas interactivas"),
        ("snowflake-connector-python", "4.3.0", "Conexion a Snowflake"),
        ("pandas", "2.3.3", "Manipulacion de datos"),
        ("keyring", "25.7.0", "Cache de MFA token"),
    ]
    pdf.tabla_simple(
        ["Libreria", "Version", "Uso"],
        deps,
        col_widths=[60, 25, 105]
    )

    # =========================================================
    # PAGINA: DEPLOY Y PROXIMOS PASOS
    # =========================================================
    pdf.add_page()
    pdf.titulo_seccion("7. Deploy en Streamlit Community Cloud")

    pdf.texto(
        "La Fase 1 incluye los artefactos necesarios para publicar el dashboard "
        "en Streamlit Community Cloud. El dashboard detecta automaticamente si corre "
        "en local (connection_name) o en la nube (st.secrets). "
        "Se creo documentacion detallada en docs/DEPLOY_STREAMLIT_CLOUD.txt."
    )

    pdf.titulo_seccion("Artefactos de Deploy", nivel=2)
    deploy_files = [
        ("requirements.txt", "Dependencias Python para la nube"),
        (".streamlit/config.toml", "Tema visual EGOS BI"),
        (".streamlit/secrets.toml.example", "Plantilla de credenciales Snowflake"),
        (".gitignore", "Exclusiones para repositorio Git"),
        ("docs/DEPLOY_STREAMLIT_CLOUD.txt", "Guia paso a paso de deploy"),
    ]
    pdf.tabla_simple(
        ["Archivo", "Descripcion"],
        deploy_files,
        col_widths=[80, 110]
    )

    pdf.ln(5)
    pdf.titulo_seccion("8. Proximos Pasos - Fase 2")

    pasos = [
        ("Enriquecimiento de datos", "LinkedIn, sitio web y tamano real via Apollo.io o web scraping"),
        ("Mejorar estimaciones", "Refinar tamano de las 29 empresas con defaults"),
        ("Seguimiento comercial", "Activar pipeline: contactar leads, registrar interacciones"),
        ("Reporte de avance", "Generar PDF de Fase 2 con metricas de conversion"),
        ("Automatizacion", "Envio programado de emails/WhatsApp con pitchs generados"),
        ("Multi-ejecutivo", "Preparar territorios por industria cuando se sumen mas vendedores"),
    ]

    for titulo_p, desc_p in pasos:
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(5, 6, "")
        pdf.cell(60, 6, f"- {titulo_p}")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRIS_OSCURO)
        pdf.cell(0, 6, desc_p, new_x="LMARGIN", new_y="NEXT")

    # =========================================================
    # PAGINA: HISTORIAL DE SESIONES
    # =========================================================
    pdf.ln(5)
    pdf.titulo_seccion("9. Historial de Sesiones")

    sesiones = [
        ("Sesion 1", "2026-03-23", "BD, esquemas, carga CSV, modelo dimensional, clasificacion IA, vistas"),
        ("Sesion 2", "2026-03-23", "Dashboard Streamlit v1: KPIs, graficas, CRUD, pitch IA (847 lineas)"),
        ("Sesion 3", "2026-03-23", "Dashboard v2: popup @st.dialog, checkbox contactado, mailto links"),
        ("Sesion 4", "2026-03-24", "Dashboard v3: contacto principal, empresa clickable, top 5, paginacion"),
        ("Sesion 5", "2026-03-25", "Pitch EGOS BI + Snowflake, deploy cloud-ready, documentacion PDF"),
    ]
    pdf.tabla_simple(
        ["Sesion", "Fecha", "Cambios principales"],
        sesiones,
        col_widths=[25, 25, 140]
    )

    # =========================================================
    # GUARDAR PDF
    # =========================================================
    output_path = os.path.join(OUTPUT_DIR, "Reporte_Leads_WT25_Fase1.pdf")
    pdf.output(output_path)
    print(f"PDF generado exitosamente: {output_path}")
    print(f"Tamano: {os.path.getsize(output_path):,} bytes")
    print(f"Paginas: {pdf.page_no()}")


if __name__ == "__main__":
    main()
