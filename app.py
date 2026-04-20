# =============================================================
# dashboard_leads_wt25.py  (v7 - EGOS BI Lead Manager: multi-evento, carga CSV, vision general)
# Dashboard interactivo para gestión de Leads - EGOS BI
# Incluye: KPIs interactivos, graficas, gestión de contactos, pitch IA, pipeline
# v6: Dialog no se cierra al ejecutar acciones, pitch IA incluye info sitio web
# Compatible: Local (connection_name) y Streamlit Community Cloud (st.secrets)
# Creado: 2026-03-23 | Actualizado: 2026-03-30 | Conexión: TXA18114
# Proyecto: Leads Snowflake WT25 - EGOS BI
# =============================================================

import os
import re
import urllib.parse
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px
import snowflake.connector
import requests
from datetime import datetime
from cryptography.hazmat.primitives import serialization

# -- Configuración de página --
st.set_page_config(
    page_title="EGOS BI Lead Manager",
    page_icon="\u2744",
    layout="wide",
    initial_sidebar_state="expanded"
)

CONN_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME", "TXA18114").strip()
DB = "DB_LEADS_SNOWFLAKE_WT25"

# Detectar si estamos en Streamlit Community Cloud (st.secrets disponible)
_USE_SECRETS = False
try:
    if "snowflake" in st.secrets:
        _USE_SECRETS = True
except Exception:
    pass

# =============================================================
# FUNCIONES DE CONEXION Y DATOS
# =============================================================

def get_connection():
    """Conexión fresca. Usa st.secrets (key-pair) en cloud, connection_name en local."""
    if _USE_SECRETS:
        sf = st.secrets["snowflake"]
        # La key puede venir con \n literales o con saltos reales (TOML triple-quote)
        raw_key = sf["private_key"]
        if "\\n" in raw_key:
            raw_key = raw_key.replace("\\n", "\n")
        private_key_pem = raw_key.strip().encode("utf-8")
        private_key = serialization.load_pem_private_key(private_key_pem, password=None)
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return snowflake.connector.connect(
            account=sf["account"],
            user=sf["user"],
            private_key=private_key_bytes,
            warehouse=sf.get("warehouse", "COMPUTE_WH"),
            database=sf.get("database", DB),
            role=sf.get("role", "LEADS_DASHBOARD_ROLE"),
        )
    return snowflake.connector.connect(connection_name=CONN_NAME)


@st.cache_data(ttl=120)
def load_data():
    """Carga datos principales: cuentas + mejor contacto + industria."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        WITH best_contact AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY CUENTA_ID
                    ORDER BY
                        CASE WHEN ES_PRINCIPAL = TRUE THEN 0 ELSE 1 END ASC,
                        CASE NIVEL_CARGO
                            WHEN 'C-Level' THEN 1 WHEN 'Director' THEN 2 WHEN 'Manager' THEN 3
                            WHEN 'Lead' THEN 4 WHEN 'Architect' THEN 5 WHEN 'Senior' THEN 6
                            WHEN 'Scientist' THEN 7 WHEN 'Engineer' THEN 8 WHEN 'Analyst' THEN 9
                            ELSE 99
                        END ASC,
                        CASE WHEN WHATSAPP IS NOT NULL THEN 0 ELSE 1 END ASC
                ) AS RN
            FROM {DB}.CORE.DIM_CONTACTOS
        )
        SELECT
            c.CUENTA_ID, c.ACCT_NAME, c.INDUSTRIA_ID, i.INDUSTRIA_NOMBRE,
            c.TAMANO_EMPRESA, c.NUM_EMPLEADOS_ESTIMADO, c.REVENUE_ESTIMADO_USD,
            c.UBICACION, c.PAIS, c.ESTADO, c.CIUDAD,
            c.SITIO_WEB, c.LINKEDIN_EMPRESA,
            c.ESTATUS, c.MOTIVO_DESCALIFICACION, c.EJECUTIVO_ID,
            c.FECHA_PRIMER_CONTACTO, c.FECHA_ULTIMO_CONTACTO,
            c.NOTAS, c.FUENTE_CLASIFICACION, c.FUENTE_TAMANO, c.FUENTE_LEAD,
            c.EVENTO_ID, COALESCE(ev.NOMBRE_EVENTO, c.FUENTE_LEAD) AS NOMBRE_EVENTO,
            bc.CONTACTO_ID, bc.NOMBRE_COMPLETO AS CONTACTO_NOMBRE,
            bc.CARGO AS CONTACTO_CARGO, bc.NIVEL_CARGO AS CONTACTO_NIVEL,
            bc.EMAIL AS CONTACTO_EMAIL, bc.WHATSAPP AS CONTACTO_WHATSAPP,
            bc.CONTACTADO, bc.RESPUESTA, bc.METODO_CONTACTO,
            (SELECT COUNT(*) FROM {DB}.CORE.DIM_CONTACTOS ct WHERE ct.CUENTA_ID = c.CUENTA_ID) AS NUM_CONTACTOS,
            (SELECT COUNT(*) FROM {DB}.CORE.DIM_CONTACTOS ct WHERE ct.CUENTA_ID = c.CUENTA_ID AND ct.CONTACTADO = TRUE) AS CONTACTOS_CONTACTADOS,
            (SELECT COUNT(*) FROM {DB}.CORE.FACT_INTERACCIONES fi WHERE fi.CUENTA_ID = c.CUENTA_ID) AS NUM_INTERACCIONES
        FROM {DB}.CORE.DIM_CUENTAS c
        JOIN {DB}.CORE.DIM_INDUSTRIAS i ON c.INDUSTRIA_ID = i.INDUSTRIA_ID
        LEFT JOIN {DB}.CORE.DIM_EVENTOS ev ON c.EVENTO_ID = ev.EVENTO_ID
        LEFT JOIN best_contact bc ON c.CUENTA_ID = bc.CUENTA_ID AND bc.RN = 1
        ORDER BY c.ACCT_NAME
    """)
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)

    cur.execute(f"""
        SELECT cu.CASO_ID, i.INDUSTRIA_NOMBRE, cu.TENDENCIAS_INDUSTRIA,
               cu.RETOS_PRINCIPALES, cu.CASOS_USO_SNOWFLAKE, cu.PROPUESTA_VALOR
        FROM {DB}.CORE.DIM_CASOS_USO cu
        JOIN {DB}.CORE.DIM_INDUSTRIAS i ON cu.INDUSTRIA_ID = i.INDUSTRIA_ID
    """)
    cols_cu = [d[0] for d in cur.description]
    df_casos = pd.DataFrame(cur.fetchall(), columns=cols_cu)

    cur.execute(f"""
        SELECT EVENTO_ID, NOMBRE_EVENTO, FECHA_EVENTO, DESCRIPCION
        FROM {DB}.CORE.DIM_EVENTOS
        ORDER BY FECHA_EVENTO DESC
    """)
    cols_ev = [d[0] for d in cur.description]
    df_eventos = pd.DataFrame(cur.fetchall(), columns=cols_ev)
    cur.close()
    conn.close()
    return df, df_casos, df_eventos


def load_contactos_cuenta(cuenta_id):
    """Carga todos los contactos de una cuenta especifica."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT CONTACTO_ID, PRIMER_NOMBRE, APELLIDO, NOMBRE_COMPLETO, CARGO,
               NIVEL_CARGO, DEPARTAMENTO, EMAIL, WHATSAPP, LINKEDIN_PERFIL,
               CONTACTADO, FECHA_CONTACTO, METODO_CONTACTO, RESPUESTA, NOTAS_CONTACTO,
               ES_PRINCIPAL
        FROM {DB}.CORE.DIM_CONTACTOS
        WHERE CUENTA_ID = {cuenta_id}
        ORDER BY ES_PRINCIPAL DESC, PRIORIDAD ASC, NOMBRE_COMPLETO
    """)
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


def load_interacciones_cuenta(cuenta_id):
    """Carga historial de interacciones de una cuenta."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT fi.INTERACCION_ID, fi.TIPO, fi.FECHA, fi.DESCRIPCION,
               fi.RESULTADO, fi.SIGUIENTE_ACCION, fi.FECHA_SIGUIENTE,
               ct.NOMBRE_COMPLETO AS CONTACTO
        FROM {DB}.CORE.FACT_INTERACCIONES fi
        LEFT JOIN {DB}.CORE.DIM_CONTACTOS ct ON fi.CONTACTO_ID = ct.CONTACTO_ID
        WHERE fi.CUENTA_ID = {cuenta_id}
        ORDER BY fi.FECHA DESC
    """)
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


@st.cache_data(ttl=120)
def load_all_interacciones():
    """Carga todas las interacciones con nombre de empresa y contacto."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT fi.INTERACCION_ID, fi.FECHA, ca.ACCT_NAME AS EMPRESA,
               ct.NOMBRE_COMPLETO AS CONTACTO, fi.TIPO, fi.DESCRIPCION,
               fi.RESULTADO, fi.SIGUIENTE_ACCION
        FROM {DB}.CORE.FACT_INTERACCIONES fi
        JOIN {DB}.CORE.DIM_CUENTAS ca ON fi.CUENTA_ID = ca.CUENTA_ID
        LEFT JOIN {DB}.CORE.DIM_CONTACTOS ct ON fi.CONTACTO_ID = ct.CONTACTO_ID
        ORDER BY fi.FECHA DESC
    """)
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    conn.close()
    return df


# =============================================================
# FUNCIONES DE ESCRITURA (UPDATE / INSERT)
# =============================================================

def marcar_contactado(contacto_id, cuenta_id, metodo, resultado, notas, ejecutivo_id=1):
    """Marca un contacto como contactado y registra la interaccion."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            UPDATE {DB}.CORE.DIM_CONTACTOS
            SET CONTACTADO = TRUE, FECHA_CONTACTO = CURRENT_TIMESTAMP(),
                METODO_CONTACTO = %s, RESPUESTA = %s, NOTAS_CONTACTO = %s,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CONTACTO_ID = %s
        """, (metodo, resultado, notas, contacto_id))
        cur.execute(f"""
            UPDATE {DB}.CORE.DIM_CUENTAS
            SET ESTATUS = CASE WHEN ESTATUS = 'PENDIENTE' THEN 'CONTACTADO' ELSE ESTATUS END,
                FECHA_PRIMER_CONTACTO = COALESCE(FECHA_PRIMER_CONTACTO, CURRENT_TIMESTAMP()),
                FECHA_ULTIMO_CONTACTO = CURRENT_TIMESTAMP(),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CUENTA_ID = %s
        """, (cuenta_id,))
        cur.execute(f"""
            INSERT INTO {DB}.CORE.FACT_INTERACCIONES
                (CUENTA_ID, CONTACTO_ID, EJECUTIVO_ID, TIPO, DESCRIPCION, RESULTADO)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (cuenta_id, contacto_id, ejecutivo_id, metodo, notas, resultado))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error al guardar: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def marcar_contactado_simple(contacto_id, cuenta_id, contactado_valor):
    """Marca/desmarca un contacto desde el checkbox de la tabla."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            UPDATE {DB}.CORE.DIM_CONTACTOS
            SET CONTACTADO = %s,
                FECHA_CONTACTO = CASE WHEN %s = TRUE THEN CURRENT_TIMESTAMP() ELSE FECHA_CONTACTO END,
                METODO_CONTACTO = CASE WHEN %s = TRUE AND METODO_CONTACTO IS NULL THEN 'CHECKBOX' ELSE METODO_CONTACTO END,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CONTACTO_ID = %s
        """, (contactado_valor, contactado_valor, contactado_valor, contacto_id))
        if contactado_valor:
            cur.execute(f"""
                UPDATE {DB}.CORE.DIM_CUENTAS
                SET ESTATUS = CASE WHEN ESTATUS = 'PENDIENTE' THEN 'CONTACTADO' ELSE ESTATUS END,
                    FECHA_PRIMER_CONTACTO = COALESCE(FECHA_PRIMER_CONTACTO, CURRENT_TIMESTAMP()),
                    FECHA_ULTIMO_CONTACTO = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE CUENTA_ID = %s
            """, (cuenta_id,))
            cur.execute(f"""
                INSERT INTO {DB}.CORE.FACT_INTERACCIONES
                    (CUENTA_ID, CONTACTO_ID, EJECUTIVO_ID, TIPO, DESCRIPCION, RESULTADO)
                VALUES (%s, %s, 1, 'CHECKBOX', 'Marcado como contactado desde el dashboard', 'CONTACTADO')
            """, (cuenta_id, contacto_id))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def agregar_contacto(cuenta_id, nombre, apellido, cargo, email, whatsapp, linkedin):
    """Agrega un nuevo contacto a una cuenta."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        nombre_completo = f"{nombre} {apellido}".strip()
        cur.execute(f"""
            INSERT INTO {DB}.CORE.DIM_CONTACTOS
                (CUENTA_ID, PRIMER_NOMBRE, APELLIDO, NOMBRE_COMPLETO, CARGO,
                 EMAIL, WHATSAPP, LINKEDIN_PERFIL, FUENTE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'MANUAL')
        """, (cuenta_id, nombre, apellido, nombre_completo, cargo, email,
              whatsapp if whatsapp else None, linkedin if linkedin else None))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error al agregar contacto: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def actualizar_cuenta(cuenta_id, campos):
    """Actualiza campos de una cuenta (excepto ACCT_NAME)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        sets = []
        vals = []
        for col, val in campos.items():
            if col == 'ACCT_NAME':
                continue
            sets.append(f"{col} = %s")
            vals.append(val if val else None)
        sets.append("UPDATED_AT = CURRENT_TIMESTAMP()")
        vals.append(cuenta_id)
        sql = f"UPDATE {DB}.CORE.DIM_CUENTAS SET {', '.join(sets)} WHERE CUENTA_ID = %s"
        cur.execute(sql, vals)
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error al actualizar: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def registrar_interaccion(cuenta_id, contacto_id, tipo, descripcion, resultado, siguiente_accion):
    """Registra una nueva interaccion en FACT_INTERACCIONES."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {DB}.CORE.FACT_INTERACCIONES
                (CUENTA_ID, CONTACTO_ID, EJECUTIVO_ID, TIPO, DESCRIPCION, RESULTADO, SIGUIENTE_ACCION)
            VALUES (%s, %s, 1, %s, %s, %s, %s)
        """, (cuenta_id, contacto_id if contacto_id else None, tipo, descripcion, resultado, siguiente_accion))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def cambiar_contacto_principal(cuenta_id, nuevo_principal_id):
    """Cambia el contacto principal de una cuenta (solo 1 por cuenta)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            UPDATE {DB}.CORE.DIM_CONTACTOS
            SET ES_PRINCIPAL = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CUENTA_ID = %s AND ES_PRINCIPAL = TRUE
        """, (cuenta_id,))
        cur.execute(f"""
            UPDATE {DB}.CORE.DIM_CONTACTOS
            SET ES_PRINCIPAL = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CONTACTO_ID = %s AND CUENTA_ID = %s
        """, (nuevo_principal_id, cuenta_id))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def make_mailto(email, subject="", body=""):
    """Genera un mailto: link URL-encoded."""
    if not email or not str(email).strip():
        return None
    email = str(email).strip()
    params = {}
    if subject:
        params["subject"] = subject
    if body:
        params["body"] = body
    if params:
        return f"mailto:{email}?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    return f"mailto:{email}"


def email_link_md(email):
    """Retorna markdown con mailto link para un email."""
    if not email or not str(email).strip():
        return ""
    e = str(email).strip()
    return f"[{e}](mailto:{e})"


def whatsapp_link_md(numero):
    """Retorna markdown con link wa.me para abrir WhatsApp."""
    if not numero or not str(numero).strip():
        return ""
    raw = str(numero).strip()
    limpio = raw.replace("+", "").replace(" ", "").replace("-", "")
    return f"[{raw}](https://wa.me/{limpio})"


@st.cache_data(ttl=300)
def fetch_website_text(url, max_chars=1500):
    """Extrae texto limpio del sitio web de una empresa para contexto del pitch.
    Retorna string con descripcion de la empresa o cadena vacia si falla."""
    if not url or not str(url).strip():
        return ""
    url = str(url).strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        resp = requests.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (compatible; EgosBI-Dashboard/1.0)"
        })
        resp.raise_for_status()
        html = resp.text[:50000]  # Limitar HTML a 50k chars para no saturar memoria
        # Remover scripts, styles y tags HTML
        html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<[^>]+>', ' ', html)
        # Limpiar entidades HTML y espacios multiples
        html = html.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        text = re.sub(r'\s+', ' ', html).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "..."
        return text
    except Exception:
        return ""


def inferir_sitio_web(dominio):
    """Intenta descubrir el sitio web de una empresa a partir del dominio del email.
    Prueba variantes comunes y valida con HTTP HEAD. Retorna URL valida o ''."""
    if not dominio or not dominio.strip():
        return ""
    dominio = dominio.strip().lower()
    # Dominios genericos que no corresponden a una empresa
    genericos = {"gmail.com", "hotmail.com", "outlook.com", "yahoo.com",
                 "live.com", "icloud.com", "protonmail.com", "aol.com",
                 "live.com.mx", "hotmail.es", "outlook.es", "yahoo.com.mx"}
    if dominio in genericos:
        return ""
    # Construir lista de URLs candidatas
    candidatas = []
    # Primero el dominio tal cual
    candidatas.append(f"https://www.{dominio}")
    candidatas.append(f"https://{dominio}")
    # Si tiene subdominio (ej: mx.att.com, correo.uia.mx), probar dominio padre
    partes = dominio.split(".")
    if len(partes) > 2:
        dominio_padre = ".".join(partes[-2:])  # ej: att.com, uia.mx
        candidatas.append(f"https://www.{dominio_padre}")
        candidatas.append(f"https://{dominio_padre}")
    for url in candidatas:
        try:
            resp = requests.head(url, timeout=5, allow_redirects=True, headers={
                "User-Agent": "Mozilla/5.0 (compatible; EgosBI-Dashboard/1.0)"
            })
            if resp.status_code < 400:
                return url
        except Exception:
            continue
    return ""


# =============================================================
# DIALOG: TARJETA DE DETALLE DE CUENTA (popup)
# =============================================================

@st.dialog("Detalle de Cuenta", width="large")
def mostrar_tarjeta_cuenta(acct_name):
    """Dialog popup con detalle completo de una cuenta + acciones."""
    row = df[df["ACCT_NAME"] == acct_name]
    if row.empty:
        st.error("Cuenta no encontrada.")
        return
    row = row.iloc[0]
    cuenta_id = int(row["CUENTA_ID"])

    # Boton cerrar al inicio del dialog
    if st.button("Cerrar ventana", key=f"close_dialog_{cuenta_id}", type="secondary"):
        st.cache_data.clear()
        st.rerun()

    # -- Datos principales --
    st.markdown(f"### {acct_name}")
    dc1, dc2, dc3, dc4 = st.columns(4)
    dc1.metric("Industria", row["INDUSTRIA_NOMBRE"])
    dc2.metric("Tamaño", row["TAMANO_EMPRESA"] if pd.notna(row["TAMANO_EMPRESA"]) else "N/A")
    dc3.metric("Empleados Est.", f"{int(row['NUM_EMPLEADOS_ESTIMADO']):,}" if pd.notna(row["NUM_EMPLEADOS_ESTIMADO"]) else "N/A")
    dc4.metric("Revenue Est.", f"${float(row['REVENUE_ESTIMADO_USD'])/1e6:,.1f}M" if pd.notna(row["REVENUE_ESTIMADO_USD"]) else "N/A")

    dc5, dc6, dc7, dc8 = st.columns(4)
    dc5.metric("Estatus", row["ESTATUS"])
    if row["ESTATUS"] == "DESCALIFICADO" and row.get("MOTIVO_DESCALIFICACION"):
        st.warning(f"**Motivo de descalificación:** {row['MOTIVO_DESCALIFICACION']}")
    dc6.markdown(f"**Ubicacion:** {row['UBICACION'] if pd.notna(row['UBICACION']) else 'N/A'}")
    if pd.notna(row["SITIO_WEB"]) and str(row["SITIO_WEB"]).strip():
        dc7.markdown(f"**Web:** [{row['SITIO_WEB']}]({row['SITIO_WEB']})")
    else:
        dc7.markdown("**Web:** N/A")
    if pd.notna(row["LINKEDIN_EMPRESA"]) and str(row["LINKEDIN_EMPRESA"]).strip():
        dc8.markdown(f"**LinkedIn:** [Ver perfil]({row['LINKEDIN_EMPRESA']})")
    else:
        dc8.markdown("**LinkedIn:** N/A")

    # Email del contacto principal como mailto
    if pd.notna(row["CONTACTO_EMAIL"]) and str(row["CONTACTO_EMAIL"]).strip():
        _nombre = row['CONTACTO_NOMBRE'] if pd.notna(row['CONTACTO_NOMBRE']) else 'N/A'
        _cargo = row['CONTACTO_CARGO'] if pd.notna(row['CONTACTO_CARGO']) else 'N/A'
        st.markdown(f"**Contacto principal:** {_nombre} ({_cargo}) - {email_link_md(row['CONTACTO_EMAIL'])}")
    if pd.notna(row["NOTAS"]) and str(row["NOTAS"]).strip():
        st.info(f"**Notas:** {row['NOTAS']}")

    # -- Contactos de la cuenta --
    st.markdown("---")
    st.markdown("**Contactos**")
    df_contactos = load_contactos_cuenta(cuenta_id)
    if not df_contactos.empty:
        for _, ct in df_contactos.iterrows():
            contactado_icon = "Si" if ct["CONTACTADO"] else "No"
            email_md = email_link_md(ct["EMAIL"])
            wa_md = whatsapp_link_md(ct["WHATSAPP"])
            principal_tag = " **[PRINCIPAL]**" if ct.get("ES_PRINCIPAL", False) else ""
            st.markdown(
                f"- **{ct['NOMBRE_COMPLETO']}**{principal_tag} | {ct['CARGO'] or 'N/A'} | {email_md} | WA: {wa_md or 'N/A'} | Contactado: {contactado_icon}"
            )
    else:
        st.warning("Sin contactos registrados.")

    # -- Historial de interacciones --
    df_inter = load_interacciones_cuenta(cuenta_id)
    if not df_inter.empty:
        st.markdown("---")
        st.markdown("**Historial de Interacciones**")
        df_int_display = df_inter[["FECHA", "TIPO", "CONTACTO", "DESCRIPCION", "RESULTADO", "SIGUIENTE_ACCION"]].copy()
        df_int_display.columns = ["Fecha", "Tipo", "Contacto", "Descripción", "Resultado", "Siguiente Acción"]
        st.dataframe(df_int_display, width="stretch", hide_index=True, height=200)

    # =============================================================
    # ACCIONES (dentro del dialog, sin st.form por limitacion de @st.dialog)
    # =============================================================
    st.markdown("---")
    st.markdown("**Acciones**")

    action = st.selectbox("Selecciona una acción:", [
        "(Seleccionar)",
        "Marcar como Contactado",
        "Agregar Contacto",
        "Cambiar Contacto Principal",
        "Editar Datos Cuenta",
        "Registrar Interacción",
        "Generar Pitch con IA",
        "Descalificar Lead"
    ], key=f"action_{cuenta_id}")

    # Limpiar pitch persistido si el usuario cambia de accion
    if action != "Generar Pitch con IA":
        st.session_state.pop(f"_pitch_{cuenta_id}", None)
        st.session_state.pop(f"_sig_accion_{cuenta_id}", None)

    # ---- MARCAR COMO CONTACTADO ----
    if action == "Marcar como Contactado":
        df_ct = load_contactos_cuenta(cuenta_id)
        if df_ct.empty:
            st.warning("No hay contactos. Agrega uno primero.")
        else:
            st.markdown("**Selecciona los contactos a marcar:**")
            contacto_checks = {}
            for _, ctrow in df_ct.iterrows():
                label = f"{ctrow['NOMBRE_COMPLETO']} ({ctrow['CARGO'] or 'Sin cargo'})"
                ya_contactado = bool(ctrow.get("CONTACTADO", False))
                if ya_contactado:
                    label += " - Ya contactado"
                contacto_checks[ctrow["CONTACTO_ID"]] = st.checkbox(
                    label, value=False, key=f"mc_chk_{cuenta_id}_{ctrow['CONTACTO_ID']}",
                    disabled=ya_contactado
                )
            metodo = st.selectbox("Método:", ["EMAIL", "WHATSAPP", "LLAMADA", "LINKEDIN", "PRESENCIAL"], key=f"mc_met_{cuenta_id}")
            resultado = st.selectbox("Resultado:", ["POSITIVA", "NEGATIVA", "SIN_RESPUESTA", "REPROGRAMADO"], key=f"mc_res_{cuenta_id}")
            notas = st.text_area("Notas:", key=f"mc_not_{cuenta_id}", placeholder="Resumen de la conversacion...")
            seleccionados = [cid for cid, checked in contacto_checks.items() if checked]
            if st.button("Guardar Contactos", key=f"mc_save_{cuenta_id}", type="primary"):
                if not seleccionados:
                    st.warning("Selecciona al menos un contacto.")
                else:
                    ok_count = 0
                    for cid in seleccionados:
                        if marcar_contactado(cid, cuenta_id, metodo, resultado, notas):
                            ok_count += 1
                    if ok_count > 0:
                        st.cache_data.clear()
                        st.session_state["_open_cuenta"] = acct_name
                        st.session_state["_toast_msg"] = f"{ok_count} contacto(s) marcado(s) como contactado(s) via {metodo}."
                        st.session_state.pop("_kpi_filter", None)
                        st.session_state.pop("_kpi_filter_prev", None)
                        st.rerun()

    # ---- AGREGAR CONTACTO ----
    elif action == "Agregar Contacto":
        ac1, ac2 = st.columns(2)
        nombre = ac1.text_input("Primer Nombre *", key=f"ac_nom_{cuenta_id}")
        apellido = ac2.text_input("Apellido *", key=f"ac_ape_{cuenta_id}")
        cargo = st.text_input("Cargo / Puesto", key=f"ac_car_{cuenta_id}")
        ae1, ae2 = st.columns(2)
        email_new = ae1.text_input("Email", key=f"ac_ema_{cuenta_id}")
        whatsapp_new = ae2.text_input("WhatsApp", key=f"ac_wa_{cuenta_id}")
        linkedin_new = st.text_input("LinkedIn (URL)", key=f"ac_li_{cuenta_id}")
        if st.button("Agregar Contacto", key=f"ac_save_{cuenta_id}", type="primary"):
            if not nombre:
                st.error("El nombre es obligatorio.")
            else:
                if agregar_contacto(cuenta_id, nombre, apellido, cargo, email_new, whatsapp_new, linkedin_new):
                    st.cache_data.clear()
                    st.session_state["_open_cuenta"] = acct_name
                    st.session_state["_toast_msg"] = f"Contacto {nombre} {apellido} agregado."
                    st.session_state.pop("_kpi_filter", None)
                    st.session_state.pop("_kpi_filter_prev", None)
                    st.rerun()

    # ---- CAMBIAR CONTACTO PRINCIPAL ----
    elif action == "Cambiar Contacto Principal":
        df_ct_p = load_contactos_cuenta(cuenta_id)
        if df_ct_p.empty or len(df_ct_p) < 2:
            st.info("Se necesitan al menos 2 contactos para cambiar el principal.")
        else:
            actual_principal = df_ct_p[df_ct_p["ES_PRINCIPAL"] == True]
            if not actual_principal.empty:
                st.markdown(f"Contacto principal actual: **{actual_principal.iloc[0]['NOMBRE_COMPLETO']}** ({actual_principal.iloc[0]['CARGO'] or 'N/A'})")
            no_principal = df_ct_p[df_ct_p["ES_PRINCIPAL"] != True]
            contacto_opts_p = dict(zip(
                no_principal["NOMBRE_COMPLETO"] + " (" + no_principal["CARGO"].fillna("") + ")",
                no_principal["CONTACTO_ID"]
            ))
            sel_nuevo = st.selectbox("Nuevo contacto principal:", list(contacto_opts_p.keys()), key=f"cp_sel_{cuenta_id}")
            if st.button("Cambiar Principal", key=f"cp_save_{cuenta_id}", type="primary"):
                nuevo_id = contacto_opts_p[sel_nuevo]
                if cambiar_contacto_principal(cuenta_id, nuevo_id):
                    st.cache_data.clear()
                    st.session_state["_open_cuenta"] = acct_name
                    st.session_state["_toast_msg"] = f"Contacto principal cambiado a {sel_nuevo}."
                    st.session_state.pop("_kpi_filter", None)
                    st.session_state.pop("_kpi_filter_prev", None)
                    st.rerun()

    # ---- EDITAR DATOS CUENTA ----
    elif action == "Editar Datos Cuenta":
        st.caption(f"Empresa: **{acct_name}** (no editable)")
        estatus_opts = ["PENDIENTE", "CONTACTADO", "EN_SEGUIMIENTO", "CALIFICADO", "OPORTUNIDAD", "DESCARTADO", "DESCALIFICADO"]
        tamano_opts = ["Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
        industria_opts = ["Automotive", "Consulting/Professional Services", "E-commerce",
                          "Education/Research", "Energy", "Fintech/Financial Services",
                          "Food & Beverage", "Government/Public Sector", "Healthcare/Pharma",
                          "Insurance", "Logistics/Transportation", "Manufacturing/Industrial",
                          "Media/Entertainment", "Retail/Consumer Goods", "Sin Clasificar",
                          "Technology", "Telecommunications"]
        industria_id_map = {
            "Automotive": 10, "Consulting/Professional Services": 7, "E-commerce": 12,
            "Education/Research": 9, "Energy": 13, "Fintech/Financial Services": 3,
            "Food & Beverage": 8, "Government/Public Sector": 14, "Healthcare/Pharma": 15,
            "Insurance": 17, "Logistics/Transportation": 16, "Manufacturing/Industrial": 5,
            "Media/Entertainment": 11, "Retail/Consumer Goods": 4, "Sin Clasificar": 1,
            "Technology": 2, "Telecommunications": 6
        }
        ec1, ec2, ec3 = st.columns(3)
        nuevo_estatus = ec1.selectbox("Estatus", estatus_opts,
            index=estatus_opts.index(row["ESTATUS"]) if row["ESTATUS"] in estatus_opts else 0, key=f"ed_est_{cuenta_id}")
        nuevo_tamano = ec2.selectbox("Tamaño", tamano_opts,
            index=tamano_opts.index(row["TAMANO_EMPRESA"]) if row["TAMANO_EMPRESA"] in tamano_opts else 0, key=f"ed_tam_{cuenta_id}")
        nueva_industria = ec3.selectbox("Industria", industria_opts,
            index=industria_opts.index(row["INDUSTRIA_NOMBRE"]) if row["INDUSTRIA_NOMBRE"] in industria_opts else 0, key=f"ed_ind_{cuenta_id}")
        motivo_descal = ""
        if nuevo_estatus == "DESCALIFICADO":
            motivo_descal = st.text_area("Motivo de descalificación (obligatorio)",
                value=row.get("MOTIVO_DESCALIFICACION") or "", key=f"ed_mdesc_{cuenta_id}",
                placeholder="Describe brevemente por que se descalifica este lead...")
        eg1, eg2, eg3 = st.columns(3)
        nuevo_pais = eg1.text_input("Pais", value=row["PAIS"] or "", key=f"ed_pai_{cuenta_id}")
        nuevo_estado = eg2.text_input("Estado", value=row["ESTADO"] or "", key=f"ed_est2_{cuenta_id}")
        nuevo_ciudad = eg3.text_input("Ciudad", value=row["CIUDAD"] or "", key=f"ed_ciu_{cuenta_id}")
        ew1, ew2 = st.columns(2)
        nuevo_web = ew1.text_input("Sitio Web", value=row["SITIO_WEB"] or "", key=f"ed_web_{cuenta_id}")
        nuevo_linkedin = ew2.text_input("LinkedIn Empresa", value=row["LINKEDIN_EMPRESA"] or "", key=f"ed_lin_{cuenta_id}")
        en1, en2 = st.columns(2)
        nuevo_empleados = en1.number_input("Empleados Est.", value=int(row["NUM_EMPLEADOS_ESTIMADO"]) if row["NUM_EMPLEADOS_ESTIMADO"] else 0, min_value=0, key=f"ed_emp_{cuenta_id}")
        nuevo_revenue = en2.number_input("Revenue Est. (USD)", value=float(row["REVENUE_ESTIMADO_USD"]) if row["REVENUE_ESTIMADO_USD"] else 0.0, min_value=0.0, format="%.2f", key=f"ed_rev_{cuenta_id}")
        nuevas_notas = st.text_area("Notas", value=row["NOTAS"] or "", key=f"ed_not_{cuenta_id}")
        if st.button("Guardar Cambios", key=f"ed_save_{cuenta_id}", type="primary"):
            if nuevo_estatus == "DESCALIFICADO" and not motivo_descal.strip():
                st.error("Debes indicar el motivo de descalificación.")
            else:
                ubicacion = ", ".join(filter(None, [nuevo_ciudad, nuevo_estado, nuevo_pais]))
                campos = {
                    "ESTATUS": nuevo_estatus, "TAMANO_EMPRESA": nuevo_tamano,
                    "INDUSTRIA_ID": industria_id_map.get(nueva_industria, 1),
                    "MOTIVO_DESCALIFICACION": motivo_descal.strip() if nuevo_estatus == "DESCALIFICADO" else None,
                    "PAIS": nuevo_pais, "ESTADO": nuevo_estado, "CIUDAD": nuevo_ciudad,
                    "UBICACION": ubicacion, "SITIO_WEB": nuevo_web, "LINKEDIN_EMPRESA": nuevo_linkedin,
                    "NUM_EMPLEADOS_ESTIMADO": nuevo_empleados if nuevo_empleados > 0 else None,
                    "REVENUE_ESTIMADO_USD": nuevo_revenue if nuevo_revenue > 0 else None,
                    "NOTAS": nuevas_notas, "FUENTE_TAMANO": "MANUAL"
                }
                if actualizar_cuenta(cuenta_id, campos):
                    st.cache_data.clear()
                    st.session_state["_open_cuenta"] = acct_name
                    st.session_state["_toast_msg"] = "Datos actualizados."
                    st.session_state.pop("_kpi_filter", None)
                    st.session_state.pop("_kpi_filter_prev", None)
                    st.rerun()

    # ---- REGISTRAR INTERACCION ----
    elif action == "Registrar Interacción":
        df_ct2 = load_contactos_cuenta(cuenta_id)
        contacto_opts2 = {"(Sin contacto especifico)": None}
        if not df_ct2.empty:
            contacto_opts2.update(dict(zip(
                df_ct2["NOMBRE_COMPLETO"] + " (" + df_ct2["CARGO"].fillna("") + ")",
                df_ct2["CONTACTO_ID"]
            )))
        sel_ct2 = st.selectbox("Contacto:", list(contacto_opts2.keys()), key=f"ri_ct_{cuenta_id}")
        tipo = st.selectbox("Tipo:", ["EMAIL", "WHATSAPP", "LLAMADA", "LINKEDIN", "PRESENCIAL", "EVENTO"], key=f"ri_tip_{cuenta_id}")
        descripcion = st.text_area("Descripción:", key=f"ri_desc_{cuenta_id}", placeholder="Qué se habló/hizo...")
        resultado_i = st.selectbox("Resultado:", ["EXITOSO", "SIN_RESPUESTA", "RECHAZADO", "REPROGRAMADO", "PENDIENTE"], key=f"ri_res_{cuenta_id}")
        siguiente = st.text_input("Siguiente acción:", key=f"ri_sig_{cuenta_id}", placeholder="Ej: Agendar demo")
        if st.button("Registrar", key=f"ri_save_{cuenta_id}", type="primary"):
            ct_id2 = contacto_opts2[sel_ct2]
            if registrar_interaccion(cuenta_id, ct_id2, tipo, descripcion, resultado_i, siguiente):
                st.cache_data.clear()
                st.session_state["_open_cuenta"] = acct_name
                st.session_state["_toast_msg"] = "Interacción registrada."
                st.session_state.pop("_kpi_filter", None)
                st.session_state.pop("_kpi_filter_prev", None)
                st.rerun()

    # ---- GENERAR PITCH CON IA ----
    elif action == "Generar Pitch con IA":
        industria_cuenta = row["INDUSTRIA_NOMBRE"]
        caso_industria = df_casos[df_casos["INDUSTRIA_NOMBRE"] == industria_cuenta]
        insights_ctx = ""
        if not caso_industria.empty:
            ci = caso_industria.iloc[0]
            insights_ctx = (
                f"\nInsights de la industria {industria_cuenta}:"
                f"\nRetos: {str(ci['RETOS_PRINCIPALES'])[:300]}"
                f"\nCasos de uso: {str(ci['CASOS_USO_SNOWFLAKE'])[:300]}"
                f"\nPropuesta: {str(ci['PROPUESTA_VALOR'])[:200]}"
            )

        contexto = f"Empresa: {row['ACCT_NAME']}\nIndustria: {row['INDUSTRIA_NOMBRE']}"
        if row["TAMANO_EMPRESA"]:
            contexto += f"\nTamano: {row['TAMANO_EMPRESA']}"
        if row["NUM_EMPLEADOS_ESTIMADO"]:
            contexto += f" (~{int(row['NUM_EMPLEADOS_ESTIMADO'])} empleados)"
        if row["REVENUE_ESTIMADO_USD"]:
            contexto += f"\nRevenue: ${float(row['REVENUE_ESTIMADO_USD'])/1e6:,.1f}M USD"
        if row["UBICACION"]:
            contexto += f"\nUbicacion: {row['UBICACION']}"
        if row["CONTACTO_NOMBRE"]:
            contexto += f"\nContacto: {row['CONTACTO_NOMBRE']}"
            if row["CONTACTO_CARGO"] and str(row["CONTACTO_CARGO"]).strip():
                contexto += f"\nCargo/Rol del contacto: {row['CONTACTO_CARGO']}"
            if row.get("CONTACTO_NIVEL") and str(row.get("CONTACTO_NIVEL", "")).strip():
                contexto += f"\nNivel del contacto: {row['CONTACTO_NIVEL']}"
        contexto += f"\nEstatus actual: {row['ESTATUS']}"
        contexto += f"\nFuente: {row.get('NOMBRE_EVENTO', 'Sin evento')}"
        contexto += insights_ctx

        # Obtener contenido del sitio web si existe
        sitio_web = str(row.get("SITIO_WEB", "") or "").strip() if row.get("SITIO_WEB") is not None and str(row.get("SITIO_WEB", "")).strip() not in ("", "None", "nan") else ""
        web_text = ""

        if sitio_web:
            # Sitio web registrado -> consultar automaticamente
            with st.spinner("Consultando sitio web de la empresa..."):
                web_text = fetch_website_text(sitio_web)
            if web_text:
                contexto += f"\n\nInformación extraída del sitio web ({sitio_web}):\n{web_text}"
        else:
            # Sin sitio web -> ofrecer opciones al usuario
            st.info("Esta cuenta no tiene sitio web registrado.")
            _sw_key = f"_sw_option_{cuenta_id}"
            sw_option = st.radio(
                "¿Que deseas hacer?",
                ["Agregar sitio web manualmente", "Continuar sin sitio web"],
                key=_sw_key, horizontal=True
            )
            if sw_option == "Agregar sitio web manualmente":
                _sw_input_key = f"_sw_input_{cuenta_id}"
                sitio_web_manual = st.text_input(
                    "URL del sitio web:", key=_sw_input_key,
                    placeholder="Ej: https://www.empresa.com"
                )
                if sitio_web_manual and sitio_web_manual.strip():
                    sitio_web_manual = sitio_web_manual.strip()
                    if st.button("Guardar sitio web y continuar", key=f"sw_save_{cuenta_id}"):
                        if actualizar_cuenta(cuenta_id, {"SITIO_WEB": sitio_web_manual}):
                            st.cache_data.clear()
                            st.session_state["_open_cuenta"] = acct_name
                            st.session_state["_toast_msg"] = f"Sitio web guardado: {sitio_web_manual}"
                            st.rerun()
                    # Mientras no guarde, consultar el sitio para el pitch actual
                    with st.spinner("Consultando sitio web..."):
                        web_text = fetch_website_text(sitio_web_manual)
                    if web_text:
                        contexto += f"\n\nInformación extraída del sitio web ({sitio_web_manual}):\n{web_text}"
            else:
                # Continuar sin sitio web: usar dominio del email como contexto alternativo
                email_contacto = str(row.get("CONTACTO_EMAIL", "") or "").strip()
                if email_contacto and "@" in email_contacto:
                    dominio = email_contacto.split("@")[1]
                    # Excluir dominios genericos que no aportan contexto empresarial
                    dominios_genericos = {"gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "live.com", "icloud.com", "protonmail.com", "aol.com"}
                    if dominio.lower() not in dominios_genericos:
                        contexto += f"\n\nNota: No se tiene sitio web registrado. El dominio del email del contacto es {dominio}, que podria corresponder al sitio web de la empresa."
                        # Intentar consultar el dominio como sitio web
                        with st.spinner(f"Consultando {dominio}..."):
                            web_text = fetch_website_text(f"https://{dominio}")
                        if web_text:
                            contexto += f"\nInformación extraída de {dominio}:\n{web_text}"

        df_hist = load_interacciones_cuenta(cuenta_id)
        if not df_hist.empty:
            hist_text = "\nHistorial de interacciones:"
            for _, hi in df_hist.head(3).iterrows():
                hist_text += f"\n- {hi['FECHA']}: {hi['TIPO']} - {hi['RESULTADO']} - {hi['DESCRIPCION'] or ''}"
            contexto += hist_text

        # Keys para persistir pitch en session_state
        _pk = f"_pitch_{cuenta_id}"
        _ak = f"_sig_accion_{cuenta_id}"

        if st.button("Generar Pitch y Siguiente Acción", key=f"pitch_gen_{cuenta_id}", type="primary"):
            with st.spinner("Generando pitch personalizado con Cortex AI..."):
                try:
                    conn_ai = get_connection()
                    cur_ai = conn_ai.cursor()

                    nombre_contacto = "colega"
                    if row["CONTACTO_NOMBRE"] and str(row["CONTACTO_NOMBRE"]).strip():
                        nombre_contacto = str(row["CONTACTO_NOMBRE"]).strip().split()[0]

                    prompt_pitch = (
                        f"Eres Pedro Ulloa, ejecutivo de ventas de EGOS BI (https://egosbi.com/), partner oficial de Snowflake en Mexico. "
                        f"EGOS BI es una consultora especializada en Modern Data Stack: migracion a Snowflake, integracion de datos, "
                        f"transformacion, analytics y Data Cloud. Ayudamos a empresas a tomar decisiones en tiempo real con arquitecturas "
                        f"de datos modernas, escalables y confiables. "
                        f"Genera un mensaje de seguimiento personalizado en español (5-6 oraciones) para esta cuenta. "
                        f"CONTEXTO IMPORTANTE: Este lead asistió al evento '{row.get('NOMBRE_EVENTO', 'Sin evento')}'. "
                        f"NO asumas que te viste en persona con el contacto. "
                        f"PERSONALIZACIÓN POR CARGO: Si se incluye el cargo o rol del contacto, usa esa información para "
                        f"personalizar el mensaje segun sus responsabilidades e intereses profesionales. Por ejemplo: "
                        f"si es Director de Datos o CDO, enfoca el mensaje en gobernanza de datos y toma de decisiones; "
                        f"si es CTO o VP de Tecnologia, habla de arquitectura escalable y modernizacion; "
                        f"si es de BI o Analytics, enfoca en reporteo avanzado y democratizacion de datos; "
                        f"si es de AI/ML, enfoca en Cortex AI y modelos sobre datos unificados; "
                        f"si es de Ingenieria de Datos, habla de pipelines eficientes y reduccion de complejidad operativa. "
                        f"El caso de uso que menciones debe hacer sentido con el rol del contacto. "
                        f"Inicia con un saludo a '{nombre_contacto}' diciendo que nos da mucho gusto que haya podido asistir "
                        f"al evento '{row.get('NOMBRE_EVENTO', 'Sin evento')}', que sabemos que en estos momentos su industria esta "
                        f"enfrentando ciertos retos (menciona uno relevante), y continua con: "
                        f"1) un caso de uso relevante para su industria Y para el rol del contacto, donde la combinacion de las "
                        f"capacidades de EGOS BI y Snowflake puede ayudarles a resolver ese reto o aprovechar una oportunidad, "
                        f"2) cierra con una pregunta casual tipo: Que te parece si agendamos una llamada de 20 minutos para rebotar ideas? "
                        f"NO menciones demos, demostraciones ni pruebas de concepto. Solo proponer una llamada breve para platicar. "
                        f"NUNCA uses la frase 'potencialidad de Snowflake' ni 'potencial de Snowflake'. "
                        f"Si mencionas potencial, di 'el potencial de tus datos'. El enfoque es el valor para el cliente, no el producto. "
                        f"Si se incluye información del sitio web de la empresa, usala para personalizar el mensaje: "
                        f"menciona algo especifico de lo que hacen o a que se dedican segun su sitio web. "
                        f"Tono: profesional, cercano, de seguimiento. Datos:\n{contexto}"
                    )
                    cur_ai.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s)", (prompt_pitch,))
                    pitch = cur_ai.fetchone()[0]

                    prompt_accion = (
                        f"Basandote en estos datos de un lead comercial, sugiere la SIGUIENTE ACCION concreta "
                        f"que el vendedor debe tomar. Responde en español con UNA oración directa y accionable. "
                        f"NO sugieras demos ni demostraciones. Enfocate en agendar llamadas, enviar emails o compartir contenido. "
                        f"Ejemplo: 'Enviar email de seguimiento con caso de uso de Cortex AI para retail'. "
                        f"Datos:\n{contexto}"
                    )
                    cur_ai.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s)", (prompt_accion,))
                    siguiente_accion = cur_ai.fetchone()[0]

                    cur_ai.close()
                    conn_ai.close()

                    # Persistir en session_state para que sobreviva reruns
                    st.session_state[_pk] = pitch
                    st.session_state[_ak] = siguiente_accion

                except Exception as e:
                    st.error(f"Error al generar pitch: {e}")

        # Mostrar pitch si existe en session_state
        if _pk in st.session_state and st.session_state[_pk]:
            pitch = st.session_state[_pk]
            siguiente_accion = st.session_state[_ak]

            st.markdown(f"**Pitch para {acct_name}** ({industria_cuenta})")
            st.write(pitch)
            st.markdown("**Siguiente Accion Sugerida:**")
            st.success(siguiente_accion)

            # Enviar por Email - incluir todos los contactos con email
            df_ct_pitch = load_contactos_cuenta(cuenta_id)
            emails_contactos = []
            nombres_contactos = []
            for _, ctr in df_ct_pitch.iterrows():
                _em = str(ctr["EMAIL"]).strip() if ctr.get("EMAIL") else ""
                if _em:
                    emails_contactos.append(_em)
                    nombres_contactos.append(ctr["NOMBRE_COMPLETO"])
            if emails_contactos:
                all_emails = ",".join(emails_contactos)
                all_nombres = ", ".join(nombres_contactos)
                subject = f"Seguimiento {row.get('NOMBRE_EVENTO', '')} - {acct_name}"
                pitch_clean = str(pitch).replace('\n', '\r\n')
                mailto_url = make_mailto(all_emails, subject=subject, body=pitch_clean)
                st.markdown(f"[Enviar pitch por email a {all_nombres}]({mailto_url})")

            # Formulario: guardar accion + marcar contactados
            st.markdown("---")
            st.markdown("**Guardar accion y marcar contactados**")
            st.caption("Selecciona los contactos a marcar como contactados:")
            contacto_checks_pitch = {}
            for _, ctr in df_ct_pitch.iterrows():
                label = f"{ctr['NOMBRE_COMPLETO']} ({ctr['CARGO'] or 'Sin cargo'})"
                ya_contactado = bool(ctr.get("CONTACTADO", False))
                if ya_contactado:
                    label += " - Ya contactado"
                contacto_checks_pitch[ctr["CONTACTO_ID"]] = st.checkbox(
                    label, value=not ya_contactado, key=f"pc_chk_{cuenta_id}_{ctr['CONTACTO_ID']}",
                    disabled=ya_contactado
                )
            pc1, pc2 = st.columns(2)
            metodo_pitch = pc1.selectbox("Método:", ["EMAIL", "WHATSAPP", "LLAMADA", "LINKEDIN", "PRESENCIAL"], key=f"pc_met_{cuenta_id}")
            resultado_pitch = pc2.selectbox("Resultado:", ["SIN_RESPUESTA", "POSITIVA", "NEGATIVA", "REPROGRAMADO"],
                                            index=0, key=f"pc_res_{cuenta_id}")
            notas_pitch = st.text_input("Notas:", value="Email de primer contacto enviado", key=f"pc_not_{cuenta_id}")

            sel_pitch = [cid for cid, checked in contacto_checks_pitch.items() if checked]
            if st.button("Guardar acción y marcar contactados", key=f"pitch_save_{cuenta_id}", type="primary"):
                # Registrar interaccion IA
                registrar_interaccion(cuenta_id, None, "IA_SUGERENCIA",
                                      "Pitch generado + acción sugerida", "PENDIENTE",
                                      str(siguiente_accion)[:500])
                # Marcar contactos seleccionados
                ok_count = 0
                for cid in sel_pitch:
                    if marcar_contactado(cid, cuenta_id, metodo_pitch, resultado_pitch, notas_pitch):
                        ok_count += 1
                msg = "Acción guardada en historial."
                if ok_count > 0:
                    msg += f" {ok_count} contacto(s) marcado(s) como contactado(s) via {metodo_pitch}."
                # Limpiar pitch del session_state
                st.session_state.pop(_pk, None)
                st.session_state.pop(_ak, None)
                st.cache_data.clear()
                st.session_state["_open_cuenta"] = acct_name
                st.session_state["_toast_msg"] = msg
                st.session_state.pop("_kpi_filter", None)
                st.session_state.pop("_kpi_filter_prev", None)
                st.rerun()

    # ---- DESCALIFICAR LEAD ----
    elif action == "Descalificar Lead":
        if row["ESTATUS"] == "DESCALIFICADO":
            st.warning(f"Esta cuenta ya esta descalificada. Motivo: {row.get('MOTIVO_DESCALIFICACION') or 'Sin motivo registrado'}")
            st.caption("Para reactivarla, usa 'Editar Datos Cuenta' y cambia el estatus.")
        else:
            st.caption(f"Estatus actual: **{row['ESTATUS']}**")
            motivo_descal_dialog = st.text_area("Motivo de descalificación (obligatorio):",
                placeholder="Ej: No responde, no tiene presupuesto, empresa cerro, no es perfil Snowflake...",
                key=f"descal_motivo_{cuenta_id}")
            if st.button("Confirmar Descalificación", key=f"descal_btn_{cuenta_id}", type="primary"):
                if not motivo_descal_dialog.strip():
                    st.error("Debes indicar el motivo de descalificación.")
                else:
                    campos = {"ESTATUS": "DESCALIFICADO", "MOTIVO_DESCALIFICACION": motivo_descal_dialog.strip()}
                    if actualizar_cuenta(cuenta_id, campos):
                        st.cache_data.clear()
                        st.session_state["_open_cuenta"] = acct_name
                        st.session_state["_toast_msg"] = f"Lead descalificado: {acct_name}"
                        st.session_state.pop("_kpi_filter", None)
                        st.session_state.pop("_kpi_filter_prev", None)
                        st.rerun()


# =============================================================
# CARGAR DATOS
# =============================================================

df, df_casos, df_eventos = load_data()

# -- Notificacion toast tras accion exitosa en dialog --
if "_toast_msg" in st.session_state:
    st.toast(st.session_state.pop("_toast_msg"), icon="✅")

# =============================================================
# SIDEBAR: FILTROS
# =============================================================

with st.sidebar:
    st.header("Filtros")

    # -- Filtro de Evento (global) --
    eventos_list = df_eventos["NOMBRE_EVENTO"].tolist() if not df_eventos.empty else []
    sel_evento = st.selectbox("Evento", ["Todos"] + eventos_list)

    estatus_opts = ["PENDIENTE", "CONTACTADO", "EN_SEGUIMIENTO", "CALIFICADO", "OPORTUNIDAD", "DESCARTADO", "DESCALIFICADO"]
    default_estatus = [e for e in estatus_opts if e != "DESCALIFICADO"]
    sel_estatus = st.multiselect("Estatus Comercial", estatus_opts, default=default_estatus)
    medio_opts = ["Todos", "Con WhatsApp", "Con Email", "Con ambos", "Sin medio"]
    sel_medio = st.selectbox("Medio de Contacto", medio_opts)
    industrias = sorted(df["INDUSTRIA_NOMBRE"].unique())
    sel_industrias = st.multiselect("Industria", industrias, default=industrias)
    tamanos = ["Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
    sel_tamanos = st.multiselect("Tamaño Empresa", tamanos, default=tamanos)
    paises = sorted(df["PAIS"].dropna().unique())
    sel_paises = st.multiselect("Pais", paises, default=paises)

    # -- Filtros de enriquecimiento --
    web_opts = ["Todos", "Con sitio web", "Sin sitio web"]
    sel_web = st.selectbox("Sitio Web", web_opts)
    linkedin_opts = ["Todos", "Con LinkedIn", "Sin LinkedIn"]
    sel_linkedin = st.selectbox("LinkedIn Empresa", linkedin_opts)

    buscar = st.text_input("Buscar empresa", placeholder="Nombre de empresa...")

    # --- Herramientas de enriquecimiento ---
    st.divider()
    st.subheader("Herramientas")
    if st.button("Enriquecer sitios web", help="Busca sitios web para cuentas sin URL usando el dominio del email"):
        # Obtener cuentas sin sitio web con email corporativo
        conn_enr = get_connection()
        cur_enr = conn_enr.cursor()
        try:
            cur_enr.execute(f"""
                SELECT c.CUENTA_ID, c.ACCT_NAME, co.EMAIL
                FROM {DB}.CORE.DIM_CUENTAS c
                LEFT JOIN {DB}.CORE.DIM_CONTACTOS co
                    ON c.CUENTA_ID = co.CUENTA_ID AND co.ES_PRINCIPAL = TRUE
                WHERE c.SITIO_WEB IS NULL OR TRIM(c.SITIO_WEB) = '' OR c.SITIO_WEB = 'nan'
            """)
            cuentas_sin_web = cur_enr.fetchall()
        finally:
            cur_enr.close()
            conn_enr.close()

        if not cuentas_sin_web:
            st.success("Todas las cuentas ya tienen sitio web registrado.")
        else:
            encontrados = 0
            omitidos_generico = 0
            fallidos = 0
            progress = st.progress(0, text="Buscando sitios web...")
            for i, (cid, nombre, email) in enumerate(cuentas_sin_web):
                progress.progress((i + 1) / len(cuentas_sin_web),
                                  text=f"Verificando {nombre}...")
                if not email or not str(email).strip() or "@" not in str(email):
                    fallidos += 1
                    continue
                dominio = str(email).strip().split("@")[1]
                url = inferir_sitio_web(dominio)
                if url == "":
                    # Verificar si fue por dominio generico
                    d = dominio.lower()
                    gens = {"gmail.com", "hotmail.com", "outlook.com", "yahoo.com",
                            "live.com", "icloud.com", "protonmail.com", "aol.com",
                            "live.com.mx", "hotmail.es", "outlook.es", "yahoo.com.mx"}
                    if d in gens:
                        omitidos_generico += 1
                    else:
                        fallidos += 1
                else:
                    actualizar_cuenta(cid, {"SITIO_WEB": url})
                    encontrados += 1
            progress.empty()
            st.cache_data.clear()
            st.success(
                f"Listo: {encontrados} sitios encontrados, "
                f"{omitidos_generico} con email generico (omitidos), "
                f"{fallidos} no encontrados."
            )
            if encontrados > 0:
                st.rerun()

    if st.button("Clasificar industrias con IA", help="Usa Cortex AI para asignar industria a cuentas marcadas como 'Sin Clasificar'"):
        conn_ind = get_connection()
        cur_ind = conn_ind.cursor()
        try:
            # Obtener ID de "Sin Clasificar"
            cur_ind.execute(f"""
                SELECT INDUSTRIA_ID FROM {DB}.CORE.DIM_INDUSTRIAS
                WHERE UPPER(TRIM(INDUSTRIA_NOMBRE)) = 'SIN CLASIFICAR'
            """)
            sin_clas_row = cur_ind.fetchone()
            sin_clas_id = int(sin_clas_row[0]) if sin_clas_row else 1

            # Obtener lista de industrias válidas (excluir Sin Clasificar)
            cur_ind.execute(f"""
                SELECT INDUSTRIA_ID, INDUSTRIA_NOMBRE FROM {DB}.CORE.DIM_INDUSTRIAS
                WHERE INDUSTRIA_ID != %s ORDER BY INDUSTRIA_ID
            """, (sin_clas_id,))
            industrias_validas = cur_ind.fetchall()
            lista_industrias = ", ".join([nombre for _, nombre in industrias_validas])

            # Obtener cuentas "Sin Clasificar"
            cur_ind.execute(f"""
                SELECT c.CUENTA_ID, c.ACCT_NAME, co.EMAIL
                FROM {DB}.CORE.DIM_CUENTAS c
                LEFT JOIN {DB}.CORE.DIM_CONTACTOS co
                    ON c.CUENTA_ID = co.CUENTA_ID AND co.ES_PRINCIPAL = TRUE
                WHERE c.INDUSTRIA_ID = %s
            """, (sin_clas_id,))
            cuentas_sin_ind = cur_ind.fetchall()

            if not cuentas_sin_ind:
                st.success("Todas las cuentas ya tienen industria asignada.")
            else:
                clasificadas = 0
                no_clasificadas = 0
                progress_ind = st.progress(0, text="Clasificando industrias con IA...")
                for i, (cid, nombre_emp, email_cta) in enumerate(cuentas_sin_ind):
                    progress_ind.progress((i + 1) / len(cuentas_sin_ind),
                                          text=f"Clasificando {nombre_emp}...")
                    dominio = ""
                    if email_cta and "@" in str(email_cta):
                        dominio = str(email_cta).strip().split("@")[1]

                    prompt_cls = (
                        "Classify the following company into exactly ONE of these industries. "
                        "Reply with ONLY the industry name, nothing else.\n\n"
                        f"Industries: {lista_industrias}\n\n"
                        f"Company: {nombre_emp}\n"
                        f"Email domain: {dominio}\n\n"
                        "Industry:"
                    )
                    try:
                        cur_ind.execute(
                            "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s)",
                            (prompt_cls,)
                        )
                        ai_resultado = cur_ind.fetchone()[0].strip().strip('"').strip("'").strip()
                        # Buscar match en industrias válidas
                        matched_id = None
                        for ind_id, ind_nombre in industrias_validas:
                            if ai_resultado.upper() == ind_nombre.upper():
                                matched_id = int(ind_id)
                                break
                        if matched_id:
                            actualizar_cuenta(cid, {"INDUSTRIA_ID": matched_id})
                            clasificadas += 1
                        else:
                            no_clasificadas += 1
                    except Exception:
                        no_clasificadas += 1

                progress_ind.empty()
                st.cache_data.clear()
                st.success(
                    f"Listo: {clasificadas} industrias clasificadas, "
                    f"{no_clasificadas} no se pudieron clasificar."
                )
                if clasificadas > 0:
                    st.rerun()
        finally:
            cur_ind.close()
            conn_ind.close()

# =============================================================
# APLICAR FILTROS
# =============================================================

# Guardar df completo para Vision General antes de filtrar por evento
df_all = df.copy()

# Filtro de evento
if sel_evento != "Todos":
    dff = df[df["NOMBRE_EVENTO"] == sel_evento].copy()
else:
    dff = df.copy()

dff = dff[dff["ESTATUS"].isin(sel_estatus)]
dff = dff[dff["INDUSTRIA_NOMBRE"].isin(sel_industrias)]
dff = dff[dff["TAMANO_EMPRESA"].isin(sel_tamanos) | dff["TAMANO_EMPRESA"].isna()]
dff = dff[dff["PAIS"].isin(sel_paises) | dff["PAIS"].isna()]

if sel_medio == "Con WhatsApp":
    dff = dff[dff["CONTACTO_WHATSAPP"].notna() & (dff["CONTACTO_WHATSAPP"].astype(str).str.strip() != "")]
elif sel_medio == "Con Email":
    dff = dff[dff["CONTACTO_EMAIL"].notna() & (dff["CONTACTO_EMAIL"].astype(str).str.strip() != "")]
elif sel_medio == "Con ambos":
    dff = dff[
        (dff["CONTACTO_WHATSAPP"].notna() & (dff["CONTACTO_WHATSAPP"].astype(str).str.strip() != "")) &
        (dff["CONTACTO_EMAIL"].notna() & (dff["CONTACTO_EMAIL"].astype(str).str.strip() != ""))
    ]
elif sel_medio == "Sin medio":
    dff = dff[
        (dff["CONTACTO_WHATSAPP"].isna() | (dff["CONTACTO_WHATSAPP"].astype(str).str.strip() == "")) &
        (dff["CONTACTO_EMAIL"].isna() | (dff["CONTACTO_EMAIL"].astype(str).str.strip() == ""))
    ]

# Filtros de sitio web y LinkedIn
_nulos = {"", "nan", "none", "null"}
if sel_web == "Con sitio web":
    dff = dff[dff["SITIO_WEB"].astype(str).str.strip().str.lower().apply(lambda v: v not in _nulos)]
elif sel_web == "Sin sitio web":
    dff = dff[dff["SITIO_WEB"].astype(str).str.strip().str.lower().apply(lambda v: v in _nulos)]

if sel_linkedin == "Con LinkedIn":
    dff = dff[dff["LINKEDIN_EMPRESA"].astype(str).str.strip().str.lower().apply(lambda v: v not in _nulos)]
elif sel_linkedin == "Sin LinkedIn":
    dff = dff[dff["LINKEDIN_EMPRESA"].astype(str).str.strip().str.lower().apply(lambda v: v in _nulos)]

if buscar:
    dff = dff[dff["ACCT_NAME"].str.contains(buscar, case=False, na=False)]

# =============================================================
# TITULO Y KPIs
# =============================================================

_evento_label = sel_evento if sel_evento != "Todos" else "Todos los eventos"
st.title("EGOS BI Lead Manager")
st.markdown(f"<span style='font-size:1.15rem;color:#888;'>{_evento_label} &nbsp;|&nbsp; {len(dff)} de {len(df)} cuentas &nbsp;|&nbsp; Pedro Ulloa - EGOS BI</span>", unsafe_allow_html=True)

total = len(dff)
contactadas = int(dff["CONTACTADO"].fillna(False).sum())
pendientes = total - contactadas
con_whatsapp = int(dff["CONTACTO_WHATSAPP"].notna().sum())
con_email = int(dff["CONTACTO_EMAIL"].notna().sum())
total_contactos = int(dff["NUM_CONTACTOS"].fillna(0).sum())
total_interacciones = int(dff["NUM_INTERACCIONES"].fillna(0).sum())

# Helper para scroll automático a seccion de Gestión de Leads
def _scroll_to_leads():
    """Inyecta JS para scroll automático al ancla de Gestión de Leads."""
    import time
    ts = int(time.time() * 1000)
    components.html(f"""
        <!-- {ts} -->
        <script>
        const doc = window.parent.document;
        function doScroll() {{
            const headers = doc.querySelectorAll('h2, h3, [data-testid="stSubheader"]');
            for (let h of headers) {{
                if (h.textContent && h.textContent.indexOf('Gestión de Leads') !== -1) {{
                    h.scrollIntoView({{behavior: 'smooth', block: 'start'}});
                    return;
                }}
            }}
            const divs = doc.querySelectorAll('div');
            for (let d of divs) {{
                if (d.id === 'gestion-leads') {{
                    d.scrollIntoView({{behavior: 'smooth', block: 'start'}});
                    return;
                }}
            }}
        }}
        setTimeout(doScroll, 600);
        setTimeout(doScroll, 1200);
        </script>
    """, height=0)

# KPIs como botones clickables que setean filtro temporal
k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
with k1:
    if st.button(f"**{total}**\n\nTotal Cuentas", key="kpi_total", use_container_width=True):
        st.session_state["_kpi_filter"] = {"tipo": "total"}
        st.rerun()
with k2:
    if st.button(f"**{contactadas}**\n\nContactadas", key="kpi_contactadas", use_container_width=True):
        st.session_state["_kpi_filter"] = {"tipo": "contactadas"}
        st.rerun()
with k3:
    if st.button(f"**{pendientes}**\n\nPendientes", key="kpi_pendientes", use_container_width=True):
        st.session_state["_kpi_filter"] = {"tipo": "pendientes"}
        st.rerun()
with k4:
    if st.button(f"**{con_whatsapp}**\n\nCon WhatsApp", key="kpi_whatsapp", use_container_width=True):
        st.session_state["_kpi_filter"] = {"tipo": "whatsapp"}
        st.rerun()
with k5:
    if st.button(f"**{con_email}**\n\nCon Email", key="kpi_email", use_container_width=True):
        st.session_state["_kpi_filter"] = {"tipo": "email"}
        st.rerun()
with k6:
    if st.button(f"**{total_contactos}**\n\nTotal Contactos", key="kpi_contactos", use_container_width=True):
        st.session_state["_kpi_filter"] = {"tipo": "contactos"}
        st.rerun()
with k7:
    if st.button(f"**{total_interacciones}**\n\nInteracciones", key="kpi_interacciones", use_container_width=True):
        st.session_state["_kpi_filter"] = {"tipo": "interacciones"}
        st.rerun()

# =============================================================
# CALCULAR SCORES (necesario para ambas pestanas)
# =============================================================

tamano_score = {"Micro": 1, "Pequeña": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
enriq_cols = ["SITIO_WEB", "UBICACION", "CONTACTO_NOMBRE", "LINKEDIN_EMPRESA", "CONTACTO_CARGO"]
df_sc = dff.copy()
df_sc["ENRIQ"] = df_sc[enriq_cols].apply(lambda r: sum(1 for v in r if v and str(v).strip()), axis=1)
df_sc["TAM_SC"] = df_sc["TAMANO_EMPRESA"].map(tamano_score).fillna(0).astype(int)
df_sc["SCORE"] = df_sc["ENRIQ"] + df_sc["TAM_SC"]

# =============================================================
# PESTANAS: VISION GENERAL | GRAFICAS | TOP 10 | TOP 5 INDUSTRIA | INSIGHTS | CARGAR LEADS
# =============================================================

tab_vision, tab_graficas, tab_top10, tab_top5, tab_insights, tab_cargar = st.tabs(
    ["Visión General", "Gráficas", "Top 10 Cuentas", "Top 5 por Industria", "Insights", "Cargar Leads"]
)

# -- TAB VISION GENERAL (cross-evento) --
with tab_vision:
    st.subheader("Vision General por Evento")
    st.caption("Metricas consolidadas de todos los eventos — no afectada por el filtro de evento")

    # Agrupar por evento usando df_all (sin filtro de evento)
    if "NOMBRE_EVENTO" in df_all.columns and not df_all.empty:
        ev_summary = df_all.groupby("NOMBRE_EVENTO").agg(
            Cuentas=("CUENTA_ID", "count"),
            Contactadas=("CONTACTADO", lambda x: int(x.fillna(False).sum())),
            Con_Email=("CONTACTO_EMAIL", lambda x: int(x.notna().sum())),
            Con_WhatsApp=("CONTACTO_WHATSAPP", lambda x: int(x.notna().sum())),
            Con_Sitio_Web=("SITIO_WEB", lambda x: int(x.apply(lambda v: bool(v and str(v).strip() and str(v).strip() not in ("", "nan", "None"))).sum())),
            Interacciones=("NUM_INTERACCIONES", lambda x: int(x.fillna(0).sum())),
        ).reset_index()
        ev_summary.columns = ["Evento", "Cuentas", "Contactadas", "Con Email", "Con WhatsApp", "Con Sitio Web", "Interacciones"]
        ev_summary["Pendientes"] = ev_summary["Cuentas"] - ev_summary["Contactadas"]
        ev_summary["% Avance"] = (ev_summary["Contactadas"] / ev_summary["Cuentas"] * 100).round(1)

        # Metricas globales
        _tot = ev_summary["Cuentas"].sum()
        _cont = ev_summary["Contactadas"].sum()
        gm1, gm2, gm3, gm4 = st.columns(4)
        gm1.metric("Total Cuentas", _tot)
        gm2.metric("Contactadas", _cont)
        gm3.metric("Pendientes", _tot - _cont)
        gm4.metric("Eventos", len(ev_summary))

        st.dataframe(ev_summary, use_container_width=True, hide_index=True)

        # Grafica comparativa
        if len(ev_summary) > 1:
            fig_ev = px.bar(ev_summary, x="Evento", y=["Contactadas", "Pendientes"],
                            barmode="stack", text_auto=True,
                            color_discrete_sequence=["#2ecc71", "#95a5a6"])
            fig_ev.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0),
                                 legend_title_text="")
            st.plotly_chart(fig_ev, use_container_width=True)

        # Pipeline por evento
        if len(ev_summary) >= 1:
            st.subheader("Pipeline por Evento")
            ev_pipe = df_all.groupby(["NOMBRE_EVENTO", "ESTATUS"]).size().reset_index(name="Cuentas")
            fig_pipe_ev = px.bar(ev_pipe, x="ESTATUS", y="Cuentas", color="NOMBRE_EVENTO",
                                 barmode="group", text_auto=True)
            fig_pipe_ev.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0),
                                      legend_title_text="Evento")
            st.plotly_chart(fig_pipe_ev, use_container_width=True)
    else:
        st.info("No hay datos de eventos disponibles.")

# -- TAB GRAFICAS --
with tab_graficas:

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.subheader("Distribucion por Industria")
            ind_counts = dff["INDUSTRIA_NOMBRE"].value_counts().reset_index()
            ind_counts.columns = ["Industria", "Cuentas"]
            fig_ind = px.bar(ind_counts, x="Cuentas", y="Industria", orientation="h",
                             color="Industria", text="Cuentas",
                             color_discrete_sequence=px.colors.qualitative.Set2)
            fig_ind.update_layout(showlegend=False, height=400, margin=dict(l=0, r=0, t=10, b=0))
            fig_ind.update_traces(textposition="outside")
            st.plotly_chart(fig_ind, width="stretch")
            # Botones clickables por industria
            if not ind_counts.empty:
                st.caption("Click en una industria para filtrar leads:")
                _n_ind = min(len(ind_counts), 4)
                _ind_cols = st.columns(_n_ind)
                for _ic, (_, _ir) in enumerate(ind_counts.iterrows()):
                    with _ind_cols[_ic % _n_ind]:
                        if st.button(f"{_ir['Industria']} ({_ir['Cuentas']})", key=f"gf_ind_{_ic}", use_container_width=True):
                            st.session_state["_kpi_filter"] = {"tipo": "industria", "valor": _ir["Industria"]}
                            st.rerun()

    with col2:
        with st.container(border=True):
            st.subheader("Pipeline Comercial")
            status_order = ["PENDIENTE", "CONTACTADO", "EN_SEGUIMIENTO", "CALIFICADO", "OPORTUNIDAD", "DESCARTADO", "DESCALIFICADO"]
            status_counts = dff["ESTATUS"].value_counts().reindex(status_order).dropna().reset_index()
            status_counts.columns = ["Estatus", "Cuentas"]
            colors_map = {"PENDIENTE": "#95a5a6", "CONTACTADO": "#3498db", "EN_SEGUIMIENTO": "#f39c12",
                          "CALIFICADO": "#2ecc71", "OPORTUNIDAD": "#9b59b6", "DESCARTADO": "#e74c3c",
                          "DESCALIFICADO": "#2c3e50"}
            fig_pipe = px.bar(status_counts, x="Estatus", y="Cuentas", text="Cuentas",
                              color="Estatus", color_discrete_map=colors_map)
            fig_pipe.update_layout(showlegend=False, height=400, margin=dict(l=0, r=0, t=10, b=0))
            fig_pipe.update_traces(textposition="outside")
            st.plotly_chart(fig_pipe, width="stretch")
            # Botones clickables por estatus del pipeline
            if not status_counts.empty:
                st.caption("Click en un estatus para filtrar leads:")
                _n_st = min(len(status_counts), 4)
                _st_cols = st.columns(_n_st)
                for _sc, (_, _sr) in enumerate(status_counts.iterrows()):
                    with _st_cols[_sc % _n_st]:
                        if st.button(f"{_sr['Estatus']} ({_sr['Cuentas']})", key=f"gf_pip_{_sc}", use_container_width=True):
                            st.session_state["_kpi_filter"] = {"tipo": "estatus", "valor": _sr["Estatus"]}
                            st.rerun()

    col3, col4 = st.columns(2)

    with col3:
        with st.container(border=True):
            st.subheader("Tamaño de Empresa")
            tamano_order = ["Micro", "Pequeña", "Mediana", "Grande", "Enterprise"]
            tam_counts = dff["TAMANO_EMPRESA"].value_counts().reindex(tamano_order).dropna().reset_index()
            tam_counts.columns = ["Tamaño", "Cuentas"]
            fig_tam = px.bar(tam_counts, x="Tamaño", y="Cuentas", text="Cuentas",
                             color="Tamaño", color_discrete_sequence=px.colors.sequential.Viridis)
            fig_tam.update_layout(showlegend=False, height=350, margin=dict(l=0, r=0, t=10, b=0))
            fig_tam.update_traces(textposition="outside")
            st.plotly_chart(fig_tam, width="stretch")

    with col4:
        with st.container(border=True):
            st.subheader("Distribución Geográfica (Top 10)")
            geo = dff["PAIS"].dropna().value_counts().head(10).reset_index()
            geo.columns = ["Pais", "Cuentas"]
            fig_geo = px.pie(geo, values="Cuentas", names="Pais",
                             color_discrete_sequence=px.colors.qualitative.Set3, hole=0.35)
            fig_geo.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_geo, width="stretch")

    # -- Grafica de Actividad de Interacciones --
    with st.container(border=True):
        st.subheader("Actividad de Interacciones")
        df_inter = load_all_interacciones()
        if df_inter.empty:
            st.info("No hay interacciones registradas aún.")
        else:
            df_act = df_inter.copy()
            df_act["FECHA"] = pd.to_datetime(df_act["FECHA"], errors="coerce")
            df_act = df_act.dropna(subset=["FECHA"])
            agrup = st.radio("Agrupar por:", ["Dia", "Semana", "Mes"], horizontal=True, key="act_agrup")
            if agrup == "Dia":
                df_act["PERIODO"] = df_act["FECHA"].dt.date
            elif agrup == "Semana":
                df_act["PERIODO"] = df_act["FECHA"].dt.to_period("W").apply(lambda p: p.start_time.date())
            else:
                df_act["PERIODO"] = df_act["FECHA"].dt.to_period("M").apply(lambda p: p.start_time.date())
            act_counts = df_act.groupby(["PERIODO", "TIPO"]).size().reset_index(name="Cantidad")
            act_counts["PERIODO"] = act_counts["PERIODO"].astype(str)
            fig_act = px.bar(act_counts, x="PERIODO", y="Cantidad", color="TIPO",
                             text="Cantidad", barmode="stack",
                             color_discrete_sequence=px.colors.qualitative.Pastel,
                             labels={"PERIODO": "Periodo", "TIPO": "Tipo"})
            fig_act.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0),
                                  xaxis_title="Periodo", yaxis_title="Interacciones")
            fig_act.update_traces(textposition="inside")
            st.plotly_chart(fig_act, use_container_width=True)

# -- TAB TOP 10 --
with tab_top10:

    st.subheader("Top 10 Cuentas de Mayor Interes")
    st.caption("Score combinado: datos obtenidos (0-5) + tamaño empresa (0-5) | Click en una fila para ver detalle")

    top10 = df_sc.nlargest(10, ["SCORE", "TAM_SC", "ENRIQ"])

    ct1, ct2 = st.columns([2, 1])
    with ct1:
        with st.container(border=True):
            t10d = top10[["ACCT_NAME", "INDUSTRIA_NOMBRE", "TAMANO_EMPRESA",
                           "ENRIQ", "TAM_SC", "SCORE", "CONTACTO_NOMBRE", "ESTATUS"]].copy()
            t10d.columns = ["Empresa", "Industria", "Tamaño", "Datos (0-5)", "Tamaño (0-5)",
                            "Score", "Contacto", "Estatus"]
            t10d = t10d.fillna("")
            # Header
            hc = st.columns([2.5, 1.5, 1, 0.8, 0.8, 0.7, 1.5, 1.2])
            headers = ["Empresa", "Industria", "Tamaño", "Datos", "Tam.", "Score", "Contacto", "Estatus"]
            for col, h in zip(hc, headers):
                col.markdown(f"**{h}**")
            # Filas con empresa como link
            for i, (_, rw) in enumerate(t10d.iterrows()):
                rc = st.columns([2.5, 1.5, 1, 0.8, 0.8, 0.7, 1.5, 1.2])
                with rc[0]:
                    if st.button(f":link: {rw['Empresa']}", key=f"t10_{i}", use_container_width=True):
                        st.session_state["_open_cuenta"] = rw["Empresa"]
                        st.rerun()
                rc[1].write(rw["Industria"])
                rc[2].write(rw["Tamaño"])
                rc[3].write(str(rw["Datos (0-5)"]))
                rc[4].write(str(rw["Tamaño (0-5)"]))
                rc[5].write(str(rw["Score"]))
                rc[6].write(rw["Contacto"])
                rc[7].write(rw["Estatus"])

    with ct2:
        with st.container(border=True):
            st.markdown("**Composición del Score**")
            fig_sc = px.bar(t10d, y="Empresa", x=["Datos (0-5)", "Tamaño (0-5)"],
                            orientation="h", barmode="stack",
                            color_discrete_sequence=["#3498db", "#2ecc71"])
            fig_sc.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0),
                                 yaxis=dict(autorange="reversed"), legend=dict(orientation="h", y=-0.15))
            st.plotly_chart(fig_sc, width="stretch")

# -- TAB TOP 5 POR INDUSTRIA --
with tab_top5:

    st.subheader("Top 5 Cuentas por Industria")
    st.caption("Score combinado: datos obtenidos (0-5) + tamaño empresa (0-5)")

    industrias_disponibles = sorted(dff["INDUSTRIA_NOMBRE"].unique())
    ti1, ti2 = st.columns([1, 2])
    with ti1:
        sel_industria_top5 = st.selectbox("Selecciona industria:", industrias_disponibles, key="sel_ind_top5")
    with ti2:
        df_ind = df_sc[df_sc["INDUSTRIA_NOMBRE"] == sel_industria_top5].nlargest(5, ["SCORE", "TAM_SC", "ENRIQ"])
        if not df_ind.empty:
            t5d = df_ind[["ACCT_NAME", "TAMANO_EMPRESA", "ENRIQ", "TAM_SC", "SCORE", "CONTACTO_NOMBRE", "ESTATUS"]].copy()
            t5d.columns = ["Empresa", "Tamaño", "Datos (0-5)", "Tamaño (0-5)", "Score", "Contacto", "Estatus"]
            t5d = t5d.fillna("")
            with st.container(border=True):
                fig_t5 = px.bar(t5d, y="Empresa", x=["Datos (0-5)", "Tamaño (0-5)"],
                                orientation="h", barmode="stack",
                                color_discrete_sequence=["#3498db", "#2ecc71"],
                                title=f"Top 5 - {sel_industria_top5}")
                fig_t5.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0),
                                     yaxis=dict(autorange="reversed"),
                                     legend=dict(orientation="h", y=-0.2))
                st.plotly_chart(fig_t5, width="stretch")
                # Links a detalle de cada empresa
                t5_cols = st.columns(len(t5d))
                for j, (_, r5) in enumerate(t5d.iterrows()):
                    with t5_cols[j]:
                        if st.button(f":link: {r5['Empresa']}", key=f"t5ind_{j}", use_container_width=True):
                            st.session_state["_open_cuenta"] = r5["Empresa"]
                            st.rerun()
        else:
            st.info("No hay cuentas en esta industria con los filtros actuales.")

# -- TAB INSIGHTS --
with tab_insights:

    st.subheader("Insights por Industria: Casos de Uso Snowflake")
    st.caption("Tendencias, retos y oportunidades generadas con Cortex AI")

    industrias_ci = sorted(df_casos["INDUSTRIA_NOMBRE"].unique())
    if industrias_ci:
        tabs_insights = st.tabs(industrias_ci)
        for tab_ins, industria in zip(tabs_insights, industrias_ci):
            with tab_ins:
                caso = df_casos[df_casos["INDUSTRIA_NOMBRE"] == industria].iloc[0]
                n_cuentas = len(dff[dff["INDUSTRIA_NOMBRE"] == industria])
                st.caption(f"{n_cuentas} cuentas en esta industria")
                with st.container(border=True):
                    st.markdown("**Propuesta de Valor**")
                    st.info(str(caso["PROPUESTA_VALOR"])[:1000])
                c1, c2, c3 = st.columns(3)
                with c1:
                    with st.container(border=True, height=350):
                        st.markdown("**Tendencias 2025-2026**")
                        st.write(str(caso["TENDENCIAS_INDUSTRIA"])[:1500])
                with c2:
                    with st.container(border=True, height=350):
                        st.markdown("**Retos Principales**")
                        st.write(str(caso["RETOS_PRINCIPALES"])[:1500])
                with c3:
                    with st.container(border=True, height=350):
                        st.markdown("**Casos de Uso Snowflake**")
                        st.write(str(caso["CASOS_USO_SNOWFLAKE"])[:1500])

# -- TAB CARGAR LEADS --
with tab_cargar:
    st.subheader("Cargar Leads desde CSV")
    st.caption("Sube un archivo CSV con leads de un evento. Mapea las columnas y carga los datos a Snowflake.")

    # Template CSV descargable
    _template_csv = "Empresa,Nombre,Apellidos,Email,Cargo,WhatsApp,Industria\nEjemplo S.A.,Juan,Perez,juan@ejemplo.com,Director de TI,5551234567,Tecnologia\n"
    st.download_button("Descargar template CSV", data=_template_csv, file_name="template_leads.csv", mime="text/csv", key="_dl_template")

    # 1. Seleccionar o crear evento
    _ev_opts = df_eventos["NOMBRE_EVENTO"].tolist() if not df_eventos.empty else []
    _ev_choice = st.radio("Evento destino:", ["Evento existente", "Crear nuevo evento"], horizontal=True, key="_ev_choice")

    evento_id_carga = None
    if _ev_choice == "Evento existente" and _ev_opts:
        _ev_sel = st.selectbox("Selecciona evento:", _ev_opts, key="_ev_sel_carga")
        _ev_row = df_eventos[df_eventos["NOMBRE_EVENTO"] == _ev_sel]
        if not _ev_row.empty:
            evento_id_carga = int(_ev_row.iloc[0]["EVENTO_ID"])
    elif _ev_choice == "Crear nuevo evento":
        nc1, nc2 = st.columns(2)
        nuevo_nombre = nc1.text_input("Nombre del evento:", key="_nuevo_ev_nombre", placeholder="Ej: Data Summit 2026")
        nueva_fecha = nc2.date_input("Fecha del evento:", key="_nuevo_ev_fecha")
        nueva_desc = st.text_input("Descripción (opcional):", key="_nuevo_ev_desc")
    elif _ev_choice == "Evento existente" and not _ev_opts:
        st.warning("No hay eventos registrados. Crea uno nuevo.")

    # 2. Subir CSV
    uploaded = st.file_uploader("Sube tu archivo CSV:", type=["csv"], key="_csv_upload")

    if uploaded is not None:
        df_csv = None
        for _enc in ("utf-8", "latin-1", "cp1252"):
            try:
                uploaded.seek(0)
                df_csv = pd.read_csv(uploaded, encoding=_enc)
                break
            except (UnicodeDecodeError, Exception):
                continue
        if df_csv is None:
            st.error("Error al leer CSV: no se pudo decodificar el archivo. Verifica que sea un CSV válido.")

        if df_csv is not None and not df_csv.empty:
            st.success(f"Archivo cargado: {len(df_csv)} filas, {len(df_csv.columns)} columnas")
            st.dataframe(df_csv.head(5), use_container_width=True, hide_index=True)

            # 3. Mapeo de columnas
            st.markdown("**Mapeo de columnas**")
            st.caption("Selecciona qué columna del CSV corresponde a cada campo del sistema.")
            csv_cols = ["— No mapear —"] + list(df_csv.columns)

            CAMPOS_SISTEMA = [
                ("Nombre", "NOMBRE"), ("Apellidos", "APELLIDOS"), ("Email", "EMAIL"),
                ("Empresa", "EMPRESA"), ("Cargo", "CARGO_CONTEXTO"),
                ("WhatsApp", "WHATSAPP"), ("Industria", "INDUSTRIA")
            ]
            mapeo = {}
            mc_cols = st.columns(len(CAMPOS_SISTEMA))
            for i, (label, campo) in enumerate(CAMPOS_SISTEMA):
                # Intentar auto-detectar por nombre similar
                default_idx = 0
                for j, cc in enumerate(csv_cols):
                    if cc != "— No mapear —" and (cc.upper() == campo or label.upper() in cc.upper()):
                        default_idx = j
                        break
                with mc_cols[i]:
                    sel = st.selectbox(label, csv_cols, index=default_idx, key=f"_map_{campo}")
                    if sel != "— No mapear —":
                        mapeo[campo] = sel

            # Validar mapeo minimo
            if "EMPRESA" not in mapeo:
                st.warning("El campo **Empresa** es obligatorio para cargar leads.")
            else:
                # 4. Boton de carga
                if st.button("Cargar leads a Snowflake", type="primary", key="_btn_cargar"):
                    # Crear evento si es nuevo
                    if _ev_choice == "Crear nuevo evento":
                        if not nuevo_nombre or not nuevo_nombre.strip():
                            st.error("Ingresa un nombre para el nuevo evento.")
                            st.stop()
                        conn_ev = get_connection()
                        cur_ev = conn_ev.cursor()
                        try:
                            cur_ev.execute(f"""
                                INSERT INTO {DB}.CORE.DIM_EVENTOS (NOMBRE_EVENTO, FECHA_EVENTO, DESCRIPCION)
                                VALUES (%s, %s, %s)
                            """, (nuevo_nombre.strip(), str(nueva_fecha), nueva_desc.strip() if nueva_desc else None))
                            cur_ev.execute("SELECT MAX(EVENTO_ID) FROM " + DB + ".CORE.DIM_EVENTOS")
                            evento_id_carga = int(cur_ev.fetchone()[0])
                            conn_ev.commit()
                        finally:
                            cur_ev.close()
                            conn_ev.close()

                    if evento_id_carga is None:
                        st.error("Selecciona o crea un evento antes de cargar.")
                        st.stop()

                    # Procesar filas del CSV
                    conn_carga = get_connection()
                    cur_carga = conn_carga.cursor()
                    cuentas_nuevas = 0
                    contactos_agregados = 0
                    errores = 0
                    progress_carga = st.progress(0, text="Cargando leads...")

                    try:
                        for idx, row_csv in df_csv.iterrows():
                            progress_carga.progress((idx + 1) / len(df_csv),
                                                    text=f"Procesando fila {idx + 1} de {len(df_csv)}...")
                            empresa = str(row_csv.get(mapeo.get("EMPRESA", ""), "") or "").strip()
                            if not empresa:
                                errores += 1
                                continue

                            nombre = str(row_csv.get(mapeo.get("NOMBRE", ""), "") or "").strip()
                            apellidos = str(row_csv.get(mapeo.get("APELLIDOS", ""), "") or "").strip()
                            email = str(row_csv.get(mapeo.get("EMAIL", ""), "") or "").strip()
                            cargo = str(row_csv.get(mapeo.get("CARGO_CONTEXTO", ""), "") or "").strip()
                            whatsapp = str(row_csv.get(mapeo.get("WHATSAPP", ""), "") or "").strip()
                            industria = str(row_csv.get(mapeo.get("INDUSTRIA", ""), "") or "").strip()

                            # Limpiar valores "nan" de pandas
                            nombre = "" if nombre.lower() in ("nan", "none", "null") else nombre
                            apellidos = "" if apellidos.lower() in ("nan", "none", "null") else apellidos
                            email = "" if email.lower() in ("nan", "none", "null") else email
                            cargo = "" if cargo.lower() in ("nan", "none", "null") else cargo
                            whatsapp = "" if whatsapp.lower() in ("nan", "none", "null") else whatsapp
                            industria = "" if industria.lower() in ("nan", "none", "null") else industria

                            # Buscar si la cuenta ya existe (por nombre de empresa, case-insensitive)
                            cur_carga.execute(f"""
                                SELECT CUENTA_ID FROM {DB}.CORE.DIM_CUENTAS
                                WHERE UPPER(TRIM(ACCT_NAME)) = %s
                            """, (empresa.upper(),))
                            cuenta_row = cur_carga.fetchone()

                            if cuenta_row:
                                cuenta_id = int(cuenta_row[0])
                            else:
                                # Buscar o crear industria
                                industria_id = None
                                if industria:
                                    cur_carga.execute(f"""
                                        SELECT INDUSTRIA_ID FROM {DB}.CORE.DIM_INDUSTRIAS
                                        WHERE UPPER(TRIM(INDUSTRIA_NOMBRE)) = %s
                                    """, (industria.upper(),))
                                    ind_row = cur_carga.fetchone()
                                    if ind_row:
                                        industria_id = int(ind_row[0])
                                    else:
                                        cur_carga.execute(f"""
                                            INSERT INTO {DB}.CORE.DIM_INDUSTRIAS (INDUSTRIA_NOMBRE)
                                            VALUES (%s)
                                        """, (industria,))
                                        cur_carga.execute(f"SELECT MAX(INDUSTRIA_ID) FROM {DB}.CORE.DIM_INDUSTRIAS")
                                        industria_id = int(cur_carga.fetchone()[0])
                                if not industria_id:
                                    # Auto-clasificar industria con Cortex AI usando empresa + email
                                    _dominio = email.split("@")[1] if email and "@" in email else ""
                                    _prompt_ind = (
                                        "Classify the following company into exactly ONE of these industries. "
                                        "Reply with ONLY the industry name, nothing else.\n\n"
                                        "Industries: Technology, Fintech/Financial Services, "
                                        "Retail/Consumer Goods, Manufacturing/Industrial, "
                                        "Telecommunications, Consulting/Professional Services, "
                                        "Food & Beverage, Education/Research, Automotive, "
                                        "Media/Entertainment, E-commerce, Energy, "
                                        "Government/Public Sector, Healthcare/Pharma, "
                                        "Logistics/Transportation, Insurance\n\n"
                                        f"Company: {empresa}\n"
                                        f"Email domain: {_dominio}\n\n"
                                        "Industry:"
                                    )
                                    try:
                                        cur_carga.execute(
                                            "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s)",
                                            (_prompt_ind,)
                                        )
                                        _ai_ind = cur_carga.fetchone()[0].strip().strip('"').strip("'").strip()
                                        # Buscar coincidencia en DIM_INDUSTRIAS
                                        cur_carga.execute(f"""
                                            SELECT INDUSTRIA_ID FROM {DB}.CORE.DIM_INDUSTRIAS
                                            WHERE UPPER(TRIM(INDUSTRIA_NOMBRE)) = %s
                                        """, (_ai_ind.upper(),))
                                        _ai_row = cur_carga.fetchone()
                                        if _ai_row:
                                            industria_id = int(_ai_row[0])
                                    except Exception:
                                        pass  # Si falla Cortex, cae al fallback abajo

                                    if not industria_id:
                                        # Fallback: "Sin Clasificar"
                                        cur_carga.execute(f"SELECT MIN(INDUSTRIA_ID) FROM {DB}.CORE.DIM_INDUSTRIAS")
                                        industria_id = int(cur_carga.fetchone()[0])

                                # Crear cuenta nueva
                                cur_carga.execute(f"""
                                    INSERT INTO {DB}.CORE.DIM_CUENTAS (ACCT_NAME, INDUSTRIA_ID, EVENTO_ID, FUENTE_LEAD)
                                    VALUES (%s, %s, %s, %s)
                                """, (empresa, industria_id, evento_id_carga, f"EVENTO_{evento_id_carga}"))
                                cur_carga.execute(f"SELECT MAX(CUENTA_ID) FROM {DB}.CORE.DIM_CUENTAS")
                                cuenta_id = int(cur_carga.fetchone()[0])
                                cuentas_nuevas += 1

                            # Agregar contacto si tiene al menos nombre o email
                            if nombre or email:
                                nombre_completo = f"{nombre} {apellidos}".strip() if nombre else ""
                                cur_carga.execute(f"""
                                    INSERT INTO {DB}.CORE.DIM_CONTACTOS
                                    (CUENTA_ID, PRIMER_NOMBRE, APELLIDO, NOMBRE_COMPLETO, CARGO, EMAIL, WHATSAPP, FUENTE, ES_PRINCIPAL)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, (cuenta_id, nombre or None, apellidos or None,
                                      nombre_completo or None, cargo or None,
                                      email or None, whatsapp or None,
                                      f"EVENTO_{evento_id_carga}", True))
                                contactos_agregados += 1

                        conn_carga.commit()
                    except Exception as e:
                        st.error(f"Error durante la carga: {e}")
                        errores += 1
                    finally:
                        cur_carga.close()
                        conn_carga.close()

                    progress_carga.empty()
                    st.cache_data.clear()
                    st.success(
                        f"Carga completada: {cuentas_nuevas} cuentas nuevas, "
                        f"{contactos_agregados} contactos agregados, "
                        f"{errores} filas con error."
                    )
                    if cuentas_nuevas > 0 or contactos_agregados > 0:
                        st.rerun()

    # ---- Seccion: Eliminar evento ----
    st.divider()
    with st.expander("Eliminar evento y sus datos", expanded=False):
        st.warning("Esta acción eliminará el evento seleccionado y **todas** las cuentas, contactos e interacciones asociadas. No se puede deshacer.")
        _ev_del_opts = df_eventos["NOMBRE_EVENTO"].tolist() if not df_eventos.empty else []
        if _ev_del_opts:
            _ev_del_sel = st.selectbox("Evento a eliminar:", _ev_del_opts, key="_ev_del_sel")
            _ev_del_row = df_eventos[df_eventos["NOMBRE_EVENTO"] == _ev_del_sel]
            _ev_del_id = int(_ev_del_row.iloc[0]["EVENTO_ID"]) if not _ev_del_row.empty else None

            # Mostrar resumen de lo que se va a borrar
            if _ev_del_id is not None:
                _n_ctas = int((df_all["EVENTO_ID"] == _ev_del_id).sum()) if "EVENTO_ID" in df_all.columns else 0
                st.info(f"El evento **{_ev_del_sel}** tiene aproximadamente **{_n_ctas}** cuentas asociadas.")

            _confirm_txt = st.text_input("Escribe ELIMINAR para confirmar:", key="_del_confirm", placeholder="ELIMINAR")
            if st.button("Eliminar evento", type="primary", key="_btn_del_evento"):
                if _confirm_txt != "ELIMINAR":
                    st.error("Escribe ELIMINAR (en mayusculas) para confirmar la eliminación.")
                elif _ev_del_id is None:
                    st.error("No se pudo identificar el evento.")
                else:
                    conn_del = get_connection()
                    cur_del = conn_del.cursor()
                    try:
                        # 1. Borrar interacciones de cuentas del evento
                        cur_del.execute(f"""
                            DELETE FROM {DB}.CORE.FACT_INTERACCIONES
                            WHERE CUENTA_ID IN (
                                SELECT CUENTA_ID FROM {DB}.CORE.DIM_CUENTAS WHERE EVENTO_ID = %s
                            )
                        """, (_ev_del_id,))
                        n_inter = cur_del.rowcount

                        # 2. Borrar contactos de cuentas del evento
                        cur_del.execute(f"""
                            DELETE FROM {DB}.CORE.DIM_CONTACTOS
                            WHERE CUENTA_ID IN (
                                SELECT CUENTA_ID FROM {DB}.CORE.DIM_CUENTAS WHERE EVENTO_ID = %s
                            )
                        """, (_ev_del_id,))
                        n_cont = cur_del.rowcount

                        # 3. Borrar cuentas del evento
                        cur_del.execute(f"""
                            DELETE FROM {DB}.CORE.DIM_CUENTAS WHERE EVENTO_ID = %s
                        """, (_ev_del_id,))
                        n_ctas = cur_del.rowcount

                        # 4. Borrar el evento
                        cur_del.execute(f"""
                            DELETE FROM {DB}.CORE.DIM_EVENTOS WHERE EVENTO_ID = %s
                        """, (_ev_del_id,))

                        conn_del.commit()
                        st.cache_data.clear()
                        st.success(
                            f"Evento **{_ev_del_sel}** eliminado. "
                            f"Se borraron {n_ctas} cuentas, {n_cont} contactos y {n_inter} interacciones."
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al eliminar: {e}")
                    finally:
                        cur_del.close()
                        conn_del.close()
        else:
            st.info("No hay eventos para eliminar.")

# =============================================================
# GESTION DE LEADS (siempre visible, debajo de las pestanas)
# =============================================================

st.divider()
# Ancla HTML para scroll automático desde KPIs / graficas
st.markdown('<div id="gestion-leads"></div>', unsafe_allow_html=True)

# Leer filtro temporal si existe
_kpi_f = st.session_state.get("_kpi_filter", None)
_filter_label = None

if _kpi_f:
    _scroll_to_leads()
    ftipo = _kpi_f.get("tipo")
    fvalor = _kpi_f.get("valor")

    # Caso especial: interacciones -> mostrar tabla resumen global
    if ftipo == "interacciones":
        st.subheader("Resumen Global de Interacciones")
        _filter_label = "Interacciones"
        if st.button("Limpiar filtro", key="limpiar_filtro_inter", type="secondary"):
            st.session_state.pop("_kpi_filter", None)
            st.session_state.pop("_kpi_filter_prev", None)
            st.rerun()
        df_inter_all = load_all_interacciones()
        if df_inter_all.empty:
            st.info("No hay interacciones registradas aún.")
        else:
            st.dataframe(df_inter_all, use_container_width=True, hide_index=True)
        st.stop()

    # Caso especial: contactos -> mostrar todos los contactos
    elif ftipo == "contactos":
        _filter_label = "Total Contactos"

    # Otros filtros aplican sobre df_leads mas abajo
    elif ftipo == "total":
        _filter_label = "Todas las Cuentas"
    elif ftipo == "contactadas":
        _filter_label = "Cuentas Contactadas"
    elif ftipo == "pendientes":
        _filter_label = "Cuentas Pendientes"
    elif ftipo == "whatsapp":
        _filter_label = "Cuentas con WhatsApp"
    elif ftipo == "email":
        _filter_label = "Cuentas con Email"
    elif ftipo == "estatus":
        _filter_label = f"Estatus: {fvalor}"
    elif ftipo == "industria":
        _filter_label = f"Industria: {fvalor}"

if _filter_label:
    st.subheader(f"Gestión de Leads — {_filter_label}")
    if st.button("Limpiar filtro", key="limpiar_filtro_leads", type="secondary"):
        st.session_state.pop("_kpi_filter", None)
        st.session_state.pop("_kpi_filter_prev", None)
        st.rerun()
else:
    st.subheader("Gestión de Leads")

st.caption("Click en el nombre de la empresa para ver detalle | Usa la seccion inferior para marcar contactado")

# Barra de busqueda de empresa / lead
busqueda_lead = st.text_input("Buscar empresa o contacto:", placeholder="Escribe para filtrar...", key="busqueda_lead")

# Preparar DataFrame para display
df_leads = dff[["CUENTA_ID", "CONTACTO_ID", "ACCT_NAME", "INDUSTRIA_NOMBRE", "TAMANO_EMPRESA",
                 "CONTACTO_NOMBRE", "CONTACTO_CARGO", "CONTACTO_EMAIL", "CONTACTO_WHATSAPP",
                 "ESTATUS", "CONTACTADO", "NUM_CONTACTOS", "NUM_INTERACCIONES"]].copy()
df_leads["CONTACTADO"] = df_leads["CONTACTADO"].fillna(False).astype(bool)

if busqueda_lead and busqueda_lead.strip():
    _q = busqueda_lead.strip().lower()
    df_leads = df_leads[
        df_leads["ACCT_NAME"].fillna("").str.lower().str.contains(_q, regex=False) |
        df_leads["CONTACTO_NOMBRE"].fillna("").str.lower().str.contains(_q, regex=False)
    ]

# Aplicar filtro KPI si existe (excepto interacciones que ya se manejó arriba con st.stop)
if _kpi_f:
    ftipo = _kpi_f.get("tipo")
    fvalor = _kpi_f.get("valor")
    if ftipo == "contactadas":
        df_leads = df_leads[df_leads["CONTACTADO"] == True]
    elif ftipo == "pendientes":
        df_leads = df_leads[df_leads["CONTACTADO"] == False]
    elif ftipo == "whatsapp":
        df_leads = df_leads[df_leads["CONTACTO_WHATSAPP"].fillna("").str.strip() != ""]
    elif ftipo == "email":
        df_leads = df_leads[df_leads["CONTACTO_EMAIL"].fillna("").str.strip() != ""]
    elif ftipo == "estatus":
        df_leads = df_leads[df_leads["ESTATUS"] == fvalor]
    elif ftipo == "industria":
        df_leads = df_leads[df_leads["INDUSTRIA_NOMBRE"] == fvalor]
    # "total" y "contactos" no filtran, muestran todo
    # Resetear página solo cuando el filtro KPI es nuevo (no en cada rerun)
    if st.session_state.get("_kpi_filter_prev") != _kpi_f:
        st.session_state["leads_page"] = 0
        st.session_state["_kpi_filter_prev"] = _kpi_f

# Paginacion para no renderizar 79 filas de botones
LEADS_PER_PAGE = 15
total_leads = len(df_leads)
total_pages = max(1, (total_leads + LEADS_PER_PAGE - 1) // LEADS_PER_PAGE)
if "leads_page" not in st.session_state:
    st.session_state["leads_page"] = 0
current_page = st.session_state["leads_page"]

pg1, pg2, pg3 = st.columns([1, 2, 1])
with pg1:
    if st.button("< Anterior", key="leads_prev", disabled=(current_page == 0)):
        st.session_state["leads_page"] = current_page - 1
        st.rerun()
with pg2:
    st.markdown(f"<div style='text-align:center'>Pagina {current_page + 1} de {total_pages} ({total_leads} cuentas)</div>", unsafe_allow_html=True)
with pg3:
    if st.button("Siguiente >", key="leads_next", disabled=(current_page >= total_pages - 1)):
        st.session_state["leads_page"] = current_page + 1
        st.rerun()

start_idx = current_page * LEADS_PER_PAGE
end_idx = min(start_idx + LEADS_PER_PAGE, total_leads)
df_page = df_leads.iloc[start_idx:end_idx]

with st.container(border=True):
    # Header
    lhc = st.columns([2, 1.2, 0.9, 1.3, 1.3, 1.3, 0.9])
    for col, h in zip(lhc, ["Empresa", "Industria", "Tamano", "Contacto Princ.", "Email", "WhatsApp", "Estatus"]):
        col.markdown(f"**{h}**")
    # Filas
    for i, (_, rw) in enumerate(df_page.iterrows()):
        rc = st.columns([2, 1.2, 0.9, 1.3, 1.3, 1.3, 0.9])
        with rc[0]:
            if st.button(f":link: {rw['ACCT_NAME']}", key=f"lead_{start_idx + i}", use_container_width=True):
                st.session_state["_open_cuenta"] = rw["ACCT_NAME"]
                st.rerun()
        rc[1].write(rw["INDUSTRIA_NOMBRE"] or "")
        rc[2].write(rw["TAMANO_EMPRESA"] or "")
        rc[3].write(rw["CONTACTO_NOMBRE"] or "")
        email_val = rw["CONTACTO_EMAIL"]
        if email_val and str(email_val).strip():
            rc[4].markdown(f"[{str(email_val).strip()}](mailto:{str(email_val).strip()})")
        else:
            rc[4].write("")
        wa_val = rw["CONTACTO_WHATSAPP"]
        if wa_val and str(wa_val).strip():
            wa_limpio = str(wa_val).strip().replace("+", "").replace(" ", "").replace("-", "")
            rc[5].markdown(f"[{str(wa_val).strip()}](https://wa.me/{wa_limpio})")
        else:
            rc[5].write("")
        rc[6].write(rw["ESTATUS"] or "")

# =============================================================
# APERTURA UNICA DEL DIALOG (evita duplicados)
# =============================================================

if "_open_cuenta" in st.session_state and st.session_state["_open_cuenta"]:
    _cuenta_abrir = st.session_state.pop("_open_cuenta")
    mostrar_tarjeta_cuenta(_cuenta_abrir)

# =============================================================
# PIE DE PAGINA
# =============================================================

st.divider()
st.caption("EGOS BI Lead Manager v7 | DB_LEADS_SNOWFLAKE_WT25 | EGOS BI + Cortex AI")
