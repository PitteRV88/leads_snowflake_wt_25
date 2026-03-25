# =============================================================
# dashboard_leads_wt25.py  (v3 - cloud-ready)
# Dashboard interactivo para gestion de Leads del Snowflake World Tour 2025
# Incluye: KPIs, graficas, gestion de contactos, pitch IA, pipeline comercial
# v3: tarjeta popup, checkbox contactado, mailto links, pitch EGOS BI + Snowflake
# Compatible: Local (connection_name) y Streamlit Community Cloud (st.secrets)
# Creado: 2026-03-23 | Actualizado: 2026-03-25 | Conexion: TXA18114
# Proyecto: Leads Snowflake WT25 - EGOS BI
# =============================================================

import os
import urllib.parse
import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from datetime import datetime
from cryptography.hazmat.primitives import serialization

# -- Configuracion de pagina --
st.set_page_config(
    page_title="Leads Snowflake WT25 - EGOS BI",
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
    """Conexion fresca. Usa st.secrets (key-pair) en cloud, connection_name en local."""
    if _USE_SECRETS:
        sf = st.secrets["snowflake"]
        # Cargar private key desde el PEM almacenado en secrets
        private_key_pem = sf["private_key"].replace("\\n", "\n").encode("utf-8")
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
            c.CUENTA_ID, c.ACCT_NAME, i.INDUSTRIA_NOMBRE,
            c.TAMANO_EMPRESA, c.NUM_EMPLEADOS_ESTIMADO, c.REVENUE_ESTIMADO_USD,
            c.UBICACION, c.PAIS, c.ESTADO, c.CIUDAD,
            c.SITIO_WEB, c.LINKEDIN_EMPRESA,
            c.ESTATUS, c.EJECUTIVO_ID,
            c.FECHA_PRIMER_CONTACTO, c.FECHA_ULTIMO_CONTACTO,
            c.NOTAS, c.FUENTE_CLASIFICACION, c.FUENTE_TAMANO, c.FUENTE_LEAD,
            bc.CONTACTO_ID, bc.NOMBRE_COMPLETO AS CONTACTO_NOMBRE,
            bc.CARGO AS CONTACTO_CARGO, bc.NIVEL_CARGO AS CONTACTO_NIVEL,
            bc.EMAIL AS CONTACTO_EMAIL, bc.WHATSAPP AS CONTACTO_WHATSAPP,
            bc.CONTACTADO, bc.RESPUESTA, bc.METODO_CONTACTO,
            (SELECT COUNT(*) FROM {DB}.CORE.DIM_CONTACTOS ct WHERE ct.CUENTA_ID = c.CUENTA_ID) AS NUM_CONTACTOS,
            (SELECT COUNT(*) FROM {DB}.CORE.DIM_CONTACTOS ct WHERE ct.CUENTA_ID = c.CUENTA_ID AND ct.CONTACTADO = TRUE) AS CONTACTOS_CONTACTADOS,
            (SELECT COUNT(*) FROM {DB}.CORE.FACT_INTERACCIONES fi WHERE fi.CUENTA_ID = c.CUENTA_ID) AS NUM_INTERACCIONES
        FROM {DB}.CORE.DIM_CUENTAS c
        JOIN {DB}.CORE.DIM_INDUSTRIAS i ON c.INDUSTRIA_ID = i.INDUSTRIA_ID
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
    cur.close()
    conn.close()
    return df, df_casos


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

    # -- Datos principales --
    st.markdown(f"### {acct_name}")
    dc1, dc2, dc3, dc4 = st.columns(4)
    dc1.metric("Industria", row["INDUSTRIA_NOMBRE"])
    dc2.metric("Tamano", row["TAMANO_EMPRESA"] or "N/A")
    dc3.metric("Empleados Est.", f"{int(row['NUM_EMPLEADOS_ESTIMADO']):,}" if row["NUM_EMPLEADOS_ESTIMADO"] else "N/A")
    dc4.metric("Revenue Est.", f"${float(row['REVENUE_ESTIMADO_USD'])/1e6:,.1f}M" if row["REVENUE_ESTIMADO_USD"] else "N/A")

    dc5, dc6, dc7, dc8 = st.columns(4)
    dc5.metric("Estatus", row["ESTATUS"])
    dc6.markdown(f"**Ubicacion:** {row['UBICACION'] or 'N/A'}")
    if row["SITIO_WEB"] and str(row["SITIO_WEB"]).strip():
        dc7.markdown(f"**Web:** [{row['SITIO_WEB']}]({row['SITIO_WEB']})")
    else:
        dc7.markdown("**Web:** N/A")
    if row["LINKEDIN_EMPRESA"] and str(row["LINKEDIN_EMPRESA"]).strip():
        dc8.markdown(f"**LinkedIn:** [Ver perfil]({row['LINKEDIN_EMPRESA']})")
    else:
        dc8.markdown("**LinkedIn:** N/A")

    # Email del contacto principal como mailto
    if row["CONTACTO_EMAIL"] and str(row["CONTACTO_EMAIL"]).strip():
        st.markdown(f"**Contacto principal:** {row['CONTACTO_NOMBRE'] or 'N/A'} ({row['CONTACTO_CARGO'] or 'N/A'}) - {email_link_md(row['CONTACTO_EMAIL'])}")
    if row["NOTAS"] and str(row["NOTAS"]).strip():
        st.info(f"**Notas:** {row['NOTAS']}")

    # -- Contactos de la cuenta --
    st.markdown("---")
    st.markdown("**Contactos**")
    df_contactos = load_contactos_cuenta(cuenta_id)
    if not df_contactos.empty:
        for _, ct in df_contactos.iterrows():
            contactado_icon = "Si" if ct["CONTACTADO"] else "No"
            email_md = email_link_md(ct["EMAIL"])
            wa = ct["WHATSAPP"] or ""
            principal_tag = " **[PRINCIPAL]**" if ct.get("ES_PRINCIPAL", False) else ""
            st.markdown(
                f"- **{ct['NOMBRE_COMPLETO']}**{principal_tag} | {ct['CARGO'] or 'N/A'} | {email_md} | WA: {wa} | Contactado: {contactado_icon}"
            )
    else:
        st.warning("Sin contactos registrados.")

    # -- Historial de interacciones --
    df_inter = load_interacciones_cuenta(cuenta_id)
    if not df_inter.empty:
        st.markdown("---")
        st.markdown("**Historial de Interacciones**")
        df_int_display = df_inter[["FECHA", "TIPO", "CONTACTO", "DESCRIPCION", "RESULTADO", "SIGUIENTE_ACCION"]].copy()
        df_int_display.columns = ["Fecha", "Tipo", "Contacto", "Descripcion", "Resultado", "Siguiente Accion"]
        st.dataframe(df_int_display, width="stretch", hide_index=True, height=200)

    # =============================================================
    # ACCIONES (dentro del dialog, sin st.form por limitacion de @st.dialog)
    # =============================================================
    st.markdown("---")
    st.markdown("**Acciones**")

    action = st.selectbox("Selecciona una accion:", [
        "(Seleccionar)",
        "Marcar como Contactado",
        "Agregar Contacto",
        "Cambiar Contacto Principal",
        "Agregar/Editar Ubicacion",
        "Editar Datos Cuenta",
        "Registrar Interaccion",
        "Generar Pitch con IA"
    ], key=f"action_{cuenta_id}")

    # ---- MARCAR COMO CONTACTADO ----
    if action == "Marcar como Contactado":
        df_ct = load_contactos_cuenta(cuenta_id)
        if df_ct.empty:
            st.warning("No hay contactos. Agrega uno primero.")
        else:
            contacto_opts = dict(zip(
                df_ct["NOMBRE_COMPLETO"] + " (" + df_ct["CARGO"].fillna("") + ")",
                df_ct["CONTACTO_ID"]
            ))
            sel_ct = st.selectbox("Contacto a marcar:", list(contacto_opts.keys()), key=f"mc_ct_{cuenta_id}")
            metodo = st.selectbox("Metodo:", ["EMAIL", "WHATSAPP", "LLAMADA", "LINKEDIN", "PRESENCIAL"], key=f"mc_met_{cuenta_id}")
            resultado = st.selectbox("Resultado:", ["POSITIVA", "NEGATIVA", "SIN_RESPUESTA", "REPROGRAMADO"], key=f"mc_res_{cuenta_id}")
            notas = st.text_area("Notas:", key=f"mc_not_{cuenta_id}", placeholder="Resumen de la conversacion...")
            if st.button("Guardar Contacto", key=f"mc_save_{cuenta_id}", type="primary"):
                ct_id = contacto_opts[sel_ct]
                if marcar_contactado(ct_id, cuenta_id, metodo, resultado, notas):
                    st.success(f"Contacto marcado como contactado via {metodo}.")
                    st.cache_data.clear()
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
                    st.success(f"Contacto {nombre} {apellido} agregado.")
                    st.cache_data.clear()
                    st.rerun()

    # ---- AGREGAR/EDITAR UBICACION ----
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
                    st.success(f"Contacto principal cambiado a {sel_nuevo}.")
                    st.cache_data.clear()
                    st.rerun()

    # ---- AGREGAR/EDITAR UBICACION ----
    elif action == "Agregar/Editar Ubicacion":
        st.caption("Actualiza la ubicacion geografica de la cuenta.")
        eg1, eg2, eg3 = st.columns(3)
        nuevo_pais = eg1.text_input("Pais", value=row["PAIS"] or "", key=f"ub_pai_{cuenta_id}")
        nuevo_estado = eg2.text_input("Estado", value=row["ESTADO"] or "", key=f"ub_est_{cuenta_id}")
        nuevo_ciudad = eg3.text_input("Ciudad", value=row["CIUDAD"] or "", key=f"ub_ciu_{cuenta_id}")
        if st.button("Guardar Ubicacion", key=f"ub_save_{cuenta_id}", type="primary"):
            ubicacion = ", ".join(filter(None, [nuevo_ciudad, nuevo_estado, nuevo_pais]))
            campos = {"PAIS": nuevo_pais, "ESTADO": nuevo_estado, "CIUDAD": nuevo_ciudad, "UBICACION": ubicacion}
            if actualizar_cuenta(cuenta_id, campos):
                st.success("Ubicacion actualizada.")
                st.cache_data.clear()
                st.rerun()

    # ---- EDITAR DATOS CUENTA ----
    elif action == "Editar Datos Cuenta":
        st.caption(f"Empresa: **{acct_name}** (no editable)")
        estatus_opts = ["PENDIENTE", "CONTACTADO", "EN_SEGUIMIENTO", "CALIFICADO", "OPORTUNIDAD", "DESCARTADO"]
        tamano_opts = ["Micro", "Pequena", "Mediana", "Grande", "Enterprise"]
        ec1, ec2 = st.columns(2)
        nuevo_estatus = ec1.selectbox("Estatus", estatus_opts,
            index=estatus_opts.index(row["ESTATUS"]) if row["ESTATUS"] in estatus_opts else 0, key=f"ed_est_{cuenta_id}")
        nuevo_tamano = ec2.selectbox("Tamano", tamano_opts,
            index=tamano_opts.index(row["TAMANO_EMPRESA"]) if row["TAMANO_EMPRESA"] in tamano_opts else 0, key=f"ed_tam_{cuenta_id}")
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
            ubicacion = ", ".join(filter(None, [nuevo_ciudad, nuevo_estado, nuevo_pais]))
            campos = {
                "ESTATUS": nuevo_estatus, "TAMANO_EMPRESA": nuevo_tamano,
                "PAIS": nuevo_pais, "ESTADO": nuevo_estado, "CIUDAD": nuevo_ciudad,
                "UBICACION": ubicacion, "SITIO_WEB": nuevo_web, "LINKEDIN_EMPRESA": nuevo_linkedin,
                "NUM_EMPLEADOS_ESTIMADO": nuevo_empleados if nuevo_empleados > 0 else None,
                "REVENUE_ESTIMADO_USD": nuevo_revenue if nuevo_revenue > 0 else None,
                "NOTAS": nuevas_notas, "FUENTE_TAMANO": "MANUAL"
            }
            if actualizar_cuenta(cuenta_id, campos):
                st.success("Datos actualizados.")
                st.cache_data.clear()
                st.rerun()

    # ---- REGISTRAR INTERACCION ----
    elif action == "Registrar Interaccion":
        df_ct2 = load_contactos_cuenta(cuenta_id)
        contacto_opts2 = {"(Sin contacto especifico)": None}
        if not df_ct2.empty:
            contacto_opts2.update(dict(zip(
                df_ct2["NOMBRE_COMPLETO"] + " (" + df_ct2["CARGO"].fillna("") + ")",
                df_ct2["CONTACTO_ID"]
            )))
        sel_ct2 = st.selectbox("Contacto:", list(contacto_opts2.keys()), key=f"ri_ct_{cuenta_id}")
        tipo = st.selectbox("Tipo:", ["EMAIL", "WHATSAPP", "LLAMADA", "LINKEDIN", "PRESENCIAL", "EVENTO"], key=f"ri_tip_{cuenta_id}")
        descripcion = st.text_area("Descripcion:", key=f"ri_desc_{cuenta_id}", placeholder="Que se hablo/hizo...")
        resultado_i = st.selectbox("Resultado:", ["EXITOSO", "SIN_RESPUESTA", "RECHAZADO", "REPROGRAMADO", "PENDIENTE"], key=f"ri_res_{cuenta_id}")
        siguiente = st.text_input("Siguiente accion:", key=f"ri_sig_{cuenta_id}", placeholder="Ej: Agendar demo")
        if st.button("Registrar", key=f"ri_save_{cuenta_id}", type="primary"):
            ct_id2 = contacto_opts2[sel_ct2]
            if registrar_interaccion(cuenta_id, ct_id2, tipo, descripcion, resultado_i, siguiente):
                st.success("Interaccion registrada.")
                st.cache_data.clear()
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
            contexto += f"\nContacto: {row['CONTACTO_NOMBRE']} ({row['CONTACTO_CARGO'] or 'N/A'})"
        contexto += f"\nEstatus actual: {row['ESTATUS']}"
        contexto += f"\nFuente: Snowflake World Tour 2025"
        contexto += insights_ctx

        df_hist = load_interacciones_cuenta(cuenta_id)
        if not df_hist.empty:
            hist_text = "\nHistorial de interacciones:"
            for _, hi in df_hist.head(3).iterrows():
                hist_text += f"\n- {hi['FECHA']}: {hi['TIPO']} - {hi['RESULTADO']} - {hi['DESCRIPCION'] or ''}"
            contexto += hist_text

        if st.button("Generar Pitch y Siguiente Accion", key=f"pitch_gen_{cuenta_id}", type="primary"):
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
                        f"Genera un mensaje de seguimiento personalizado en espanol (5-6 oraciones) para esta cuenta. "
                        f"CONTEXTO IMPORTANTE: Este lead asistio al Snowflake World Tour celebrado el ano pasado "
                        f"en Ciudad de Mexico. NO asumas que te viste en persona con el contacto. "
                        f"Inicia con un saludo a '{nombre_contacto}' diciendo que nos da mucho gusto que haya podido asistir "
                        f"al Snowflake World Tour celebrado el ano pasado, que sabemos que en estos momentos su industria esta "
                        f"enfrentando ciertos retos (menciona uno relevante), y continua con: "
                        f"1) un caso de uso relevante para su industria donde la combinacion de las capacidades de EGOS BI y Snowflake "
                        f"puede ayudarles a resolver ese reto o aprovechar una oportunidad, "
                        f"2) cierra con una pregunta casual tipo: Que te parece si agendamos una llamada de 20 minutos para rebotar ideas? "
                        f"NO menciones demos, demostraciones ni pruebas de concepto. Solo proponer una llamada breve para platicar. "
                        f"NUNCA uses la frase 'potencialidad de Snowflake' ni 'potencial de Snowflake'. "
                        f"Si mencionas potencial, di 'el potencial de tus datos'. El enfoque es el valor para el cliente, no el producto. "
                        f"Tono: profesional, cercano, de seguimiento. Datos:\n{contexto}"
                    )
                    cur_ai.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s)", (prompt_pitch,))
                    pitch = cur_ai.fetchone()[0]

                    prompt_accion = (
                        f"Basandote en estos datos de un lead comercial, sugiere la SIGUIENTE ACCION concreta "
                        f"que el vendedor debe tomar. Responde en espanol con UNA oracion directa y accionable. "
                        f"NO sugieras demos ni demostraciones. Enfocate en agendar llamadas, enviar emails o compartir contenido. "
                        f"Ejemplo: 'Enviar email de seguimiento con caso de uso de Cortex AI para retail'. "
                        f"Datos:\n{contexto}"
                    )
                    cur_ai.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', %s)", (prompt_accion,))
                    siguiente_accion = cur_ai.fetchone()[0]

                    cur_ai.close()
                    conn_ai.close()

                    # Mostrar pitch
                    st.markdown(f"**Pitch para {acct_name}** ({industria_cuenta})")
                    st.write(pitch)
                    st.markdown("**Siguiente Accion Sugerida:**")
                    st.success(siguiente_accion)

                    # Boton: Enviar por Email (mailto link estandar)
                    contact_email = str(row["CONTACTO_EMAIL"]).strip() if row["CONTACTO_EMAIL"] else ""
                    if contact_email:
                        subject = f"Seguimiento Snowflake World Tour 2025 - {acct_name}"
                        pitch_clean = str(pitch).replace('\n', '\r\n')
                        mailto_url = make_mailto(contact_email, subject=subject, body=pitch_clean)
                        st.markdown(f"[Enviar pitch por email a {nombre_contacto} ({contact_email})]({mailto_url})")

                    # Boton: Guardar siguiente accion
                    if st.button("Guardar siguiente accion en historial", key=f"pitch_save_{cuenta_id}"):
                        if registrar_interaccion(cuenta_id, None, "IA_SUGERENCIA",
                                                 "Pitch generado + accion sugerida", "PENDIENTE",
                                                 str(siguiente_accion)[:500]):
                            st.success("Siguiente accion guardada.")
                            st.cache_data.clear()
                            st.rerun()

                except Exception as e:
                    st.error(f"Error al generar pitch: {e}")


# =============================================================
# CARGAR DATOS
# =============================================================

df, df_casos = load_data()

# =============================================================
# SIDEBAR: FILTROS
# =============================================================

with st.sidebar:
    st.header("Filtros")
    estatus_opts = sorted(df["ESTATUS"].dropna().unique())
    sel_estatus = st.multiselect("Estatus Comercial", estatus_opts, default=estatus_opts)
    sel_contactado = st.selectbox("Contactado", ["Todos", "Ya contactados", "Pendientes"])
    industrias = sorted(df["INDUSTRIA_NOMBRE"].unique())
    sel_industrias = st.multiselect("Industria", industrias, default=industrias)
    tamanos = ["Micro", "Pequena", "Mediana", "Grande", "Enterprise"]
    sel_tamanos = st.multiselect("Tamano Empresa", tamanos, default=tamanos)
    paises = sorted(df["PAIS"].dropna().unique())
    sel_paises = st.multiselect("Pais", paises, default=paises)
    buscar = st.text_input("Buscar empresa", placeholder="Nombre de empresa...")

# =============================================================
# APLICAR FILTROS
# =============================================================

dff = df[df["ESTATUS"].isin(sel_estatus)]
dff = dff[dff["INDUSTRIA_NOMBRE"].isin(sel_industrias)]
dff = dff[dff["TAMANO_EMPRESA"].isin(sel_tamanos) | dff["TAMANO_EMPRESA"].isna()]
dff = dff[dff["PAIS"].isin(sel_paises) | dff["PAIS"].isna()]

if sel_contactado == "Ya contactados":
    dff = dff[dff["CONTACTADO"] == True]
elif sel_contactado == "Pendientes":
    dff = dff[(dff["CONTACTADO"] == False) | (dff["CONTACTADO"].isna())]

if buscar:
    dff = dff[dff["ACCT_NAME"].str.contains(buscar, case=False, na=False)]

# =============================================================
# TITULO Y KPIs
# =============================================================

st.title("Leads Snowflake World Tour 2025")
st.caption(f"DB_LEADS_SNOWFLAKE_WT25 | {len(dff)} de {len(df)} cuentas | Pedro Ulloa - EGOS BI")

total = len(dff)
contactadas = int(dff["CONTACTADO"].fillna(False).sum())
pendientes = total - contactadas
con_whatsapp = int(dff["CONTACTO_WHATSAPP"].notna().sum())
con_email = int(dff["CONTACTO_EMAIL"].notna().sum())
total_contactos = int(dff["NUM_CONTACTOS"].fillna(0).sum())
total_interacciones = int(dff["NUM_INTERACCIONES"].fillna(0).sum())

k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
k1.metric("Total Cuentas", total)
k2.metric("Contactadas", contactadas)
k3.metric("Pendientes", pendientes)
k4.metric("Con WhatsApp", con_whatsapp)
k5.metric("Con Email", con_email)
k6.metric("Total Contactos", total_contactos)
k7.metric("Interacciones", total_interacciones)

# =============================================================
# GRAFICAS
# =============================================================

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

with col2:
    with st.container(border=True):
        st.subheader("Pipeline Comercial")
        status_order = ["PENDIENTE", "CONTACTADO", "EN_SEGUIMIENTO", "CALIFICADO", "OPORTUNIDAD", "DESCARTADO"]
        status_counts = dff["ESTATUS"].value_counts().reindex(status_order).dropna().reset_index()
        status_counts.columns = ["Estatus", "Cuentas"]
        colors_map = {"PENDIENTE": "#95a5a6", "CONTACTADO": "#3498db", "EN_SEGUIMIENTO": "#f39c12",
                      "CALIFICADO": "#2ecc71", "OPORTUNIDAD": "#9b59b6", "DESCARTADO": "#e74c3c"}
        fig_pipe = px.bar(status_counts, x="Estatus", y="Cuentas", text="Cuentas",
                          color="Estatus", color_discrete_map=colors_map)
        fig_pipe.update_layout(showlegend=False, height=400, margin=dict(l=0, r=0, t=10, b=0))
        fig_pipe.update_traces(textposition="outside")
        st.plotly_chart(fig_pipe, width="stretch")

col3, col4 = st.columns(2)

with col3:
    with st.container(border=True):
        st.subheader("Tamano de Empresa")
        tamano_order = ["Micro", "Pequena", "Mediana", "Grande", "Enterprise"]
        tam_counts = dff["TAMANO_EMPRESA"].value_counts().reindex(tamano_order).dropna().reset_index()
        tam_counts.columns = ["Tamano", "Cuentas"]
        fig_tam = px.bar(tam_counts, x="Tamano", y="Cuentas", text="Cuentas",
                         color="Tamano", color_discrete_sequence=px.colors.sequential.Viridis)
        fig_tam.update_layout(showlegend=False, height=350, margin=dict(l=0, r=0, t=10, b=0))
        fig_tam.update_traces(textposition="outside")
        st.plotly_chart(fig_tam, width="stretch")

with col4:
    with st.container(border=True):
        st.subheader("Distribucion Geografica (Top 10)")
        geo = dff["PAIS"].dropna().value_counts().head(10).reset_index()
        geo.columns = ["Pais", "Cuentas"]
        fig_geo = px.pie(geo, values="Cuentas", names="Pais",
                         color_discrete_sequence=px.colors.qualitative.Set3, hole=0.35)
        fig_geo.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_geo, width="stretch")

# Heatmap
with st.container(border=True):
    st.subheader("Heatmap: Industria vs Tamano")
    tamano_ord = ["Micro", "Pequena", "Mediana", "Grande", "Enterprise"]
    df_heat = dff[dff["TAMANO_EMPRESA"].notna()].groupby(
        ["INDUSTRIA_NOMBRE", "TAMANO_EMPRESA"]).size().reset_index(name="Cuentas")
    if not df_heat.empty:
        df_pivot = df_heat.pivot(index="INDUSTRIA_NOMBRE", columns="TAMANO_EMPRESA", values="Cuentas").fillna(0)
        df_pivot = df_pivot.reindex(columns=[t for t in tamano_ord if t in df_pivot.columns])
        fig_heat = px.imshow(
            df_pivot.values, x=df_pivot.columns.tolist(), y=df_pivot.index.tolist(),
            color_continuous_scale="Viridis", text_auto=True,
            labels=dict(x="Tamano", y="Industria", color="Cuentas"), aspect="auto"
        )
        fig_heat.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_heat, width="stretch")

# =============================================================
# TOP 10 CUENTAS DE INTERES (con click para abrir tarjeta)
# =============================================================

st.divider()
st.subheader("Top 10 Cuentas de Mayor Interes")
st.caption("Score combinado: datos obtenidos (0-5) + tamano empresa (0-5) | Click en una fila para ver detalle")

tamano_score = {"Micro": 1, "Pequena": 2, "Mediana": 3, "Grande": 4, "Enterprise": 5}
enriq_cols = ["SITIO_WEB", "UBICACION", "CONTACTO_NOMBRE", "LINKEDIN_EMPRESA", "CONTACTO_CARGO"]
df_sc = dff.copy()
df_sc["ENRIQ"] = df_sc[enriq_cols].apply(lambda r: sum(1 for v in r if v and str(v).strip()), axis=1)
df_sc["TAM_SC"] = df_sc["TAMANO_EMPRESA"].map(tamano_score).fillna(0).astype(int)
df_sc["SCORE"] = df_sc["ENRIQ"] + df_sc["TAM_SC"]
top10 = df_sc.nlargest(10, ["SCORE", "TAM_SC", "ENRIQ"])

ct1, ct2 = st.columns([2, 1])
with ct1:
    with st.container(border=True):
        t10d = top10[["ACCT_NAME", "INDUSTRIA_NOMBRE", "TAMANO_EMPRESA",
                       "ENRIQ", "TAM_SC", "SCORE", "CONTACTO_NOMBRE", "ESTATUS"]].copy()
        t10d.columns = ["Empresa", "Industria", "Tamano", "Datos (0-5)", "Tamano (0-5)",
                        "Score", "Contacto", "Estatus"]
        t10d = t10d.fillna("")
        # Header
        hc = st.columns([2.5, 1.5, 1, 0.8, 0.8, 0.7, 1.5, 1.2])
        headers = ["Empresa", "Industria", "Tamano", "Datos", "Tam.", "Score", "Contacto", "Estatus"]
        for col, h in zip(hc, headers):
            col.markdown(f"**{h}**")
        # Filas con empresa como link
        for i, (_, row) in enumerate(t10d.iterrows()):
            rc = st.columns([2.5, 1.5, 1, 0.8, 0.8, 0.7, 1.5, 1.2])
            with rc[0]:
                if st.button(f":link: {row['Empresa']}", key=f"t10_{i}", use_container_width=True):
                    st.session_state["_open_cuenta"] = row["Empresa"]
                    st.rerun()
            rc[1].write(row["Industria"])
            rc[2].write(row["Tamano"])
            rc[3].write(str(row["Datos (0-5)"]))
            rc[4].write(str(row["Tamano (0-5)"]))
            rc[5].write(str(row["Score"]))
            rc[6].write(row["Contacto"])
            rc[7].write(row["Estatus"])

with ct2:
    with st.container(border=True):
        st.markdown("**Composicion del Score**")
        fig_sc = px.bar(t10d, y="Empresa", x=["Datos (0-5)", "Tamano (0-5)"],
                        orientation="h", barmode="stack",
                        color_discrete_sequence=["#3498db", "#2ecc71"])
        fig_sc.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0),
                             yaxis=dict(autorange="reversed"), legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig_sc, width="stretch")

# =============================================================
# GESTION DE LEADS: tabla con empresa como link clickable
# =============================================================

st.divider()
st.subheader("Gestion de Leads")
st.caption("Click en el nombre de la empresa para ver detalle | Usa la seccion inferior para marcar contactado")

# Preparar DataFrame para display
df_leads = dff[["CUENTA_ID", "CONTACTO_ID", "ACCT_NAME", "INDUSTRIA_NOMBRE", "TAMANO_EMPRESA",
                 "CONTACTO_NOMBRE", "CONTACTO_CARGO", "CONTACTO_EMAIL", "CONTACTO_WHATSAPP",
                 "ESTATUS", "CONTACTADO", "NUM_CONTACTOS", "NUM_INTERACCIONES"]].copy()
df_leads["CONTACTADO"] = df_leads["CONTACTADO"].fillna(False).astype(bool)

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
    lhc = st.columns([2.2, 1.3, 1, 1.5, 1.2, 1.5, 1, 0.8])
    for col, h in zip(lhc, ["Empresa", "Industria", "Tamano", "Contacto", "Cargo", "Email", "Estatus", "Cont."]):
        col.markdown(f"**{h}**")
    # Filas
    for i, (_, row) in enumerate(df_page.iterrows()):
        rc = st.columns([2.2, 1.3, 1, 1.5, 1.2, 1.5, 1, 0.8])
        with rc[0]:
            if st.button(f":link: {row['ACCT_NAME']}", key=f"lead_{start_idx + i}", use_container_width=True):
                st.session_state["_open_cuenta"] = row["ACCT_NAME"]
                st.rerun()
        rc[1].write(row["INDUSTRIA_NOMBRE"] or "")
        rc[2].write(row["TAMANO_EMPRESA"] or "")
        rc[3].write(row["CONTACTO_NOMBRE"] or "")
        rc[4].write(row["CONTACTO_CARGO"] or "")
        email_val = row["CONTACTO_EMAIL"]
        if email_val and str(email_val).strip():
            rc[5].markdown(f"[{str(email_val).strip()}](mailto:{str(email_val).strip()})")
        else:
            rc[5].write("")
        rc[6].write(row["ESTATUS"] or "")
        contactado_txt = "Si" if row["CONTACTADO"] else "No"
        rc[7].write(contactado_txt)

# Quick-action: Marcar/desmarcar contactado
with st.expander("Marcar / Desmarcar Contactado (rapido)"):
    cuentas_no_contactadas = df_leads[df_leads["CONTACTADO"] == False]
    cuentas_contactadas = df_leads[df_leads["CONTACTADO"] == True]

    qa1, qa2 = st.columns(2)
    with qa1:
        if not cuentas_no_contactadas.empty:
            st.markdown("**Marcar como contactado:**")
            opts_marcar = dict(zip(
                cuentas_no_contactadas["ACCT_NAME"] + " - " + cuentas_no_contactadas["CONTACTO_NOMBRE"].fillna("Sin contacto"),
                zip(cuentas_no_contactadas["CONTACTO_ID"], cuentas_no_contactadas["CUENTA_ID"])
            ))
            sel_marcar = st.selectbox("Cuenta:", list(opts_marcar.keys()), key="qa_marcar_sel")
            if st.button("Marcar Contactado", key="qa_marcar_btn", type="primary"):
                c_id, cu_id = opts_marcar[sel_marcar]
                if c_id and marcar_contactado_simple(int(c_id), int(cu_id), True):
                    st.toast(f"Contactado: {sel_marcar}")
                    st.cache_data.clear()
                    st.rerun()
    with qa2:
        if not cuentas_contactadas.empty:
            st.markdown("**Desmarcar contactado:**")
            opts_desmarcar = dict(zip(
                cuentas_contactadas["ACCT_NAME"] + " - " + cuentas_contactadas["CONTACTO_NOMBRE"].fillna("Sin contacto"),
                zip(cuentas_contactadas["CONTACTO_ID"], cuentas_contactadas["CUENTA_ID"])
            ))
            sel_desmarcar = st.selectbox("Cuenta:", list(opts_desmarcar.keys()), key="qa_desmarcar_sel")
            if st.button("Desmarcar", key="qa_desmarcar_btn"):
                c_id, cu_id = opts_desmarcar[sel_desmarcar]
                if c_id and marcar_contactado_simple(int(c_id), int(cu_id), False):
                    st.toast(f"Desmarcado: {sel_desmarcar}")
                    st.cache_data.clear()
                    st.rerun()

# =============================================================
# TOP 5 CUENTAS POR INDUSTRIA (grafico dinamico)
# =============================================================

st.divider()
st.subheader("Top 5 Cuentas por Industria")
st.caption("Score combinado: datos obtenidos (0-5) + tamano empresa (0-5)")

industrias_disponibles = sorted(dff["INDUSTRIA_NOMBRE"].unique())
ti1, ti2 = st.columns([1, 2])
with ti1:
    sel_industria_top5 = st.selectbox("Selecciona industria:", industrias_disponibles, key="sel_ind_top5")
with ti2:
    df_ind = df_sc[df_sc["INDUSTRIA_NOMBRE"] == sel_industria_top5].nlargest(5, ["SCORE", "TAM_SC", "ENRIQ"])
    if not df_ind.empty:
        t5d = df_ind[["ACCT_NAME", "TAMANO_EMPRESA", "ENRIQ", "TAM_SC", "SCORE", "CONTACTO_NOMBRE", "ESTATUS"]].copy()
        t5d.columns = ["Empresa", "Tamano", "Datos (0-5)", "Tamano (0-5)", "Score", "Contacto", "Estatus"]
        t5d = t5d.fillna("")
        with st.container(border=True):
            fig_t5 = px.bar(t5d, y="Empresa", x=["Datos (0-5)", "Tamano (0-5)"],
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

# =============================================================
# INSIGHTS POR INDUSTRIA (Tabs)
# =============================================================

st.divider()
st.subheader("Insights por Industria: Casos de Uso Snowflake")
st.caption("Tendencias, retos y oportunidades generadas con Cortex AI")

industrias_ci = sorted(df_casos["INDUSTRIA_NOMBRE"].unique())
if industrias_ci:
    tabs = st.tabs(industrias_ci)
    for tab, industria in zip(tabs, industrias_ci):
        with tab:
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
st.caption("Dashboard Leads Snowflake World Tour 2025 v3 (cloud-ready) | DB_LEADS_SNOWFLAKE_WT25 | EGOS BI + Cortex AI")
