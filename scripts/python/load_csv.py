# =============================================================
# load_csv.py
# Carga del CSV de leads del Snowflake World Tour 2025 a RAW
# Usa PUT (subir archivo a stage) + COPY INTO (cargar en tabla)
# Ejecutado: 2026-03-23 | Conexion: TXA18114
# Proyecto: Leads Snowflake WT25 - EGOS BI
# =============================================================

import snowflake.connector
import os

# -- Configuracion de conexion --
# Usa la conexion por nombre (snowflake-connector-python >= 3.x)
CONNECTION_NAME = os.getenv('SNOWFLAKE_CONNECTION_NAME', 'TXA18114')

# -- Ruta al CSV --
# El CSV esta en la raiz del proyecto
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))  # sube 2 niveles: scripts/python -> proyecto
CSV_PATH = os.path.join(PROJECT_DIR, 'leads_datos_clickup.csv')

def safe_print(text):
    """Wrapper para evitar UnicodeEncodeError en Windows cp1252."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))

def main():
    safe_print(f'CSV a cargar: {CSV_PATH}')
    
    if not os.path.exists(CSV_PATH):
        safe_print(f'ERROR: No se encontro el CSV en {CSV_PATH}')
        return
    
    # Conectar a Snowflake
    safe_print(f'Conectando a Snowflake (conexion: {CONNECTION_NAME})...')
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cursor = conn.cursor()
    
    try:
        # Usar la BD y esquema RAW
        cursor.execute('USE DATABASE DB_LEADS_SNOWFLAKE_WT25')
        cursor.execute('USE SCHEMA RAW')
        cursor.execute('USE WAREHOUSE COMPUTE_WH')
        
        # Paso 1: Subir CSV al stage interno
        safe_print('Subiendo CSV al stage STG_LEADS_CSV...')
        put_cmd = f"PUT 'file://{CSV_PATH.replace(os.sep, '/')}' @STG_LEADS_CSV AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        safe_print(f'  CMD: {put_cmd}')
        cursor.execute(put_cmd)
        result = cursor.fetchall()
        for row in result:
            safe_print(f'  PUT resultado: {row}')
        
        # Paso 2: Limpiar tabla RAW antes de cargar (idempotente)
        safe_print('Limpiando tabla LEADS_RAW (TRUNCATE)...')
        cursor.execute('TRUNCATE TABLE LEADS_RAW')
        
        # Paso 3: COPY INTO desde stage a tabla RAW
        # El CSV tiene header, separador coma, encoding UTF-8 o Windows-1252
        safe_print('Ejecutando COPY INTO LEADS_RAW...')
        copy_cmd = """
        COPY INTO LEADS_RAW (NOMBRE, APELLIDOS, EMAIL, EMPRESA, CARGO_CONTEXTO, WHATSAPP, INDUSTRIA)
        FROM @STG_LEADS_CSV
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ENCODING = 'WINDOWS-1252'
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            NULL_IF = ('')
        )
        ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_cmd)
        result = cursor.fetchall()
        for row in result:
            safe_print(f'  COPY resultado: {row}')
        
        # Paso 4: Verificar carga
        cursor.execute('SELECT COUNT(*) FROM LEADS_RAW')
        count = cursor.fetchone()[0]
        safe_print(f'\nTotal filas cargadas en LEADS_RAW: {count}')
        
        # Muestra primeras 5 filas
        cursor.execute('SELECT NOMBRE, APELLIDOS, EMAIL, EMPRESA, CARGO_CONTEXTO FROM LEADS_RAW LIMIT 5')
        safe_print('\nPrimeras 5 filas:')
        for row in cursor.fetchall():
            safe_print(f'  {row}')
        
    finally:
        cursor.close()
        conn.close()
        safe_print('\nConexion cerrada.')

if __name__ == '__main__':
    main()
