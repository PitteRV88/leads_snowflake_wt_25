-- =============================================================
-- 02_staging_limpieza.sql
-- Limpieza, deduplicacion y normalizacion de datos RAW a STAGING
-- Ejecutado: 2026-03-23 | Conexion: TXA18114 | Role: ACCOUNTADMIN
-- Proyecto: Leads Snowflake WT25 - EGOS BI
-- =============================================================

USE DATABASE DB_LEADS_SNOWFLAKE_WT25;

-- =============================================================
-- SECCION 1: CONTACTOS_STAGING - Contactos unicos deduplicados
-- De 100 filas RAW a 93 contactos unicos (7 duplicados por EMAIL)
-- =============================================================

CREATE OR REPLACE TABLE STAGING.CONTACTOS_STAGING AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY EMAIL) AS CONTACTO_STG_ID,
    INITCAP(TRIM(NOMBRE)) AS NOMBRE,
    INITCAP(TRIM(APELLIDOS)) AS APELLIDOS,
    INITCAP(TRIM(NOMBRE)) || ' ' || INITCAP(TRIM(APELLIDOS)) AS NOMBRE_COMPLETO,
    LOWER(TRIM(
        CASE 
            WHEN EMAIL = 'jorge.zarate@compsa.com.mxom' THEN 'jorge.zarate@compsa.com.mx'
            ELSE EMAIL
        END
    )) AS EMAIL,
    TRIM(REGEXP_REPLACE(EMPRESA, '\\s+', ' ')) AS EMPRESA,
    TRIM(CARGO_CONTEXTO) AS CARGO,
    CASE 
        WHEN WHATSAPP IS NULL THEN NULL
        WHEN TRY_TO_DOUBLE(WHATSAPP) IS NOT NULL AND CONTAINS(WHATSAPP, 'E')
            THEN TO_VARCHAR(TRY_TO_DOUBLE(WHATSAPP)::NUMBER(15,0))
        ELSE TRIM(WHATSAPP)
    END AS WHATSAPP,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM RAW.LEADS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(EMAIL)) ORDER BY LOADED_AT) = 1;

-- Verificacion
SELECT COUNT(*) AS CONTACTOS_UNICOS FROM STAGING.CONTACTOS_STAGING;

-- =============================================================
-- SECCION 2: CUENTAS_STAGING - Empresas unicas extraidas
-- 79 empresas unicas con conteo de contactos
-- =============================================================

CREATE OR REPLACE TABLE STAGING.CUENTAS_STAGING AS
SELECT
    ROW_NUMBER() OVER (ORDER BY EMPRESA_NORM) AS CUENTA_STG_ID,
    EMPRESA_NORM AS EMPRESA,
    COUNT(*) AS NUM_CONTACTOS,
    LISTAGG(DISTINCT CARGO, ' | ') WITHIN GROUP (ORDER BY CARGO) AS CARGOS_CONTACTOS,
    MIN(LOADED_AT) AS LOADED_AT
FROM (
    SELECT 
        UPPER(TRIM(REGEXP_REPLACE(EMPRESA, '\\s+', ' '))) AS EMPRESA_NORM,
        CARGO,
        LOADED_AT
    FROM STAGING.CONTACTOS_STAGING
)
GROUP BY EMPRESA_NORM;

-- Verificacion
SELECT COUNT(*) AS EMPRESAS_UNICAS FROM STAGING.CUENTAS_STAGING;
