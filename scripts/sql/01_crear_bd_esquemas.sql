-- =============================================================
-- 01_crear_bd_esquemas.sql
-- Creacion de la base de datos, esquemas, stage y tabla RAW
-- para el proyecto de Leads del Snowflake World Tour 2025.
-- Ejecutado: 2026-03-23 | Conexion: TXA18114 | Role: ACCOUNTADMIN
-- Proyecto: Leads Snowflake WT25 - EGOS BI
-- =============================================================

-- =============================================================
-- SECCION 1: Base de datos
-- =============================================================

CREATE DATABASE IF NOT EXISTS DB_LEADS_SNOWFLAKE_WT25
    COMMENT = 'Leads comerciales del Snowflake World Tour 2025 - EGOS BI';

-- =============================================================
-- SECCION 2: Esquemas (RAW -> STAGING -> CORE -> ANALYTICS)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS DB_LEADS_SNOWFLAKE_WT25.RAW
    COMMENT = 'Landing zone - datos crudos del CSV de ClickUp sin transformar';

CREATE SCHEMA IF NOT EXISTS DB_LEADS_SNOWFLAKE_WT25.STAGING
    COMMENT = 'Datos limpios, deduplicados y normalizados';

CREATE SCHEMA IF NOT EXISTS DB_LEADS_SNOWFLAKE_WT25.CORE
    COMMENT = 'Modelo dimensional - fuente de verdad';

CREATE SCHEMA IF NOT EXISTS DB_LEADS_SNOWFLAKE_WT25.ANALYTICS
    COMMENT = 'Capas de consumo - vistas, metricas, pitch personalizado';

-- =============================================================
-- SECCION 3: Stage para carga de CSV
-- =============================================================

CREATE STAGE IF NOT EXISTS DB_LEADS_SNOWFLAKE_WT25.RAW.STG_LEADS_CSV
    COMMENT = 'Stage para archivos CSV de leads del evento WT25';

-- =============================================================
-- SECCION 4: Tabla RAW (espejo exacto del CSV)
-- Columnas mapeadas del CSV original:
--   Nombre -> NOMBRE
--   Apellidos -> APELLIDOS
--   Email -> EMAIL
--   Empresa_ -> EMPRESA
--   Contexto de la empresa -> CARGO_CONTEXTO (realmente es el cargo/rol)
--   Whatsapp -> WHATSAPP
--   Industria del cliente -> INDUSTRIA (vacia en el CSV)
-- =============================================================

CREATE TABLE IF NOT EXISTS DB_LEADS_SNOWFLAKE_WT25.RAW.LEADS_RAW (
    NOMBRE          VARCHAR(200),
    APELLIDOS       VARCHAR(300),
    EMAIL           VARCHAR(300),
    EMPRESA         VARCHAR(500),
    CARGO_CONTEXTO  VARCHAR(500),
    WHATSAPP        VARCHAR(50),
    INDUSTRIA       VARCHAR(200),
    LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verificacion
SELECT 'BD y esquemas creados' AS STATUS;
