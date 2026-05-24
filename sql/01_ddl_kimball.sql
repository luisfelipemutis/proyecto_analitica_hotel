-- =============================================================================
--  MODELO DIMENSIONAL KIMBALL — HOTEL DANN MONASTERIO
--  Solución Analítica de Datos — Proyecto Final
--  Unicomfacauca — Diplomado en Analítica de Datos
-- =============================================================================
--  Archivo  : 01_ddl_kimball.sql
--  Versión  : 1.0
--  Fecha    : 2026-05-18
--  Motor    : MySQL 8.x / MySQL Workbench 8.x
--  Encoding : UTF-8
-- =============================================================================
--  INSTRUCCIONES DE USO EN MYSQL WORKBENCH:
--    1. File → Open SQL Script → seleccionar este archivo
--    2. Ctrl+Shift+Enter (Execute All)
--    3. Verificar en "Schemas" que aparece "hotel_dann_dw"
--    4. Cargar datos: Server → Data Import → Import from Self-Contained File
--       Orden de carga: Dims primero → Fact_Reservas al final
-- =============================================================================

DROP DATABASE IF EXISTS hotel_dann_dw;
CREATE DATABASE hotel_dann_dw
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_spanish_ci;
USE hotel_dann_dw;

-- =============================================================================
-- 1. Dim_Fecha
-- =============================================================================
CREATE TABLE Dim_Fecha (
    id_fecha        INT         NOT NULL  COMMENT 'Clave YYYYMMDD (ej. 20220315)',
    fecha           DATE        NOT NULL,
    anio            SMALLINT    NOT NULL,
    trimestre       TINYINT     NOT NULL  COMMENT '1-4',
    mes             TINYINT     NOT NULL  COMMENT '1-12',
    nombre_mes      VARCHAR(12) NOT NULL  COMMENT 'Enero..Diciembre',
    semana_anio     TINYINT     NOT NULL  COMMENT 'Semana ISO 1-53',
    dia_semana_num  TINYINT     NOT NULL  COMMENT '1=Lun..7=Dom',
    dia_semana      VARCHAR(12) NOT NULL,
    es_fin_semana   TINYINT(1)  NOT NULL DEFAULT 0,
    PRIMARY KEY (id_fecha)
) ENGINE=InnoDB COMMENT='Calendario con rango global de todas las fechas del parquet';

-- =============================================================================
-- 2. Dim_Segmento
-- =============================================================================
CREATE TABLE Dim_Segmento (
    id_segmento      INT          NOT NULL AUTO_INCREMENT,
    codigo_segmento  CHAR(5)      NOT NULL COMMENT 'COR|CE|ME|EM|T&T',
    nombre_segmento  VARCHAR(50)  NOT NULL,
    descripcion      VARCHAR(200),
    tipo_cliente     VARCHAR(30)  NOT NULL COMMENT 'Corporativo|Transiente|Agencias|Interno',
    PRIMARY KEY (id_segmento),
    UNIQUE KEY uq_seg_cod (codigo_segmento)
) ENGINE=InnoDB;

-- =============================================================================
-- 3. Dim_Canal
-- =============================================================================
CREATE TABLE Dim_Canal (
    id_canal       INT         NOT NULL AUTO_INCREMENT,
    codigo_canal   VARCHAR(10) NOT NULL,
    nombre_canal   VARCHAR(100)NOT NULL,
    tipo_canal     VARCHAR(30) NOT NULL,
    es_online      TINYINT(1)  NOT NULL DEFAULT 0,
    PRIMARY KEY (id_canal),
    UNIQUE KEY uq_canal_cod (codigo_canal)
) ENGINE=InnoDB;

-- =============================================================================
-- 4. Dim_Habitacion
-- =============================================================================
CREATE TABLE Dim_Habitacion (
    id_habitacion    INT          NOT NULL AUTO_INCREMENT,
    tipo_hab         CHAR(3)      NOT NULL COMMENT 'S3|SE|ST|SP|SC|ST',
    nombre_tipo      VARCHAR(80)  NOT NULL COMMENT 'Nombre comercial del tipo de habitacion',
    clase_hab        CHAR(3)      NOT NULL,
    nombre_clase     VARCHAR(30)  NOT NULL,
    descripcion_tipo VARCHAR(350) NOT NULL COMMENT 'Descripcion completa del tipo de suite',
    num_habitacion   VARCHAR(20),
    capacidad_max    TINYINT      NOT NULL DEFAULT 2,
    categoria        VARCHAR(20)  NOT NULL COMMENT 'Suite|Estandar',
    PRIMARY KEY (id_habitacion)
) ENGINE=InnoDB;

-- =============================================================================
-- 5. Dim_Huesped
-- =============================================================================
CREATE TABLE Dim_Huesped (
    id_registro_huesped  INT          NOT NULL AUTO_INCREMENT,
    id_huesped           CHAR(16)     NOT NULL COMMENT 'Hash SHA-256 truncado de ident_aco (notebook 02) - 16 chars hex',
    PRIMARY KEY (id_registro_huesped),
    UNIQUE KEY uq_huesped_hash (id_huesped)
) ENGINE=InnoDB COMMENT='Dimension de identidad del huesped (estable)';

-- =============================================================================
-- 6. Dim_Contexto_Huesped
-- =============================================================================
CREATE TABLE Dim_Contexto_Huesped (
    id_contexto_huesped  INT          NOT NULL AUTO_INCREMENT,
    bk_contexto_huesped  CHAR(20)     NOT NULL COMMENT 'Hash SHA-256 truncado del contexto por reserva',
    rol                  VARCHAR(20)           COMMENT 'Rol del huesped: Titular|Acompanante|Dependiente|No registra',
    clasificacion        VARCHAR(20)           COMMENT 'Clasificacion: Adulto|Nino|No registra',
    privacidad           VARCHAR(20)           COMMENT 'Modo incognito: Si|No|No registra',
    sexo                 VARCHAR(15)           COMMENT 'Masculino|Femenino|No especificado',
    edad                 TINYINT UNSIGNED      COMMENT 'Edad de la reserva (si aplica)',
    rango_edad           VARCHAR(20)           COMMENT '<18|18-25|26-35|36-50|51-65|66+|No registra',
    nacionalidad         VARCHAR(80)           COMMENT 'Nacionalidad reportada en la reserva',
    PRIMARY KEY (id_contexto_huesped),
    UNIQUE KEY uq_ctx_huesped_bk (bk_contexto_huesped)
) ENGINE=InnoDB COMMENT='Contexto del huesped por reserva (mini dimension)';

-- =============================================================================
-- 7. Dim_Empresa
-- =============================================================================
CREATE TABLE Dim_Empresa (
    id_empresa      INT          NOT NULL AUTO_INCREMENT,
    nombre_empresa  VARCHAR(150) NOT NULL COMMENT 'Nombre de la empresa asociada a la reserva',
    PRIMARY KEY (id_empresa),
    UNIQUE KEY uq_empresa_nombre (nombre_empresa)
) ENGINE=InnoDB COMMENT='Empresas asociadas a reservas corporativas - campo nombre_emp del parquet';

-- =============================================================================
-- 8. Dim_Temporada
-- =============================================================================
CREATE TABLE Dim_Temporada (
    id_temporada      INT         NOT NULL,
    codigo_temporada  CHAR(5)     NOT NULL COMMENT 'A|B|M|ND',
    nombre_temporada  VARCHAR(50) NOT NULL,
    descripcion       VARCHAR(200),
    PRIMARY KEY (id_temporada),
    UNIQUE KEY uq_temp_cod (codigo_temporada)
) ENGINE=InnoDB;

-- =============================================================================
-- 9. Fact_Reservas  (cargar DESPUES de todas las dimensiones)
-- =============================================================================
CREATE TABLE Fact_Reservas (
    id_reserva             INT            NOT NULL AUTO_INCREMENT,

    -- Llaves foraneas
    id_fecha               INT            NOT NULL,
    id_segmento            INT            NOT NULL,
    id_canal               INT            NOT NULL,
    id_habitacion          INT            NOT NULL,
    id_empresa             INT            NOT NULL,
    id_temporada           INT            NOT NULL DEFAULT 99,
    id_huesped             INT            NOT NULL DEFAULT 1,
    id_contexto_huesped    INT            NOT NULL,

    -- Metricas monetarias (COP)
    tarifa                 DECIMAL(14,2)  NOT NULL DEFAULT 0.00,
    valorplan              DECIMAL(14,2),
    ivaplan                DECIMAL(14,2),
    servicioplan           DECIMAL(14,2),
    valorconsumoadicional  DECIMAL(14,2),
    totalconsumosadicional DECIMAL(14,2),
    totalconsumosplan      DECIMAL(14,2),
    ingreso_total          DECIMAL(14,2)  NOT NULL DEFAULT 0.00,

    -- Metricas temporales
    duracion_estancia      SMALLINT UNSIGNED,
    lead_time              SMALLINT,

    PRIMARY KEY (id_reserva),
    INDEX idx_fecha       (id_fecha),
    INDEX idx_segmento    (id_segmento),
    INDEX idx_canal       (id_canal),
    INDEX idx_habitacion  (id_habitacion),
    INDEX idx_empresa     (id_empresa),
    INDEX idx_temporada   (id_temporada),
    INDEX idx_huesped     (id_huesped),
    INDEX idx_ctx_huesped (id_contexto_huesped),
    INDEX idx_ingreso     (ingreso_total),

    CONSTRAINT fk_fecha       FOREIGN KEY (id_fecha)       REFERENCES Dim_Fecha(id_fecha)           ON UPDATE CASCADE,
    CONSTRAINT fk_segmento    FOREIGN KEY (id_segmento)    REFERENCES Dim_Segmento(id_segmento)     ON UPDATE CASCADE,
    CONSTRAINT fk_canal       FOREIGN KEY (id_canal)       REFERENCES Dim_Canal(id_canal)           ON UPDATE CASCADE,
    CONSTRAINT fk_habitacion  FOREIGN KEY (id_habitacion)  REFERENCES Dim_Habitacion(id_habitacion) ON UPDATE CASCADE,
    CONSTRAINT fk_empresa     FOREIGN KEY (id_empresa)     REFERENCES Dim_Empresa(id_empresa)       ON UPDATE CASCADE,
    CONSTRAINT fk_temporada   FOREIGN KEY (id_temporada)   REFERENCES Dim_Temporada(id_temporada)   ON UPDATE CASCADE,
    CONSTRAINT fk_huesped     FOREIGN KEY (id_huesped)     REFERENCES Dim_Huesped(id_registro_huesped) ON UPDATE CASCADE,
    CONSTRAINT fk_ctx_huesped FOREIGN KEY (id_contexto_huesped) REFERENCES Dim_Contexto_Huesped(id_contexto_huesped) ON UPDATE CASCADE

) ENGINE=InnoDB COMMENT='Hechos: 70,882 reservas Jun 2020 - Abr 2026';

-- =============================================================================
-- VISTAS ANALITICAS (para Power BI / Tableau)
-- =============================================================================

-- Ingresos por segmento y anio
CREATE OR REPLACE VIEW vw_ingresos_segmento_anio AS
SELECT
    f.anio,
    s.nombre_segmento,
    s.tipo_cliente,
    COUNT(*)                 AS total_reservas,
    SUM(r.ingreso_total)     AS ingreso_total,
    AVG(r.ingreso_total)     AS ticket_promedio,
    AVG(r.duracion_estancia) AS estancia_promedio
FROM Fact_Reservas r
JOIN Dim_Fecha    f ON r.id_fecha    = f.id_fecha
JOIN Dim_Segmento s ON r.id_segmento = s.id_segmento
GROUP BY f.anio, s.nombre_segmento, s.tipo_cliente
ORDER BY f.anio, ingreso_total DESC;

-- KPIs mensuales
CREATE OR REPLACE VIEW vw_kpis_mensuales AS
SELECT
    f.anio, f.mes, f.nombre_mes, f.trimestre,
    t.nombre_temporada,
    COUNT(*)                       AS num_reservas,
    SUM(r.ingreso_total)           AS ingreso_mes,
    AVG(r.ingreso_total)           AS adr,
    AVG(r.duracion_estancia)       AS estancia_prom,
    AVG(r.lead_time)               AS lead_time_prom,
    SUM(r.totalconsumosadicional)  AS consumos_adicionales
FROM Fact_Reservas r
JOIN Dim_Fecha     f ON r.id_fecha     = f.id_fecha
JOIN Dim_Temporada t ON r.id_temporada = t.id_temporada
GROUP BY f.anio, f.mes, f.nombre_mes, f.trimestre, t.nombre_temporada
ORDER BY f.anio, f.mes;

-- Ranking canales
CREATE OR REPLACE VIEW vw_ranking_canales AS
SELECT
    c.nombre_canal, c.tipo_canal, c.es_online,
    COUNT(*)             AS num_reservas,
    SUM(r.ingreso_total) AS ingreso_total,
    ROUND(SUM(r.ingreso_total) / SUM(SUM(r.ingreso_total)) OVER() * 100, 2) AS pct_ingreso
FROM Fact_Reservas r
JOIN Dim_Canal c ON r.id_canal = c.id_canal
GROUP BY c.nombre_canal, c.tipo_canal, c.es_online
ORDER BY ingreso_total DESC;

-- =============================================================================
-- VERIFICACION FINAL
-- =============================================================================
SELECT 'Dim_Fecha'      AS tabla, COUNT(*) AS filas FROM Dim_Fecha      UNION ALL
SELECT 'Dim_Segmento',              COUNT(*)         FROM Dim_Segmento  UNION ALL
SELECT 'Dim_Canal',                 COUNT(*)         FROM Dim_Canal     UNION ALL
SELECT 'Dim_Habitacion',            COUNT(*)         FROM Dim_Habitacion UNION ALL
SELECT 'Dim_Huesped',               COUNT(*)         FROM Dim_Huesped   UNION ALL
SELECT 'Dim_Contexto_Huesped',      COUNT(*)         FROM Dim_Contexto_Huesped UNION ALL
SELECT 'Dim_Empresa',               COUNT(*)         FROM Dim_Empresa   UNION ALL
SELECT 'Dim_Temporada',             COUNT(*)         FROM Dim_Temporada UNION ALL
SELECT 'Fact_Reservas',             COUNT(*)         FROM Fact_Reservas;

-- FIN DEL SCRIPT
-- Orden carga ETL: Dim_Fecha > Dim_Segmento > Dim_Canal > Dim_Habitacion > Dim_Huesped > Dim_Contexto_Huesped > Dim_Empresa > Dim_Temporada > Fact_Reservas
