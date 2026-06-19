# Solución Analítica de Datos — Hotel

Proyecto final de grado del Diplomado en Analítica de Datos (Unicomfacauca).  
Diseña e implementa un pipeline de inteligencia de negocios sobre el dataset transaccional de reservas y huéspedes de un Hotel, aplicando metodología CRISP-DM y arquitectura dimensional Kimball (Star Schema).

## Descripción General

El proyecto transforma un dataset transaccional crudo de reservas y huéspedes (70,882 registros, 79 columnas, 2 hojas Excel) en un Data Warehouse dimensional consultable desde herramientas BI.

Flujo general:

```text
Raw Data (Excel) -> Limpieza (Python/Pandas) -> Star Schema (MySQL) -> Visualización (Power BI / Tableau)
```

## Estructura Actual Del Proyecto

```text
proyecto_analitica_hotel/
|
|-- 00_setup_proyecto.ipynb
|-- README.md
|-- requirements.txt
|
|-- dags/
|   |-- dag_analitica.py
|
|-- data/
|   |-- raw/                    # Dataset fuente (no versionado)
|   |-- processed/
|       |-- reservas_clean.parquet
|       |-- reservas_clean.xlsx
|
|-- docker/
|   |-- docker-compose.yml
|   |-- dockerfile
|   |-- requirements-docker.txt
|
|-- notebooks/
|   |-- 01_exploracion_dataset.ipynb
|   |-- 02_limpieza_datos.ipynb
|
|-- reports/
|   |-- figures/                # Evidencias gráficas del EDA
|   |-- dashboard/
|       |-- capturas/           # Evidencia visual de dashboards
|       |-- powerbi/            # Archivos .pbix/.pbit
|
|-- sql/
|   |-- 01_ddl_kimball.sql
|
|-- src/
|   |-- db_connection.py
|   |-- etl_dim_canal.py
|   |-- etl_dim_contexto_huesped.py
|   |-- etl_dim_empresa.py
|   |-- etl_dim_fecha.py
|   |-- etl_dim_habitacion.py
|   |-- etl_dim_huesped.py
|   |-- etl_dim_segmento.py
|   |-- etl_dim_temporada.py
|   |-- etl_fact_reservas.py
```

## Modelo Dimensional (Kimball)

El modelo actual incluye 1 tabla de hechos y 8 dimensiones:

- Fact_Reservas
- Dim_Fecha
- Dim_Segmento
- Dim_Canal
- Dim_Habitacion
- Dim_Huesped
- Dim_Contexto_Huesped
- Dim_Empresa
- Dim_Temporada

DDL completo:

- `sql/01_ddl_kimball.sql`

## Notebooks Activos

### 00_setup_proyecto.ipynb

Notebook de preparación del entorno y estructura del proyecto (configuración inicial, verificación de Python y organización de carpetas).

### notebooks/01_exploracion_dataset.ipynb

Fase de comprensión de datos (EDA): perfilado inicial, calidad de datos, distribuciones, correlaciones y outliers.

Salidas de evidencia generadas en:

- `reports/figures/00_calidad_datos.png`
- `reports/figures/01_segmento_comercial.png`
- `reports/figures/02_temporada.png`
- `reports/figures/03_top10_canales.png`
- `reports/figures/04_tipo_habitacion.png`
- `reports/figures/05_sexo_huesped.png`
- `reports/figures/06_histogramas_numericas.png`
- `reports/figures/07_serie_temporal_mensual.png`
- `reports/figures/08_correlacion.png`
- `reports/figures/09_boxplot_*.png`

### notebooks/02_limpieza_datos.ipynb

Fase de preparación de datos: limpieza, anonimización de PII, imputaciones, estandarización de fechas y generación del dataset final para consumo ETL.

Salidas principales:

- `data/processed/reservas_clean.parquet`
- `data/processed/reservas_clean.xlsx`

## ETL De Producción (src/)

Scripts invocados por Airflow (patrón ETL: extraer -> transformar -> cargar):

- `db_connection.py`: conexión centralizada MySQL con creación idempotente de base.
- `etl_dim_fecha.py`: calendario completo para llaves de fecha.
- `etl_dim_segmento.py`: catálogo de segmentos.
- `etl_dim_canal.py`: canales de distribución y clasificación online/offline.
- `etl_dim_habitacion.py`: atributos de habitación.
- `etl_dim_huesped.py`: identidad anonimizada de huésped.
- `etl_dim_contexto_huesped.py`: contexto de reserva/perfil del huésped para análisis.
- `etl_dim_empresa.py`: empresas asociadas a la reserva.
- `etl_dim_temporada.py`: temporadas operativas.
- `etl_fact_reservas.py`: carga de hechos con control de integridad referencial.

## Orquestación Airflow

- DAG actual: `etl_kimball_reserva_hotelera`
- Archivo DAG: `dags/dag_analitica.py`

Orden de ejecución:

```text
crear_schema_mysql -> cargar_dimensiones (paralelo) -> fact_reservas
```

En `cargar_dimensiones` se ejecutan:

- dim_fecha
- dim_segmento
- dim_canal
- dim_habitacion
- dim_temporada
- dim_huesped
- dim_contexto_huesped
- dim_empresa

## Docker Y Operación

Levantar por primera vez:

```bash
cd docker/
docker compose up -d --build
```

Operación diaria:

```bash
docker compose up -d
docker compose ps
docker compose down
```

Airflow UI:

- URL: `http://localhost:8091`
- Usuario: `admin`
- Password: `admin`

## Evidencia BI Y Trazabilidad Profesional

### Rutas para entregables

- `reports/dashboard/capturas/`

- `reports/dashboard/powerbi/`

## Requisitos Previos

- Docker Desktop en ejecución.
- MySQL local en puerto 3306.
- Python para notebooks locales.
- Dependencias locales:

```bash
pip install -r requirements.txt
```

## Stack Tecnológico

| Capa | Tecnología |
|---|---|
| Orquestación | Apache Airflow 2.9.2 + LocalExecutor |
| Contenedores | Docker / Docker Compose |
| Procesamiento | Python (Pandas, NumPy, Scikit-Learn) |
| Almacenamiento | MySQL 8 (DW) + PostgreSQL 15 (metadatos Airflow) |
| Conexión | SQLAlchemy + mysql-connector-python |
| Formato intermedio | Parquet (PyArrow) |
| BI | Power BI |
| Metodología | CRISP-DM + Kimball |
