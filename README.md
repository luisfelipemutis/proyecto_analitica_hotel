# Solución Analítica de Datos — Hotel Dann Monasterio

Proyecto final de grado del Diplomado en Analítica de Datos (Unicomfacauca).  
Diseña e implementa un pipeline completo de inteligencia de negocios sobre el dataset transaccional de reservas y huéspedes del **Hotel Dann Monasterio**, aplicando la metodología **CRISP-DM** y arquitectura dimensional **Kimball (Star Schema)**.

---

## Descripción del proyecto

El Hotel Dann Monasterio genera diariamente registros transaccionales de reservas que contienen información financiera, operativa y demográfica de sus huéspedes. Este proyecto transforma ese dataset crudo —**70.882 registros, 79 columnas, dos hojas Excel**— en un Data Warehouse dimensional consultable desde herramientas de BI (Power BI / Tableau), siguiendo el ciclo completo:

```
Raw Data (Excel)  →  Limpieza (Python/Pandas)  →  Star Schema (MySQL)  →  Visualización (BI)
```

### Tipo de análisis aplicado

El proyecto implementa tres capas de análisis sobre el dataset:

**1. Análisis Descriptivo (EDA)**  
Distribución de variables financieras (`tarifa`, `valorplan`, `ingreso_total`), perfil demográfico de huéspedes (`edad_aco`, `sexo_aco`, `nacionalidad`), estacionalidad por temporada (`codigotemporada`: A=Alta, B=Baja) y mix de canales de distribución (`codiga_age`: BKNG, RECE, EXPD, etc.).

**2. Análisis de Segmentación**  
Comportamiento por segmento de mercado (`codsegmento`: COR=Corporativo, T&T=Turismo, CE=Corporativo Especial), tipo de habitación (`tiphab_tip`: S3, SE=Suite, ST) y clase (`clahab_clh`: SG=Sencilla, DB=Doble, CD). Permite identificar qué segmentos generan mayor ingreso promedio por noche.

**3. Análisis de Patrones Temporales**  
`lead_time` (días entre reserva y llegada), `duracion_estancia` (noches por estadía) y concentración de ocupación por mes/temporada. Responde preguntas como: ¿con cuánta anticipación reservan los huéspedes corporativos vs. turistas?

### Variables clave del dataset

| Categoría | Variables |
|---|---|
| **Financieras** | `tarifa`, `valorplan`, `ivaplan`, `servicioplan`, `valorconsumoadicional`, `totalconsumosadicional`, `ingreso_total` |
| **Temporales** | `fllega_aco` (llegada), `fsalid_aco` (salida), `fechasischin` (fecha reserva), `duracion_estancia`, `lead_time` |
| **Canal / Origen** | `codiga_age`, `nombre_age` (BKNG=Booking, RECE=Recepción, EXPD=Expedia...) |
| **Segmento** | `codsegmento` (COR, T&T, CE), `nombre_segmento` |
| **Habitación** | `tiphab_tip`, `clahab_clh`, `codigotemporada` |
| **Huésped** | `edad_aco`, `sexo_aco`, `nacionalidad`, `oficio`, `nombre_emp` |

---

## Estructura del proyecto

```
proyecto_analitica_hotel/
│
├── data/
│   ├── raw/                          # Dataset original Excel (no versionado)
│   └── processed/
│       └── reservas_clean.parquet    # Dataset limpio tras ETL de ingesta
│
├── notebooks/                        # Análisis y exploración (Jupyter)
│   ├── 01_exploracion_dataset.ipynb
│   ├── 02_limpieza_datos.ipynb
│   ├── 03_analisis_exploratorio.ipynb
│   ├── 04_analisis_descriptivo.ipynb
│   └── 05_etl_kimball.ipynb
│
├── src/                              # Scripts ETL de producción (Python)
│   ├── db_connection.py              # Conexión centralizada MySQL
│   ├── ingestion.py                  # Carga inicial del Excel a Parquet
│   ├── etl_dim_fecha.py
│   ├── etl_dim_segmento.py
│   ├── etl_dim_canal.py
│   ├── etl_dim_habitacion.py
│   ├── etl_dim_huesped.py
│   ├── etl_dim_temporada.py
│   └── etl_fact_reservas.py
│
├── sql/
│   └── 01_ddl_kimball.sql            # DDL completo del Star Schema
│
├── dags/
│   └── dag_analitica.py              # DAG Airflow: orquesta el pipeline completo
│
├── docker/
│   ├── docker-compose.yml            # Orquestación de servicios (Airflow + Postgres)
│   ├── Dockerfile                    # Imagen custom de Airflow
│   └── requirements-docker.txt       # Dependencias Python del contenedor
│
├── reports/                          # Visualizaciones exportadas
├── requirements.txt                  # Dependencias entorno local (Jupyter)
└── README.md
```

---

## Modelo dimensional (Star Schema — Kimball)

La tabla de hechos `Fact_Reservas` se conecta a 6 dimensiones:

```
                    Dim_Fecha
                       │
   Dim_Temporada ──────┤
                       │
   Dim_Segmento ───── Fact_Reservas ───── Dim_Canal
                       │
   Dim_Habitacion ─────┤
                       │
                    Dim_Huesped
```

| Tabla | Tipo | Descripción |
|---|---|---|
| `Fact_Reservas` | Hechos | 70K+ reservas con métricas financieras y duraciones |
| `Dim_Fecha` | Dimensión | Calendario completo (año, mes, trimestre, día semana) |
| `Dim_Canal` | Dimensión | Canal de distribución (OTA, Directo, Agencia, GDS) |
| `Dim_Segmento` | Dimensión | Segmento de mercado (Corporativo, Turismo, etc.) |
| `Dim_Habitacion` | Dimensión | Tipo, clase, capacidad y categoría de habitación |
| `Dim_Huesped` | Dimensión | Perfil demográfico anonimizado del huésped |
| `Dim_Temporada` | Dimensión | Alta (A) / Baja (B) con fechas de vigencia |

El DDL completo con llaves foráneas y comentarios está en `sql/01_ddl_kimball.sql`.

---

## Notebooks

Los notebooks siguen la secuencia de la metodología CRISP-DM y deben ejecutarse en orden:

### `01_exploracion_dataset.ipynb` — Comprensión del dato
Carga el Excel original (dos hojas), inspecciona tipos de dato, detecta las **14 columnas completamente vacías** (ruido estructural), identifica nulos por variable y genera el primer perfil del dataset. Punto de partida para el plan de limpieza.

### `02_limpieza_datos.ipynb` — Preparación de datos
Elimina las columnas vacías, estandariza fechas (`fllega_aco`, `fsalid_aco`, `fechasischin`), corrige tipos numéricos en variables financieras, calcula `duracion_estancia` y `lead_time`, y exporta el resultado a `data/processed/reservas_clean.parquet`. Este parquet es la fuente única de verdad para todos los ETLs.

### `03_analisis_exploratorio.ipynb` — EDA univariado y bivariado
Distribuciones de `tarifa` e `ingreso_total` por canal y segmento, análisis de outliers (IQR), heatmaps de correlación entre variables financieras, y boxplots de `duracion_estancia` por temporada y tipo de habitación.

### `04_analisis_descriptivo.ipynb` — Análisis de negocio
Agrupaciones estratégicas: ingresos por canal de distribución, tasa de ocupación por mes, ADR (Average Daily Rate) por segmento, perfil demográfico del huésped promedio (edad, nacionalidad, sexo) y análisis de concentración de reservas por empresa corporativa (`nombre_emp`).

### `05_etl_kimball.ipynb` — Prototipo del pipeline ETL
Versión interactiva del pipeline de carga al Data Warehouse. Permite ejecutar y validar cada dimensión de forma independiente antes de automatizarlo con Airflow. Incluye conteo de registros por tabla, verificación de FK y muestra de datos cargados.

---

## Scripts ETL (`src/`)

Los scripts de producción son invocados por el DAG de Airflow. Cada uno sigue el patrón `extraer → transformar → cargar` (ETL) con manejo de nulos y tipado explícito.

### `db_connection.py`
Conexión centralizada a MySQL. Lee credenciales desde variables de entorno (`MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_PORT`). Implementa estrategia de **dos pasos**: conecta primero sin especificar base de datos para crearla si no existe, luego retorna el engine con la DB garantizada. Esto previene errores `Unknown database` en reinicios o retries de Airflow.

```python
# Uso en cualquier ETL
from db_connection import get_engine
engine = get_engine()
```

### `ingestion.py`
Lee el Excel original (Hoja1 + Hoja2), concatena ambas hojas y guarda el resultado en `data/processed/reservas_clean.parquet`. Paso previo a todos los ETLs de dimensión.

### `etl_dim_fecha.py`
Genera el calendario dimensional completo desde la fecha mínima a máxima del dataset. Columnas: `id_fecha` (YYYYMMDD), `fecha`, `anio`, `trimestre`, `mes`, `nombre_mes`, `semana_anio`, `dia_semana`, `nombre_dia`, `es_fin_semana`, `es_festivo_co`.

### `etl_dim_segmento.py`
Extrae los segmentos únicos de `codsegmento` con su descripción (`nombre_segmento`). Mapea códigos internos del hotel (COR, T&T, CE, etc.) a categorías de negocio legibles.

### `etl_dim_canal.py`
Extrae y deduplica los canales de distribución desde `codiga_age` / `nombre_age`. Asigna `tipo_canal` (OTA, Directo Presencial, Agencia Nacional, GDS / Tecnología, Mayorista Online, Agencia Corporativa, Agencia Internacional) y el flag `es_online` (0/1). Maneja canales sin nombre registrado rellenando con el código como fallback.

### `etl_dim_habitacion.py`
Deduplica tipos de habitación por `tiphab_tip`. Enriquece cada tipo con descripción completa (`descripcion_tipo` VARCHAR 350), `capacidad_max` y `categoria` de lujo derivadas del catálogo interno del hotel.

### `etl_dim_huesped.py`
Genera el perfil demográfico del huésped: `edad_aco`, `sexo_aco`, `nacionalidad`, `oficio`, `nombre_emp`. El campo `id_huesped` es un hash SHA-256 de 12 caracteres del documento de identidad (`ident_aco`) para garantizar **anonimización** de datos personales.

### `etl_dim_temporada.py`
Carga los periodos de temporada Alta (A) y Baja (B) con sus rangos de fechas de vigencia. Permite análisis de revenue por temporada sin depender de la columna cruda del dataset.

### `etl_fact_reservas.py`
El ETL más complejo. Carga el parquet completo (70.882 registros), resuelve todas las FKs consultando las dimensiones ya cargadas en MySQL, calcula `ingreso_total = totalconsumosplan + totalconsumosadicional`, y aplica el filtro de **integridad referencial Kimball**: descarta registros donde alguna FK no resuelve (ej. `codiga_age` nulo) en lugar de insertar IDs fantasma que violarían las constraints. Reporta cuántos registros fueron excluidos y por qué.

---

## Docker y Airflow

### Arquitectura de servicios

El pipeline de producción corre en **3 contenedores Docker** que se comunican entre sí. La base de datos MySQL del hotel (`hotel_dann_dw`) corre en el **host local** (no en Docker) y se accede desde los contenedores vía `host.docker.internal`.

```
┌─────────────────────────────────────────────────────────┐
│  Docker Network: analitica_network                       │
│                                                          │
│  ┌──────────────────┐    ┌──────────────────────────┐   │
│  │  hotel_airflow_  │    │  hotel_airflow_webserver  │   │
│  │  scheduler       │    │  puerto: 8091 → 8080      │   │
│  └──────────────────┘    └──────────────────────────┘   │
│           │                         │                    │
│           └──────────┬──────────────┘                   │
│                      ▼                                   │
│         ┌────────────────────────┐                       │
│         │  hotel_airflow_postgres│                       │
│         │  (metadatos Airflow)   │                       │
│         └────────────────────────┘                       │
└─────────────────────────────────────────────────────────┘
                       │
                       ▼  host.docker.internal:3306
            MySQL local (hotel_dann_dw)
```

### Variables de entorno (`docker/.env`)

```env
MYSQL_HOST=host.docker.internal
MYSQL_PORT=3306
MYSQL_DATABASE=hotel_dann_dw
MYSQL_USER=root
MYSQL_PASSWORD=root
MYSQL_ROOT_PASSWORD=root
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_PORT=8091
```

> El archivo `.env` **no se sube al repositorio**. Crear una copia local a partir de este template.

### DAG: `etl_kimball_hotel_dann`

El DAG orquesta el pipeline en tres fases con dependencias estrictas:

```
crear_schema_mysql
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  dims_group  (ejecución paralela)                      │
│  dim_fecha  dim_segmento  dim_canal                    │
│  dim_habitacion  dim_huesped  dim_temporada            │
└───────────────────────────────────────────────────────┘
        │
        ▼
  fact_reservas
```

- **`crear_schema_mysql`** (PythonOperator): crea la DB si no existe y ejecuta el DDL de tablas.
- **`dims_group`** (BashOperators en paralelo): carga las 6 dimensiones de forma independiente.
- **`fact_reservas`** (BashOperator): carga la tabla de hechos una vez que todas las dims están listas.

---

## Comandos Docker

### Primera vez (construir y levantar)

```bash
# Abrir Docker Desktop primero, luego en la terminal:
cd docker/
docker compose up -d --build
```

### Verificar que todos los contenedores están corriendo

```bash
docker compose ps
```

### Operación diaria

```bash
# Levantar
docker compose up -d

# Bajar
docker compose down
```

### Reconstruir desde cero (sin caché)

```bash
# 1. Bajar todos los contenedores (incluyendo el que está en Restarting)
docker compose down

# 2. Reconstruir la imagen limpia (sin cache para forzar reinstalación)
docker compose build --no-cache

# 3. Levantar todo en background
docker compose up -d

# 4. Verificar que todos los contenedores estén healthy
docker compose ps
```

### Borrar volúmenes y empezar completamente limpio

```bash
docker compose down -v
docker compose up -d --build
```

> Usar `down -v` elimina el volumen de Postgres (metadatos Airflow). Después de esto, Airflow vuelve a su estado inicial y hay que re-crear el usuario admin con `airflow-init`.

### Acceder a la UI de Airflow

```
URL:      http://localhost:8091
Usuario:  admin
Password: admin
```

### Ver logs de un contenedor específico

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

### Ejecutar el DAG manualmente desde la UI

1. Ir a `http://localhost:8091`
2. Activar el DAG `etl_kimball_hotel_dann` (toggle ON)
3. Hacer clic en **▶ Trigger DAG**
4. Monitorear en la vista **Grid** o **Graph**

> **Importante:** Si un task falla y la base de datos MySQL fue reiniciada, no hacer *Retry* del task fallido. En cambio, hacer un **Trigger DAG** nuevo para que `crear_schema_mysql` vuelva a correr y garantice la existencia del schema antes de cargar los datos.

---

## Requisitos previos

- **Docker Desktop** instalado y corriendo
- **MySQL** corriendo en el host local en el puerto 3306 con usuario `root`
- Python 3.11+ (solo para ejecutar los notebooks localmente)
- Las dependencias del entorno local están en `requirements.txt`

```bash
# Instalar dependencias locales para Jupyter
pip install -r requirements.txt
```

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Orquestación | Apache Airflow 2.9.2 + LocalExecutor |
| Contenedores | Docker / Docker Compose |
| Procesamiento | Python 3.11 — Pandas, NumPy, Scikit-Learn |
| Almacenamiento | MySQL 8 (Data Warehouse) + PostgreSQL 15 (metadatos Airflow) |
| ORM / Conexión | SQLAlchemy 1.4.x + mysql-connector-python |
| Formato intermedio | Apache Parquet (via PyArrow) |
| Análisis | Jupyter Notebook |
| BI / Visualización | Power BI / Tableau |
| Metodología | CRISP-DM + Kimball Star Schema |
