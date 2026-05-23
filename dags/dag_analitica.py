# pyright: reportMissingImports=false, reportMissingModuleSource=false
"""
dag_analitica.py
DAG de Apache Airflow — Pipeline ETL Kimball - Hotel Dann Monasterio

Orden de ejecucion:
  crear_schema -> [Dims en paralelo] -> fact_reservas
"""

import os
import re
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

PROJECT = "/opt/airflow/project"
SRC = f"{PROJECT}/src"
SQL_DDL = f"{PROJECT}/sql/01_ddl_kimball.sql"

# PYTHONPATH incluye PROJECT y SRC para que los scripts ETL
# puedan importar db_connection (que vive en src/).
PYTHONPATH_ETL = f"{PROJECT}:{SRC}"

MYSQL_ENV = (
    f"MYSQL_HOST={os.getenv('MYSQL_HOST','host.docker.internal')} "
    f"MYSQL_PORT={os.getenv('MYSQL_PORT','3306')} "
    f"MYSQL_USER={os.getenv('MYSQL_USER','root')} "
    f"MYSQL_PASSWORD={os.getenv('MYSQL_PASSWORD','root')} "
    f"MYSQL_DATABASE={os.getenv('MYSQL_DATABASE','hotel_dann_dw')} "
)


def _crear_schema():
    """
    Crea la base de datos hotel_dann_dw si no existe y ejecuta el DDL de tablas y vistas.

    Estrategia idempotente:
      1. Conecta SIN base de datos -> CREATE DATABASE IF NOT EXISTS
        2. Conecta CON base -> extrae solo CREATE TABLE / CREATE VIEW del DDL
            (ignora DROP/CREATE DATABASE y USE que son para MySQL Workbench)
    """
    import sys

    sys.path.insert(0, SRC)
    from sqlalchemy import create_engine, text

    host = os.getenv("MYSQL_HOST", "host.docker.internal")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER", "root")
    pwd = os.getenv("MYSQL_PASSWORD", "root")
    db = os.getenv("MYSQL_DATABASE", "hotel_dann_dw")

    # PASO 1: Conectar SIN base -> crear schema si no existe
    url_sin_db = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/"
    engine_sin_db = create_engine(url_sin_db, pool_pre_ping=True, echo=False)
    with engine_sin_db.begin() as conn:
        conn.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{db}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci"
            )
        )
        print(f"OK Base de datos '{db}' creada o ya existia.")
    engine_sin_db.dispose()

    # Conectar CON la base -> ejecutar solo CREATE TABLE
    url_con_db = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(url_con_db, pool_pre_ping=True, echo=False)

    raw_ddl = Path(SQL_DDL).read_text(encoding="utf-8")

    # El regex anterior fallaba cuando había paréntesis dentro de comentarios
    # SQL (ej. COMMENT='texto (detalle)'). Se usa split por ';' sobre el DDL
    # sin comentarios de línea para extraer CREATE TABLE de forma estable.
    ddl_sin_comentarios = re.sub(r"(?m)^\s*--.*$", "", raw_ddl)
    table_stmts = [
        stmt.strip()
        for stmt in ddl_sin_comentarios.split(";")
        if stmt.strip().upper().startswith("CREATE TABLE")
    ]

    view_stmts = [
        stmt.strip()
        for stmt in ddl_sin_comentarios.split(";")
        if stmt.strip().upper().startswith("CREATE OR REPLACE VIEW")
        or stmt.strip().upper().startswith("CREATE VIEW")
    ]

    if not table_stmts:
        raise ValueError(f"No se encontraron CREATE TABLE en {SQL_DDL}.")

    print(f"  Tablas a crear/verificar: {len(table_stmts)}")
    print(f"  Vistas a crear/reemplazar: {len(view_stmts)}")

    with engine.begin() as conn:
        for stmt in table_stmts:
            stmt_clean = stmt.strip()
            m = re.search(
                r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`]?(\w+)[`]?",
                stmt_clean,
                re.IGNORECASE,
            )
            tabla = m.group(1) if m else "?"
            try:
                conn.execute(text(stmt_clean))
                print(f"  OK Tabla '{tabla}' creada.")
            except Exception as e:
                err_str = str(e)
                if "1050" in err_str or "already exists" in err_str.lower():
                    print(f"  (ya existe) '{tabla}'")
                else:
                    print(f"  WARN [{tabla}]: {err_str[:120]}")

        for stmt in view_stmts:
            stmt_clean = stmt.strip()
            m = re.search(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+[`]?(\w+)[`]?(?:\s+AS)?",
                stmt_clean,
                re.IGNORECASE,
            )
            vista = m.group(1) if m else "?"
            try:
                conn.execute(text(stmt_clean))
                print(f"  OK Vista '{vista}' creada/reemplazada.")
            except Exception as e:
                print(f"  WARN [vista:{vista}]: {str(e)[:120]}")

    engine.dispose()
    print("OK Schema hotel_dann_dw verificado en MySQL")


with DAG(
    dag_id="etl_kimball_reserva_hotelera",
    description="Pipeline ETL Kimball - Reserva Hotelera",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hotel", "kimball", "etl", "mysql"],
) as dag:

    crear_schema = PythonOperator(
        task_id="crear_schema_mysql",
        python_callable=_crear_schema,
    )

    with TaskGroup("cargar_dimensiones") as dims_group:

        dim_fecha = BashOperator(
            task_id="dim_fecha",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_fecha.py"
            ),
        )
        dim_segmento = BashOperator(
            task_id="dim_segmento",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_segmento.py"
            ),
        )
        dim_canal = BashOperator(
            task_id="dim_canal",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_canal.py"
            ),
        )
        dim_habitacion = BashOperator(
            task_id="dim_habitacion",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_habitacion.py"
            ),
        )
        dim_temporada = BashOperator(
            task_id="dim_temporada",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_temporada.py"
            ),
        )
        dim_huesped = BashOperator(
            task_id="dim_huesped",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_huesped.py"
            ),
        )
        dim_contexto_huesped = BashOperator(
            task_id="dim_contexto_huesped",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_contexto_huesped.py"
            ),
        )
        dim_empresa = BashOperator(
            task_id="dim_empresa",
            bash_command=(
                f"cd {PROJECT} && "
                f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
                f"python {SRC}/etl_dim_empresa.py"
            ),
        )

    fact_reservas = BashOperator(
        task_id="fact_reservas",
        bash_command=(
            f"cd {PROJECT} && "
            f"PYTHONPATH={PYTHONPATH_ETL} {MYSQL_ENV} "
            f"python {SRC}/etl_fact_reservas.py"
        ),
    )

    crear_schema >> dims_group >> fact_reservas
