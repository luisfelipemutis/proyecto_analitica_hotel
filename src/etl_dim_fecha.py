"""
etl_dim_fecha.py
ETL — Dim_Fecha
Lee el dataset limpio, genera el calendario completo y carga en MySQL.

Estrategia de rango:
    El calendario cubre desde la fecha más antigua hasta la fecha más reciente
    encontrada en TODAS las columnas de fecha del parquet:
        fecha, fllega_aco, fsalid_aco, fechasischin, fcheckout
    Esto garantiza que cualquier FK de fecha en Fact_Reservas tenga su
    correspondiente registro en Dim_Fecha, sin importar qué campo de fecha
    se use en el join. Al ser un calendario en MySQL no hay problema de
    rendimiento por la cantidad de filas generadas (~2.200 días).
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Fecha"

COLS_FECHA = ["fecha", "fllega_aco", "fsalid_aco", "fechasischin", "fcheckout"]


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer_rango_fechas() -> tuple:
    """
    Lee todas las columnas de fecha del parquet y retorna
    (fecha_global_minima, fecha_global_maxima) normalizadas a medianoche.

    Al evaluar el rango sobre todos los campos de fecha simultáneamente
    se evita que días de reserva (fecha), check-out real (fcheckout) u
    otras fechas queden fuera del calendario y generen NULLs en los
    FKs de Fact_Reservas durante el join con Dim_Fecha.
    """
    # Leer solo las columnas de fecha para minimizar uso de memoria
    df = pd.read_parquet(PARQUET, columns=COLS_FECHA)

    # Apilar todas las columnas de fecha en una sola Serie y calcular rango global
    fechas_stack = pd.concat(
        [pd.to_datetime(df[col], errors="coerce") for col in COLS_FECHA],
        ignore_index=True,
    ).dropna()

    if fechas_stack.empty:
        raise ValueError(
            f"No se encontraron fechas válidas en ninguna de las columnas: {COLS_FECHA}"
        )

    fecha_min = fechas_stack.min().normalize()
    fecha_max = fechas_stack.max().normalize()

    print(f"  Columnas evaluadas  : {COLS_FECHA}")
    print(f"  Fecha mínima global : {fecha_min.date()}")
    print(f"  Fecha máxima global : {fecha_max.date()}")
    print(f"  Días a generar      : {(fecha_max - fecha_min).days + 1:,}")
    return fecha_min, fecha_max


# ── TRANSFORMAR ───────────────────────────────────────────────────────────────
def transformar(fecha_min, fecha_max) -> pd.DataFrame:
    """
    Genera el DataFrame calendario con un registro por día.

    Columnas generadas (alineadas con VARS_FINAL del parquet)
    ----------------------------------------------------------
    id_fecha        : clave surrogate entera YYYYMMDD (ej. 20220315)
    fecha           : valor DATE del día
    anio            : año (2020 … 2026)          ← viene de VARS_FINAL
    trimestre       : 1-4                         ← viene de VARS_FINAL
    mes             : número de mes 1-12          ← viene de VARS_FINAL
    nombre_mes      : nombre del mes en español (Enero … Diciembre)
    semana_anio     : semana ISO 1-53
    dia_semana_num  : 1 = lunes … 7 = domingo
    dia_semana      : nombre completo (Monday … Sunday) ← viene de VARS_FINAL
    es_fin_semana   : 1 si sábado o domingo, 0 en caso contrario

    Campos excluidos intencionalmente
    ----------------------------------
    semestre      : no está en VARS_FINAL, no aplica al modelo.
    """
    rango = pd.date_range(start=fecha_min, end=fecha_max, freq="D")
    dim = pd.DataFrame({"fecha": rango})

    dim["id_fecha"] = dim["fecha"].dt.strftime("%Y%m%d").astype(int)
    dim["anio"] = dim["fecha"].dt.year
    dim["trimestre"] = dim["fecha"].dt.quarter
    dim["mes"] = dim["fecha"].dt.month
    meses_es = {
        1: "Enero",    2: "Febrero",   3: "Marzo",
        4: "Abril",    5: "Mayo",      6: "Junio",
        7: "Julio",    8: "Agosto",    9: "Septiembre",
        10: "Octubre", 11: "Noviembre", 12: "Diciembre",
    }
    dim["nombre_mes"] = dim["mes"].map(meses_es)
    dim["semana_anio"] = dim["fecha"].dt.isocalendar().week.astype(int)
    dim["dia_semana_num"] = dim["fecha"].dt.dayofweek + 1  # lunes=1 … domingo=7
    dim["dia_semana"] = dim["fecha"].dt.day_name()
    dim["es_fin_semana"] = (dim["dia_semana_num"] >= 6).astype(int)

    return dim[
        [
            "id_fecha",
            "fecha",
            "anio",
            "trimestre",
            "mes",
            "nombre_mes",
            "semana_anio",
            "dia_semana_num",
            "dia_semana",
            "es_fin_semana",
        ]
    ]


# ── CARGAR ────────────────────────────────────────────────────────────────────
def cargar(df: pd.DataFrame, engine) -> None:
    """
    Carga Dim_Fecha en MySQL.

    Estrategia idempotente: DELETE + INSERT.
    Si el DAG hace un retry o se re-ejecuta manualmente la tabla queda
    siempre en un estado consistente sin filas duplicadas.
    chunksize=1000 evita saturar el buffer de MySQL con un INSERT masivo.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))

    df.to_sql(
        TABLE,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"  {TABLE} cargada : {len(df):,} filas")


# ── ORQUESTADOR ───────────────────────────────────────────────────────────────
def run():
    print(f"\n{'='*55}")
    print(f"  ETL [{TABLE}]")
    print(f"{'='*55}")

    engine = get_engine()
    fecha_min, fecha_max = extraer_rango_fechas()
    df = transformar(fecha_min, fecha_max)

    print(f"  Filas generadas     : {len(df):,}")
    print(f"  Columnas            : {list(df.columns)}")
    cargar(df, engine)
    print(f"  ETL {TABLE} completado.\n")


if __name__ == "__main__":
    run()
