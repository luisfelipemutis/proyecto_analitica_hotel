"""
etl_dim_fecha.py
ETL — Dim_Fecha
Lee el dataset limpio, genera el calendario completo y carga en MySQL.
Granularidad: un registro por día del rango Jun 2020 – Abr 2026.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
RAW     = Path(__file__).parent.parent / "data" / "raw" / "DataSet_ReservaYHuespedes_V.Full.xlsx"
TABLE   = "Dim_Fecha"


def extraer_rango_fechas() -> tuple:
    """Extrae fecha mínima y máxima del dataset."""
    if PARQUET.exists():
        df = pd.read_parquet(PARQUET, columns=["fllega_aco"])
    else:
        h1 = pd.read_excel(RAW, sheet_name="Hoja1", usecols=["fllega_aco"])
        h2 = pd.read_excel(RAW, sheet_name="Hoja2", usecols=["fllega_aco"])
        df = pd.concat([h1, h2], ignore_index=True)
    fechas = pd.to_datetime(df["fllega_aco"], errors="coerce").dropna()
    return fechas.min().normalize(), fechas.max().normalize()


def transformar(fecha_min, fecha_max) -> pd.DataFrame:
    """Genera el dataframe calendario completo."""
    rango = pd.date_range(start=fecha_min, end=fecha_max, freq="D")
    dim = pd.DataFrame({"fecha": rango})
    dim["id_fecha"]       = dim["fecha"].dt.strftime("%Y%m%d").astype(int)
    dim["anio"]           = dim["fecha"].dt.year
    dim["semestre"]       = dim["fecha"].dt.month.apply(lambda m: 1 if m <= 6 else 2)
    dim["trimestre"]      = dim["fecha"].dt.quarter
    dim["mes"]            = dim["fecha"].dt.month
    dim["nombre_mes"]     = dim["fecha"].dt.strftime("%b").str.upper()
    dim["semana_anio"]    = dim["fecha"].dt.isocalendar().week.astype(int)
    dim["dia_semana_num"] = dim["fecha"].dt.dayofweek + 1
    dim["dia_semana"]     = dim["fecha"].dt.day_name()
    dim["es_fin_semana"]  = (dim["dia_semana_num"] >= 6).astype(int)
    return dim[["id_fecha", "fecha", "anio", "semestre", "trimestre",
                "mes", "nombre_mes", "semana_anio", "dia_semana_num",
                "dia_semana", "es_fin_semana"]]


def cargar(df: pd.DataFrame, engine) -> None:
    """Carga Dim_Fecha en MySQL con INSERT IGNORE para idempotencia."""
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(TABLE, con=engine, if_exists="append", index=False,
              method="multi", chunksize=1000)
    print(f"✅ {TABLE} cargada: {len(df):,} filas")


def run():
    print(f"[{TABLE}] Iniciando ETL...")
    engine = get_engine()
    fecha_min, fecha_max = extraer_rango_fechas()
    print(f"  Rango: {fecha_min.date()} → {fecha_max.date()}")
    df = transformar(fecha_min, fecha_max)
    cargar(df, engine)


if __name__ == "__main__":
    run()
