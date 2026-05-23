"""
etl_dim_empresa.py
ETL — Dim_Empresa
Extrae las empresas únicas asociadas a reservas del parquet y las carga en MySQL.

Origen de cada campo
---------------------
nombre_empresa : campo nombre_emp del parquet. Nombre de la empresa asociada a la reserva.
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Empresa"


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer() -> pd.DataFrame:
    """
    Lee nombre_emp desde el parquet limpio.
    El parquet es la única fuente de verdad; si no existe se lanza error.
    """
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )

    df = pd.read_parquet(PARQUET, columns=["nombre_emp"])

    print(f"  Registros leídos del parquet : {len(df):,}")
    return df


# ── TRANSFORMAR ───────────────────────────────────────────────────────────────
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica empresas y limpia espacios.

    Pasos
    -----
    1. Limpiar espacios y unificar mayúsculas/minúsculas (strip + title case).
    2. Deduplicar por nombre_empresa limpio.
    3. Ordenar alfabéticamente para consistencia entre ejecuciones.
    """

    if "nombre_emp" not in df.columns:
        raise KeyError("No existe la columna 'nombre_emp' en el parquet de entrada.")

    # Partir de la fuente y normalizar texto para poder limpiar y deduplicar.
    empresas = (
        df["nombre_emp"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    # Estandarizar capitalización para unificar variantes del mismo nombre.
    empresas = empresas.str.title()

    # Deduplicar y ordenar
    empresas = (
        pd.DataFrame({"nombre_empresa": empresas})
        .drop_duplicates(subset=["nombre_empresa"])
        .sort_values("nombre_empresa")
        .reset_index(drop=True)
    )
    print(f"  Total filas a cargar         : {len(empresas):,}")

    return empresas


# ── CARGAR ────────────────────────────────────────────────────────────────────
def cargar(df: pd.DataFrame, engine) -> None:
    """
    Carga Dim_Empresa en MySQL.
    Estrategia idempotente: DELETE + RESET AUTO_INCREMENT + INSERT.

    INSERT estático del DDL, por lo que re-ejecutar este script es seguro.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
        conn.execute(text(f"ALTER TABLE {TABLE} AUTO_INCREMENT = 1"))

    df.to_sql(
        TABLE,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"  {TABLE} cargada : {len(df):,} filas")


# ── ORQUESTADOR ───────────────────────────────────────────────────────────────
def run():
    print(f"\n{'='*55}")
    print(f"  ETL [{TABLE}]")
    print(f"{'='*55}")

    engine = get_engine()
    df_raw = extraer()
    df = transformar(df_raw)
    cargar(df, engine)
    print(f"  ETL {TABLE} completado.\n")


if __name__ == "__main__":
    run()
