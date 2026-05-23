"""
etl_dim_temporada.py
ETL — Dim_Temporada
Extrae temporadas desde el parquet limpio (codigotemporada, nombretemporada)
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Temporada"

DESC_POR_CODIGO = {
    "A": "Temporada alta demanda: dic-ene, julio, semana santa y festivos nacionales",
    "B": "Temporada baja demanda: meses intermedios sin festivos",
    "ND": "Sin temporada registrada en el sistema fuente.",
}


def extraer() -> pd.DataFrame:
    """
    Lee codigotemporada y nombretemporada desde el parquet limpio.
    """
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )

    df = pd.read_parquet(PARQUET, columns=["codigotemporada", "nombretemporada"])
    print(f"  Registros leidos del parquet : {len(df):,}")
    print(
        f"  Codigos temporada unicos     : {df['codigotemporada'].nunique(dropna=True)}"
    )
    return df


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye Dim_Temporada desde codigos reales del parquet
    """
    work = df.copy()
    work["codigo_temporada"] = (
        work["codigotemporada"].astype(str).str.strip().str.upper()
    )
    work["nombre_temporada"] = work["nombretemporada"].astype(str).str.strip()

    # Normalizar nulos originales para limpieza posterior
    work.loc[work["codigotemporada"].isna(), "codigo_temporada"] = ""
    work.loc[work["nombretemporada"].isna(), "nombre_temporada"] = ""

    nulos_codigo = (work["codigo_temporada"] == "").sum()
    nulos_nombre = (work["nombre_temporada"] == "").sum()

    # Mantener temporadas validas (con codigo), deduplicadas por codigo
    validas = work[work["codigo_temporada"] != ""].copy()
    validas = validas[["codigo_temporada", "nombre_temporada"]]

    # Si nombre viene vacio para un codigo valido, usar etiqueta generica
    validas.loc[validas["nombre_temporada"] == "", "nombre_temporada"] = (
        "Temporada "
        + validas.loc[validas["nombre_temporada"] == "", "codigo_temporada"]
    )

    validas = (
        validas.sort_values(["codigo_temporada", "nombre_temporada"])
        .drop_duplicates(subset=["codigo_temporada"], keep="first")
        .reset_index(drop=True)
    )
    validas["descripcion"] = validas["codigo_temporada"].map(DESC_POR_CODIGO)
    validas["descripcion"] = validas["descripcion"].fillna(
        "Temporada registrada en el sistema fuente."
    )

    temporada = pd.concat([validas], ignore_index=True)
    temporada.insert(0, "id_temporada", range(1, len(temporada) + 1))

    print(f"  Codigos sin temporada (nulos): {nulos_codigo:,}")
    print(f"  Nombres temporada vacios      : {nulos_nombre:,}")
    print(f"  Temporadas a cargar           : {len(temporada):,}")

    return temporada


def cargar(df: pd.DataFrame, engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(TABLE, con=engine, if_exists="append", index=False, method="multi")
    print(f" {TABLE} cargada: {len(df)} filas")


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
