"""
etl_dim_huesped.py
ETL — Dim_Huesped
Dimensión de identidad estable del huésped.

Solo conserva el identificador anonimizado (`id_huesped`) para evitar mezclar
atributos que cambian por reserva (sexo, edad, nacionalidad, rol, etc.).
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Huesped"

COLS = ["id_huesped"]


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer() -> pd.DataFrame:
    """
    Lee id_huesped desde el parquet limpio.
    El parquet es la única fuente de verdad; si no existe se lanza error.

    Nota: id_huesped ya viene como hash SHA-256 (16 chars hex)
    calculado en notebook 02 sobre ident_aco (dato PII que ya no existe).
    """
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )

    # Verificar columnas usando solo el esquema del parquet (sin cargar datos).
    # NOTA: pd.read_parquet(PARQUET, columns=[]) devuelve DataFrame vacío en
    # muchas versiones de PyArrow — NO usar para leer el esquema.
    cols_parquet = pq.read_schema(str(PARQUET)).names
    cols_faltantes = [c for c in COLS if c not in cols_parquet]
    if cols_faltantes:
        raise KeyError(
            f"Columnas no encontradas en el parquet: {cols_faltantes}\n"
            "El parquet fue generado antes de que el notebook 02 calculara\n"
            "id_huesped. Vuelve a ejecutar\n"
            "el notebook 02_limpieza_datos.ipynb completo y reintenta."
        )

    df = pd.read_parquet(PARQUET, columns=COLS)

    print(f"  Registros leídos del parquet : {len(df):,}")
    sin_id = df["id_huesped"].isna().sum()
    if sin_id:
        print(
            f"  WARN: {sin_id:,} registros sin id_huesped → se excluyen de la dimensión"
        )
    return df


# ── TRANSFORMAR ───────────────────────────────────────────────────────────────
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera la dimensión de identidad estable del huésped.

    Pasos
    -----
    1. Normalizar id_huesped (trim + uppercase).
    2. Excluir registros vacíos.
    3. Deduplicar por id_huesped.
    4. Agregar fila ANONIMO para fallback en hechos.
    """
    dim = df.copy()
    dim["id_huesped"] = dim["id_huesped"].astype("string").str.strip().str.upper()
    dim = dim[dim["id_huesped"].notna() & (dim["id_huesped"] != "")]

    excluidos = len(df) - len(dim)
    if excluidos > 0:
        print(f"  Registros excluidos sin id_huesped: {excluidos:,}")

    dim = dim.drop_duplicates(subset=["id_huesped"], keep="first").reset_index(
        drop=True
    )

    fila_anonimo = pd.DataFrame({"id_huesped": ["ANONIMO"]})
    dim = pd.concat([fila_anonimo, dim], ignore_index=True)

    # Generar surrogate key secuencial
    dim.insert(0, "id_registro_huesped", range(1, len(dim) + 1))

    print(f"  Huespedes unicos en la dimension : {len(dim):,}")

    return dim


# ── CARGAR ────────────────────────────────────────────────────────────────────
def cargar(df: pd.DataFrame, engine) -> None:
    """
    Carga Dim_Huesped en MySQL.
    Estrategia idempotente: DELETE + INSERT.

    chunksize=500 evita saturar el buffer de MySQL con decenas de miles de filas.
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
        chunksize=500,
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
