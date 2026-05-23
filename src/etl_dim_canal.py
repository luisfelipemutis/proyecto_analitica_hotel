"""
etl_dim_canal.py
ETL — Dim_Canal
Extrae los canales/agencias únicos del parquet limpio y los carga en hotel_dann_dw.

Origen de cada campo
---------------------
codigo_canal  : campo codiga_age del parquet (código interno del hotel).
nombre_canal  : campo nombre_age del parquet. Si viene nulo se usa
                "Canal <codigo>" como fallback legible.
tipo_canal    : clasificación de negocio derivada del diccionario TIPO_CANAL.
                No existe como campo en el parquet; se asigna aquí según
                el código del canal (ej. BKNG → OTA, RECE → Directo Presencial).
es_online     : flag 0/1 derivado del mismo diccionario TIPO_CANAL.
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Canal"

# Clasificación de negocio por código de agencia/canal.
# Fuente: conocimiento operativo del Hotel Dann Monasterio.
# Para códigos no listados se aplica el fallback ("Otro", 0).
TIPO_CANAL = {
    "RECE": ("VENTAS DIRECTAS RECEPCION", 0),
    "HDAN": ("Directo Digital", 1),
    "BKNG": ("OTA", 1),
    "BKNE": ("OTA", 1),
    "EXPD": ("OTA", 1),
    "WEBB": ("Mayorista Online", 1),
    "TBOH": ("Mayorista Online", 1),
    "NTEE": ("Mayorista Online", 1),
    "Z000": ("Mayorista Online", 1),
    "TCK": ("GDS / Tecnologia", 1),
    "AMA": ("GDS / Tecnologia", 1),
    "GDS": ("GDS / Tecnologia", 1),
    "AVIA": ("Agencia Nacional", 0),
    "ACL": ("Agencia Nacional", 0),
    "AERO": ("Agencia Nacional", 0),
    "PULL": ("Agencia Nacional", 0),
    "BCD": ("Agencia Corporativa", 0),
    "Z008": ("Agencia Corporativa", 0),
    "Z003": ("Agencia Corporativa", 0),
    "CARL": ("Agencia Corporativa", 0),
    "Z002": ("Agencia Internacional", 1),
    "Z005": ("Agencia Internacional", 0),
    "Z020": ("Agencia Internacional", 0),
    "Z011": ("Agencia Internacional", 0),
    "HPG": ("Mayorista Online", 1),
}


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer() -> pd.DataFrame:
    """
    Lee codiga_age y nombre_age desde el parquet limpio.
    """
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )
    df = pd.read_parquet(PARQUET, columns=["codiga_age", "nombre_age"])
    print(f"  Registros leídos del parquet : {len(df):,}")
    return df


# ── TRANSFORMAR ───────────────────────────────────────────────────────────────
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica canales, rellena nombres nulos y agrega tipo_canal / es_online.

    Pasos
    -----
    1. Deduplicar por codiga_age (un registro por canal único).
    2. Rellenar nombre_canal nulo con "Canal <codigo>".
    3. Asignar tipo_canal y es_online desde el diccionario TIPO_CANAL.
    4. Generar id_canal secuencial.
    """
    # Deduplicar, excluyendo nulos de codiga_age
    canales = (
        df[["codiga_age", "nombre_age"]]
        .dropna(subset=["codiga_age"])
        .drop_duplicates(subset=["codiga_age"])
        .sort_values("codiga_age")
        .reset_index(drop=True)
    )
    canales.columns = ["codigo_canal", "nombre_canal"]

    # Rellenar nombre_canal nulo con fallback legible
    nulos_nombre = canales["nombre_canal"].isna().sum()
    if nulos_nombre > 0:
        canales["nombre_canal"] = canales.apply(
            lambda r: (
                f"Canal {r['codigo_canal']}"
                if pd.isna(r["nombre_canal"])
                else r["nombre_canal"]
            ),
            axis=1,
        )
        print(f"  INFO: {nulos_nombre} canal(es) sin nombre → rellenados con codigo.")

    # Clasificación de negocio (no existe en el parquet, se deriva aquí)
    canales["tipo_canal"] = canales["codigo_canal"].map(
        lambda x: TIPO_CANAL.get(x, ("Otro", 0))[0]
    )
    canales["es_online"] = canales["codigo_canal"].map(
        lambda x: TIPO_CANAL.get(x, ("Otro", 0))[1]
    )

    print(f"  Canales únicos cargados      : {len(canales):,}")
    print(f"\n  Distribución por tipo_canal:")
    print(canales["tipo_canal"].value_counts().to_string())
    return canales


# ── CARGAR ────────────────────────────────────────────────────────────────────
def cargar(df: pd.DataFrame, engine) -> None:
    """
    Carga Dim_Canal en MySQL.
    Estrategia idempotente: DELETE + INSERT.
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
    print(f"  {TABLE} cargada : {len(df)} filas")


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
