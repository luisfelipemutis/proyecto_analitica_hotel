"""
etl_dim_canal.py
ETL — Dim_Canal
Extrae los canales/agencias únicos del dataset limpio y los carga en MySQL.
"""
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
RAW     = Path(__file__).parent.parent / "data" / "raw" / "DataSet_ReservaYHuespedes_V.Full.xlsx"
TABLE   = "Dim_Canal"

TIPO_CANAL = {
    "RECE": ("Directo Presencial", 0),
    "HDAN": ("Directo Digital",    1),
    "BKNG": ("OTA",                1),
    "BKNE": ("OTA",                1),
    "EXPD": ("OTA",                1),
    "WEBB": ("Mayorista Online",   1),
    "TBOH": ("Mayorista Online",   1),
    "NTEE": ("Mayorista Online",   1),
    "Z000": ("Mayorista Online",   1),
    "TCK":  ("GDS / Tecnologia",   1),
    "AMA":  ("GDS / Tecnologia",   1),
    "GDS":  ("GDS / Tecnologia",   1),
    "AVIA": ("Agencia Nacional",   0),
    "ACL":  ("Agencia Nacional",   0),
    "AERO": ("Agencia Nacional",   0),
    "PULL": ("Agencia Nacional",   0),
    "BCD":  ("Agencia Corporativa",0),
    "Z008": ("Agencia Corporativa",0),
    "Z003": ("Agencia Corporativa",0),
    "CARL": ("Agencia Corporativa",0),
    "Z002": ("Agencia Internacional", 1),
    "Z005": ("Agencia Internacional", 0),
    "Z020": ("Agencia Internacional", 0),
    "Z011": ("Agencia Internacional", 0),
    "HPG":  ("Mayorista Online",   1),
}


def extraer() -> pd.DataFrame:
    cols = ["codiga_age", "nombre_age"]
    if PARQUET.exists():
        df = pd.read_parquet(PARQUET, columns=cols)
    else:
        h1 = pd.read_excel(RAW, sheet_name="Hoja1", usecols=cols)
        h2 = pd.read_excel(RAW, sheet_name="Hoja2", usecols=cols)
        df = pd.concat([h1, h2], ignore_index=True)
    return df


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    canales = (df[["codiga_age", "nombre_age"]]
               .dropna(subset=["codiga_age"])
               .drop_duplicates(subset=["codiga_age"])
               .sort_values("codiga_age")
               .reset_index(drop=True))
    canales.insert(0, "id_canal", range(1, len(canales) + 1))
    canales.columns = ["id_canal", "codigo_canal", "nombre_canal"]
    # Algunos canales del dataset no tienen nombre registrado (nombre_age nulo).
    # La columna nombre_canal es NOT NULL en MySQL → rellenar con el código
    # del canal como fallback legible en lugar de dejar NULL.
    nulos = canales["nombre_canal"].isna().sum()
    if nulos > 0:
        canales["nombre_canal"] = canales.apply(
            lambda r: f"Canal {r['codigo_canal']}" if pd.isna(r["nombre_canal"]) else r["nombre_canal"],
            axis=1
        )
        print(f"  INFO: {nulos} canal(es) sin nombre rellenados con codigo.")
    canales["tipo_canal"] = canales["codigo_canal"].map(
        lambda x: TIPO_CANAL.get(x, ("Otro", 0))[0])
    canales["es_online"] = canales["codigo_canal"].map(
        lambda x: TIPO_CANAL.get(x, ("Otro", 0))[1])
    return canales


def cargar(df: pd.DataFrame, engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(TABLE, con=engine, if_exists="append", index=False,
              method="multi")
    print(f"✅ {TABLE} cargada: {len(df)} filas")


def run():
    print(f"[{TABLE}] Iniciando ETL...")
    engine = get_engine()
    df_raw = extraer()
    df = transformar(df_raw)
    cargar(df, engine)


if __name__ == "__main__":
    run()
