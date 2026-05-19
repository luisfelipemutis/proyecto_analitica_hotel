"""
etl_dim_huesped.py
ETL — Dim_Huesped
Perfil demográfico anonimizado del huésped (SHA-256 sobre ident_aco).
"""
import hashlib
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
RAW     = Path(__file__).parent.parent / "data" / "raw" / "DataSet_ReservaYHuespedes_V.Full.xlsx"
TABLE   = "Dim_Huesped"

COLS = ["ident_aco", "sexo_aco", "edad_aco", "nacionalidad", "oficio", "nombre_emp"]


def hash_id(valor) -> str:
    if pd.isna(valor):
        return "ANONIMO"
    return hashlib.sha256(str(valor).encode()).hexdigest()[:12].upper()


def extraer() -> pd.DataFrame:
    cols_disponibles = [c for c in COLS
                        if c in pd.read_parquet(PARQUET, columns=[]).columns] \
                       if PARQUET.exists() else COLS
    if PARQUET.exists():
        df = pd.read_parquet(PARQUET, columns=[c for c in COLS
                             if c in pd.read_parquet(PARQUET).columns])
    else:
        usar = COLS
        h1 = pd.read_excel(RAW, sheet_name="Hoja1", usecols=usar)
        h2 = pd.read_excel(RAW, sheet_name="Hoja2", usecols=usar)
        df = pd.concat([h1, h2], ignore_index=True)
    return df


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    # Anonimizar
    id_col = "ident_aco" if "ident_aco" in df.columns else df.columns[0]
    df["id_huesped"] = df[id_col].apply(hash_id)

    # Rango de edad
    if "edad_aco" in df.columns:
        df["edad_aco"] = pd.to_numeric(df["edad_aco"], errors="coerce")
        bins   = [0, 25, 35, 50, 65, 120]
        labels = ["18-25", "26-35", "36-50", "51-65", "65+"]
        df["rango_edad"] = pd.cut(df["edad_aco"], bins=bins, labels=labels, right=True).astype(str)
        df["rango_edad"] = df["rango_edad"].replace("nan", None)

    # Estandarizar sexo
    if "sexo_aco" in df.columns:
        df["sexo_aco"] = df["sexo_aco"].str.upper().map(
            {"M": "Masculino", "F": "Femenino",
             "MASCULINO": "Masculino", "FEMENINO": "Femenino"}
        ).fillna("No especificado")

    cols_out = ["id_huesped", "sexo_aco", "rango_edad", "nacionalidad",
                "oficio", "nombre_emp"]
    cols_out = [c for c in cols_out if c in df.columns or c == "id_huesped"]

    dim = (df[cols_out]
           .drop_duplicates(subset=["id_huesped"])
           .reset_index(drop=True))
    dim.insert(0, "id_registro_huesped", range(1, len(dim) + 1))
    return dim


def cargar(df: pd.DataFrame, engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(TABLE, con=engine, if_exists="append", index=False,
              method="multi", chunksize=500)
    print(f"✅ {TABLE} cargada: {len(df):,} filas")


def run():
    print(f"[{TABLE}] Iniciando ETL...")
    engine = get_engine()
    df_raw = extraer()
    df = transformar(df_raw)
    cargar(df, engine)


if __name__ == "__main__":
    run()
