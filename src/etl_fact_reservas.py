"""
etl_fact_reservas.py
ETL — Fact_Reservas
Lee el dataset limpio, asigna FKs desde las dimensiones ya cargadas en MySQL
y carga la tabla de hechos (70,882 registros).

Jerarquía financiera:
  valorplan + ivaplan + servicioplan  =  totalconsumosplan
  totalconsumosplan + totalconsumosadicional  =  ingreso_total
"""
import hashlib
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
RAW     = Path(__file__).parent.parent / "data" / "raw" / "DataSet_ReservaYHuespedes_V.Full.xlsx"
TABLE   = "Fact_Reservas"

MEDIDAS = ["numvoucher", "tarifa", "valorplan", "ivaplan", "servicioplan",
           "valorconsumoadicional", "totalconsumosadicional",
           "totalconsumosplan", "ingreso_total",
           "duracion_estancia", "lead_time"]


def hash_id(valor) -> str:
    if pd.isna(valor):
        return "ANONIMO"
    return hashlib.sha256(str(valor).encode()).hexdigest()[:12].upper()


def cargar_dataset() -> pd.DataFrame:
    if PARQUET.exists():
        print("  Cargando desde parquet...")
        df = pd.read_parquet(PARQUET)
    else:
        print("  Parquet no encontrado. Cargando desde Excel (puede tardar)...")
        h1 = pd.read_excel(RAW, sheet_name="Hoja1")
        h2 = pd.read_excel(RAW, sheet_name="Hoja2")
        df = pd.concat([h1, h2], ignore_index=True)
    print(f"  Dataset: {df.shape}")
    return df


def cargar_maps(engine) -> dict:
    """Carga los mapas codigo → id desde las dimensiones en MySQL."""
    maps = {}
    with engine.connect() as conn:
        maps["seg"]  = {r[0]: r[1] for r in conn.execute(
            text("SELECT codigo_segmento, id_segmento FROM Dim_Segmento"))}
        maps["canal"]= {r[0]: r[1] for r in conn.execute(
            text("SELECT codigo_canal, id_canal FROM Dim_Canal"))}
        maps["hab"]  = {r[0]: r[1] for r in conn.execute(
            text("SELECT tipo_hab, id_habitacion FROM Dim_Habitacion"))}
        maps["temp"] = {r[0]: r[1] for r in conn.execute(
            text("SELECT codigo_temporada, id_temporada FROM Dim_Temporada"))}
    return maps


def transformar(df: pd.DataFrame, maps: dict) -> pd.DataFrame:
    # Fechas
    for col in ["fllega_aco", "fsalid_aco", "fcheckout", "fechasischin"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # Llaves dimensionales
    if "fllega_aco" in df.columns:
        df["id_fecha"] = pd.to_numeric(
            df["fllega_aco"].dt.strftime("%Y%m%d"), errors="coerce"
        ).fillna(0).astype(int)

    df["id_segmento"]   = df.get("codsegmento", pd.Series()).map(maps["seg"]).fillna(0).astype(int)
    df["id_canal"]      = df.get("codiga_age",  pd.Series()).map(maps["canal"]).fillna(0).astype(int)
    df["id_habitacion"] = df.get("tiphab_tip",  pd.Series()).map(maps["hab"]).fillna(0).astype(int)
    df["id_temporada"]  = df.get("codigotemporada", pd.Series()).map(maps["temp"]).fillna(99).astype(int)

    # id_huesped anonimizado
    id_col = "ident_aco" if "ident_aco" in df.columns else None
    df["id_huesped"] = df[id_col].apply(hash_id) if id_col else "ANONIMO"

    # ingreso_total con fórmula correcta
    if "totalconsumosplan" in df.columns:
        df["ingreso_total"] = (
            pd.to_numeric(df["totalconsumosplan"], errors="coerce").fillna(0) +
            pd.to_numeric(df.get("totalconsumosadicional", 0), errors="coerce").fillna(0)
        ).clip(lower=0).round(2)
    else:
        val = pd.to_numeric(df.get("valorplan", 0),    errors="coerce").fillna(0)
        iva = pd.to_numeric(df.get("ivaplan", 0),      errors="coerce").fillna(0)
        srv = pd.to_numeric(df.get("servicioplan", 0), errors="coerce").fillna(0)
        adi = pd.to_numeric(df.get("totalconsumosadicional", 0), errors="coerce").fillna(0)
        df["ingreso_total"] = (val + iva + srv + adi).clip(lower=0).round(2)

    # Variables temporales derivadas
    if "fllega_aco" in df.columns and "fsalid_aco" in df.columns:
        df["duracion_estancia"] = (df["fsalid_aco"] - df["fllega_aco"]).dt.days
        df.loc[df["duracion_estancia"] < 0, "duracion_estancia"] = np.nan
        df.loc[df["duracion_estancia"] > 60, "duracion_estancia"] = np.nan

    if "fechasischin" in df.columns and "fllega_aco" in df.columns:
        df["lead_time"] = (df["fllega_aco"] - df["fechasischin"]).dt.days
        df.loc[df["lead_time"] < 0,   "lead_time"] = 0
        df.loc[df["lead_time"] > 365, "lead_time"] = np.nan

    # Métricas monetarias: redondear
    for col in ["tarifa", "valorplan", "ivaplan", "servicioplan",
                "valorconsumoadicional", "totalconsumosadicional", "totalconsumosplan"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Selección de columnas finales
    fk_cols = ["id_fecha", "id_segmento", "id_canal", "id_habitacion",
                "id_temporada", "id_huesped"]
    med_cols = [c for c in MEDIDAS if c in df.columns]
    fact = df[fk_cols + med_cols].copy()
    fact.insert(0, "id_reserva", range(1, len(fact) + 1))
    return fact


def cargar(df: pd.DataFrame, engine) -> None:
    print(f"  Cargando {len(df):,} registros en {TABLE}...")
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(TABLE, con=engine, if_exists="append", index=False,
              method="multi", chunksize=500)
    print(f"✅ {TABLE} cargada: {len(df):,} filas "
          f"| ingreso total: COP {df['ingreso_total'].sum():,.0f}")


def run():
    print(f"[{TABLE}] Iniciando ETL...")
    engine = get_engine()
    df_raw = cargar_dataset()
    maps   = cargar_maps(engine)
    df     = transformar(df_raw, maps)
    cargar(df, engine)


if __name__ == "__main__":
    run()
