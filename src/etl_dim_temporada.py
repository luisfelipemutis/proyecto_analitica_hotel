"""
etl_dim_temporada.py
ETL — Dim_Temporada
Catálogo fijo: Alta (A), Baja (B) + registro ND para el 53.27% de nulos documentados.
"""
import pandas as pd
from sqlalchemy import text
from db_connection import get_engine

TABLE = "Dim_Temporada"

CATALOGO = [
    (1,  "A",  "Alta",         "ALTA",  "Temporada alta demanda: dic-ene, julio, semana santa y festivos nacionales"),
    (2,  "B",  "Baja",         "BAJA",  "Temporada baja demanda: meses intermedios sin festivos"),
    (99, "ND", "No Disponible","NULL",  "Sin temporada registrada en el sistema fuente. Limitacion documentada: 53.27% de registros"),
]


def transformar() -> pd.DataFrame:
    return pd.DataFrame(CATALOGO,
        columns=["id_temporada", "codigo_temporada", "nombre_temporada",
                 "nombre_en_sistema", "descripcion"])


def cargar(df: pd.DataFrame, engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(TABLE, con=engine, if_exists="append", index=False,
              method="multi")
    print(f"✅ {TABLE} cargada: {len(df)} filas")


def run():
    print(f"[{TABLE}] Iniciando ETL...")
    engine = get_engine()
    df = transformar()
    cargar(df, engine)


if __name__ == "__main__":
    run()
