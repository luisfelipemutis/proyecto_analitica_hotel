"""
etl_dim_segmento.py
ETL — Dim_Segmento
Carga estática: los segmentos comerciales son catálogo fijo del hotel.
Se complementa con conteo real del dataset limpio.
"""
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

TABLE = "Dim_Segmento"

# Catálogo maestro de segmentos (fuente: Diccionario_Datos_Hotel.xlsx)
CATALOGO = [
    (1,  "ME",  "Mostrador / Externo",      "Reservas walk-in o directas sin convenio previo",               "Transiente"),
    (2,  "EM",  "Empleados",                "Reservas para empleados del hotel o convenios laborales",        "Interno"),
    (3,  "COR", "Corporativo",              "Empresas con contrato corporativo regular negociado",            "Corporativo"),
    (4,  "T&T", "Tour & Travel",            "Agencias de viaje, operadores y mayoristas de turismo",          "Agencias"),
    (5,  "CE",  "Corporativo Especial",     "Empresas con tarifas preferenciales por volumen o acuerdo",      "Corporativo"),
    (6,  "PAR", "Particular",               "Reservas personales o familiares sin clasificacion especial",     "Transiente"),
    (7,  "GRU", "Grupos",                   "Eventos, congresos y viajes corporativos grupales",              "Grupos"),
    (8,  "ATN", "Atencion / Cortesia",      "Reservas de cortesia, invitados institucionales del hotel",      "Interno"),
    (9,  "BI",  "Banca / Inversion",        "Entidades financieras con convenio negociado",                   "Corporativo"),
    (10, "AER", "Aerolineas",               "Tripulaciones de aerolinea (crew) con convenio",                 "Aerolineas"),
    (11, "TG",  "Turismo Grupal",           "Grupos organizados de turismo por operadores",                   "Agencias"),
    (12, "BG",  "Banca / Gobierno",         "Entidades bancarias o gubernamentales con convenio",             "Corporativo"),
]


def transformar() -> pd.DataFrame:
    return pd.DataFrame(CATALOGO,
        columns=["id_segmento", "codigo_segmento", "nombre_segmento",
                 "descripcion", "tipo_cliente"])


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
