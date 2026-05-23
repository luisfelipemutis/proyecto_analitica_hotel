"""
etl_dim_segmento.py
ETL — Dim_Segmento
Extrae los codigos de segmento desde el parquet limpio y los mapea
contra el diccionario de datos de negocio.

Si un codigo no existe en el diccionario, se crea automaticamente
con nombre y descripcion genericos.
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Segmento"

# Diccionario de negocio: codigo -> (nombre, descripcion, tipo_cliente)
DICCIONARIO_SEGMENTOS = {
    "ME": (
        "Mercado Empresarial",
        "Reservas walk-in o directas sin convenio previo. Clientes que llegan sin contrato.",
        "Transiente",
    ),
    "EM": (
        "Empresas Medianas",
        "Reservas para empleados del hotel, convenios laborales internos o beneficios.",
        "Interno",
    ),
    "COR": (
        "Corporativo",
        "Empresas con contrato corporativo regular negociado. Tarifa fija acordada.",
        "Corporativo",
    ),
    "T&T": (
        "Tour & Travel",
        "Agencias de viaje, operadores turisticos y mayoristas de turismo.",
        "Agencias",
    ),
    "CE": (
        "Corporativo Especial",
        "Empresas con tarifas preferenciales especiales por volumen o acuerdo estrategico.",
        "Corporativo",
    ),
    "PAR": (
        "Particular",
        "Reservas personales o familiares no clasificadas en otro segmento.",
        "Transiente",
    ),
    "GRU": (
        "Grupos",
        "Reservas de grupos (eventos, congresos, viajes corporativos grupales).",
        "Grupos",
    ),
    "ATN": (
        "Atencion / Cortesia",
        "Reservas de cortesia, invitados del hotel o atenciones institucionales.",
        "Interno",
    ),
    "BI": (
        "Banca / Inversion",
        "Segmento asociado a entidades financieras o de inversion con convenio.",
        "Corporativo",
    ),
    "AER": (
        "Aerolineas",
        "Reservas para tripulaciones de aerolineas (crew) con convenio.",
        "Aerolineas",
    ),
    "TG": (
        "Turismo Grupal",
        "Turismo de grupos organizados, posiblemente paquetes de operadores.",
        "Agencias",
    ),
    "BG": (
        "Banca / Gobierno",
        "Segmento menor, posiblemente entidades bancarias o gubernamentales.",
        "Corporativo",
    ),
}


def extraer() -> pd.DataFrame:
    """
    Lee el campo codsegmento desde el parquet limpio.
    """
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )

    df = pd.read_parquet(PARQUET, columns=["codsegmento"])
    print(f"  Registros leidos del parquet : {len(df):,}")
    print(f"  Segmentos unicos crudos      : {df['codsegmento'].nunique(dropna=True)}")
    return df


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia codigos de segmento, mapea diccionario de negocio y construye
    filas para codigos no catalogados.
    """
    serie = df["codsegmento"]
    nulos = serie.isna().sum()
    codigos = serie.dropna().astype(str).str.strip().str.upper()
    vacios = (codigos == "").sum()
    codigos = codigos[codigos != ""]

    codigos_unicos = sorted(codigos.unique())
    filas = []
    codigos_no_catalogados = []

    for codigo in codigos_unicos:
        if codigo in DICCIONARIO_SEGMENTOS:
            nombre, descripcion, tipo = DICCIONARIO_SEGMENTOS[codigo]
        else:
            nombre = f"Segmento {codigo}"
            descripcion = f"Segmento {codigo} no catalogado en diccionario de datos."
            tipo = "No Clasificado"
            codigos_no_catalogados.append(codigo)

        filas.append((codigo, nombre, descripcion, tipo))

    segmentos = pd.DataFrame(
        filas,
        columns=["codigo_segmento", "nombre_segmento", "descripcion", "tipo_cliente"],
    )
    segmentos.insert(0, "id_segmento", range(1, len(segmentos) + 1))

    print(f"  Codigos nulos descartados    : {nulos:,}")
    print(f"  Codigos vacios descartados   : {vacios:,}")
    print(f"  Segmentos mapeados           : {len(segmentos):,}")
    if codigos_no_catalogados:
        print(f"  WARN: codigos sin diccionario: {codigos_no_catalogados}")

    return segmentos


def cargar(df: pd.DataFrame, engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(TABLE, con=engine, if_exists="append", index=False, method="multi")
    print(f"✅ {TABLE} cargada: {len(df)} filas")


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
