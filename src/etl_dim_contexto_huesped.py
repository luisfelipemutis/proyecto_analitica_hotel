"""
etl_dim_contexto_huesped.py
ETL — Dim_Contexto_Huesped
Mini dimension para contexto del huesped por reserva.
"""

import hashlib
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Contexto_Huesped"

COLS = [
    "idn_aco",
    "clasi_aco",
    "incognito",
    "sexo_aco",
    "edad_aco_limpia",
    "rango_edad",
    "nacionalidad",
]


def _to_norm(value, default="NO REGISTRA") -> str:
    if pd.isna(value):
        return default
    text_value = str(value).strip().upper()
    return text_value if text_value else default


def _map_privacidad(value) -> str:
    norm = _to_norm(value, "NO REGISTRA")
    norm = norm.replace("Í", "I")

    if norm in {"S", "SI", "Y", "YES", "TRUE", "1"}:
        return "Si"
    if norm in {"N", "NO", "FALSE", "0"}:
        return "No"
    return "No registra"


def _map_rol(value) -> str:
    norm = _to_norm(value, "NO REGISTRA")
    rol_map = {
        "A": "Acompañante",
        "D": "Dependiente",
        "T": "Titular",
    }
    return rol_map.get(norm, "No registra")


def _map_nacionalidad(value) -> str:
    norm = _to_norm(value, "NO REGISTRA")
    nacionalidad_map = {
        "ALE": "Alemania",
        "ANG": "Inglaterra",
        "ARG": "Argentina",
        "ARU": "Aruba",
        "AUS": "Australia",
        "AUT": "Austria",
        "BER": "Bermudas",
        "BLG": "Belgica",
        "BOL": "Bolivia",
        "BRA": "Brasil",
        "BUL": "Bulgaria",
        "CAN": "Canada",
        "CHE": "Suiza",
        "CHI": "Chile",
        "CHN": "China",
        "CHP": "Chipre",
        "CO": "Colombia",
        "COL": "Colombia",
        "COR": "Corea",
        "CR": "Costa Rica",
        "CUB": "Cuba",
        "DIN": "Dinamarca",
        "ECU": "Ecuador",
        "ESP": "Espana",
        "FIL": "Filipinas",
        "FIN": "Finlandia",
        "FRA": "Francia",
        "GRA": "Gran Bretana",
        "GRE": "Grecia",
        "GUA": "Guatemala",
        "HGA": "Hungria",
        "HOL": "Paises Bajos",
        "HON": "Honduras",
        "IDN": "Indonesia",
        "ILN": "Irlanda del Norte",
        "IND": "India",
        "ING": "Inglaterra",
        "IRL": "Irlanda",
        "ISR": "Israel",
        "ITA": "Italia",
        "JAP": "Japon",
        "KOR": "Corea del Sur",
        "KUW": "Kuwait",
        "LET": "Letonia",
        "LHI": "Liechtenstein",
        "LI": "Liechtenstein",
        "LUX": "Luxemburgo",
        "MEX": "Mexico",
        "MNC": "Monaco",
        "NET": "Paises Bajos",
        "NIC": "Nicaragua",
        "NOR": "Noruega",
        "NUE": "Nueva Zelanda",
        "PAN": "Panama",
        "PAR": "Paraguay",
        "PER": "Peru",
        "POL": "Polonia",
        "POR": "Portugal",
        "PTR": "Puerto Rico",
        "RCH": "Republica Checa",
        "REP": "Republica Dominicana",
        "REU": "Reunion",
        "RUM": "Rumania",
        "RUS": "Rusia",
        "SAL": "El Salvador",
        "SIN": "Singapur",
        "SUE": "Suecia",
        "SUI": "Suiza",
        "SUR": "Surinam",
        "SVK": "Eslovaquia",
        "TAI": "Taiwan",
        "THA": "Tailandia",
        "TRI": "Trinidad y Tobago",
        "TUQ": "Turquia",
        "UGA": "Uganda",
        "URU": "Uruguay",
        "USA": "Estados Unidos",
        "UZB": "Uzbekistan",
        "VEN": "Venezuela",
        "ZRD": "Republica Democratica del Congo",
    }
    if norm == "NO REGISTRA":
        return "No registra"
    return nacionalidad_map.get(norm, norm.title())


def _bk_contexto(row: pd.Series) -> str:
    key = "|".join(
        [
            row["rol"],
            row["clasificacion"],
            row["privacidad"],
            row["sexo"],
            row["edad"],
            row["rango_edad"],
            row["nacionalidad"],
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:20].upper()


def extraer() -> pd.DataFrame:
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )

    cols_parquet = set(pq.read_schema(str(PARQUET)).names)
    cols_presentes = [c for c in COLS if c in cols_parquet]
    if not cols_presentes:
        raise KeyError(
            "No se encontraron columnas de contexto de huesped en el parquet."
        )

    df = pd.read_parquet(PARQUET, columns=cols_presentes)
    for col in COLS:
        if col not in df.columns:
            df[col] = pd.NA

    print(f"  Registros leidos del parquet : {len(df):,}")
    return df[COLS]


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    dim = df.copy()

    dim["rol"] = dim["idn_aco"].map(_map_rol)

    clas_map = {
        "A": "Adulto",
        "N": "Nino",
    }
    dim["clasificacion"] = (
        dim["clasi_aco"]
        .map(lambda x: _to_norm(x, "NO REGISTRA"))
        .map(clas_map)
        .fillna("No registra")
    )

    dim["privacidad"] = dim["incognito"].map(_map_privacidad)

    sexo_map = {
        "M": "Masculino",
        "MASCULINO": "Masculino",
        "F": "Femenino",
        "FEMENINO": "Femenino",
        "NO ESPECIFICADO": "No especificado",
    }
    dim["sexo"] = (
        dim["sexo_aco"]
        .astype("string")
        .str.strip()
        .str.upper()
        .map(sexo_map)
        .fillna("No especificado")
    )

    edad = pd.to_numeric(dim["edad_aco_limpia"], errors="coerce").round(0)
    dim["edad"] = edad.astype("Int64")

    edad_key = (
        dim["edad"]
        .astype("string")
        .replace({"<NA>": "NO REGISTRA"})
        .fillna("NO REGISTRA")
    )

    dim["rango_edad"] = dim["rango_edad"].astype("string").str.strip()
    dim["rango_edad"] = (
        dim["rango_edad"]
        .replace({"": "No registra", "nan": "No registra", "<NA>": "No registra"})
        .fillna("No registra")
    )

    dim["nacionalidad"] = dim["nacionalidad"].map(_map_nacionalidad)

    dim["edad"] = pd.to_numeric(dim["edad"], errors="coerce").astype("Int64")

    dim_bk = pd.DataFrame(
        {
            "rol": dim["rol"],
            "clasificacion": dim["clasificacion"],
            "privacidad": dim["privacidad"],
            "sexo": dim["sexo"],
            "edad": edad_key,
            "rango_edad": dim["rango_edad"],
            "nacionalidad": dim["nacionalidad"],
        }
    )
    dim["bk_contexto_huesped"] = dim_bk.apply(_bk_contexto, axis=1)

    dim = (
        dim[
            [
                "bk_contexto_huesped",
                "rol",
                "clasificacion",
                "privacidad",
                "sexo",
                "edad",
                "rango_edad",
                "nacionalidad",
            ]
        ]
        .drop_duplicates(subset=["bk_contexto_huesped"])
        .reset_index(drop=True)
    )

    dim.insert(0, "id_contexto_huesped", range(1, len(dim) + 1))

    print(f"  Contextos unicos en la dimension : {len(dim):,}")
    return dim


def cargar(df: pd.DataFrame, engine) -> None:
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
