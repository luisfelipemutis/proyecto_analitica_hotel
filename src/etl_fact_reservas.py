"""
etl_fact_reservas.py
ETL — Fact_Reservas
Lee el dataset limpio, asigna FKs desde las dimensiones ya cargadas en MySQL
y carga la tabla de hechos (70,882 registros).

Jerarquía financiera:
  valorplan + ivaplan + servicioplan  =  totalconsumosplan
  totalconsumosplan + totalconsumosadicional  =  ingreso_total
"""

# idn_aco (rol), clasi_aco (adulto/niño), incognito (S/N)

import hashlib
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
RAW = (
    Path(__file__).parent.parent
    / "data"
    / "raw"
    / "DataSet_ReservaYHuespedes_V.Full.xlsx"
)
TABLE = "Fact_Reservas"

MEDIDAS = [
    "tarifa",
    "valorplan",
    "ivaplan",
    "servicioplan",
    "valorconsumoadicional",
    "totalconsumosadicional",
    "totalconsumosplan",
    "ingreso_total",
    "duracion_estancia",
    "lead_time",
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


def _bk_contexto_series(df: pd.DataFrame) -> pd.Series:
    rol = df.get("idn_aco", pd.Series([pd.NA] * len(df), index=df.index)).map(_map_rol)

    clas_map = {
        "A": "Adulto",
        "N": "Nino",
    }
    clasificacion = (
        df.get("clasi_aco", pd.Series([pd.NA] * len(df), index=df.index))
        .map(lambda x: _to_norm(x, "NO REGISTRA"))
        .map(clas_map)
        .fillna("No registra")
    )

    privacidad = df.get("incognito", pd.Series([pd.NA] * len(df), index=df.index)).map(
        _map_privacidad
    )

    sexo_map = {
        "M": "Masculino",
        "MASCULINO": "Masculino",
        "F": "Femenino",
        "FEMENINO": "Femenino",
        "NO ESPECIFICADO": "No especificado",
    }
    sexo = (
        df.get("sexo_aco", pd.Series([pd.NA] * len(df), index=df.index))
        .astype("string")
        .str.strip()
        .str.upper()
        .map(sexo_map)
        .fillna("No especificado")
    )

    edad = (
        pd.to_numeric(
            df.get("edad_aco_limpia", pd.Series([pd.NA] * len(df), index=df.index)),
            errors="coerce",
        )
        .round(0)
        .astype("Int64")
        .astype("string")
        .replace({"<NA>": "NO REGISTRA"})
        .fillna("NO REGISTRA")
    )

    rango = (
        df.get("rango_edad", pd.Series([pd.NA] * len(df), index=df.index))
        .astype("string")
        .str.strip()
        .replace({"": "No registra", "nan": "No registra", "<NA>": "No registra"})
        .fillna("No registra")
    )

    nacionalidad = df.get("nacionalidad", pd.Series([pd.NA] * len(df), index=df.index)).map(
        _map_nacionalidad
    )

    keys = (
        rol.astype(str)
        + "|"
        + clasificacion.astype(str)
        + "|"
        + privacidad.astype(str)
        + "|"
        + sexo.astype(str)
        + "|"
        + edad.astype(str)
        + "|"
        + rango.astype(str)
        + "|"
        + nacionalidad.astype(str)
    )
    return keys.map(
        lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest()[:20].upper()
    )


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
        maps["seg"] = {
            r[0]: r[1]
            for r in conn.execute(
                text("SELECT codigo_segmento, id_segmento FROM Dim_Segmento")
            )
        }
        maps["canal"] = {
            r[0]: r[1]
            for r in conn.execute(text("SELECT codigo_canal, id_canal FROM Dim_Canal"))
        }
        maps["hab"] = {
            r[0]: r[1]
            for r in conn.execute(
                text("SELECT tipo_hab, id_habitacion FROM Dim_Habitacion")
            )
        }
        maps["temp"] = {
            r[0]: r[1]
            for r in conn.execute(
                text("SELECT codigo_temporada, id_temporada FROM Dim_Temporada")
            )
        }
        maps["huesped"] = {
            str(r[0]).strip().upper(): r[1]
            for r in conn.execute(
                text("SELECT id_huesped, id_registro_huesped FROM Dim_Huesped")
            )
            if r[0] is not None
        }
        maps["ctx_huesped"] = {
            str(r[0]).strip().upper(): r[1]
            for r in conn.execute(
                text(
                    "SELECT bk_contexto_huesped, id_contexto_huesped FROM Dim_Contexto_Huesped"
                )
            )
            if r[0] is not None
        }
    return maps


def transformar(df: pd.DataFrame, maps: dict) -> pd.DataFrame:
    # Fechas
    for col in ["fllega_aco", "fsalid_aco", "fcheckout", "fechasischin"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # Llaves dimensionales — se deja NaN para codigos sin match en dimension
    # Los registros con NaN en FKs criticas seran descartados antes del INSERT
    if "fllega_aco" in df.columns:
        df["id_fecha"] = pd.to_numeric(
            df["fllega_aco"].dt.strftime("%Y%m%d"), errors="coerce"
        )  # NaT → NaN (sin fechas validas no hay clave de fecha)

    df["id_segmento"] = df.get("codsegmento", pd.Series(dtype=object)).map(maps["seg"])
    df["id_canal"] = df.get("codiga_age", pd.Series(dtype=object)).map(maps["canal"])
    df["id_habitacion"] = df.get("tiphab_tip", pd.Series(dtype=object)).map(maps["hab"])
    df["id_temporada"] = df.get("codigotemporada", pd.Series(dtype=object)).map(
        maps["temp"]
    )

    # id_huesped (identidad estable): hash del parquet -> surrogate key de Dim_Huesped.
    if "id_huesped" in df.columns:
        ids_huesped_hash = df["id_huesped"].astype("string").str.strip().str.upper()
    else:
        ids_huesped_hash = pd.Series([pd.NA] * len(df), index=df.index, dtype="string")

    map_huesped = maps.get("huesped", {})
    df["id_huesped"] = ids_huesped_hash.map(map_huesped).fillna(1)

    sin_match_huesped = int((df["id_huesped"] == 1).sum())
    if sin_match_huesped > 0:
        print(
            f"  ⚠️  {sin_match_huesped:,} reservas sin match en Dim_Huesped "
            "(se asigna id_huesped=1 [ANONIMO])"
        )

    # id_contexto_huesped: hash de contexto -> surrogate key de Dim_Contexto_Huesped.
    bks_contexto = _bk_contexto_series(df)
    map_ctx = maps.get("ctx_huesped", {})
    df["id_contexto_huesped"] = bks_contexto.map(map_ctx)

    sin_match_ctx = int(df["id_contexto_huesped"].isna().sum())
    if sin_match_ctx > 0:
        print(
            f"  ⚠️  {sin_match_ctx:,} reservas sin match en Dim_Contexto_Huesped "
            "(se excluiran por integridad referencial)"
        )

    # ingreso_total con fórmula correcta
    if "totalconsumosplan" in df.columns:
        df["ingreso_total"] = (
            (
                pd.to_numeric(df["totalconsumosplan"], errors="coerce").fillna(0)
                + pd.to_numeric(
                    df.get("totalconsumosadicional", 0), errors="coerce"
                ).fillna(0)
            )
            .clip(lower=0)
            .round(2)
        )
    else:
        val = pd.to_numeric(df.get("valorplan", 0), errors="coerce").fillna(0)
        iva = pd.to_numeric(df.get("ivaplan", 0), errors="coerce").fillna(0)
        srv = pd.to_numeric(df.get("servicioplan", 0), errors="coerce").fillna(0)
        adi = pd.to_numeric(
            df.get("totalconsumosadicional", 0), errors="coerce"
        ).fillna(0)
        df["ingreso_total"] = (val + iva + srv + adi).clip(lower=0).round(2)

    # Variables temporales derivadas
    if "fllega_aco" in df.columns and "fsalid_aco" in df.columns:
        df["duracion_estancia"] = (df["fsalid_aco"] - df["fllega_aco"]).dt.days
        df.loc[df["duracion_estancia"] < 0, "duracion_estancia"] = np.nan
        df.loc[df["duracion_estancia"] > 60, "duracion_estancia"] = np.nan

    if "fechasischin" in df.columns and "fllega_aco" in df.columns:
        df["lead_time"] = (df["fllega_aco"] - df["fechasischin"]).dt.days
        df.loc[df["lead_time"] < 0, "lead_time"] = 0
        df.loc[df["lead_time"] > 365, "lead_time"] = np.nan

    # Métricas monetarias: redondear
    for col in [
        "tarifa",
        "valorplan",
        "ivaplan",
        "servicioplan",
        "valorconsumoadicional",
        "totalconsumosadicional",
        "totalconsumosplan",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Selección de columnas finales
    fk_cols = [
        "id_fecha",
        "id_segmento",
        "id_canal",
        "id_habitacion",
        "id_temporada",
        "id_huesped",
        "id_contexto_huesped",
    ]
    med_cols = [c for c in MEDIDAS if c in df.columns]
    fact = df[fk_cols + med_cols].copy()

    # --- Filtro de integridad referencial (Kimball: descartar orphan facts) ---
    # Columnas de FK numericas que deben tener un match valido en su dimension
    fks_criticas = [
        "id_fecha",
        "id_segmento",
        "id_canal",
        "id_habitacion",
        "id_temporada",
        "id_huesped",
        "id_contexto_huesped",
    ]
    antes = len(fact)
    mascara_invalida = fact[fks_criticas].isnull().any(axis=1)
    n_invalidos = mascara_invalida.sum()
    if n_invalidos > 0:
        print(
            f"  ⚠️  {n_invalidos:,} registros sin FK valida excluidos "
            f"({n_invalidos/antes:.1%} del total)"
        )
        # Diagnostico: que codigos de canal no tienen match en Dim_Canal
        sin_canal = fact.loc[fact["id_canal"].isnull(), "id_canal"]
        print(f"     Registros sin id_canal: {sin_canal.shape[0]:,}")
        fact = fact[~mascara_invalida].copy()

    # Cast a entero ahora que no hay NaN en las columnas FK
    for col in fks_criticas:
        fact[col] = fact[col].astype(int)

    fact.insert(0, "id_reserva", range(1, len(fact) + 1))
    print(f"  ✅ Registros validos para insertar: {len(fact):,} / {antes:,}")
    return fact


def cargar(df: pd.DataFrame, engine) -> None:
    print(f"  Cargando {len(df):,} registros en {TABLE}...")
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df.to_sql(
        TABLE,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(
        f"✅ {TABLE} cargada: {len(df):,} filas "
        f"| ingreso total: COP {df['ingreso_total'].sum():,.0f}"
    )


def run():
    print(f"[{TABLE}] Iniciando ETL...")
    engine = get_engine()
    df_raw = cargar_dataset()
    maps = cargar_maps(engine)
    df = transformar(df_raw, maps)
    cargar(df, engine)


if __name__ == "__main__":
    run()
