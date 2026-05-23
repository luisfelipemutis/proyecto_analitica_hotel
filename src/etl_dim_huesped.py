"""
etl_dim_huesped.py
ETL — Dim_Huesped
Perfil demográfico anonimizado del huésped. La anonimización (SHA-256)
se realiza en el notebook 02_limpieza_datos.ipynb; aquí solo se lee
el campo ya hasheado y se deduplica por huésped único.

Origen de cada campo
---------------------
id_huesped      : campo id_huesped del parquet. Hash SHA-256 truncado (16 chars hex)
                  generado en notebook 02 sobre ident_aco. Ya no existe
                  ident_aco en el parquet (datos PII eliminados en ETL).
sexo_aco        : campo sexo_aco del parquet. Estandarizado en notebook 02
                  a "Masculino" | "Femenino" | "No especificado".
edad_aco_limpia : campo edad_aco_limpia del parquet. Edad numérica limpiada
                  e imputada (si edad_fue_imputada=True) en notebook 02.
rango_edad      : campo rango_edad del parquet. Calculado en notebook 02
                  a partir de edad_aco: "18-25"|"26-35"|"36-50"|"51-65"|"65+".
nacionalidad    : campo nacionalidad del parquet, enriquecido aquí con
                  el mapeo ISO 3166-1 alpha-3 → nombre completo del país
                  (ej. "COL" → "COLOMBIA"). Los códigos DIAN numéricos ya
                  fueron corregidos en el Paso 10 del notebook 02.

Campos eliminados del DDL anterior:
------------------------------------
oficio     : no está en VARS_FINAL; no se carga.
nombre_emp : trasladado a Dim_Empresa (dimensión propia relacionada a
             Fact_Reservas). Ya no forma parte del perfil del huésped.
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Huesped"

# Columnas que se leerán del parquet (alineadas con VARS_FINAL)
COLS = ["id_huesped", "sexo_aco", "edad_aco_limpia", "rango_edad", "nacionalidad"]

# Mapeo ISO 3166-1 alpha-3 → nombre completo del país en español.
# Cubre los principales orígenes de huéspedes del Hotel Dann Monasterio.
# Los códigos DIAN (numéricos: 063, 169, etc.) ya se convirtieron en notebook 02.
# Los valores que ya son nombres completos ("COLOMBIA", "ARGENTINA", etc.) pasan sin cambio.
ISO3_PAISES = {
    # América Latina y el Caribe
    "COL": "COLOMBIA",
    "VEN": "VENEZUELA",
    "ECU": "ECUADOR",
    "PER": "PERU",
    "BRA": "BRASIL",
    "ARG": "ARGENTINA",
    "CHL": "CHILE",
    "BOL": "BOLIVIA",
    "URY": "URUGUAY",
    "PRY": "PARAGUAY",
    "PAN": "PANAMA",
    "CRI": "COSTA RICA",
    "GTM": "GUATEMALA",
    "HND": "HONDURAS",
    "SLV": "EL SALVADOR",
    "NIC": "NICARAGUA",
    "CUB": "CUBA",
    "DOM": "REPUBLICA DOMINICANA",
    "MEX": "MEXICO",
    "JAM": "JAMAICA",
    "TTO": "TRINIDAD Y TOBAGO",
    # América del Norte
    "USA": "ESTADOS UNIDOS",
    "CAN": "CANADA",
    # Europa
    "ESP": "ESPAÑA",
    "FRA": "FRANCIA",
    "DEU": "ALEMANIA",
    "ITA": "ITALIA",
    "GBR": "REINO UNIDO",
    "PRT": "PORTUGAL",
    "NLD": "PAISES BAJOS",
    "BEL": "BELGICA",
    "CHE": "SUIZA",
    "AUT": "AUSTRIA",
    "SWE": "SUECIA",
    "NOR": "NORUEGA",
    "DNK": "DINAMARCA",
    "FIN": "FINLANDIA",
    "POL": "POLONIA",
    "RUS": "RUSIA",
    "TUR": "TURQUIA",
    "GRC": "GRECIA",
    "IRL": "IRLANDA",
    # Asia y Oceanía
    "CHN": "CHINA",
    "JPN": "JAPON",
    "KOR": "COREA DEL SUR",
    "IND": "INDIA",
    "ISR": "ISRAEL",
    "AUS": "AUSTRALIA",
    "NZL": "NUEVA ZELANDA",
    # África
    "ZAF": "SUDAFRICA",
    "EGY": "EGIPTO",
    "MAR": "MARRUECOS",
    "AGO": "ANGOLA",
    "UZB": "UZBEKISTAN",
}


def _mapear_nacionalidad(valor) -> str:
    """
    Convierte un código ISO 3166-1 alpha-3 al nombre completo del país.
    Si el valor ya es un nombre (no tiene exactamente 3 letras mayúsculas)
    se devuelve sin cambio. Nulos retornan None.
    """
    if pd.isna(valor):
        return None
    codigo = str(valor).strip().upper()
    # Si ya es un nombre de país (más de 3 chars o tiene espacios) → pasa directo
    if len(codigo) != 3 or " " in codigo:
        return codigo.title() if codigo.isupper() else valor
    return ISO3_PAISES.get(codigo, codigo)  # fallback: devuelve el código original


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer() -> pd.DataFrame:
    """
    Lee las columnas de perfil del huésped desde el parquet limpio.
    El parquet es la única fuente de verdad; si no existe se lanza error.

    Nota: id_huesped ya viene como hash SHA-256 (12 chars uppercase)
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
            "id_huesped / rango_edad / edad_aco_limpia. Vuelve a ejecutar\n"
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
    Deduplica el perfil del huésped y genera la dimensión final.

    Pasos
    -----
    1. Eliminar filas sin id_huesped (no pueden identificarse en la dimensión).
    2. Deduplicar por id_huesped: un huésped = una fila con su perfil más
       reciente (primera aparición en el parquet, ya ordenado por fecha).
    3. Limpiar "nan" string en rango_edad (artefacto de pd.cut en notebook 02).
    4. Mapear nacionalidad: código ISO 3166-1 alpha-3 → nombre completo del país.
    5. Generar id_registro_huesped secuencial (surrogate key de la tabla).

    Campos ya limpios desde parquet (no se reprocesa aquí):
    -------------------------------------------------------
    - sexo_aco       : estandarizado en notebook 02.
    - edad_aco_limpia: imputada en notebook 02.
    - rango_edad     : calculado en notebook 02 a partir de edad_aco.
    """
    # 1. Eliminar filas sin id_huesped
    df_validos = df.dropna(subset=["id_huesped"]).copy()
    excluidos = len(df) - len(df_validos)
    if excluidos > 0:
        print(f"  Registros excluidos sin id_huesped: {excluidos:,}")

    # 2. Deduplicar por id_huesped (primer registro encontrado por huésped)
    dim = df_validos.drop_duplicates(subset=["id_huesped"], keep="first").reset_index(
        drop=True
    )

    # 3. Limpiar "nan" string en rango_edad
    if "rango_edad" in dim.columns:
        dim["rango_edad"] = dim["rango_edad"].replace("nan", None)

    # 4. Estandarizar sexo_aco: M/F → Masculino/Femenino
    # El parquet puede traer los códigos crudos si notebook 02 no los convirtió.
    SEXO_MAP = {
        "M": "Masculino", "MASCULINO": "Masculino",
        "F": "Femenino",  "FEMENINO":  "Femenino",
    }
    dim["sexo_aco"] = (
        dim["sexo_aco"]
        .astype(str)
        .str.strip()
        .str.upper()
        .map(SEXO_MAP)
        .fillna("No especificado")
    )

    # 5. Mapeo ISO 3166-1 alpha-3 → nombre completo del país
    dim["nacionalidad"] = dim["nacionalidad"].apply(_mapear_nacionalidad)

    # 6. Convertir edad_aco_limpia: float64 → Int64 (entero nullable de Pandas).
    # Pandas lee columnas con NaN como float64; Int64 (mayúscula) soporta <NA>
    # y SQLAlchemy lo mapea a INT en MySQL, eliminando los decimales ".0".
    if "edad_aco_limpia" in dim.columns:
        dim["edad_aco_limpia"] = (
            dim["edad_aco_limpia"]
            .round(0)              # por si hay artefactos de punto flotante (ej. 34.9999)
            .astype("Int64")       # Int64 nullable: NaN → <NA> (no lanza error)
        )

    # 7. Generar surrogate key secuencial
    dim.insert(0, "id_registro_huesped", range(1, len(dim) + 1))

    print(f"  Huéspedes únicos en la dimensión : {len(dim):,}")
    print(f"\n  Distribución por sexo_aco:")
    print(dim["sexo_aco"].value_counts(dropna=False).to_string())
    print(f"\n  Distribución por rango_edad:")
    print(dim["rango_edad"].value_counts(dropna=False).sort_index().to_string())
    print(f"\n  Top 10 nacionalidades:")
    print(dim["nacionalidad"].value_counts(dropna=False).head(10).to_string())

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
