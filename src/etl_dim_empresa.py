"""
etl_dim_empresa.py
ETL — Dim_Empresa
Extrae las empresas únicas asociadas a reservas del parquet y las carga en MySQL.

Origen de cada campo
---------------------
nombre_empresa : campo nombre_emp del parquet. Nombre de la empresa asociada a la reserva.

Registro por defecto (Sin Empresa)
------------------------------------
Las reservas del parquet donde nombre_emp es nulo o vacío no tienen
empresa asociada. Para no perderlos en Fact_Reservas se inserta un registro
'Sin Empresa' al final de la dimensión, sin usar un id predefinido.
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Empresa"

# Registro por defecto para reservas sin empresa registrada
EMPRESA_ND = {
    "nombre_empresa": "Sin Empresa",
}


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer() -> pd.DataFrame:
    """
    Lee nombre_emp desde el parquet limpio.
    El parquet es la única fuente de verdad; si no existe se lanza error.
    """
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )

    df = pd.read_parquet(PARQUET, columns=["nombre_emp"])

    print(f"  Registros leídos del parquet : {len(df):,}")
    sin_empresa = df["nombre_emp"].isna().sum()
    print(
        f"  Registros sin nombre_emp     : {sin_empresa:,}  → se mapearán a 'Sin Empresa'"
    )
    return df


# ── TRANSFORMAR ───────────────────────────────────────────────────────────────
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica empresas, limpia espacios y agrega el registro por defecto.

    Pasos
    -----
    1. Eliminar filas con nombre_emp nulo o vacío.
    2. Limpiar espacios y unificar mayúsculas/minúsculas (strip + title case).
    3. Ignorar nombres con menos de 5 caracteres (no válidos) y reportarlos en logs.
    4. Deduplicar por nombre_empresa limpio.
    5. Ordenar alfabéticamente para consistencia entre ejecuciones.
    6. Agregar registro por 'Sin Empresa' al final.
    7. Asignar id_empresa consecutivo para garantizar que quede con el último id.
    """
    # 1. Eliminar filas con nombre_emp nulo o vacío
    serie = df["nombre_emp"]
    nulos = serie.isna().sum()
    empresas = serie.dropna().astype(str).str.strip()
    vacios = (empresas == "").sum()
    empresas = empresas[empresas != ""]

    # 2. Limpiar: strip + title case (ej. "GILMEDICA S.A." → "Gilmedica S.A.")
    empresas = empresas.str.replace(r"\s+", " ", regex=True).str.title()

    # Excluir 'Sin Empresa'/'Sim Empresa' si vienen desde el origen para insertarlo solo al final
    sin_empresa_origen = (
        empresas.str.casefold().isin(
            [EMPRESA_ND["nombre_empresa"].casefold(), "sim empresa"]
        )
    ).sum()
    empresas = empresas[
        ~empresas.str.casefold().isin(
            [EMPRESA_ND["nombre_empresa"].casefold(), "sim empresa"]
        )
    ]

    # 3. Ignorar empresas con menos de 5 caracteres
    mascara_cortas = empresas.str.len() < 5
    cortas_descartadas = mascara_cortas.sum()
    muestras_cortas = empresas[mascara_cortas].drop_duplicates().head(10).tolist()
    empresas = empresas[~mascara_cortas]

    # 4-5. Deduplicar y ordenar
    empresas = (
        pd.DataFrame({"nombre_empresa": empresas})
        .drop_duplicates(subset=["nombre_empresa"])
        .sort_values("nombre_empresa")
        .reset_index(drop=True)
    )

    # 6. Agregar 'Sin Empresa' al final
    fila_nd = pd.DataFrame([EMPRESA_ND])
    empresas = pd.concat([empresas, fila_nd], ignore_index=True)

    # 7. Asignar id consecutivo garantizando que 'Sin Empresa' sea el último id
    empresas.insert(0, "id_empresa", range(1, len(empresas) + 1))

    print(f"  Registros nulos descartados  : {nulos:,}")
    print(f"  Registros vacíos descartados : {vacios:,}")
    print(f"  Registros < 5 chars descart. : {cortas_descartadas:,}")
    if muestras_cortas:
        print(f"  Ejemplos descartados (<5)    : {muestras_cortas}")
    print(f"  'Sin Empresa' en origen      : {sin_empresa_origen:,}")

    print(f"  Total filas a cargar         : {len(empresas):,}")
    print(f"  id 'Sin Empresa' asignado    : {int(empresas.iloc[-1]['id_empresa'])}")

    return empresas


# ── CARGAR ────────────────────────────────────────────────────────────────────
def cargar(df: pd.DataFrame, engine) -> None:
    """
    Carga Dim_Empresa en MySQL.
    Estrategia idempotente: DELETE + RESET AUTO_INCREMENT + INSERT.

    INSERT estático del DDL, por lo que re-ejecutar este script es seguro.
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
