"""
etl_dim_habitacion.py
ETL — Dim_Habitacion
Extrae las combinaciones únicas tipo_hab × clase_hab del parquet y las
enriquece con metadatos de catálogo estático del hotel para cargar en MySQL.

Origen de cada campo
---------------------
tipo_hab        : campo tiphab_tip del parquet  (código de tipo de suite/hab).
clase_hab       : campo clahab_clh del parquet  (código de clase/ocupación).
num_hab         : campo nrohab_hab del parquet  (número de habitación).
nombre_tipo     : derivado del diccionario META_TIPO según tipo_hab.
nombre_clase    : derivado del diccionario META_CLASE según clase_hab.
descripcion_tipo: derivado del diccionario META_TIPO según tipo_hab.
capacidad_max   : derivado del diccionario META_TIPO según tipo_hab.
categoria       : derivado del diccionario META_TIPO según tipo_hab.

Los diccionarios META_TIPO y META_CLASE son catálogos internos del hotel;
no existen como columnas en el parquet. Se mantienen aquí como fuente de
enriquecimiento dimensional siguiendo la metodología Kimball (atributos
descriptivos en la dimensión, no en la tabla de hechos).
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
TABLE = "Dim_Habitacion"

# Catálogo de tipos de habitación del Hotel Dann Monasterio.
# Estructura: codigo → (nombre_tipo, descripcion_tipo, capacidad_max, categoria)
META_TIPO = {
    "ST": (
        "Suite Estandar",
        "Suite estandar con sala de estar separada, zona de descanso premium y amenidades "
        "de alta categoria. Configuracion clasica del portafolio Dann Monasterio.",
        2,
        "Suite",
    ),
    "SE": (
        "Suite Ejecutiva",
        "Suite ejecutiva con area de trabajo dedicada, escritorio ejecutivo, acceso a "
        "business center y vistas preferenciales al paisaje urbano o jardines del claustro.",
        2,
        "Suite",
    ),
    "S3": (
        "Suite Junior",
        "Suite Junior con sala integrada al area de descanso, ideal para estancias cortas "
        "de viajeros corporativos. Distribucion compacta con todos los servicios de suite.",
        2,
        "Suite",
    ),
    "S4": (
        "Suite Tipo 4",
        "Suite de categoria superior con caracteristicas diferenciales de confort, espacio "
        "ampliado y acabados de lujo. Posicion intermedia en el portafolio premium del hotel.",
        2,
        "Suite",
    ),
    "SP": (
        "Suite Presidencial",
        "Suite Presidencial de maxima categoria con sala de reuniones privada, comedor "
        "ejecutivo, jacuzzi y vistas panoramicas al centro historico de Popayan o jardines "
        "del convento.",
        2,
        "Suite",
    ),
    "SC": (
        "Suite Confort",
        "Suite Confort como categoria de entrada al segmento suite, ofrece sala independiente, "
        "amenidades basicas de suite y acceso a todos los servicios del hotel a tarifa competitiva.",
        2,
        "Suite",
    ),
}

# Catálogo de clases de ocupación (número de huéspedes por habitación).
# Estructura: codigo → (nombre_clase, descripcion_ocupacion)
META_CLASE = {
    "SG": ("Sencilla", "1 persona"),
    "DB": ("Doble", "2 personas"),
    "TP": ("Triple", "3 personas"),
    "CD": ("Cuadruple", "4 personas"),
    "M5": ("Multiple 5+", "5 o mas personas"),
}


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer() -> pd.DataFrame:
    """
    Lee tiphab_tip, clahab_clh y nrohab_hab desde el parquet limpio.
    """
    if not PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet no encontrado: {PARQUET}\n"
            "Ejecuta primero el notebook 02_limpieza_datos.ipynb."
        )
    cols = ["tiphab_tip", "clahab_clh", "nrohab_hab"]
    df = pd.read_parquet(PARQUET, columns=cols)

    print(f"  Registros leídos del parquet : {len(df):,}")
    print(f"  Tipos únicos (tiphab_tip)    : {df['tiphab_tip'].nunique()}")
    print(f"  Clases únicas (clahab_clh)   : {df['clahab_clh'].nunique()}")
    print(f"  Habitaciones únicas          : {df['nrohab_hab'].nunique()}")
    return df


# ── TRANSFORMAR ───────────────────────────────────────────────────────────────
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera una fila por combinación única tipo_hab × clase_hab × num_habitacion y la
    enriquece con los atributos descriptivos de los catálogos del hotel.
    """

    # Deduplicar y ordenar
    combo = (
        df.dropna(subset=["tiphab_tip"])
        .drop_duplicates()
        .sort_values(["tiphab_tip", "clahab_clh", "nrohab_hab"])
        .reset_index(drop=True)
    )
    combo.columns = ["tipo_hab", "clase_hab", "num_habitacion"]

    # Enriquecer con metadatos de catálogos internos
    tipo_meta = combo["tipo_hab"].map(META_TIPO)
    clase_meta = combo["clase_hab"].map(META_CLASE)

    combo["nombre_tipo"] = tipo_meta.apply(lambda x: x[0] if isinstance(x, tuple) else "No definido")
    combo["descripcion_tipo"] = tipo_meta.apply(lambda x: x[1] if isinstance(x, tuple) else "Tipo no definido en catalogo")
    combo["capacidad_max"] = tipo_meta.apply(lambda x: x[2] if isinstance(x, tuple) else 2).astype(int)
    combo["categoria"] = tipo_meta.apply(lambda x: x[3] if isinstance(x, tuple) else "No definida")
    combo["nombre_clase"] = clase_meta.apply(lambda x: x[0] if isinstance(x, tuple) else "No definida")

    # id_habitacion secuencial
    combo.insert(0, "id_habitacion", range(1, len(combo) + 1))

    print(f"  Combinaciones tipo × clase   : {len(combo)}")
    print(f"\n  Detalle generado:")
    print(
        combo[
            [
                "num_habitacion",
                "tipo_hab",
                "nombre_tipo",
                "clase_hab",
                "nombre_clase",
                "capacidad_max",
                "categoria",
            ]
        ].to_string(index=False)
    )
    return combo


# ── CARGAR ────────────────────────────────────────────────────────────────────
def cargar(df: pd.DataFrame, engine) -> None:
    """
    Carga Dim_Habitacion en MySQL.
    Estrategia idempotente: DELETE + INSERT.
    """
    COLS_DDL = [
        "id_habitacion",
        "num_habitacion",
        "tipo_hab",
        "nombre_tipo",
        "clase_hab",
        "nombre_clase",
        "descripcion_tipo",
        "capacidad_max",
        "categoria",
    ]
    df_load = df[COLS_DDL]

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))

    df_load.to_sql(
        TABLE,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"  {TABLE} cargada : {len(df_load)} filas")


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
