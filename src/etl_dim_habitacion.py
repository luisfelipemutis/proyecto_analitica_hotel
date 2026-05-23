"""
etl_dim_habitacion.py
ETL — Dim_Habitacion
Extrae las combinaciones únicas tipo_hab × clase_hab del parquet y las
enriquece con metadatos de catálogo estático del hotel para cargar en MySQL.

Origen de cada campo
---------------------
tipo_hab        : campo tiphab_tip del parquet  (código de tipo de suite/hab).
clase_hab       : campo clahab_clh del parquet  (código de clase/ocupación).
num_habitacion  : campo nrohab_hab del parquet  (número de habitación).
nombre_tipo     : derivado del diccionario META_TIPO según tipo_hab.
descripcion_tipo: derivado del diccionario META_TIPO según tipo_hab.
capacidad_max   : derivado del diccionario META_TIPO según tipo_hab.
categoria       : derivado del diccionario META_TIPO según tipo_hab.
nombre_clase    : derivado del diccionario META_CLASE según clase_hab.

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
# descripcion_tipo es VARCHAR(350) en MySQL.
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

# Fallback para tipos y clases no catalogados en META_TIPO / META_CLASE
TIPO_FALLBACK = (
    "Sin Clasificar",
    "Tipo de habitacion no catalogado.",
    2,
    "Sin Clasificar",
)
CLASE_FALLBACK = ("Sin Clasificar", "Clase de ocupacion no catalogada.")


# ── EXTRAER ──────────────────────────────────────────────────────────────────
def extraer() -> pd.DataFrame:
    """
    Lee tiphab_tip, clahab_clh y nrohab_hab desde el parquet limpio.
    El parquet es la única fuente de verdad; si no existe se lanza error.
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
    sin_tipo = df["tiphab_tip"].isna().sum()
    if sin_tipo:
        print(
            f"  WARN: {sin_tipo:,} registros sin tiphab_tip → se excluyen de la dimensión"
        )
    return df


# ── TRANSFORMAR ───────────────────────────────────────────────────────────────
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera una fila por combinación única tipo_hab × clase_hab × num_habitacion y la
    enriquece con los atributos descriptivos de los catálogos del hotel.

    Pasos
    -----
    1. Eliminar filas sin tiphab_tip (no pueden clasificarse en la dimensión).
    2. Deduplicar por la combinación tipo_hab × clase_hab × num_habitacion.
    3. Ordenar para consistencia entre ejecuciones.
    4. Enriquecer con nombre_tipo, descripcion_tipo, capacidad_max, categoria
       desde META_TIPO (fallback si el código no está catalogado).
    5. Enriquecer con nombre_clase desde META_CLASE.
    6. Generar id_habitacion secuencial.
    """
    # 1-3. Deduplicar y ordenar
    combo = (
        df.dropna(subset=["tiphab_tip"])
        .drop_duplicates()
        .sort_values(["tiphab_tip", "clahab_clh", "nrohab_hab"])
        .reset_index(drop=True)
    )
    combo.columns = ["tipo_hab", "clase_hab", "num_habitacion"]

    # 4. Enriquecer con catálogo de tipos de habitación
    combo["nombre_tipo"] = combo["tipo_hab"].map(
        lambda x: META_TIPO.get(x, TIPO_FALLBACK)[0]
    )
    combo["descripcion_tipo"] = combo["tipo_hab"].map(
        lambda x: META_TIPO.get(x, TIPO_FALLBACK)[1]
    )
    combo["capacidad_max"] = combo["tipo_hab"].map(
        lambda x: META_TIPO.get(x, TIPO_FALLBACK)[2]
    )
    combo["categoria"] = combo["tipo_hab"].map(
        lambda x: META_TIPO.get(x, TIPO_FALLBACK)[3]
    )

    # 5. Enriquecer con catálogo de clases de ocupación
    combo["nombre_clase"] = combo["clase_hab"].map(
        lambda x: META_CLASE.get(x, CLASE_FALLBACK)[0]
    )

    # 6. id_habitacion secuencial
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

    Solo se cargan las columnas definidas en el DDL de MySQL.
    nombre_tipo y nombre_clase son atributos intermedios de enriquecimiento
    que no están en el DDL actual; se excluyen en esta capa.
    """
    COLS_DDL = [
        "id_habitacion",
        "num_habitacion",
        "tipo_hab",
        "clase_hab",
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
