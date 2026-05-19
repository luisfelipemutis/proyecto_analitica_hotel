"""
etl_dim_habitacion.py
ETL — Dim_Habitacion
Catálogo estático de combinaciones tipo_hab x clase_hab, verificado con el dataset.
"""
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db_connection import get_engine

PARQUET = Path(__file__).parent.parent / "data" / "processed" / "reservas_clean.parquet"
RAW     = Path(__file__).parent.parent / "data" / "raw" / "DataSet_ReservaYHuespedes_V.Full.xlsx"
TABLE   = "Dim_Habitacion"

# descripcion_tipo es VARCHAR(350) en MySQL — espacio suficiente para descripcion completa.
META_TIPO = {
    "ST": ("Suite Estandar",    "Suite estandar con sala de estar separada, zona de descanso premium y amenidades de alta categoria. Configuracion clasica del portafolio Dann Monasterio.", 2, "Suite"),
    "SE": ("Suite Ejecutiva",   "Suite ejecutiva con area de trabajo dedicada, escritorio ejecutivo, acceso a business center y vistas preferenciales al paisaje urbano o jardines del claustro.", 2, "Suite"),
    "S3": ("Suite Junior",      "Suite Junior con sala integrada al area de descanso, ideal para estancias cortas de viajeros corporativos. Distribucion compacta con todos los servicios de suite.", 2, "Suite"),
    "S4": ("Suite Tipo 4",      "Suite de categoria superior con caracteristicas diferenciales de confort, espacio ampliado y acabados de lujo. Posicion intermedia en el portafolio premium del hotel.", 2, "Suite"),
    "SP": ("Suite Presidencial","Suite Presidencial de maxima categoria con sala de reuniones privada, comedor ejecutivo, jacuzzi y vistas panoramicas al centro historico de Popayan o jardines del convento.", 2, "Suite"),
    "SC": ("Suite Confort",     "Suite Confort como categoria de entrada al segmento suite, ofrece sala independiente, amenidades basicas de suite y acceso a todos los servicios del hotel a tarifa competitiva.", 2, "Suite"),
}
META_CLASE = {
    "SG": ("Sencilla",    "1 persona"),
    "DB": ("Doble",       "2 personas"),
    "TP": ("Triple",      "3 personas"),
    "CD": ("Cuadruple",   "4 personas"),
    "M5": ("Multiple 5+", "5 o mas personas"),
}


def extraer() -> pd.DataFrame:
    cols = ["tiphab_tip", "clahab_clh"]
    if PARQUET.exists():
        df = pd.read_parquet(PARQUET, columns=cols)
    else:
        h1 = pd.read_excel(RAW, sheet_name="Hoja1", usecols=cols)
        h2 = pd.read_excel(RAW, sheet_name="Hoja2", usecols=cols)
        df = pd.concat([h1, h2], ignore_index=True)
    return df


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    combo = (df.dropna(subset=["tiphab_tip"])
               .drop_duplicates()
               .sort_values(["tiphab_tip", "clahab_clh"])
               .reset_index(drop=True))
    combo.insert(0, "id_habitacion", range(1, len(combo) + 1))
    combo.columns = ["id_habitacion", "tipo_hab", "clase_hab"]
    combo["nombre_tipo"]     = combo["tipo_hab"].map(lambda x: META_TIPO.get(x, (x,"",2,"Suite"))[0])
    combo["descripcion_tipo"]= combo["tipo_hab"].map(lambda x: META_TIPO.get(x, (x,"",2,"Suite"))[1])
    combo["capacidad_max"]   = combo["tipo_hab"].map(lambda x: META_TIPO.get(x, (x,"",2,"Suite"))[2])
    combo["categoria"]       = combo["tipo_hab"].map(lambda x: META_TIPO.get(x, (x,"",2,"Suite"))[3])
    combo["nombre_clase"]    = combo["clase_hab"].map(lambda x: META_CLASE.get(x, (x,""))[0])
    return combo


def cargar(df: pd.DataFrame, engine) -> None:
    # Seleccionar solo las columnas que existen en el DDL de Dim_Habitacion.
    # El dataframe tiene nombre_tipo y nombre_clase como enriquecimiento
    # intermedio, pero la tabla MySQL solo tiene las columnas del DDL.
    COLS_DDL = ["id_habitacion", "tipo_hab", "clase_hab",
                "descripcion_tipo", "capacidad_max", "categoria"]
    df_load = df[COLS_DDL]
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    df_load.to_sql(TABLE, con=engine, if_exists="append", index=False,
                   method="multi")
    print(f"✅ {TABLE} cargada: {len(df_load)} filas")


def run():
    print(f"[{TABLE}] Iniciando ETL...")
    engine = get_engine()
    df_raw = extraer()
    df = transformar(df_raw)
    cargar(df, engine)


if __name__ == "__main__":
    run()
