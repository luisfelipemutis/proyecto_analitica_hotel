from pathlib import Path
from typing import Dict, Union

import pandas as pd


def cargar_excel_multihojas(
    ruta_excel: Union[str, Path],
    unir_hojas: bool = True,
    agregar_origen_hoja: bool = True,
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Carga un archivo Excel con una o multiples hojas de forma robusta.

    Args:
        ruta_excel: Ruta al archivo .xlsx o .xls.
        unir_hojas: Si es True, concatena todas las hojas en un solo DataFrame.
        agregar_origen_hoja: Si es True, agrega columna 'origen_hoja' para trazabilidad.

    Returns:
        Si unir_hojas=True: DataFrame con todas las hojas concatenadas.
        Si unir_hojas=False: Diccionario {nombre_hoja: DataFrame}.
    """
    ruta_excel = Path(ruta_excel)

    if not ruta_excel.exists():
        raise FileNotFoundError(f"No se encontro el archivo: {ruta_excel}")

    hojas = pd.read_excel(ruta_excel, sheet_name=None)

    hojas_limpias: Dict[str, pd.DataFrame] = {}
    for nombre_hoja, df_hoja in hojas.items():
        df_hoja = df_hoja.copy()
        df_hoja.columns = [str(col).strip() for col in df_hoja.columns]

        if agregar_origen_hoja:
            df_hoja["origen_hoja"] = nombre_hoja

        hojas_limpias[nombre_hoja] = df_hoja

    if not unir_hojas:
        return hojas_limpias

    return pd.concat(hojas_limpias.values(), ignore_index=True, sort=False)
