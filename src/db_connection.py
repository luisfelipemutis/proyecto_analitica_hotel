"""
db_connection.py
Conexión centralizada a MySQL para el pipeline ETL Kimball.
Todos los parámetros se leen desde variables de entorno (docker/.env).
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    """Retorna un engine de SQLAlchemy conectado a MySQL hotel_dann_dw.

    Estrategia de conexion en dos pasos:
      1. Conectar SIN especificar la base de datos y crearla si no existe.
      2. Retornar el engine ya apuntando a la DB correcta.
    Esto garantiza que un restart de MySQL o un retry de Airflow no bloquee
    los ETLs con 'Unknown database'.
    """
    host = os.getenv("MYSQL_HOST",     "host.docker.internal")
    port = os.getenv("MYSQL_PORT",     "3306")
    user = os.getenv("MYSQL_USER",     "root")
    pwd  = os.getenv("MYSQL_PASSWORD", "root")
    db   = os.getenv("MYSQL_DATABASE", "hotel_dann_dw")

    # Paso 1: conexion sin DB → asegurar que la base de datos existe
    base_url = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/"
    base_eng = create_engine(base_url, pool_pre_ping=True, echo=False)
    try:
        with base_eng.connect() as conn:
            conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS `{db}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            ))
    finally:
        base_eng.dispose()

    # Paso 2: engine definitivo apuntando a la DB ya garantizada
    url = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True, echo=False)


def test_connection() -> bool:
    """Verifica que la conexión a MySQL es exitosa."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"✅ Conexión MySQL OK → {os.getenv('MYSQL_HOST','mysql')}:"
              f"{os.getenv('MYSQL_PORT','3306')}/{os.getenv('MYSQL_DATABASE','hotel_dann_dw')}")
        return True
    except Exception as e:
        print(f"❌ Error de conexión MySQL: {e}")
        return False


if __name__ == "__main__":
    test_connection()
