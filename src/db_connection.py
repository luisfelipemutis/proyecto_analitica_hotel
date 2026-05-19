"""
db_connection.py
Conexión centralizada a MySQL para el pipeline ETL Kimball.
Todos los parámetros se leen desde variables de entorno (docker/.env).
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    """Retorna un engine de SQLAlchemy conectado a MySQL hotel_dann_dw."""
    host   = os.getenv("MYSQL_HOST",     "mysql")
    port   = os.getenv("MYSQL_PORT",     "3306")
    user   = os.getenv("MYSQL_USER",     "airflow")
    pwd    = os.getenv("MYSQL_PASSWORD", "airflow")
    db     = os.getenv("MYSQL_DATABASE", "hotel_dann_dw")
    url    = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(url, pool_pre_ping=True, echo=False)
    return engine


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
