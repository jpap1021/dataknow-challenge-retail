import os
import pandas as pd

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("AZURE_SQL_SERVER")
DATABASE = os.getenv("AZURE_SQL_DATABASE")
USERNAME = os.getenv("AZURE_SQL_USERNAME")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=60;"
)

params = quote_plus(connection_string)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True,
    pool_pre_ping=True
)

tables = {
    "MSTR_PROVEEDORES": "data-generation/output/csv/MSTR_PROVEEDORES.csv",
    "MSTR_TIENDAS": "data-generation/output/csv/MSTR_TIENDAS.csv",
    "CRM_MIEMBROS": "data-generation/output/csv/CRM_MIEMBROS.csv",
    "MSTR_ARTICULOS": "data-generation/output/csv/MSTR_ARTICULOS.csv",
    "TRANS_VENTAS": "data-generation/output/csv/TRANS_VENTAS.csv",
    "INV_STOCK_DIARIO": "data-generation/output/csv/INV_STOCK_DIARIO.csv",
    "POST_DEVOLUCIONES": "data-generation/output/csv/POST_DEVOLUCIONES.csv"
}

CHUNK_SIZE = 5000

for table_name, path in tables.items():

    print("\n" + "="*50)
    print(f"Cargando {table_name}")
    print("="*50)

    # Leer CSV
    df = pd.read_csv(path)

    # Reemplazar tabla vacía
    with engine.begin() as conn:

        conn.execute(text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"))

        # crear estructura
        df.head(0).to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )

    total_chunks = (len(df) // CHUNK_SIZE) + 1

    for i, start in enumerate(range(0, len(df), CHUNK_SIZE), start=1):

        end = start + CHUNK_SIZE

        chunk = df.iloc[start:end]

        print(f"Subiendo chunk {i}/{total_chunks}...")

        try:

            with engine.begin() as conn:

                chunk.to_sql(
                    table_name,
                    conn,
                    if_exists="append",
                    index=False,
                    method=None
                )

            print(f"Chunk {i} cargado")

        except Exception as e:

            print(f"Error en chunk {i}: {e}")

            break

    print(f"{table_name} cargada correctamente")

print("\nCarga finalizada")