from utils.config_loader import load_config
from utils.helpers import set_seed

from generators.generate_proveedores import generate_proveedores
from generators.generate_tiendas import generate_tiendas
from generators.generate_clientes import generate_clientes
from generators.generate_articulos import generate_articulos
from generators.generate_ventas import generate_ventas
from generators.generate_stock import generate_stock
from generators.generate_devoluciones import generate_devoluciones
from anomalies.inject_anomalies import (
    inject_duplicate_sales,
    inject_future_dates,
    inject_invalid_customer,
    inject_negative_stock
)

import os

config = load_config()

set_seed(config["seed"])

OUTPUT_CSV = "data-generation/output/csv"
OUTPUT_PARQUET = "data-generation/output/parquet"

os.makedirs(OUTPUT_CSV, exist_ok=True)
os.makedirs(OUTPUT_PARQUET, exist_ok=True)

# Generar proveedores
df_proveedores = generate_proveedores(
    config["volumes"]["proveedores"]
)

# Guardar CSV
df_proveedores.to_csv(
    f"{OUTPUT_CSV}/MSTR_PROVEEDORES.csv",
    index=False
)

# Guardar Parquet
df_proveedores.to_parquet(
    f"{OUTPUT_PARQUET}/MSTR_PROVEEDORES.parquet",
    index=False
)

# Generar tiendas
df_tiendas = generate_tiendas(
    config["volumes"]["tiendas"]
)

df_tiendas.to_csv(
    f"{OUTPUT_CSV}/MSTR_TIENDAS.csv",
    index=False
)

df_tiendas.to_parquet(
    f"{OUTPUT_PARQUET}/MSTR_TIENDAS.parquet",
    index=False
)

# Generar clientes
df_clientes = generate_clientes(
    config["volumes"]["miembros"]
)

df_clientes.to_csv(
    f"{OUTPUT_CSV}/CRM_MIEMBROS.csv",
    index=False
)

df_clientes.to_parquet(
    f"{OUTPUT_PARQUET}/CRM_MIEMBROS.parquet",
    index=False
)

# Generar artículos
df_articulos = generate_articulos(
    df_proveedores,
    config["volumes"]["productos"]
)

df_articulos.to_csv(
    f"{OUTPUT_CSV}/MSTR_ARTICULOS.csv",
    index=False
)

df_articulos.to_parquet(
    f"{OUTPUT_PARQUET}/MSTR_ARTICULOS.parquet",
    index=False
)

# Generar ventas
df_ventas = generate_ventas(
    df_clientes,
    df_tiendas,
    df_articulos,
    config["volumes"]["ventas"]
)

df_ventas = inject_duplicate_sales(df_ventas)
df_ventas = inject_future_dates(df_ventas)
df_ventas = inject_invalid_customer(df_ventas)

df_ventas.to_csv(
    f"{OUTPUT_CSV}/TRANS_VENTAS.csv",
    index=False
)

df_ventas.to_parquet(
    f"{OUTPUT_PARQUET}/TRANS_VENTAS.parquet",
    index=False
)



# Generar inventario
df_stock = generate_stock(
    df_articulos,
    df_tiendas,
    config["volumes"]["inventario"]
)

df_stock = inject_negative_stock(df_stock)

df_stock.to_csv(
    f"{OUTPUT_CSV}/INV_STOCK_DIARIO.csv",
    index=False
)

df_stock.to_parquet(
    f"{OUTPUT_PARQUET}/INV_STOCK_DIARIO.parquet",
    index=False
)



# Generar devoluciones
df_devoluciones = generate_devoluciones(
    df_ventas,
    config["volumes"]["devoluciones"]
)

df_devoluciones.to_csv(
    f"{OUTPUT_CSV}/POST_DEVOLUCIONES.csv",
    index=False
)

df_devoluciones.to_parquet(
    f"{OUTPUT_PARQUET}/POST_DEVOLUCIONES.parquet",
    index=False
)

print(df_proveedores.head())
print("Archivo generado correctamente.")