# Databricks notebook source
# ==========================================
# PATHS SILVER
# ==========================================
catalog_name = "retail_catalog"
schema_name = "silver"

silver_base_path = "abfss://silver@stretailmaxdev01.dfs.core.windows.net"

quality_path = f"{silver_base_path}/quality_reports"
error_path = f"{silver_base_path}/error_records"


print(silver_base_path)

# COMMAND ----------

# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

import uuid
import time
import builtins

# COMMAND ----------

tables_config = {

    "MSTR_ARTICULOS": {
        "pk": "art_id",
        "mandatory_cols": ["art_id", "cod_barra"],
        "bronze_path": "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/mstr_articulos",
        "silver_table": "dim_articulos"
    },

    "MSTR_PROVEEDORES": {
        "pk": "id_proveedor",
        "mandatory_cols": ["id_proveedor", "razon_social"],
        "bronze_path": "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/mstr_proveedores",
        "silver_table": "dim_proveedores"
    },

    "MSTR_TIENDAS": {
        "pk": "id_tienda",
        "mandatory_cols": ["id_tienda", "nom_tienda"],
        "bronze_path": "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/mstr_tiendas",
        "silver_table": "dim_tiendas"
    },

    "CRM_MIEMBROS": {
        "pk": "id_miembro",
        "mandatory_cols": ["id_miembro"],
        "bronze_path": "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/crm_miembros",
        "silver_table": "dim_clientes"
    },

    "TRANS_VENTAS": {
        "pk": "id_trans",
        "mandatory_cols": ["id_trans", "id_miembro", "art_id"],
        "bronze_path": "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/trans_ventas",
        "silver_table": "fact_ventas"
    },

    "INV_STOCK_DIARIO": {
        "pk": "id_snapshot",
        "mandatory_cols": ["id_snapshot", "art_id"],
        "bronze_path": "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/inv_stock_diario",
        "silver_table": "fact_stock_diario"
    },

    "POST_DEVOLUCIONES": {
        "pk": "id_devolucion",
        "mandatory_cols": ["id_devolucion", "id_trans_origen"],
        "bronze_path": "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/post_devoluciones",
        "silver_table": "fact_devoluciones"
    }
}

# COMMAND ----------

# ==========================================
# FUNCION SILVER
# ==========================================

def process_silver_table(table_name, config):

    print(f"Procesando {table_name}")

    start_time = time.time()

    batch_id = str(uuid.uuid4())

    bronze_df = spark.read.format("delta").load(
        config["bronze_path"]
    )

    original_count = bronze_df.count()

    # ==========================================
    # 1. ELIMINAR DUPLICADOS
    # ==========================================

    df = bronze_df.dropDuplicates()

    dedup_count = df.count()

    # ==========================================
    # 2. ELIMINAR NULOS OBLIGATORIOS
    # ==========================================

    condition = None

    for col_name in config["mandatory_cols"]:

        current_condition = col(col_name).isNotNull()

        if condition is None:
            condition = current_condition
        else:
            condition = condition & current_condition

    rejected_df = df.filter(~condition) \
        .withColumn(
            "error_reason",
            lit("NULL_MANDATORY_FIELD")
        )

    df = df.filter(condition)

    # ==========================================
    # 3. HASH DATOS PERSONALES
    # ==========================================

    if table_name == "CRM_MIEMBROS":

        df = df.withColumn(
            "id_ciudad_hash",
            sha2(col("id_ciudad"), 256)
        )

        df = df.drop("id_ciudad")

    # ==========================================
    # 4. ESTANDARIZAR TIPOS
    # ==========================================

    if "activo" in df.columns:

        df = df.withColumn(
            "activo",
            col("activo").cast(BooleanType())
        )

    # ==========================================
    # 5. MANEJO NULOS
    # ==========================================

    if "canal_pref" in df.columns:

        df = df.withColumn(
            "canal_pref_null_flag",
            when(col("canal_pref").isNull(), 1).otherwise(0)
        )

        df = df.fillna({
            "canal_pref": "NO_DEFINIDO"
        })

    # ==========================================
    # 6. VALIDACION REFERENCIAL
    # FACT_VENTAS -> DIM_CLIENTES
    # ==========================================

    if config["silver_table"] == "fact_ventas":

        print("Validando integridad referencial clientes")

        clientes_df = spark.read.format("delta").load(
            f"{silver_base_path}/dim_clientes"
        )

        invalid_customers_df = df.join(
            clientes_df.select("id_miembro"),
            on="id_miembro",
            how="left_anti"
        ).withColumn(
            "error_reason",
            lit("INVALID_CUSTOMER_ID")
        )

        invalid_count = invalid_customers_df.count()

        # guardar errores FK
        if invalid_count > 0:

            invalid_customers_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(f"{error_path}/fact_ventas")

            print(f"Registros invalidos encontrados: {invalid_count}")

        # mantener solo registros validos
        df = df.join(
            clientes_df.select("id_miembro"),
            on="id_miembro",
            how="inner"
        )

    # ==========================================
    # 7. WRITE SILVER
    # ==========================================

    silver_table_path = f"{silver_base_path}/{config['silver_table']}"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_table_path)

    # ==========================================
    # 8. CREATE TABLE UNITY CATALOG
    # ==========================================

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS
    {catalog_name}.{schema_name}.{config['silver_table']}
    USING DELTA
    LOCATION '{silver_table_path}'
    """)

    # ==========================================
    # 9. GUARDAR ERRORES NULLS
    # ==========================================

    if rejected_df.count() > 0:

        rejected_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{error_path}/{config['silver_table']}_nulls")

    # ==========================================
    # 10. QUALITY REPORT
    # ==========================================

    null_metrics = []

    total_records = df.count()

    rejected_records = rejected_df.count()

    conform_records = total_records

    conform_percentage = builtins.round(
        (conform_records / original_count) * 100,
        2
    ) if original_count > 0 else 0

    for c in df.columns:

        null_count = df.filter(
            col(c).isNull()
        ).count()

        null_percentage = builtins.round(
            (null_count / total_records) * 100,
            2
        ) if total_records > 0 else 0

        null_metrics.append(
            (
                table_name,
                c,
                null_count,
                null_percentage,
                rejected_records,
                conform_percentage,
                batch_id
            )
        )

    quality_df = spark.createDataFrame(
        null_metrics,
        [
            "table_name",
            "column_name",
            "null_count",
            "null_percentage",
            "rejected_records",
            "conform_percentage",
            "batch_id"
        ]
    )

    quality_df.write \
        .format("delta") \
        .mode("append") \
        .save(quality_path)

    # ==========================================
    # 11. LOG FINAL
    # ==========================================

    duration = builtins.round(
        time.time() - start_time,
        2
    )

    print(f"""
    ==========================================
    Tabla: {table_name}
    Original: {original_count}
    Sin duplicados: {dedup_count}
    Final Silver: {total_records}
    Rechazados: {rejected_records}
    Duracion: {duration} segundos
    ==========================================
    """)

# COMMAND ----------

# ==========================================
# EJECUCION SILVER
# ==========================================

for table_name, config in tables_config.items():

    process_silver_table(
        table_name,
        config
    )

# COMMAND ----------

quality_df = spark.read.format("delta").load(quality_path)

display(quality_df)