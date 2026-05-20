# Databricks notebook source
import time
import uuid
from datetime import datetime

from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col
)

# COMMAND ----------

jdbcHostname = "retailmaxsql4935.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "retailmaxdb"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

connectionProperties = {
  "user": dbutils.secrets.get(
      scope="retailmax-jpap",
      key="sql-user"
  ),

  "password": dbutils.secrets.get(
      scope="retailmax-jpap",
      key="sql-password"
  ),

  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

batch_id = str(uuid.uuid4())

current_date = datetime.now()

year = current_date.strftime("%Y")
month = current_date.strftime("%m")
day = current_date.strftime("%d")

# COMMAND ----------

incremental_config = {
    "MSTR_ARTICULOS": "fec_alta",
    "MSTR_TIENDAS": "fec_apertura",
    "CRM_MIEMBROS": "fec_ultima_compra",
    "TRANS_VENTAS": "fec_trans",
    "INV_STOCK_DIARIO": "fec_snapshot",
    "POST_DEVOLUCIONES": "fec_devolucion"
}

# COMMAND ----------

tables = [
    "MSTR_ARTICULOS",
    "MSTR_PROVEEDORES",
    "MSTR_TIENDAS",
    "CRM_MIEMBROS",
    "TRANS_VENTAS",
    "INV_STOCK_DIARIO",
    "POST_DEVOLUCIONES"
]

# COMMAND ----------

watermark_path = (
    "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/watermarks"
)

# verificar si existe carpeta watermark
watermark_exists = False

try:

    dbutils.fs.ls(watermark_path)

    watermark_exists = True

except:

    watermark_exists = False

# si no existe -> crear
if not watermark_exists:

    print("Creando tabla watermark inicial")

    empty_df = spark.createDataFrame(
        [],
        "table_name STRING, last_watermark STRING"
    )

    (
        empty_df.write
        .format("delta")
        .mode("overwrite")
        .save(watermark_path)
    )

# leer watermark
watermark_df = spark.read.format("delta").load(watermark_path)

print("Watermark table lista")

# COMMAND ----------

# DBTITLE 1,Load tables to bronze
log_data = []

for table_name in tables:

    start_time = time.time()

    try:

        print(f"\nProcesando tabla: {table_name}")

        bronze_base_path = (
            f"abfss://bronze@stretailmaxdev01.dfs.core.windows.net/"
            f"{table_name.lower()}"
        )

        bronze_path = (
            f"{bronze_base_path}/"
            f"year={year}/month={month}/day={day}"
        )

        # =========================
        # FULL LOAD SIN INCREMENTAL
        # =========================

        if table_name == "MSTR_PROVEEDORES":

            query = f"(SELECT * FROM dbo.{table_name}) AS src"

        else:

            incremental_column = incremental_config[table_name]

            existing_watermark = (
                watermark_df
                .filter(col("table_name") == table_name)
                .collect()
            )

            # Primera carga
            if len(existing_watermark) == 0:

                print("Primera carga FULL")

                query = f"(SELECT * FROM dbo.{table_name}) AS src"

            else:

                last_watermark = existing_watermark[0]["last_watermark"]

                print(f"Watermark encontrado: {last_watermark}")

                query = f"""
                (
                    SELECT *
                    FROM dbo.{table_name}
                    WHERE {incremental_column} > '{last_watermark}'
                ) AS src
                """

        # =========================
        # LECTURA SQL
        # =========================

        df = spark.read.jdbc(
            url=jdbcUrl,
            table=query,
            properties=connectionProperties
        )

        # =========================
        # VALIDAR SI HAY DATOS
        # =========================

        record_count = df.count()

        if record_count == 0:

            print("No hay nuevos registros")

            duration = round(time.time() - start_time, 2)

            log_data.append((
                table_name,
                0,
                0,
                duration,
                batch_id,
                "SUCCESS",
                "Sin nuevos registros"
            ))

            continue

        # =========================
        # METADATOS AUDITORIA
        # =========================

        bronze_df = (
            df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("Azure_SQL_Retailmax"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn("year", lit(year))
            .withColumn("month", lit(month))
            .withColumn("day", lit(day))
        )

        # =========================
        # ESCRITURA DELTA
        # =========================

        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .partitionBy("year", "month", "day")
            .save(bronze_base_path)
        )

        # =========================
        # CALCULAR TAMAÑO ARCHIVOS
        # =========================

        files = dbutils.fs.ls(bronze_base_path)

        total_size = sum(file.size for file in files)

        # =========================
        # ACTUALIZAR WATERMARK
        # =========================

        if table_name != "MSTR_PROVEEDORES":

            max_watermark = (
                df.agg({incremental_column: "max"})
                .collect()[0][0]
            )

            new_watermark = spark.createDataFrame(
                [(table_name, str(max_watermark))],
                ["table_name", "last_watermark"]
            )

            updated_watermark = (
                watermark_df
                .filter(col("table_name") != table_name)
                .union(new_watermark)
            )

            (
                updated_watermark.write
                .format("delta")
                .mode("overwrite")
                .save(watermark_path)
            )

            watermark_df = spark.read.format("delta").load(watermark_path)

        # =========================
        # LOGGING
        # =========================

        duration = round(time.time() - start_time, 2)

        log_data.append((
            table_name,
            record_count,
            total_size,
            duration,
            batch_id,
            "SUCCESS",
            None
        ))

        print(f"Tabla {table_name} cargada correctamente")

    except Exception as e:

        duration = round(time.time() - start_time, 2)

        log_data.append((
            table_name,
            0,
            0,
            duration,
            batch_id,
            "FAILED",
            str(e)
        ))

        print(f"Error en tabla {table_name}")
        print(str(e))

# COMMAND ----------

log_schema = """
table_name STRING,
record_count INT,
file_size_bytes LONG,
duration_seconds DOUBLE,
batch_id STRING,
status STRING,
error_message STRING
"""

log_df = spark.createDataFrame(log_data, schema=log_schema)

# COMMAND ----------

log_path = (
    "abfss://bronze@stretailmaxdev01.dfs.core.windows.net/logs"
)

(
    log_df.write
    .format("delta")
    .mode("append")
    .save(log_path)
)

# COMMAND ----------

display(log_df)