# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.window import Window

import uuid
import time

# COMMAND ----------

# ==========================================
# VARIABLES GOLD
# ==========================================

catalog_name = "retail_catalog"

gold_schema = "gold"

gold_base_path = "abfss://gold@stretailmaxdev01.dfs.core.windows.net"

silver_base_path = "abfss://silver@stretailmaxdev01.dfs.core.windows.net"

# COMMAND ----------

# ==========================================
# CREAR SCHEMA GOLD
# ==========================================

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS
{catalog_name}.{gold_schema}
MANAGED LOCATION '{gold_base_path}'
""")

print("Schema GOLD creado")

# COMMAND ----------

# ==========================================
# CARGAR TABLAS SILVER
# ==========================================

clientes_df = spark.read.format("delta").load(
    f"{silver_base_path}/dim_clientes"
)

articulos_df = spark.read.format("delta").load(
    f"{silver_base_path}/dim_articulos"
)

proveedores_df = spark.read.format("delta").load(
    f"{silver_base_path}/dim_proveedores"
)

tiendas_df = spark.read.format("delta").load(
    f"{silver_base_path}/dim_tiendas"
)

ventas_df = spark.read.format("delta").load(
    f"{silver_base_path}/fact_ventas"
)

inventario_df = spark.read.format("delta").load(
    f"{silver_base_path}/fact_stock_diario"
)

devoluciones_df = spark.read.format("delta").load(
    f"{silver_base_path}/fact_devoluciones"
)

print("Tablas Silver cargadas")

# COMMAND ----------

# ==========================================
# DIM_CLIENTES
# ==========================================

dim_clientes = clientes_df \
    .withColumn(
        "antiguedad_dias",
        datediff(current_date(), to_date(col("fec_registro")))
    ) \
    .withColumn(
        "genero_std",
        when(col("genero") == "M", "M")
        .when(col("genero") == "F", "F")
        .otherwise("NO INFORMADO")
    ) \
    .fillna({
        "rango_edad": "26-35"
    }) \
    .withColumn(
        "cliente_activo_flag",
        when(col("activo") == True, 1).otherwise(0)
    )

gold_path = f"{gold_base_path}/dim_clientes"

dim_clientes.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.dim_clientes
USING DELTA
LOCATION '{gold_path}'
""")

print("dim_clientes creada")

# COMMAND ----------

# ==========================================
# DIM_TIENDAS
# ==========================================

dim_tiendas = tiendas_df \
    .withColumn(
        "tipo_tienda_std",
        when(
            upper(col("tipo_tienda")).like("%HIPER%"),
            "HIPERMERCADO"
        )
        .when(
            upper(col("tipo_tienda")).like("%SUPER%"),
            "SUPERMERCADO"
        )
        .otherwise("OTROS")
    ) \
    .withColumn(
        "zona_distribucion",
        when(
            upper(col("id_pais")) == "ECUADOR",
            "NORTE"
        )
        .when(
            upper(col("id_pais")) == "COLOMBIA",
            "SUR"
        )
        .otherwise("CENTRO")
    )

gold_path = f"{gold_base_path}/dim_tiendas"

dim_tiendas.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.dim_tiendas
USING DELTA
LOCATION '{gold_path}'
""")

print("dim_tiendas creada")

# COMMAND ----------

# ==========================================
# DIM PRODUCTOS
# ==========================================

articulos_df = spark.read.format("delta").load(
    f"{silver_base_path}/dim_articulos"
)

proveedores_df = spark.read.format("delta").load(
    f"{silver_base_path}/dim_proveedores"
)

dim_productos = articulos_df.alias("a") \
    .join(
        proveedores_df.alias("p"),
        col("a.id_proveedor") == col("p.id_proveedor"),
        "left"
    ) \
    .select(
        col("a.art_id"),
        col("a.cod_barra"),
        col("a.desc_art"),

        col("a.id_categ_n1"),
        col("a.id_categ_n2"),
        col("a.id_categ_n3"),

        col("a.id_proveedor"),

        col("p.razon_social").alias("nombre_proveedor"),

        col("p.tiempo_repo_dias"),

        col("a.precio_lista"),

        col("a.peso_kg"),

        col("a.unid_medida"),

        col("a.activo"),

        (
            col("a.precio_lista") * 0.30
        ).alias("margen_estimado_categoria")
    )

gold_path = f"{gold_base_path}/dim_productos"

dim_productos.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.dim_productos
USING DELTA
LOCATION '{gold_path}'
""")

print("dim_productos creada")

display(dim_productos.limit(10))

# COMMAND ----------

# ==========================================
# FACT_VENTAS
# ==========================================

fact_ventas = ventas_df.alias("v") \
    .join(
        dim_clientes.alias("c"),
        col("v.id_miembro") == col("c.id_miembro"),
        "left"
    ) \
    .select(
        col("v.id_trans"),
        col("v.id_miembro"),
        col("v.id_tienda"),
        col("v.art_id"),
        col("v.fec_trans"),
        col("v.hra_trans"),
        col("v.qty_vendida"),
        col("v.precio_unitario_venta"),
        col("v.descuento_aplicado"),
        col("v.tipo_pago"),
        col("v.canal_venta"),
        col("v.ingestion_timestamp"),
        col("v.source_system"),
        col("v.batch_id"),
        col("v.year"),
        col("v.month"),
        col("v.day"),
        col("c.id_miembro").alias("cliente_validado")
    ) \
    .withColumn(
        "cliente_anonimo_flag",
        when(col("cliente_validado").isNull(), 1)
        .otherwise(0)
    ) \
    .withColumn(
        "vr_venta_neto",
        (
            col("qty_vendida") *
            col("precio_unitario_venta")
        ) - col("descuento_aplicado")
    ) \
    .withColumn(
        "venta_descuento_flag",
        when(col("descuento_aplicado") > 0, 1)
        .otherwise(0)
    ) \
    .withColumn(
        "fec_venta",
        to_date(col("fec_trans"))
    )

gold_path = f"{gold_base_path}/fact_ventas"

fact_ventas.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("fec_venta") \
    .save(gold_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.fact_ventas
USING DELTA
LOCATION '{gold_path}'
""")

print("fact_ventas creada")

# COMMAND ----------

# ==========================================
# FACT INVENTARIO
# ==========================================

from pyspark.sql.functions import *

inventario_df = spark.read.format("delta").load(
    f"{silver_base_path}/fact_stock_diario"
)

dim_productos = spark.read.format("delta").load(
    f"{gold_base_path}/dim_productos"
)

# ==========================================
# JOIN LIMPIO
# SOLO TRAER tiempo_repo_dias
# ==========================================

fact_inventario = inventario_df.alias("i") \
    .join(
        dim_productos.select(
            "art_id",
            "tiempo_repo_dias"
        ).dropDuplicates(["art_id"]).alias("p"),
        col("i.art_id") == col("p.art_id"),
        "left"
    ) \
    .select(
        "i.*",
        "p.tiempo_repo_dias"
    )

# ==========================================
# PROMEDIO CONSUMO 14 DIAS
# ==========================================

fact_inventario = fact_inventario.withColumn(
    "promedio_consumo_14dias",
    round(
        col("stock_fisico") / 14,
        2
    )
)

# ==========================================
# COBERTURA DIAS
# ==========================================

fact_inventario = fact_inventario.withColumn(
    "cobertura_dias",
    when(
        col("promedio_consumo_14dias") > 0,
        round(
            col("stock_fisico") /
            col("promedio_consumo_14dias"),
            2
        )
    ).otherwise(0)
)

# ==========================================
# ALERTA QUIEBRE
# ==========================================

fact_inventario = fact_inventario.withColumn(
    "alerta_quiebre",
    when(
        (col("cobertura_dias") < col("tiempo_repo_dias")) &
        (col("promedio_consumo_14dias") > 0),
        1
    ).otherwise(0)
)

# ==========================================
# DIFERENCIA VS STOCK MINIMO
# ==========================================

fact_inventario = fact_inventario.withColumn(
    "diferencia_stock_min",
    col("stock_fisico") - col("stock_minimo_config")
)

# ==========================================
# WRITE GOLD
# ==========================================

gold_path = f"{gold_base_path}/fact_inventario"

fact_inventario.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

# ==========================================
# CREATE TABLE GOLD
# ==========================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.fact_inventario
USING DELTA
LOCATION '{gold_path}'
""")

print("fact_inventario creada")

display(fact_inventario.limit(10))

# COMMAND ----------

# ==========================================
# FACT DEVOLUCIONES
# ==========================================

from pyspark.sql.functions import *

devoluciones_df = spark.read.format("delta").load(
    f"{silver_base_path}/fact_devoluciones"
)

ventas_df = spark.read.format("delta").load(
    f"{gold_base_path}/fact_ventas"
)

dim_productos = spark.read.format("delta").load(
    f"{gold_base_path}/dim_productos"
)

# ==========================================
# JOIN CONTROLADO
# ==========================================

fact_devoluciones = devoluciones_df.alias("d") \
    .join(
        ventas_df.select(
            "id_trans",
            "qty_vendida",
            "precio_unitario_venta",
            "canal_venta"
        ).alias("v"),
        col("d.id_trans_origen") == col("v.id_trans"),
        "left"
    ) \
    .join(
        dim_productos.select(
            "art_id",
            "id_categ_n1",
            "id_proveedor"
        ).dropDuplicates(["art_id"]).alias("p"),
        col("d.art_id") == col("p.art_id"),
        "left"
    ) \
    .select(
        col("d.id_devolucion"),
        col("d.id_trans_origen"),
        col("d.art_id"),
        col("d.id_tienda"),
        col("d.fec_devolucion"),

        col("d.qty_devuelta"),

        col("d.motivo_cod"),

        col("d.canal_devolucion"),

        col("d.estado_devolucion"),

        col("d.vr_reembolso"),

        col("p.id_categ_n1"),

        col("p.id_proveedor"),

        col("v.precio_unitario_venta"),

        col("v.qty_vendida")
    )

# ==========================================
# MOTIVO LEGIBLE
# ==========================================

fact_devoluciones = fact_devoluciones.withColumn(
    "motivo_desc",
    when(
        col("motivo_cod") == "DAM",
        "Producto Dañado"
    ).when(
        col("motivo_cod") == "ERR",
        "Error Cliente"
    ).otherwise("Otros")
)

# ==========================================
# PRECIO ORIGINAL
# ==========================================

fact_devoluciones = fact_devoluciones.withColumn(
    "precio_original",
    col("precio_unitario_venta")
)

# ==========================================
# TASA DEVOLUCION
# ==========================================

fact_devoluciones = fact_devoluciones.withColumn(
    "tasa_devolucion",
    when(
        col("qty_vendida") > 0,
        round(
            (
                col("qty_devuelta") /
                col("qty_vendida")
            ) * 100,
            2
        )
    ).otherwise(0)
)

# ==========================================
# WRITE GOLD
# ==========================================

gold_path = f"{gold_base_path}/fact_devoluciones"

fact_devoluciones.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

# ==========================================
# CREATE TABLE
# ==========================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.fact_devoluciones
USING DELTA
LOCATION '{gold_path}'
""")

print("fact_devoluciones creada")

display(fact_devoluciones.limit(10))

# COMMAND ----------

# ==========================================
# FACT RFM CLIENTES
# ==========================================

from pyspark.sql.window import Window
from pyspark.sql.functions import *

ventas_df = spark.read.format("delta").load(
    f"{gold_base_path}/fact_ventas"
)

# ==========================================
# CLIENTES ACTIVOS 180 DIAS
# ==========================================

clientes_activos = ventas_df \
    .groupBy("id_miembro") \
    .agg(
        max("fec_venta").alias("ultima_compra")
    ) \
    .filter(
        datediff(
            current_date(),
            col("ultima_compra")
        ) <= 180
    )

# ==========================================
# VENTAS ULTIMOS 90 DIAS
# ==========================================

ventas_90 = ventas_df.filter(
    col("fec_venta") >= date_sub(
        current_date(),
        90
    )
)

# ==========================================
# SOLO CLIENTES ACTIVOS
# ==========================================

ventas_90 = ventas_90.join(
    clientes_activos.select("id_miembro"),
    "id_miembro",
    "inner"
)

# ==========================================
# CALCULO RFM
# ==========================================

rfm_df = ventas_90 \
    .groupBy("id_miembro") \
    .agg(
        datediff(
            current_date(),
            max("fec_venta")
        ).alias("recency"),

        countDistinct("id_trans").alias("frequency"),

        round(
            sum("vr_venta_neto"),
            2
        ).alias("monetary")
    )

# ==========================================
# SCORING QUINTILES
# ==========================================

window_spec = Window.orderBy("recency")

rfm_df = rfm_df \
    .withColumn(
        "r_score",
        6 - ntile(5).over(
            Window.orderBy("recency")
        )
    ) \
    .withColumn(
        "f_score",
        ntile(5).over(
            Window.orderBy(col("frequency").desc())
        )
    ) \
    .withColumn(
        "m_score",
        ntile(5).over(
            Window.orderBy(col("monetary").desc())
        )
    )

# ==========================================
# SEGMENTO RFM
# ==========================================

rfm_df = rfm_df.withColumn(
    "segmento_rfm",
    concat(
        lit("R"),
        col("r_score"),
        lit("-F"),
        col("f_score"),
        lit("-M"),
        col("m_score")
    )
)

# ==========================================
# CLASIFICACION NEGOCIO
# ==========================================

rfm_df = rfm_df.withColumn(
    "grupo_valor",
    when(
        (col("r_score") >= 4) &
        (col("f_score") >= 4) &
        (col("m_score") >= 4),
        "Champions"
    ).when(
        col("r_score") >= 4,
        "Clientes Recientes"
    ).when(
        col("f_score") >= 4,
        "Frecuentes"
    ).otherwise("Estandar")
)

# ==========================================
# WRITE GOLD
# ==========================================

gold_path = f"{gold_base_path}/fact_rfm_clientes"

rfm_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.fact_rfm_clientes
USING DELTA
LOCATION '{gold_path}'
""")

print("fact_rfm_clientes creada")

# COMMAND ----------

# ==========================================
# AGREGACION VENTAS DIARIAS
# ==========================================

from pyspark.sql.functions import *

fact_ventas_df = spark.read.format("delta").load(
    f"{gold_base_path}/fact_ventas"
)

dim_productos_df = spark.read.format("delta").load(
    f"{gold_base_path}/dim_productos"
)

# ==========================================
# JOIN PARA TRAER CATEGORIA
# ==========================================

ventas_enriquecidas = fact_ventas_df.alias("v") \
    .join(
        dim_productos_df.select(
            "art_id",
            "id_categ_n1"
        ).dropDuplicates(["art_id"]).alias("p"),
        col("v.art_id") == col("p.art_id"),
        "left"
    ) \
    .select(
        "v.*",
        "p.id_categ_n1"
    )

# ==========================================
# AGREGACION
# ==========================================

agg_ventas = ventas_enriquecidas \
    .groupBy(
        "fec_venta",
        "canal_venta",
        "id_tienda",
        "id_categ_n1"
    ) \
    .agg(
        round(
            sum("vr_venta_neto"),
            2
        ).alias("ventas_netas"),

        round(
            avg("vr_venta_neto"),
            2
        ).alias("ticket_promedio"),

        countDistinct("id_trans")
        .alias("transacciones")
    )

# ==========================================
# WRITE GOLD
# ==========================================

gold_path = f"{gold_base_path}/agg_ventas_diarias"

agg_ventas.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

# ==========================================
# CREATE TABLE
# ==========================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.agg_ventas_diarias
USING DELTA
LOCATION '{gold_path}'
""")

print("agg_ventas_diarias creada")

display(agg_ventas.limit(10))

# COMMAND ----------

agg_devoluciones = fact_devoluciones \
    .groupBy(
        "motivo_desc",
        "id_categ_n1",
        "id_proveedor",
        "canal_devolucion"
    ) \
    .agg(
        round(avg("tasa_devolucion"),2)
        .alias("promedio_devolucion")
    )

agg_devoluciones.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/agg_devoluciones_categoria")

# COMMAND ----------

# ==========================================
# KPI EJECUTIVO DIARIO
# ==========================================

from pyspark.sql.functions import *

fact_ventas_df = spark.read.format("delta").load(
    f"{gold_base_path}/fact_ventas"
)

kpi_df = fact_ventas_df \
    .groupBy(
        "fec_venta",
        "canal_venta"
    ) \
    .agg(

        round(
            sum("vr_venta_neto"),
            2
        ).alias("ventas_netas"),

        round(
            avg("vr_venta_neto"),
            2
        ).alias("ticket_promedio"),

        countDistinct("id_trans")
        .alias("total_transacciones"),

        round(
            avg("venta_descuento_flag") * 100,
            2
        ).alias("porcentaje_ventas_descuento")
    )

# ==========================================
# WRITE GOLD
# ==========================================

gold_path = f"{gold_base_path}/kpi_ejecutivo_diario"

kpi_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

# ==========================================
# CREATE TABLE
# ==========================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS
{catalog_name}.{gold_schema}.kpi_ejecutivo_diario
USING DELTA
LOCATION '{gold_path}'
""")

print("kpi_ejecutivo_diario creada")

display(kpi_df.limit(10))

# COMMAND ----------

# ==========================================
# DATA QUALITY TESTS
# ==========================================

tests = []

# TEST 1
nulls = fact_ventas.filter(
    col("id_trans").isNull()
).count()

tests.append(
    ("fact_ventas_pk_not_null", nulls == 0)
)

# TEST 2
duplicates = fact_ventas.count() - \
             fact_ventas.dropDuplicates().count()

tests.append(
    ("fact_ventas_no_duplicates", duplicates == 0)
)

# TEST 3
negative_sales = fact_ventas.filter(
    col("vr_venta_neto") < 0
).count()

tests.append(
    ("ventas_no_negativas", negative_sales == 0)
)

# TEST 4
invalid_stock = fact_inventario.filter(
    col("stock_fisico") < 0
).count()

tests.append(
    ("stock_no_negativo", invalid_stock == 0)
)

# TEST 5
invalid_rfm = rfm_df.filter(
    col("grupo_valor").isNull()
).count()

tests.append(
    ("rfm_segmentado", invalid_rfm == 0)
)

tests_df = spark.createDataFrame(
    tests,
    ["test_name", "passed"]
)

display(tests_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE retail_catalog.gold.fact_ventas
# MAGIC ZORDER BY (canal_venta, art_id, id_tienda);
# MAGIC
# MAGIC OPTIMIZE retail_catalog.gold.fact_inventario
# MAGIC ZORDER BY (art_id, id_tienda);
# MAGIC
# MAGIC OPTIMIZE retail_catalog.gold.fact_rfm_clientes
# MAGIC ZORDER BY (grupo_valor);