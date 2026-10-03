# Databricks notebook source
# MAGIC %md
# MAGIC # Silver paso a paso · 02 · Referencia externa: gana el último archivo
# MAGIC
# MAGIC **Familia:** *Catálogo de referencia externa* — `tipos_cambio`. (Sesión 15)
# MAGIC
# MAGIC Un proveedor externo publica el tipo de cambio de cada día. Cuando se equivoca, **reenvía el mismo día
# MAGIC corregido**. La fila no trae una fecha de modificación confiable, así que el orden lo da el
# MAGIC **momento de ingesta**: la versión del archivo más reciente gana.
# MAGIC
# MAGIC | | Notebook 01 (transaccional) | Este notebook |
# MAGIC |---|---|---|
# MAGIC | Clave | Un id | Compuesta: `(fecha, moneda_origen, moneda_destino)` |
# MAGIC | Orden | `fecha_actualizacion` de la fuente | `_bronze_ingestion_ts` (cuándo llegó el archivo) |
# MAGIC | Corrección | Una versión con fecha más nueva | El mismo día reenviado en otro archivo |

# COMMAND ----------

dbutils.widgets.text("catalogo_bronze", "bronze", "Catálogo Bronze")
dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("sufijo_destino", "_lab", "Sufijo destino (vacío = tabla real)")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver", "Raíz de checkpoints")

TABLA = "tipos_cambio"
CLAVE = ["fecha", "moneda_origen", "moneda_destino"]
REGLAS = {
    "clave": "fecha IS NOT NULL AND moneda_origen IS NOT NULL AND moneda_destino IS NOT NULL",
    "tasas_validas": "tasa_compra > 0 AND tasa_venta >= tasa_compra",
}

sufijo = dbutils.widgets.get("sufijo_destino")
silver = dbutils.widgets.get("catalogo_silver")
origen = f"{dbutils.widgets.get('catalogo_bronze')}.lending.{TABLA}_brz"
tabla_ddl = f"{silver}.lending.{TABLA}"
destino = f"{tabla_ddl}{sufijo}"
cuarentena = f"{silver}.lending._cuarentena{sufijo}"
checkpoint = f"{dbutils.widgets.get('checkpoints').rstrip('/')}/lending/{TABLA}{sufijo}"

if sufijo:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {tabla_ddl}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cuarentena} LIKE {silver}.lending._cuarentena")

print(f"Origen: {origen}\nDestino: {destino}\nCheckpoint: {checkpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El problema: el mismo día, varias versiones

# COMMAND ----------

display(spark.sql(f"""
    SELECT fecha, moneda_origen, moneda_destino, tasa_compra, tasa_venta,
           _metadata.file_name AS archivo, _ingestion_ts
    FROM {origen}
    WHERE (fecha, moneda_origen) IN (SELECT fecha, moneda_origen FROM {origen}
                                     GROUP BY fecha, moneda_origen, moneda_destino
                                     HAVING count(*) > 1 LIMIT 3)
    ORDER BY fecha, moneda_origen, _ingestion_ts
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El motor
# MAGIC
# MAGIC Los mismos pasos del notebook 01. Lo único distinto: el **orden** es `_bronze_ingestion_ts`
# MAGIC y la guarda del MERGE compara ese valor.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TECNICAS = ["_origen_archivo", "_bronze_ingestion_ts", "_procesado_ts"]


def tipar(lote, schema_destino):
    columnas = [F.expr(f"try_cast(`{f.name}` AS {f.dataType.simpleString()})").alias(f.name)
                for f in schema_destino if f.name not in TECNICAS and f.name in lote.columns]
    return lote.select(*columnas,
                       F.col("_metadata.file_path").alias("_origen_archivo"),
                       F.col("_ingestion_ts").alias("_bronze_ingestion_ts"),
                       F.current_timestamp().alias("_procesado_ts"))


def separar_invalidas(df, reglas):
    fallos = [F.when(~F.coalesce(F.expr(e), F.lit(False)), F.lit(n)) for n, e in reglas.items()]
    df = df.withColumn("_motivos", F.filter(F.array(*fallos), lambda x: x.isNotNull()))
    return df.filter(F.size("_motivos") == 0).drop("_motivos"), df.filter(F.size("_motivos") > 0)


def enviar_a_cuarentena(invalidas, batch_id):
    (invalidas.select(F.lit(TABLA).alias("tabla"),
                      F.array_join("_motivos", ", ").alias("motivos"),
                      F.to_json(F.struct(*[c for c in invalidas.columns if c != "_motivos"])).alias("fila"),
                      "_origen_archivo", F.lit(batch_id).alias("_batch_id"),
                      F.current_timestamp().alias("_procesado_ts"))
     .write.mode("append").saveAsTable(cuarentena))


def ultimo_archivo(df):
    """Una fila por día y moneda: la del archivo ingestado más tarde.
    Si dos archivos entraron a Bronze en la misma corrida (mismo _ingestion_ts), desempata el nombre."""
    w = Window.partitionBy(*CLAVE).orderBy(F.col("_bronze_ingestion_ts").desc(),
                                           F.col("_origen_archivo").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def mergear(df):
    valores = {c: f"s.{c}" for c in df.columns}
    (DeltaTable.forName(spark, destino).alias("t")
        .merge(df.alias("s"), " AND ".join(f"t.{c} = s.{c}" for c in CLAVE))
        # Guarda: solo una versión que llegó DESPUÉS pisa a la guardada
        .whenMatchedUpdate(condition="s._bronze_ingestion_ts > t._bronze_ingestion_ts", set=valores)
        .whenNotMatchedInsert(values=valores)
        .execute())


def procesar_lote(lote, batch_id):
    validas, invalidas = separar_invalidas(tipar(lote, spark.table(destino).schema), REGLAS)
    enviar_a_cuarentena(invalidas, batch_id)
    mergear(ultimo_archivo(validas))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Correr

# COMMAND ----------

(spark.readStream.table(origen)
    .writeStream.foreachBatch(procesar_lote)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start().awaitTermination())

print(f"Silver: {spark.table(destino).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validar

# COMMAND ----------

# Una fila por día y moneda
display(spark.sql(f"""
    SELECT count(*) AS filas,
           count(DISTINCT fecha, moneda_origen, moneda_destino) AS claves
    FROM {destino}
"""))

# COMMAND ----------

# Cada día quedó con la tasa del último archivo que lo trajo
display(spark.sql(f"""
    WITH ultima AS (
      SELECT fecha, moneda_origen, moneda_destino,
             max_by(tasa_venta, struct(_ingestion_ts, _metadata.file_path)) AS tasa_venta_ultimo_archivo
      FROM {origen}
      WHERE try_cast(tasa_venta AS DECIMAL(12,6)) >= try_cast(tasa_compra AS DECIMAL(12,6))
      GROUP BY ALL)
    SELECT count(*) AS dias_que_no_calzan
    FROM {destino} s
    JOIN ultima u ON s.fecha = try_cast(u.fecha AS DATE)
                 AND s.moneda_origen = u.moneda_origen AND s.moneda_destino = u.moneda_destino
    WHERE s.tasa_venta != try_cast(u.tasa_venta_ultimo_archivo AS DECIMAL(12,6))
"""))

# COMMAND ----------

display(spark.sql(f"SELECT motivos, count(*) AS filas FROM {cuarentena} WHERE tabla = '{TABLA}' GROUP BY motivos"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Probar la corrección.** El delta de `generar_datos_wizard_bank.py` trae casos `correccion` (mismo día,
# MAGIC tasa distinta). Ingéstalo a Bronze y corre el paso 3: esos días cambian a la tasa nueva.
# MAGIC
# MAGIC ## Reiniciar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC dbutils.fs.rm(checkpoint, recurse=True)
# MAGIC ```
