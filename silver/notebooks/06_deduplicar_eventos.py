# Databricks notebook source
# MAGIC %md
# MAGIC # Silver paso a paso · 06 · Eventos: dedup con watermark
# MAGIC
# MAGIC **Familia:** *Evento* — `eventos_app_brz` → `eventos_app`. (Sesión 17)
# MAGIC
# MAGIC Los eventos de la app no se comportan como una tabla:
# MAGIC
# MAGIC - **No cambian**: un `oferta_click` pasó y ya. No hay MERGE.
# MAGIC - **Llegan duplicados**: Kafka entrega *at-least-once*; el productor reintenta y el mismo evento entra dos veces.
# MAGIC - **Llegan tarde y desordenados**: el teléfono estuvo sin red y manda todo junto.
# MAGIC - **Son muchos y no paran**: no se puede recordar cada `id_evento` para siempre.
# MAGIC
# MAGIC La herramienta: **watermark** + `dropDuplicatesWithinWatermark`. Recuerda cada `id_evento` solo mientras
# MAGIC esté dentro de la ventana de tolerancia; lo que llega más tarde que eso se descarta.

# COMMAND ----------

dbutils.widgets.text("catalogo_bronze", "bronze", "Catálogo Bronze")
dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("sufijo_destino", "_lab", "Sufijo destino (vacío = tabla real)")
dbutils.widgets.text("watermark", "2 hours", "Watermark (tolerancia de retraso)")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver", "Raíz de checkpoints")

sufijo = dbutils.widgets.get("sufijo_destino")
silver = dbutils.widgets.get("catalogo_silver")
watermark = dbutils.widgets.get("watermark")
origen = f"{dbutils.widgets.get('catalogo_bronze')}.app.eventos_app_brz"
tabla_ddl = f"{silver}.app.eventos_app"
destino = f"{tabla_ddl}{sufijo}"
checkpoint = f"{dbutils.widgets.get('checkpoints').rstrip('/')}/app/eventos_app{sufijo}"

if sufijo:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {tabla_ddl}")

print(f"Origen: {origen}\nDestino: {destino}\nWatermark: {watermark}\nCheckpoint: {checkpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Elegir el watermark con datos, no a ojo
# MAGIC
# MAGIC El watermark es un trato: *"espero hasta X de retraso; lo que llegue después, lo pierdo"*.
# MAGIC Se elige mirando el retraso real en Bronze (cuándo llegó vs. cuándo ocurrió).

# COMMAND ----------

display(spark.sql(f"""
    SELECT percentile(retraso, 0.50)  AS p50_seg,
           percentile(retraso, 0.99)  AS p99_seg,
           percentile(retraso, 0.999) AS p999_seg,
           max(retraso)               AS max_seg
    FROM (SELECT unix_timestamp(_kafka_timestamp) - unix_timestamp(timestamp_evento) AS retraso
          FROM {origen} WHERE timestamp_evento IS NOT NULL)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El stream
# MAGIC
# MAGIC - **Tipar y renombrar** a las columnas de Silver; `retraso_seg` mide cuánto tardó cada evento.
# MAGIC - **Filtrar lo que no tiene clave ni tiempo**: los payloads corruptos (`id_evento` NULL) se quedan en
# MAGIC   Bronze con su `_payload_crudo` — ahí se revisan, no se pierden.
# MAGIC - **`withWatermark` + `dropDuplicatesWithinWatermark(["id_evento"])`**: dedup con estado acotado.
# MAGIC - **Append**: cada evento se escribe una vez. Sin MERGE.
# MAGIC
# MAGIC Un `tipo_evento` desconocido **se acepta**: un tipo nuevo es una señal (la app cambió), no un error.

# COMMAND ----------

from pyspark.sql import functions as F

eventos = (spark.readStream.table(origen)
    .select(
        "id_evento", "tipo_evento",
        F.col("timestamp_evento").alias("ts_evento"),
        F.to_date("timestamp_evento").alias("fecha_evento"),
        F.col("id_cliente").cast("bigint").alias("id_cliente"),
        F.col("id_oferta").cast("bigint").alias("id_oferta"),
        F.col("id_pais").cast("smallint").alias("id_pais"),
        "canal", "id_sesion", "version_app", "sistema_operativo", "ubicacion_pantalla",
        F.col("posicion").cast("int").alias("posicion"),
        F.col("tiempo_visible_seg").cast("decimal(6,2)").alias("tiempo_visible_seg"),
        F.col("monto_simulado").cast("decimal(14,2)").alias("monto_simulado"),
        F.col("plazo_simulado").cast("smallint").alias("plazo_simulado"),
        (F.unix_timestamp("_ingestion_ts") - F.unix_timestamp("timestamp_evento")).alias("retraso_seg"),
        "_kafka_offset",
        F.col("_ingestion_ts").alias("_bronze_ingestion_ts"),
        F.current_timestamp().alias("_procesado_ts"),
    )
    .filter("id_evento IS NOT NULL AND ts_evento IS NOT NULL AND id_cliente IS NOT NULL")
    .withWatermark("ts_evento", watermark)
    .dropDuplicatesWithinWatermark(["id_evento"]))

(eventos.writeStream
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(destino)
    .awaitTermination())

print(f"Silver: {spark.table(destino).count():,} eventos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validar

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      (SELECT count(*) FROM {origen})                              AS mensajes_bronze,
      (SELECT count(*) FROM {origen} WHERE id_evento IS NULL)      AS corruptos_en_bronze,
      (SELECT count(DISTINCT id_evento) FROM {origen})             AS eventos_unicos_bronze,
      (SELECT count(*) FROM {destino})                             AS eventos_silver,
      (SELECT count(*) - count(DISTINCT id_evento) FROM {destino}) AS duplicados_en_silver
"""))

# COMMAND ----------

# Distribución por tipo (el funnel), incluidos los tipos nuevos
display(spark.sql(f"SELECT tipo_evento, count(*) AS eventos FROM {destino} GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qué pasa con cada caso del productor
# MAGIC
# MAGIC Cada archivo de `productor_eventos.py` trae un manifiesto (`casos_silver_<sufijo>.csv`):
# MAGIC
# MAGIC | Caso | Resultado esperado en Silver |
# MAGIC |---|---|
# MAGIC | `duplicado` | Una sola fila |
# MAGIC | `llegada_tardia` (30–90 min) | Entra; `retraso_seg` alto |
# MAGIC | `fuera_watermark` (3–6 h) | Se descarta **si ya se procesó un lote más nuevo** (el watermark avanza entre lotes) |
# MAGIC | `reenvio_anterior` | Se cuela: el estado de dedup ya olvidó ese `id_evento` (estado acotado) |
# MAGIC | `payload_corrupto` | No llega: se queda en Bronze con `id_evento` NULL |
# MAGIC | `tipo_desconocido` | Entra |
# MAGIC
# MAGIC Para ver el watermark en acción: corre este notebook, genera otro archivo con el productor, ingéstalo a
# MAGIC Bronze (notebook de Bronze 02 o el job) y vuelve a correr el paso 2. Compara los `id_evento` del
# MAGIC manifiesto nuevo con Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reiniciar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC dbutils.fs.rm(checkpoint, recurse=True)
# MAGIC ```
