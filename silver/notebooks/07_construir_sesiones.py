# Databricks notebook source
# MAGIC %md
# MAGIC # Silver paso a paso · 07 · Sesiones: agregación con ventanas de sesión
# MAGIC
# MAGIC **Familia:** *Evento agregado* — `eventos_app` → `sesiones_app`. (Sesión 17)
# MAGIC
# MAGIC Un evento suelto dice poco. Una **sesión** cuenta una historia: *el cliente vio 3 ofertas, hizo click,
# MAGIC simuló y no inició la solicitud*. La sesión se cierra tras **30 minutos sin actividad**.
# MAGIC
# MAGIC En streaming hay un problema: ¿cuándo sé que una sesión terminó? Si llega un evento tarde, la sesión podría
# MAGIC seguir abierta. El **watermark** responde: cuando el tiempo de los eventos avanza más allá del fin de la
# MAGIC sesión + la tolerancia, la sesión se da por cerrada y se escribe **una sola vez** (modo append).
# MAGIC
# MAGIC Lee la salida del notebook 06 (`eventos_app_lab` por defecto): córrelo antes.

# COMMAND ----------

dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("sufijo_destino", "_lab", "Sufijo (origen y destino; vacío = tablas reales)")
dbutils.widgets.text("watermark", "2 hours", "Watermark (tolerancia de retraso)")
dbutils.widgets.text("inactividad", "30 minutes", "Inactividad que cierra la sesión")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver", "Raíz de checkpoints")

sufijo = dbutils.widgets.get("sufijo_destino")
silver = dbutils.widgets.get("catalogo_silver")
watermark = dbutils.widgets.get("watermark")
inactividad = dbutils.widgets.get("inactividad")
origen = f"{silver}.app.eventos_app{sufijo}"
tabla_ddl = f"{silver}.app.sesiones_app"
destino = f"{tabla_ddl}{sufijo}"
checkpoint = f"{dbutils.widgets.get('checkpoints').rstrip('/')}/app/sesiones_app{sufijo}"

if sufijo:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {tabla_ddl}")

print(f"Origen: {origen}\nDestino: {destino}\nWatermark: {watermark} · inactividad: {inactividad}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Las ventanas de sesión
# MAGIC
# MAGIC `session_window(ts, "30 minutes")` agrupa eventos del mismo grupo (`id_sesion`, cliente, canal) mientras
# MAGIC entre uno y otro haya menos de 30 minutos. A diferencia de una ventana fija, su largo depende de los datos.

# COMMAND ----------

from pyspark.sql import functions as F

es = lambda tipo: F.col("tipo_evento") == tipo   # noqa: E731

sesiones = (spark.readStream.table(origen)
    .withWatermark("ts_evento", watermark)
    .groupBy("id_sesion", "id_cliente", "canal",
             F.session_window("ts_evento", inactividad).alias("ventana"))
    .agg(F.count("*").alias("n_eventos"),
         F.sum(F.when(es("oferta_vista"), 1).otherwise(0)).alias("n_vistas"),
         F.max(es("oferta_click")).alias("hizo_click"),
         F.max(es("simulacion_realizada")).alias("simulo"),
         F.max(es("solicitud_iniciada")).alias("inicio_solicitud"),
         F.max("monto_simulado").alias("monto_simulado_max"))
    .select("id_sesion",
            F.col("ventana.start").alias("inicio_sesion"),
            F.col("ventana.end").alias("fin_sesion"),
            "id_cliente", "canal", "n_eventos", "n_vistas", "hizo_click", "simulo",
            "inicio_solicitud", "monto_simulado_max",
            F.current_timestamp().alias("_procesado_ts")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Escribir en modo append
# MAGIC
# MAGIC En modo `append` una sesión se escribe **cuando el watermark confirma que cerró**. Las sesiones de los
# MAGIC últimos `watermark + inactividad` todavía pueden recibir eventos: quedan en el estado del checkpoint y
# MAGIC salen en una corrida posterior.

# COMMAND ----------

(sesiones.writeStream
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(destino)
    .awaitTermination())

print(f"Sesiones cerradas en Silver: {spark.table(destino).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validar y usar

# COMMAND ----------

# Cuántas sesiones cerró y cuántas siguen abiertas (eventos más nuevos que el watermark)
display(spark.sql(f"""
    SELECT
      (SELECT count(DISTINCT id_sesion) FROM {origen})  AS sesiones_con_eventos,
      (SELECT count(*) FROM {destino})                  AS sesiones_cerradas,
      (SELECT max(ts_evento) FROM {origen})             AS ultimo_evento,
      (SELECT max(fin_sesion) FROM {destino})           AS ultima_sesion_cerrada
"""))

# COMMAND ----------

# El funnel por sesión: de las que vieron una oferta, cuántas hicieron click, simularon y solicitaron
display(spark.sql(f"""
    SELECT canal,
           count(*)                       AS sesiones,
           round(avg(n_eventos), 2)       AS eventos_por_sesion,
           count_if(hizo_click)           AS con_click,
           count_if(simulo)               AS con_simulacion,
           count_if(inicio_solicitud)     AS con_solicitud
    FROM {destino}
    GROUP BY canal ORDER BY sesiones DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Probar.** Genera más eventos con el productor, ingéstalos a Bronze, corre el notebook 06 y luego este:
# MAGIC aparecen las sesiones que quedaron abiertas en la corrida anterior.
# MAGIC
# MAGIC ## Reiniciar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC dbutils.fs.rm(checkpoint, recurse=True)
# MAGIC ```
