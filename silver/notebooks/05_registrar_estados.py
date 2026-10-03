# Databricks notebook source
# MAGIC %md
# MAGIC # Silver paso a paso · 05 · Historial de estados
# MAGIC
# MAGIC **Familia:** *Historial de estados* — `solicitudes_prestamo_brz` → `solicitudes_estados`. (Sesión 16)
# MAGIC
# MAGIC Sigamos a la solicitud 363 durante una semana. Cada día la fuente manda un archivo con lo que cambió:
# MAGIC
# MAGIC | Archivo | Estado |
# MAGIC |---|---|
# MAGIC | lunes | `Iniciada` |
# MAGIC | martes | `En evaluacion` |
# MAGIC | jueves | `Aprobada` |
# MAGIC
# MAGIC Bronze guarda las tres. Silver tiene dos formas de quedarse con ellas:
# MAGIC
# MAGIC | | `solicitudes_prestamo` (notebook 01) | `solicitudes_estados` (este) |
# MAGIC |---|---|---|
# MAGIC | Guarda de la 363 | 1 fila: `Aprobada` | 3 filas: el recorrido |
# MAGIC | Responde | ¿En qué estado está **ahora**? | ¿Por qué estados pasó y **cuánto tardó** en cada uno? |
# MAGIC | Clave | `id_solicitud` | `(id_solicitud, fecha_actualizacion)` |
# MAGIC | Carga | Upsert: la nueva pisa a la anterior | **Insert-only**: cada versión se agrega |

# COMMAND ----------

dbutils.widgets.text("catalogo_bronze", "bronze", "Catálogo Bronze")
dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("sufijo_destino", "_lab", "Sufijo destino (vacío = tabla real)")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver", "Raíz de checkpoints")

CLAVE = ["id_solicitud", "fecha_actualizacion"]
REGLAS = {
    "clave": "id_solicitud IS NOT NULL",
    "estado_valido": "estado_solicitud IN ('Iniciada','En evaluacion','Aprobada','Rechazada','Desistida')",
}

sufijo = dbutils.widgets.get("sufijo_destino")
silver = dbutils.widgets.get("catalogo_silver")
origen = f"{dbutils.widgets.get('catalogo_bronze')}.lending.solicitudes_prestamo_brz"
tabla_ddl = f"{silver}.lending.solicitudes_estados"
destino = f"{tabla_ddl}{sufijo}"
cuarentena = f"{silver}.lending._cuarentena{sufijo}"
checkpoint = f"{dbutils.widgets.get('checkpoints').rstrip('/')}/lending/solicitudes_estados{sufijo}"

if sufijo:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {tabla_ddl}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cuarentena} LIKE {silver}.lending._cuarentena")

print(f"Origen: {origen}\nDestino: {destino}\nCheckpoint: {checkpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El motor
# MAGIC
# MAGIC 1. **Tipar** y **validar** como en el notebook 01.
# MAGIC 2. **La versión siempre tiene fecha**: las filas de la primera carga no traen `fecha_actualizacion`;
# MAGIC    toman `fecha_creacion` o `fecha_hora_solicitud`.
# MAGIC 3. **Dedup por `(id, fecha)`**: el mismo archivo reprocesado trae la misma versión → una sola.
# MAGIC 4. **Insert-only**: si la versión `(id, fecha)` ya está, no se toca. Reprocesar no duplica.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def tipar(lote):
    """Castea cada columna al tipo del DDL de solicitudes_estados."""
    return lote.selectExpr(
        "try_cast(id_solicitud AS BIGINT) AS id_solicitud",
        "try_cast(fecha_actualizacion AS TIMESTAMP) AS fecha_actualizacion",
        "try_cast(estado_solicitud AS STRING) AS estado_solicitud",
        "try_cast(score_evaluacion AS SMALLINT) AS score_evaluacion",
        "try_cast(decision_motor AS STRING) AS decision_motor",
        "try_cast(monto_aprobado AS DECIMAL(14,2)) AS monto_aprobado",
        "try_cast(tasa_aprobada AS DECIMAL(6,3)) AS tasa_aprobada",
        "try_cast(motivo_rechazo AS STRING) AS motivo_rechazo",
        "try_cast(fecha_hora_resolucion AS TIMESTAMP) AS fecha_hora_resolucion",
        # No están en el destino, pero hacen falta como respaldo del orden
        "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
        "try_cast(fecha_hora_solicitud AS TIMESTAMP) AS fecha_hora_solicitud",
        "_metadata.file_path AS _origen_archivo",
        "_ingestion_ts AS _bronze_ingestion_ts",
        "current_timestamp() AS _procesado_ts",
    )


def separar_invalidas(df, reglas):
    fallos = [F.when(~F.coalesce(F.expr(e), F.lit(False)), F.lit(n)) for n, e in reglas.items()]
    df = df.withColumn("_motivos", F.filter(F.array(*fallos), lambda x: x.isNotNull()))
    return df.filter(F.size("_motivos") == 0).drop("_motivos"), df.filter(F.size("_motivos") > 0)


def enviar_a_cuarentena(invalidas, batch_id):
    (invalidas.select(F.lit("solicitudes_estados").alias("tabla"),
                      F.array_join("_motivos", ", ").alias("motivos"),
                      F.to_json(F.struct(*invalidas.drop("_motivos").columns)).alias("fila"),
                      "_origen_archivo", F.lit(batch_id).alias("_batch_id"),
                      F.current_timestamp().alias("_procesado_ts"))
     .write.mode("append").saveAsTable(cuarentena))


def procesar_lote(lote, batch_id):
    validas, invalidas = separar_invalidas(tipar(lote), REGLAS)
    enviar_a_cuarentena(invalidas, batch_id)

    versiones = (validas
        .withColumn("fecha_actualizacion",
                    F.coalesce(F.col("fecha_actualizacion"), F.col("fecha_creacion"), F.col("fecha_hora_solicitud")))
        .dropDuplicates(CLAVE)
        .select("id_solicitud", "fecha_actualizacion", "estado_solicitud", "score_evaluacion",
                "decision_motor", "monto_aprobado", "tasa_aprobada", "motivo_rechazo",
                "fecha_hora_resolucion", "_origen_archivo", "_bronze_ingestion_ts", "_procesado_ts"))

    (DeltaTable.forName(spark, destino).alias("t")
        .merge(versiones.alias("s"),
               "t.id_solicitud = s.id_solicitud AND t.fecha_actualizacion = s.fecha_actualizacion")
        .whenNotMatchedInsert(values={
            "id_solicitud": "s.id_solicitud",
            "fecha_actualizacion": "s.fecha_actualizacion",
            "estado_solicitud": "s.estado_solicitud",
            "score_evaluacion": "s.score_evaluacion",
            "decision_motor": "s.decision_motor",
            "monto_aprobado": "s.monto_aprobado",
            "tasa_aprobada": "s.tasa_aprobada",
            "motivo_rechazo": "s.motivo_rechazo",
            "fecha_hora_resolucion": "s.fecha_hora_resolucion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts"})
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Correr

# COMMAND ----------

(spark.readStream.table(origen)
    .writeStream.foreachBatch(procesar_lote)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start().awaitTermination())

display(spark.sql(f"""
    SELECT count(*) AS versiones, count(DISTINCT id_solicitud) AS solicitudes,
           round(count(*) / count(DISTINCT id_solicitud), 2) AS versiones_por_solicitud
    FROM {destino}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consultar el recorrido

# COMMAND ----------

# El recorrido de una solicitud con más de un estado
display(spark.sql(f"""
    SELECT id_solicitud, fecha_actualizacion, estado_solicitud, score_evaluacion, monto_aprobado
    FROM {destino}
    WHERE id_solicitud = (SELECT id_solicitud FROM {destino}
                          GROUP BY id_solicitud HAVING count(DISTINCT estado_solicitud) > 1 LIMIT 1)
    ORDER BY fecha_actualizacion
"""))

# COMMAND ----------

# Cuánto tiempo pasan las solicitudes en cada estado (lo que la tabla de estado vigente no puede responder)
display(spark.sql(f"""
    SELECT estado_solicitud,
           count(*)                                         AS pasos,
           round(avg(horas_en_estado), 1)                   AS horas_promedio
    FROM (
      SELECT estado_solicitud,
             (unix_timestamp(lead(fecha_actualizacion) OVER (PARTITION BY id_solicitud
                                                             ORDER BY fecha_actualizacion))
              - unix_timestamp(fecha_actualizacion)) / 3600 AS horas_en_estado
      FROM {destino})
    WHERE horas_en_estado IS NOT NULL
    GROUP BY estado_solicitud ORDER BY pasos DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Deltas frente a CDC.** Un delta es una foto: si una solicitud pasa por dos estados entre dos
# MAGIC extracciones, el historial solo ve el último. CDC (S11.2) captura cada cambio, también los borrados.
# MAGIC
# MAGIC **Probar.** Los deltas de `generar_datos_wizard_bank.py` dejan solicitudes abiertas y las hacen avanzar
# MAGIC en el delta siguiente: después de dos deltas, el recorrido muestra varios estados.
# MAGIC
# MAGIC `desembolsos_estados` se construye igual, con la clave `(id_desembolso, fecha_actualizacion)`.
# MAGIC
# MAGIC ## Reiniciar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC dbutils.fs.rm(checkpoint, recurse=True)
# MAGIC ```
