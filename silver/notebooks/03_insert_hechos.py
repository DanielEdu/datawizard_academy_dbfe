# Databricks notebook source
# MAGIC %md
# MAGIC # Silver paso a paso · 03 · Hechos inmutables: insert-only
# MAGIC
# MAGIC **Familia:** *Hecho inmutable* — `pagos`, `gestiones_cobranza`. (Sesión 15)
# MAGIC
# MAGIC Un pago **pasó** y no cambia: si mañana llega otra vez (un reproceso, un reenvío), no es una versión
# MAGIC nueva, es un duplicado. Silver solo tiene que **no duplicarlo**.
# MAGIC
# MAGIC Diferencia con el notebook 01: el MERGE **no tiene `WHEN MATCHED`**. Si la clave ya existe, no se toca.
# MAGIC Reprocesar un archivo no cambia nada, ni siquiera si trae un valor distinto.

# COMMAND ----------

CONFIG = {
    "pagos": {
        "clave": ["id_pago"],
        "orden": ["fecha_modificacion", "fecha_creacion", "fecha_pago"],
        "reglas": {"clave": "id_pago IS NOT NULL",
                   "monto_positivo": "monto_pagado > 0"},
        # Tipar: los tipos del DDL de silver.cobranzas.pagos
        "columnas": [
            "try_cast(id_pago AS BIGINT) AS id_pago",
            "try_cast(numero_credito AS BIGINT) AS numero_credito",
            "try_cast(numero_cuota AS SMALLINT) AS numero_cuota",
            "try_cast(fecha_pago AS TIMESTAMP) AS fecha_pago",
            "try_cast(monto_pagado AS DECIMAL(12,2)) AS monto_pagado",
            "try_cast(medio_pago AS STRING) AS medio_pago",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_modificacion AS TIMESTAMP) AS fecha_modificacion",
        ],
        # MERGE: condición y valores columna por columna
        "condicion": "t.id_pago = s.id_pago",
        "valores": {
            "id_pago": "s.id_pago",
            "numero_credito": "s.numero_credito",
            "numero_cuota": "s.numero_cuota",
            "fecha_pago": "s.fecha_pago",
            "monto_pagado": "s.monto_pagado",
            "medio_pago": "s.medio_pago",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_modificacion": "s.fecha_modificacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
    },
    "gestiones_cobranza": {
        "clave": ["id_gestion"],
        "orden": ["fecha_modificacion", "fecha_creacion", "fecha_gestion"],
        "reglas": {"clave": "id_gestion IS NOT NULL",
                   "dias_mora_no_negativos": "dias_mora_al_momento >= 0"},
        "columnas": [
            "try_cast(id_gestion AS BIGINT) AS id_gestion",
            "try_cast(numero_credito AS BIGINT) AS numero_credito",
            "try_cast(fecha_gestion AS TIMESTAMP) AS fecha_gestion",
            "try_cast(tipo_gestion AS STRING) AS tipo_gestion",
            "try_cast(resultado AS STRING) AS resultado",
            "try_cast(dias_mora_al_momento AS SMALLINT) AS dias_mora_al_momento",
            "try_cast(gestor AS STRING) AS gestor",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_modificacion AS TIMESTAMP) AS fecha_modificacion",
        ],
        "condicion": "t.id_gestion = s.id_gestion",
        "valores": {
            "id_gestion": "s.id_gestion",
            "numero_credito": "s.numero_credito",
            "fecha_gestion": "s.fecha_gestion",
            "tipo_gestion": "s.tipo_gestion",
            "resultado": "s.resultado",
            "dias_mora_al_momento": "s.dias_mora_al_momento",
            "gestor": "s.gestor",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_modificacion": "s.fecha_modificacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
    },
}

dbutils.widgets.dropdown("tabla", "pagos", list(CONFIG), "Tabla")
dbutils.widgets.text("catalogo_bronze", "bronze", "Catálogo Bronze")
dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("sufijo_destino", "_lab", "Sufijo destino (vacío = tabla real)")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver", "Raíz de checkpoints")

tabla = dbutils.widgets.get("tabla")
cfg = CONFIG[tabla]
sufijo = dbutils.widgets.get("sufijo_destino")
silver = dbutils.widgets.get("catalogo_silver")
origen = f"{dbutils.widgets.get('catalogo_bronze')}.cobranzas.{tabla}_brz"
tabla_ddl = f"{silver}.cobranzas.{tabla}"
destino = f"{tabla_ddl}{sufijo}"
cuarentena = f"{silver}.cobranzas._cuarentena{sufijo}"
checkpoint = f"{dbutils.widgets.get('checkpoints').rstrip('/')}/cobranzas/{tabla}{sufijo}"

if sufijo:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {tabla_ddl}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cuarentena} LIKE {silver}.cobranzas._cuarentena")

print(f"Origen: {origen}\nDestino: {destino}\nCheckpoint: {checkpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El motor
# MAGIC
# MAGIC Tipar y validar son iguales al notebook 01. Cambian dos cosas:
# MAGIC
# MAGIC - **Dedup en el lote**: si el mismo pago llega dos veces en el lote, se queda **la primera** que llegó
# MAGIC   (un hecho no se corrige con una versión posterior).
# MAGIC - **MERGE**: solo `whenNotMatchedInsert`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def tipar(lote, columnas):
    return lote.selectExpr(*columnas,
                           "_metadata.file_path AS _origen_archivo",
                           "_ingestion_ts AS _bronze_ingestion_ts",
                           "current_timestamp() AS _procesado_ts")


def separar_invalidas(df, reglas):
    fallos = [F.when(~F.coalesce(F.expr(e), F.lit(False)), F.lit(n)) for n, e in reglas.items()]
    df = df.withColumn("_motivos", F.filter(F.array(*fallos), lambda x: x.isNotNull()))
    return df.filter(F.size("_motivos") == 0).drop("_motivos"), df.filter(F.size("_motivos") > 0)


def enviar_a_cuarentena(invalidas, batch_id):
    (invalidas.select(F.lit(tabla).alias("tabla"),
                      F.array_join("_motivos", ", ").alias("motivos"),
                      F.to_json(F.struct(*invalidas.drop("_motivos").columns)).alias("fila"),
                      "_origen_archivo", F.lit(batch_id).alias("_batch_id"),
                      F.current_timestamp().alias("_procesado_ts"))
     .write.mode("append").saveAsTable(cuarentena))


def primera_version(df, clave, orden):
    """Una fila por clave: la PRIMERA que llegó. La columna de orden queda siempre llena (NOT NULL)."""
    df = df.withColumn(orden[0], F.coalesce(*orden))
    w = Window.partitionBy(*clave).orderBy(F.col("_bronze_ingestion_ts").asc(), F.col(orden[0]).asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def insertar_nuevos(df, condicion, valores):
    """Insert-only: sin WHEN MATCHED. Lo que ya existe no se toca."""
    (DeltaTable.forName(spark, destino).alias("t")
        .merge(df.alias("s"), condicion)
        .whenNotMatchedInsert(values=valores)
        .execute())


def procesar_lote(lote, batch_id):
    validas, invalidas = separar_invalidas(tipar(lote, cfg["columnas"]), cfg["reglas"])
    enviar_a_cuarentena(invalidas, batch_id)
    insertar_nuevos(primera_version(validas, cfg["clave"], cfg["orden"]), cfg["condicion"], cfg["valores"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Correr

# COMMAND ----------

(spark.readStream.table(origen)
    .writeStream.foreachBatch(procesar_lote)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start().awaitTermination())

print(f"Silver: {spark.table(destino).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validar

# COMMAND ----------

clave_sql = ", ".join(cfg["clave"])
display(spark.sql(f"""
    SELECT
      (SELECT count(*) FROM {origen})                                  AS filas_bronze,
      (SELECT count(DISTINCT {clave_sql}) FROM {origen})               AS claves_bronze,
      (SELECT count(*) FROM {destino})                                 AS filas_silver,
      (SELECT count(*) FROM {cuarentena} WHERE tabla = '{tabla}')      AS filas_cuarentena
"""))

# COMMAND ----------

# Hechos reenviados con otro valor: en Bronze hay dos versiones, en Silver quedó la primera
valor = "monto_pagado" if tabla == "pagos" else "resultado"
display(spark.sql(f"""
    WITH reenviados AS (
      SELECT {clave_sql} FROM {origen}
      GROUP BY {clave_sql} HAVING count(DISTINCT {valor}) > 1)
    SELECT b.{clave_sql}, b.{valor} AS valor_bronze, b._metadata.file_name AS archivo,
           s.{valor} AS valor_silver
    FROM {origen} b
    JOIN reenviados USING ({clave_sql})
    JOIN {destino} s ON s.{clave_sql} = try_cast(b.{clave_sql} AS BIGINT)
    ORDER BY b.{clave_sql}, b._ingestion_ts
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC La consulta de arriba se llena después de ingestar un delta de cobranzas: trae casos `hecho_modificado`
# MAGIC (el mismo pago con otro monto). Silver conserva el valor original.
# MAGIC
# MAGIC ## Reiniciar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC dbutils.fs.rm(checkpoint, recurse=True)
# MAGIC ```
