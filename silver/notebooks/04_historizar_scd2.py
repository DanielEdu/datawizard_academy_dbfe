# Databricks notebook source
# MAGIC %md
# MAGIC # Silver paso a paso · 04 · Maestra historizada: SCD Tipo 2
# MAGIC
# MAGIC **Familia:** *Maestra historizada* — `clientes_brz` → `dim_clientes`. (Sesión 16)
# MAGIC
# MAGIC El notebook 01 guarda solo el estado vigente: si un cliente sube de ingreso, el valor viejo se pierde.
# MAGIC Pero la pregunta del regulador es *¿con qué ingreso y score se aprobó esta solicitud?* — hace falta
# MAGIC **la historia**: una fila por **versión** del cliente, con su vigencia.
# MAGIC
# MAGIC El tipo SCD se decide **por columna**:
# MAGIC
# MAGIC | Tipo | Columnas | Un cambio… |
# MAGIC |---|---|---|
# MAGIC | **2** | ingreso, situación laboral, score, nivel de riesgo | Cierra la versión vigente y abre otra |
# MAGIC | **1** | email, ciudad, nombres, apellidos | Se sobrescribe en la versión vigente |
# MAGIC | **0** | documento, fecha de nacimiento, país, campaña, fecha de registro | Nunca cambia |

# COMMAND ----------

dbutils.widgets.text("catalogo_bronze", "bronze", "Catálogo Bronze")
dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("sufijo_destino", "_lab", "Sufijo destino (vacío = tabla real)")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver", "Raíz de checkpoints")

COLS_T2 = ["ingreso_mensual_declarado", "situacion_laboral", "score_interno", "nivel_riesgo"]
COLS_T1 = ["email", "ciudad", "nombres", "apellidos"]
COLS_T0 = ["id_pais", "tipo_documento", "numero_documento", "fecha_nacimiento",
           "id_campania_captacion", "fecha_registro"]

sufijo = dbutils.widgets.get("sufijo_destino")
silver = dbutils.widgets.get("catalogo_silver")
origen = f"{dbutils.widgets.get('catalogo_bronze')}.lending.clientes_brz"
tabla_ddl = f"{silver}.lending.dim_clientes"
destino = f"{tabla_ddl}{sufijo}"
cuarentena = f"{silver}.lending._cuarentena{sufijo}"
checkpoint = f"{dbutils.widgets.get('checkpoints').rstrip('/')}/lending/dim_clientes{sufijo}"

if sufijo:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {tabla_ddl}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cuarentena} LIKE {silver}.lending._cuarentena")

print(f"Origen: {origen}\nDestino: {destino}\nCheckpoint: {checkpoint}")
display(spark.sql(f"DESCRIBE TABLE {destino}"))   # sk_cliente, vigencia, hash_atributos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparar el origen
# MAGIC
# MAGIC - **Tipar** con `try_cast` (Bronze es STRING).
# MAGIC - **Fecha efectiva del cambio**: `fecha_actualizacion`; si viene vacía (primera carga), `fecha_creacion`
# MAGIC   y luego `fecha_registro`.
# MAGIC - **Una fila por cliente en el lote**: la más reciente.
# MAGIC - **Hash de las columnas Tipo 2**: comparar un hash es más barato y seguro que comparar 4 columnas con nulos.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def preparar_origen(lote):
    tipos = {f.name: f.dataType.simpleString() for f in spark.table(destino).schema}   # el DDL es el contrato
    columnas = ["id_cliente", *COLS_T2, *COLS_T1, *COLS_T0]
    clientes = lote.select(
        *[F.expr(f"try_cast(`{c}` AS {tipos[c]})").alias(c) for c in columnas],
        F.coalesce(*[F.expr(f"try_cast(`{c}` AS TIMESTAMP)")
                     for c in ["fecha_actualizacion", "fecha_creacion", "fecha_registro"] if c in lote.columns]
                   ).alias("fecha_actualizacion"),
        F.col("_metadata.file_path").alias("_origen_archivo"),
        F.col("_ingestion_ts").alias("_bronze_ingestion_ts"),
    )
    w = Window.partitionBy("id_cliente").orderBy(F.col("fecha_actualizacion").desc(),
                                                 F.col("_bronze_ingestion_ts").desc())
    return (clientes.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
            .withColumn("hash_atributos", F.sha2(F.concat_ws(
                "||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in COLS_T2]), 256)))


# Probar sobre un pedazo de Bronze
display(preparar_origen(spark.table(origen).limit(1000)).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detectar qué cambió de verdad
# MAGIC
# MAGIC Contra la versión **vigente** de cada cliente:
# MAGIC
# MAGIC | Situación | Qué se hace |
# MAGIC |---|---|
# MAGIC | Cliente que no existe | Insertar su primera versión |
# MAGIC | Hash Tipo 2 distinto y fecha **posterior** a la vigente | Cerrar la vigente + abrir una nueva |
# MAGIC | Hash Tipo 2 distinto y fecha **anterior** a la vigente | **Llegada tardía**: no puede cerrar una versión que empezó después → cuarentena |
# MAGIC | Hash igual | Nada de Tipo 2 (puede haber cambios Tipo 1, paso 4) |

# COMMAND ----------

def clasificar(origen_df):
    vigentes = (spark.table(destino).filter("es_vigente")
                .select("id_cliente", F.col("hash_atributos").alias("hash_actual"),
                        F.col("fecha_inicio_vigencia").alias("inicio_actual")))
    df = origen_df.join(vigentes, "id_cliente", "left")
    cambios = df.filter("hash_actual IS NULL OR "
                        "(hash_atributos != hash_actual AND fecha_actualizacion > inicio_actual)")
    tardias = df.filter("hash_actual IS NOT NULL AND hash_atributos != hash_actual "
                        "AND fecha_actualizacion <= inicio_actual")
    return cambios, tardias

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. El MERGE de doble source (Tipo 2)
# MAGIC
# MAGIC Un MERGE hace una sola acción por fila de origen, pero un cambio necesita **dos**: cerrar la vigente e
# MAGIC insertar la nueva. El truco: cada cambio entra **dos veces** al MERGE.
# MAGIC
# MAGIC - (a) `clave_merge = id_cliente` → **coincide** con la vigente → la cierra.
# MAGIC - (b) `clave_merge = NULL` → **nunca coincide** → inserta la versión nueva.
# MAGIC
# MAGIC Los clientes nuevos solo necesitan (b).

# COMMAND ----------

COLS_VERSION = ["id_cliente", *COLS_T2, *COLS_T1, *COLS_T0]


def merge_tipo_2(cambios):
    a_cerrar = cambios.filter("hash_actual IS NOT NULL").withColumn("clave_merge", F.col("id_cliente"))
    a_insertar = cambios.withColumn("clave_merge", F.lit(None).cast("bigint"))
    source = a_cerrar.unionByName(a_insertar)

    (DeltaTable.forName(spark, destino).alias("d")
        .merge(source.alias("o"), "d.id_cliente = o.clave_merge AND d.es_vigente")
        .whenMatchedUpdate(set={
            "fecha_fin_vigencia": "o.fecha_actualizacion",
            "es_vigente": "false",
            "_procesado_ts": "current_timestamp()"})
        .whenNotMatchedInsert(values={
            **{c: f"o.{c}" for c in COLS_VERSION},
            "fecha_inicio_vigencia": "o.fecha_actualizacion",
            "fecha_fin_vigencia": "NULL",
            "es_vigente": "true",
            "hash_atributos": "o.hash_atributos",
            "_origen_archivo": "o._origen_archivo",
            "_bronze_ingestion_ts": "o._bronze_ingestion_ts",
            "_procesado_ts": "current_timestamp()"})   # sk_cliente lo genera la tabla (IDENTITY)
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tipo 1: sobrescribir en la versión vigente
# MAGIC
# MAGIC Email o ciudad corregidos no abren versión: se pisan en la vigente. `<=>` compara tratando NULL como
# MAGIC un valor. Solo si el cambio no es más viejo que la versión vigente.

# COMMAND ----------

def merge_tipo_1(origen_df):
    distinto = " OR ".join(f"NOT (d.{c} <=> o.{c})" for c in COLS_T1)
    (DeltaTable.forName(spark, destino).alias("d")
        .merge(origen_df.alias("o"), "d.id_cliente = o.id_cliente AND d.es_vigente")
        .whenMatchedUpdate(
            condition=f"o.fecha_actualizacion >= d.fecha_inicio_vigencia AND ({distinto})",
            set={**{c: f"o.{c}" for c in COLS_T1}, "_procesado_ts": "current_timestamp()"})
        .execute())


def enviar_a_cuarentena(df, motivo, batch_id):
    (df.select(F.lit("dim_clientes").alias("tabla"), F.lit(motivo).alias("motivos"),
               F.to_json(F.struct(*df.columns)).alias("fila"), "_origen_archivo",
               F.lit(batch_id).alias("_batch_id"), F.current_timestamp().alias("_procesado_ts"))
     .write.mode("append").saveAsTable(cuarentena))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Correr: todo junto en `foreachBatch`

# COMMAND ----------

def scd2_clientes(lote, batch_id):
    sin_clave = lote.filter("try_cast(id_cliente AS BIGINT) IS NULL")
    enviar_a_cuarentena(sin_clave.select("*", F.col("_metadata.file_path").alias("_origen_archivo"))
                                 .drop("_metadata"), "clave", batch_id)
    origen_df = preparar_origen(lote.filter("try_cast(id_cliente AS BIGINT) IS NOT NULL"))
    cambios, tardias = clasificar(origen_df)
    enviar_a_cuarentena(tardias, "llegada_tardia", batch_id)
    merge_tipo_2(cambios)
    merge_tipo_1(origen_df)


(spark.readStream.table(origen)
    .writeStream.foreachBatch(scd2_clientes)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start().awaitTermination())

display(spark.sql(f"""
    SELECT count(*) AS versiones, count(DISTINCT id_cliente) AS clientes,
           count_if(es_vigente) AS vigentes, count_if(NOT es_vigente) AS cerradas
    FROM {destino}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validar los invariantes (las tres deben dar 0)

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      -- 1. Exactamente una versión vigente por cliente
      (SELECT count(*) FROM (SELECT id_cliente FROM {destino} WHERE es_vigente
                             GROUP BY id_cliente HAVING count(*) > 1))          AS clientes_con_2_vigentes,
      -- 2. Sin huecos ni solapes: cada fin coincide con el inicio de la versión siguiente
      (SELECT count(*) FROM (
          SELECT fecha_fin_vigencia,
                 lead(fecha_inicio_vigencia) OVER (PARTITION BY id_cliente
                                                  ORDER BY fecha_inicio_vigencia) AS siguiente
          FROM {destino})
       WHERE fecha_fin_vigencia IS NOT NULL AND fecha_fin_vigencia != siguiente)  AS huecos_o_solapes,
      -- 3. Vigente ⇔ sin fecha de fin
      (SELECT count(*) FROM {destino}
       WHERE (es_vigente AND fecha_fin_vigencia IS NOT NULL)
          OR (NOT es_vigente AND fecha_fin_vigencia IS NULL))                    AS vigencias_incoherentes
"""))

# COMMAND ----------

# La historia de un cliente con más de una versión
display(spark.sql(f"""
    SELECT sk_cliente, id_cliente, ingreso_mensual_declarado, score_interno, nivel_riesgo, email,
           fecha_inicio_vigencia, fecha_fin_vigencia, es_vigente
    FROM {destino}
    WHERE id_cliente = (SELECT id_cliente FROM {destino} GROUP BY id_cliente
                        HAVING count(*) > 1 LIMIT 1)
    ORDER BY fecha_inicio_vigencia
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Join temporal** — con qué datos del cliente se aprobó cada solicitud:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT s.id_solicitud, s.fecha_hora_solicitud, c.ingreso_mensual_declarado, c.score_interno
# MAGIC FROM silver.lending.solicitudes_prestamo s
# MAGIC JOIN silver.lending.dim_clientes c
# MAGIC   ON  c.id_cliente = s.id_cliente
# MAGIC   AND s.fecha_hora_solicitud >= c.fecha_inicio_vigencia
# MAGIC   AND (s.fecha_hora_solicitud < c.fecha_fin_vigencia OR c.fecha_fin_vigencia IS NULL)
# MAGIC ```
# MAGIC
# MAGIC **Probar.** El delta de `generar_datos_wizard_bank.py` trae cambios Tipo 2 (ingreso/score), Tipo 1
# MAGIC (email/ciudad) y llegadas tardías. Ingéstalo a Bronze y corre el paso 5: aparecen versiones cerradas,
# MAGIC emails actualizados sin versión nueva, y `llegada_tardia` en la cuarentena.
# MAGIC
# MAGIC `dim_productos` es el mismo patrón con `tasa_interes_anual` (y montos, plazos, `es_activo`) como Tipo 2.
# MAGIC
# MAGIC ## Reiniciar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC dbutils.fs.rm(checkpoint, recurse=True)
# MAGIC ```
