# Databricks notebook source
# MAGIC %md
# MAGIC # Silver paso a paso · 01 · Upsert con guarda de orden
# MAGIC
# MAGIC **Familias:** *Catálogo* (paises, campanias) y *Transaccional* (ofertas, solicitudes, desembolsos, cuotas).
# MAGIC Son filas que **cambian**: una solicitud pasa de `Iniciada` a `Aprobada`, una cuota de `Pendiente` a `Pagada`.
# MAGIC Silver guarda **el estado vigente**: una fila por clave. (Sesión 15)
# MAGIC
# MAGIC El motor tiene cuatro pasos por lote:
# MAGIC
# MAGIC 1. **Tipar** — Bronze es todo STRING; el DDL de Silver dice el tipo de cada columna.
# MAGIC 2. **Validar** — reglas de calidad; lo que falla va a `_cuarentena`, nunca se descarta en silencio.
# MAGIC 3. **Deduplicar en el lote** — una fila por clave, la más nueva.
# MAGIC 4. **MERGE con guarda** — solo actualiza si la versión que llega es más nueva que la guardada.
# MAGIC
# MAGIC Escribe en tablas de práctica (`<tabla>_lab`, `_cuarentena_lab`) con su propio checkpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros y configuración de la tabla
# MAGIC
# MAGIC Cada tabla se describe con: **clave** (qué identifica la fila), **orden** (qué columna dice cuál
# MAGIC versión es más nueva; si viene vacía se usa la siguiente) y **reglas** de calidad (SQL que debe ser verdadero).

# COMMAND ----------

CONFIG = {
    "paises": {
        "schema": "lending", "clave": ["id_pais"],
        "orden": ["fecha_actualizacion", "fecha_creacion"],
        "reglas": {"clave": "id_pais IS NOT NULL",
                   "iso_valido": "codigo_iso RLIKE '^[A-Z]{2}$'"},
    },
    "campanias": {
        "schema": "lending", "clave": ["id_campania"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_inicio"],
        "reglas": {"clave": "id_campania IS NOT NULL"},
    },
    "ofertas_preaprobadas": {
        "schema": "lending", "clave": ["id_oferta"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_generacion"],
        "reglas": {"clave": "id_oferta IS NOT NULL AND id_cliente IS NOT NULL",
                   "monto_positivo": "monto_ofertado > 0"},
    },
    "solicitudes_prestamo": {
        "schema": "lending", "clave": ["id_solicitud"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_hora_solicitud"],
        "reglas": {"clave": "id_solicitud IS NOT NULL AND id_cliente IS NOT NULL",
                   "monto_positivo": "monto_solicitado > 0",
                   "estado_valido": "estado_solicitud IN ('Iniciada','En evaluacion','Aprobada','Rechazada','Desistida')"},
    },
    "desembolsos": {
        "schema": "lending", "clave": ["id_desembolso"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_hora_desembolso"],
        "reglas": {"clave": "id_desembolso IS NOT NULL",
                   "monto_positivo": "monto_desembolsado > 0"},
    },
    "cuotas": {
        "schema": "cobranzas", "clave": ["id_cuota"],
        "orden": ["fecha_modificacion", "fecha_creacion"],
        "reglas": {"clave": "id_cuota IS NOT NULL",
                   "estado_valido": "estado_cuota IN ('Pendiente','Pagada','Vencida')",
                   "cuota_cuadra": "abs(monto_cuota - (monto_capital + monto_interes)) <= 0.01"},
    },
}

dbutils.widgets.dropdown("tabla", "cuotas", list(CONFIG), "Tabla")
dbutils.widgets.text("catalogo_bronze", "bronze", "Catálogo Bronze")
dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("sufijo_destino", "_lab", "Sufijo destino (vacío = tabla real)")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver", "Raíz de checkpoints")

tabla = dbutils.widgets.get("tabla")
cfg = CONFIG[tabla]
sufijo = dbutils.widgets.get("sufijo_destino")
schema = cfg["schema"]

origen = f"{dbutils.widgets.get('catalogo_bronze')}.{schema}.{tabla}_brz"
silver = dbutils.widgets.get("catalogo_silver")
tabla_ddl = f"{silver}.{schema}.{tabla}"
destino = f"{tabla_ddl}{sufijo}"
cuarentena = f"{silver}.{schema}._cuarentena{sufijo}"
checkpoint = f"{dbutils.widgets.get('checkpoints').rstrip('/')}/{schema}/{tabla}{sufijo}"

print(f"Origen    : {origen}\nDestino   : {destino}\nCuarentena: {cuarentena}\nCheckpoint: {checkpoint}")
print(f"Clave {cfg['clave']} · orden {cfg['orden']} · reglas {list(cfg['reglas'])}")

# COMMAND ----------

# Tablas de práctica: copia vacía del DDL real (el DDL es el contrato)
if sufijo:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {tabla_ddl}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cuarentena} LIKE {silver}.{schema}._cuarentena")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El problema: qué hay en Bronze
# MAGIC
# MAGIC Bronze tiene **una fila por cada vez que un registro viajó en un archivo**: la carga inicial, cada delta,
# MAGIC cada reenvío. La misma clave aparece varias veces.

# COMMAND ----------

clave_sql = ", ".join(cfg["clave"])
display(spark.sql(f"""
    SELECT count(*) AS filas_bronze,
           count(DISTINCT {clave_sql}) AS claves_distintas,
           count(*) - count(DISTINCT {clave_sql}) AS filas_repetidas
    FROM {origen}
"""))

# COMMAND ----------

# Un ejemplo: una clave con varias versiones en Bronze
display(spark.sql(f"""
    SELECT * FROM {origen}
    WHERE ({clave_sql}) IN (SELECT {clave_sql} FROM {origen}
                            GROUP BY {clave_sql} HAVING count(*) > 1 LIMIT 1)
    ORDER BY _ingestion_ts
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Los cuatro pasos, como funciones
# MAGIC
# MAGIC Se definen aquí y se aplican a cada micro-lote con `foreachBatch` (paso 4 del notebook).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TECNICAS = ["_origen_archivo", "_bronze_ingestion_ts", "_procesado_ts"]


def tipar(lote, schema_destino):
    """Paso 1 · Castea cada columna al tipo del DDL de Silver.
    try_cast da NULL si un valor no convierte, en vez de romper el lote."""
    columnas = [F.expr(f"try_cast(`{f.name}` AS {f.dataType.simpleString()})").alias(f.name)
                for f in schema_destino if f.name not in TECNICAS and f.name in lote.columns]
    return lote.select(
        *columnas,
        F.col("_metadata.file_path").alias("_origen_archivo"),
        F.col("_ingestion_ts").alias("_bronze_ingestion_ts"),
        F.current_timestamp().alias("_procesado_ts"),
    )


def separar_invalidas(df, reglas):
    """Paso 2 · Cada regla que falla deja su nombre en _motivos. Una regla que da NULL cuenta como falla."""
    fallos = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(nombre))
              for nombre, expr in reglas.items()]
    df = df.withColumn("_motivos", F.filter(F.array(*fallos), lambda x: x.isNotNull()))
    return (df.filter(F.size("_motivos") == 0).drop("_motivos"),
            df.filter(F.size("_motivos") > 0))


def enviar_a_cuarentena(invalidas, batch_id):
    """La fila completa en JSON + los motivos. Así Bronze = Silver + cuarentena + duplicados."""
    (invalidas.select(
        F.lit(tabla).alias("tabla"),
        F.array_join("_motivos", ", ").alias("motivos"),
        F.to_json(F.struct(*[c for c in invalidas.columns if c != "_motivos"])).alias("fila"),
        "_origen_archivo",
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_procesado_ts"))
     .write.mode("append").saveAsTable(cuarentena))


def ultima_version(df, clave, orden):
    """Paso 3 · Una fila por clave: la de mayor orden (si empatan, la última ingestada).
    Sin esto el MERGE falla: dos filas del lote coincidirían con la misma fila destino."""
    orden = [c for c in orden if c in df.columns]
    df = df.withColumn("_orden", F.coalesce(*[F.col(c).cast("timestamp") for c in orden],
                                            F.col("_bronze_ingestion_ts")))
    w = Window.partitionBy(*clave).orderBy(F.col("_orden").desc(), F.col("_bronze_ingestion_ts").desc())
    return (df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
              .withColumn(orden[0], F.col("_orden"))   # la columna de orden queda siempre llena
              .drop("_rn", "_orden"))


def mergear(df, clave, columna_orden):
    """Paso 4 · MERGE idempotente con guarda: una versión vieja que llega tarde no pisa a una nueva."""
    cond = " AND ".join(f"t.{c} = s.{c}" for c in clave)
    valores = {c: f"s.{c}" for c in df.columns}
    (DeltaTable.forName(spark, destino).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdate(condition=f"s.{columna_orden} > t.{columna_orden}", set=valores)
        .whenNotMatchedInsert(values=valores)
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Probar los pasos sobre un pedazo de Bronze (sin escribir)

# COMMAND ----------

muestra = spark.table(origen).limit(2000)
tipado = tipar(muestra, spark.table(destino).schema)
validas, invalidas = separar_invalidas(tipado, cfg["reglas"])
print(f"Muestra: {muestra.count()} · válidas: {validas.count()} · inválidas: {invalidas.count()}")
display(tipado.limit(5))   # ya con tipos reales

# COMMAND ----------

display(invalidas.select("_motivos", *cfg["clave"]).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Correr el motor: stream de Bronze + `foreachBatch`
# MAGIC
# MAGIC - `readStream.table(bronze)`: Bronze es append-only, así que se puede leer como stream. El checkpoint
# MAGIC   recuerda hasta dónde se leyó: cada corrida procesa **solo lo nuevo**.
# MAGIC - `foreachBatch`: entrega cada lote como un DataFrame normal, donde sí se puede hacer MERGE.
# MAGIC - `availableNow`: procesa lo pendiente y termina (un batch con contabilidad de stream).

# COMMAND ----------

def procesar_lote(lote, batch_id):
    validas, invalidas = separar_invalidas(tipar(lote, spark.table(destino).schema), cfg["reglas"])
    enviar_a_cuarentena(invalidas, batch_id)
    mergear(ultima_version(validas, cfg["clave"], cfg["orden"]), cfg["clave"], cfg["orden"][0])


(spark.readStream.table(origen)
    .writeStream
    .foreachBatch(procesar_lote)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

print(f"Silver: {spark.table(destino).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validar

# COMMAND ----------

# Una fila por clave (debe dar 0 repetidas)
display(spark.sql(f"""
    SELECT count(*) AS filas, count(DISTINCT {clave_sql}) AS claves,
           count(*) - count(DISTINCT {clave_sql}) AS repetidas
    FROM {destino}
"""))

# COMMAND ----------

# Cuadre: toda clave de Bronze está en Silver o en cuarentena
display(spark.sql(f"""
    SELECT
      (SELECT count(DISTINCT {clave_sql}) FROM {origen})              AS claves_bronze,
      (SELECT count(*) FROM {destino})                                 AS filas_silver,
      (SELECT count(*) FROM {cuarentena} WHERE tabla = '{tabla}')      AS filas_cuarentena
"""))

# COMMAND ----------

# Qué se rechazó y por qué
display(spark.sql(f"""
    SELECT motivos, count(*) AS filas, first(fila) AS ejemplo
    FROM {cuarentena} WHERE tabla = '{tabla}'
    GROUP BY motivos ORDER BY filas DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Idempotencia.** Vuelve a correr el paso 4: no cambia nada (el checkpoint ya leyó todo).
# MAGIC
# MAGIC **La guarda en acción.** Genera un delta (`scripts/data_generetor`, trae casos `llegada_tardia` y
# MAGIC `reenvio`), ingéstalo a Bronze y corre el paso 4. Las claves de esos casos (ver el manifiesto
# MAGIC `casos_silver_<sufijo>.csv`) **no cambian** en Silver. Para ver por qué importa la guarda, quita
# MAGIC `condition=` en `mergear`, reinicia y repite: las versiones viejas pisan a las nuevas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reiniciar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {cuarentena}")   # es append-only: se borra entera (todas las tablas)
# MAGIC dbutils.fs.rm(checkpoint, recurse=True)
# MAGIC ```
