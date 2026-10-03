# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze paso a paso: eventos de Kafka → tabla Bronze
# MAGIC
# MAGIC Este notebook hace lo mismo que el job `job_bronze_app_ingest`, paso a paso.
# MAGIC
# MAGIC La app de Wizard Bank publica su telemetría en el topic Kafka `wizard.lending.eventos-app`.
# MAGIC En el curso no hay broker: `productor_eventos.py --destino parquet` escribe los mensajes como
# MAGIC `.parquet` con **la misma estructura que entrega `spark.readStream.format("kafka")`**, y los sube a
# MAGIC `landing/wizard_bank_kafka/eventos_app/`. Así el código de parseo es el mismo que con Kafka real.
# MAGIC
# MAGIC 1. Parámetros
# MAGIC 2. Mirar un mensaje de Kafka: `key` y `value` en binario
# MAGIC 3. Del binario al JSON y del JSON a columnas
# MAGIC 4. Leer con Auto Loader
# MAGIC 5. Parsear, aplanar y agregar metadata
# MAGIC 6. Escribir en Bronze
# MAGIC 7. Validar: lo que Bronze guarda y lo que deja para Silver
# MAGIC
# MAGIC Por defecto escribe en una **tabla de práctica** (`eventos_app_brz_lab`) con su propio checkpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

dbutils.widgets.text("catalogo", "bronze", "Catálogo")
dbutils.widgets.text("schema", "app", "Schema")
dbutils.widgets.text("tabla", "eventos_app", "Tabla (carpeta en landing)")
dbutils.widgets.text("bucket", "s3://lakehouse-datawizard", "Bucket")
dbutils.widgets.text("landing", "landing/wizard_bank_kafka", "Prefijo de landing")
dbutils.widgets.text("sufijo_destino", "_brz_lab", "Sufijo destino (_brz = tabla real)")

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
tabla = dbutils.widgets.get("tabla")
bucket = dbutils.widgets.get("bucket").rstrip("/")
landing = dbutils.widgets.get("landing").strip("/")
sufijo = dbutils.widgets.get("sufijo_destino")

ruta_landing = f"{bucket}/{landing}/{tabla}/"
ruta_checkpoint = f"{bucket}/checkpoint/notebooks/{schema}/{tabla}{sufijo}/"

tabla_ddl = f"{catalogo}.{schema}.{tabla}_brz"
tabla_destino = f"{catalogo}.{schema}.{tabla}{sufijo}"

print(f"Landing      : {ruta_landing}")
print(f"Checkpoint   : {ruta_checkpoint}")
print(f"Tabla destino: {tabla_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mirar un mensaje de Kafka
# MAGIC
# MAGIC Un mensaje de Kafka no es una fila de negocio: es un **sobre**. Estas son las columnas que entrega
# MAGIC la fuente Kafka de Spark (y que traen nuestros `.parquet`):
# MAGIC
# MAGIC | Columna | Tipo | Qué es |
# MAGIC |---|---|---|
# MAGIC | `key` | binary | La clave del mensaje (`id_cliente`). Misma clave → misma partición → orden garantizado por cliente |
# MAGIC | `value` | binary | **El evento**, serializado como JSON en bytes |
# MAGIC | `topic` | string | El topic |
# MAGIC | `partition` | int | La partición del topic |
# MAGIC | `offset` | bigint | Posición dentro de la partición. `(topic, partition, offset)` identifica el mensaje |
# MAGIC | `timestamp` | timestamp | Cuándo lo recibió el broker (no cuándo ocurrió el evento) |
# MAGIC | `timestampType` | int | `0` = CreateTime |

# COMMAND ----------

display(dbutils.fs.ls(ruta_landing))

# COMMAND ----------

mensajes = spark.read.parquet(ruta_landing)
mensajes.printSchema()
display(mensajes.limit(5))   # key y value se ven como bytes: Spark no sabe qué hay dentro

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Del binario al JSON y del JSON a columnas
# MAGIC
# MAGIC Kafka no sabe qué hay en `value`: son bytes. El **contrato** con la app dice que es un JSON UTF-8 con
# MAGIC esta forma (Sesión 11). Dos pasos:
# MAGIC
# MAGIC 1. `value.cast("string")` → el texto JSON
# MAGIC 2. `from_json(texto, esquema)` → un struct con los campos tipados

# COMMAND ----------

from pyspark.sql import functions as F

display(mensajes.select(
    F.col("key").cast("string").alias("key_texto"),
    F.col("value").cast("string").alias("value_texto"),
    "partition", "offset", "timestamp",
).limit(5))

# COMMAND ----------

from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

esquema_evento = StructType([
    StructField("id_evento", StringType()),
    StructField("tipo_evento", StringType()),
    StructField("timestamp_evento", StringType()),   # ISO-8601: se castea después
    StructField("id_cliente", LongType()),
    StructField("id_oferta", LongType()),
    StructField("id_pais", IntegerType()),
    StructField("canal", StringType()),
    StructField("sesion", StructType([
        StructField("id_sesion", StringType()),
        StructField("version_app", StringType()),
        StructField("sistema_operativo", StringType()),
        StructField("modelo_dispositivo", StringType()),
    ])),
    StructField("contexto", StructType([
        StructField("ubicacion_pantalla", StringType()),
        StructField("posicion", IntegerType()),
        StructField("tiempo_visible_seg", DoubleType()),
        StructField("monto_simulado", DoubleType()),   # solo simulacion_realizada
        StructField("plazo_simulado", IntegerType()),  # solo simulacion_realizada
    ])),
])

parseado = mensajes.select(F.from_json(F.col("value").cast("string"), esquema_evento).alias("evento"))
parseado.printSchema()
display(parseado.select("evento.*").limit(5))   # sesion y contexto todavía son structs anidados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leer con Auto Loader
# MAGIC
# MAGIC Igual que en el notebook 01, pero el formato es `parquet` y el schema se da **explícito**:
# MAGIC el sobre de Kafka siempre tiene las mismas 7 columnas. Lo que puede cambiar vive dentro de `value`.

# COMMAND ----------

schema_kafka = ("key BINARY, value BINARY, topic STRING, partition INT, offset BIGINT, "
                "timestamp TIMESTAMP, timestampType INT")

lectura = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .schema(schema_kafka)
           .load(ruta_landing))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parsear, aplanar y agregar metadata
# MAGIC
# MAGIC - Los campos de `sesion` y `contexto` suben a columnas de primer nivel.
# MAGIC - La metadata de Kafka se guarda con prefijo `_kafka_`: el offset sirve para rastrear un mensaje.
# MAGIC - `_payload_crudo` guarda el texto original: si el JSON viene roto, `from_json` deja todo en NULL
# MAGIC   pero el mensaje **no se pierde**.

# COMMAND ----------

campos_metadata = ["file_path", "file_name", "file_size",
                   "file_block_start", "file_block_length", "file_modification_time"]

payload = F.col("value").cast("string")
e = F.from_json(payload, esquema_evento)

bronze = lectura.select(
    # El evento, aplanado
    e["id_evento"].alias("id_evento"),
    e["tipo_evento"].alias("tipo_evento"),
    F.to_timestamp(e["timestamp_evento"]).alias("timestamp_evento"),
    e["id_cliente"].alias("id_cliente"),
    e["id_oferta"].alias("id_oferta"),
    e["id_pais"].alias("id_pais"),
    e["canal"].alias("canal"),
    e["sesion"]["id_sesion"].alias("id_sesion"),
    e["sesion"]["version_app"].alias("version_app"),
    e["sesion"]["sistema_operativo"].alias("sistema_operativo"),
    e["sesion"]["modelo_dispositivo"].alias("modelo_dispositivo"),
    e["contexto"]["ubicacion_pantalla"].alias("ubicacion_pantalla"),
    e["contexto"]["posicion"].alias("posicion"),
    e["contexto"]["tiempo_visible_seg"].alias("tiempo_visible_seg"),
    e["contexto"]["monto_simulado"].alias("monto_simulado"),
    e["contexto"]["plazo_simulado"].alias("plazo_simulado"),
    # El sobre de Kafka
    F.col("key").cast("string").alias("_kafka_key"),
    F.col("topic").alias("_kafka_topic"),
    F.col("partition").alias("_kafka_particion"),
    F.col("offset").alias("_kafka_offset"),
    F.col("timestamp").alias("_kafka_timestamp"),
    payload.alias("_payload_crudo"),
    # Metadata de ingesta
    F.struct(*[F.col(f"_metadata.{c}").alias(c) for c in campos_metadata]).alias("_metadata"),
    F.current_timestamp().alias("_ingestion_ts"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Escribir en Bronze
# MAGIC
# MAGIC Tabla de práctica con el mismo DDL que `bundle_ddl` (`eventos_app_brz.sql`), append-only.

# COMMAND ----------

if tabla_destino != tabla_ddl:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tabla_destino} LIKE {tabla_ddl}")

filas_antes = spark.table(tabla_destino).count()

consulta = (bronze.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", ruta_checkpoint)
            .trigger(availableNow=True)
            .toTable(tabla_destino))

consulta.awaitTermination()

filas_despues = spark.table(tabla_destino).count()
print(f"Mensajes agregados: {filas_despues - filas_antes:,}  (total {filas_despues:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validar
# MAGIC
# MAGIC Bronze guarda **todo lo que llegó**, tal cual. Kafka entrega *at-least-once* y los teléfonos se
# MAGIC quedan sin red, así que Bronze trae cosas que Silver tendrá que resolver (Sesión 17):

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      count(*)                                              AS mensajes,
      count(DISTINCT _kafka_particion, _kafka_offset)       AS mensajes_unicos_kafka,
      count(DISTINCT id_evento)                             AS eventos_unicos,
      count(id_evento) - count(DISTINCT id_evento)          AS eventos_duplicados,
      count(*) - count(id_evento)                           AS payloads_corruptos,
      count_if(tipo_evento NOT IN ('oferta_vista', 'oferta_click',
               'simulacion_realizada', 'solicitud_iniciada')) AS tipos_desconocidos
    FROM {tabla_destino}
"""))

# COMMAND ----------

# Payload corrupto: el JSON llegó cortado. Bronze lo conserva en _payload_crudo
display(spark.sql(f"""
    SELECT _kafka_particion, _kafka_offset, _payload_crudo
    FROM {tabla_destino}
    WHERE id_evento IS NULL
    LIMIT 5
"""))

# COMMAND ----------

# Duplicado: el mismo id_evento en dos mensajes distintos (otro offset). El productor reintentó
display(spark.sql(f"""
    SELECT id_evento, _kafka_particion, _kafka_offset, _kafka_timestamp
    FROM {tabla_destino}
    WHERE id_evento IN (SELECT id_evento FROM {tabla_destino}
                        WHERE id_evento IS NOT NULL
                        GROUP BY id_evento HAVING count(*) > 1)
    ORDER BY id_evento, _kafka_offset
    LIMIT 10
"""))

# COMMAND ----------

# Tiempo del evento vs. tiempo de llegada: el retraso que Silver usará para elegir el watermark
display(spark.sql(f"""
    SELECT
      percentile(retraso_seg, 0.50) AS p50_seg,
      percentile(retraso_seg, 0.99) AS p99_seg,
      max(retraso_seg)              AS max_seg
    FROM (SELECT unix_timestamp(_kafka_timestamp) - unix_timestamp(timestamp_evento) AS retraso_seg
          FROM {tabla_destino}
          WHERE timestamp_evento IS NOT NULL)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Cada archivo del productor trae un manifiesto local (`casos_silver_<sufijo>.csv`) con los
# MAGIC `id_evento` de cada caso: duplicados, llegadas tardías, fuera del watermark, payloads corruptos y
# MAGIC tipos desconocidos. Úsalo para comprobar Silver.
# MAGIC
# MAGIC **Idempotencia.** Vuelve a correr la celda del paso 6: debe decir *Mensajes agregados: 0*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Del notebook al job
# MAGIC
# MAGIC | En el notebook | En el job (`bronze/app/src/main.py`) |
# MAGIC |---|---|
# MAGIC | Widgets | `argparse` + `parameters` en `resources/job_bronze_app_ingest.yml` |
# MAGIC | Paso 4 | `leer_landing()` |
# MAGIC | Paso 5 | `aplanar()` |
# MAGIC | Paso 6 | `writeStream ... toTable()` en `main()` |
# MAGIC | Tabla `_brz_lab` | `bronze.app.eventos_app_brz` y checkpoint `checkpoint/app/eventos_app/` |
# MAGIC
# MAGIC Con un broker Kafka real (Sesión 11) solo cambia el paso 4: `spark.readStream.format("kafka")` con
# MAGIC `kafka.bootstrap.servers` y `subscribe`. Los pasos 5 a 7 son idénticos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpiar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {tabla_destino}")
# MAGIC dbutils.fs.rm(ruta_checkpoint, recurse=True)
# MAGIC ```
