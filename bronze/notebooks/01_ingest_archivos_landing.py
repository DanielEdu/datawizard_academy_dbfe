# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze paso a paso: archivos de landing → tabla Bronze con Auto Loader
# MAGIC
# MAGIC Este notebook hace lo mismo que los jobs `job_bronze_lending_ingest` y `job_bronze_cobranzas_ingest`,
# MAGIC pero para **una tabla** y paso a paso, para ver qué pasa en cada etapa:
# MAGIC
# MAGIC 1. Parámetros
# MAGIC 2. Mirar la fuente: qué archivos hay en landing y qué traen
# MAGIC 3. Mirar el destino: la tabla Bronze (el DDL es el contrato)
# MAGIC 4. Leer con Auto Loader
# MAGIC 5. Agregar la metadata de ingesta
# MAGIC 6. Escribir en Bronze
# MAGIC 7. Validar: conteo, metadata e idempotencia
# MAGIC
# MAGIC Por defecto escribe en una **tabla de práctica** (`<tabla>_brz_lab`) con su propio checkpoint,
# MAGIC para no duplicar la tabla real que carga el job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros
# MAGIC
# MAGIC Cambia los widgets de arriba para usar otra fuente o tabla. Ejemplos:
# MAGIC
# MAGIC | Fuente | `schema` | `landing` | `tabla` |
# MAGIC |---|---|---|---|
# MAGIC | Cobranzas (legado on-premise) | `cobranzas` | `landing/wizard_bank_onp` | `cuotas`, `pagos`, `gestiones_cobranza` |
# MAGIC | Lending (Azure SQL) | `lending` | `landing/wizard_bank_rdb` | `clientes`, `paises`, `solicitudes_prestamo`… |

# COMMAND ----------

dbutils.widgets.text("catalogo", "bronze", "Catálogo")
dbutils.widgets.text("schema", "cobranzas", "Schema")
dbutils.widgets.text("tabla", "pagos", "Tabla (carpeta en landing)")
dbutils.widgets.text("bucket", "s3://lakehouse-datawizard", "Bucket")
dbutils.widgets.text("landing", "landing/wizard_bank_onp", "Prefijo de landing")
dbutils.widgets.dropdown("formato", "csv", ["csv", "json", "parquet"], "Formato")
dbutils.widgets.text("sufijo_destino", "_brz_lab", "Sufijo destino (_brz = tabla real)")

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
tabla = dbutils.widgets.get("tabla")
bucket = dbutils.widgets.get("bucket").rstrip("/")
landing = dbutils.widgets.get("landing").strip("/")
formato = dbutils.widgets.get("formato")
sufijo = dbutils.widgets.get("sufijo_destino")

# Rutas: la carpeta de la tabla en landing, y dónde guarda Auto Loader su estado
ruta_landing = f"{bucket}/{landing}/{tabla}/"
ruta_checkpoint = f"{bucket}/checkpoint/notebooks/{schema}/{tabla}{sufijo}/"
ruta_schema = f"{bucket}/schemas/notebooks/{schema}/{tabla}{sufijo}/"

tabla_ddl = f"{catalogo}.{schema}.{tabla}_brz"        # la que crea bundle_ddl
tabla_destino = f"{catalogo}.{schema}.{tabla}{sufijo}"  # donde escribe este notebook

print(f"Landing      : {ruta_landing}")
print(f"Checkpoint   : {ruta_checkpoint}")
print(f"Schema AL    : {ruta_schema}")
print(f"Tabla DDL    : {tabla_ddl}")
print(f"Tabla destino: {tabla_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mirar la fuente
# MAGIC
# MAGIC Antes de ingestar, ver qué hay: cuántos archivos, cómo se llaman y qué columnas traen.
# MAGIC El acceso a S3 va por la *external location* de Unity Catalog: no hay credenciales en el código.

# COMMAND ----------

archivos = dbutils.fs.ls(ruta_landing)
display(archivos)

# COMMAND ----------

# Lectura batch normal, solo para mirar: todo como texto, igual que lo guardará Bronze
muestra = (spark.read.format(formato)
           .option("header", "true")
           .option("inferSchema", "false")
           .load(ruta_landing))

print(f"Filas en landing: {muestra.count():,}")
print(f"Columnas        : {muestra.columns}")
display(muestra.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Mirar el destino
# MAGIC
# MAGIC La tabla Bronze ya existe: la crea `bundle_ddl` con su DDL. Es el **contrato**: columnas de negocio
# MAGIC en STRING, `_metadata`, `_rescued_data`, `_ingestion_ts` y `delta.appendOnly = true`.
# MAGIC
# MAGIC Para practicar se crea una copia vacía con el mismo DDL (`CREATE TABLE ... LIKE`).

# COMMAND ----------

if tabla_destino != tabla_ddl:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tabla_destino} LIKE {tabla_ddl}")

display(spark.sql(f"DESCRIBE TABLE {tabla_destino}"))

# COMMAND ----------

# Las columnas del archivo deben llamarse igual que las de la tabla
tecnicas = {"_metadata", "_rescued_data", "_ingestion_ts"}
columnas_tabla = [c for c in spark.table(tabla_destino).columns if c not in tecnicas]

print("En el archivo y no en la tabla:", [c for c in muestra.columns if c not in columnas_tabla])
print("En la tabla y no en el archivo:", [c for c in columnas_tabla if c not in muestra.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leer con Auto Loader
# MAGIC
# MAGIC `cloudFiles` es Auto Loader: un **stream** sobre la carpeta. Recuerda en el checkpoint qué archivos
# MAGIC ya leyó, así que cada corrida solo procesa los nuevos.
# MAGIC
# MAGIC | Opción | Por qué |
# MAGIC |---|---|
# MAGIC | `cloudFiles.inferColumnTypes = false` | Todo llega como STRING: Bronze es copia fiel, los tipos se ponen en Silver |
# MAGIC | `cloudFiles.schemaLocation` | Dónde Auto Loader guarda el schema que descubrió |
# MAGIC | `cloudFiles.schemaEvolutionMode = addNewColumns` | Una columna nueva en el archivo se agrega a la tabla |
# MAGIC | `rescuedDataColumn = _rescued_data` | Lo que no calza con el schema se guarda aquí, no se pierde |

# COMMAND ----------

lectura = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", formato)
           .option("header", "true")
           .option("cloudFiles.inferColumnTypes", "false")
           .option("cloudFiles.schemaLocation", ruta_schema)
           .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
           .option("rescuedDataColumn", "_rescued_data")
           .load(ruta_landing))

lectura.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agregar la metadata de ingesta
# MAGIC
# MAGIC - `_metadata`: columna oculta de Auto Loader con el archivo de origen. Se guardan 6 campos (los del DDL).
# MAGIC - `_ingestion_ts`: cuándo entró la fila a Bronze.
# MAGIC
# MAGIC Nada más: sin filtros, sin casts, sin dedup.

# COMMAND ----------

from pyspark.sql import functions as F

campos_metadata = ["file_path", "file_name", "file_size",
                   "file_block_start", "file_block_length", "file_modification_time"]

bronze = lectura.select(
    "*",
    F.struct(*[F.col(f"_metadata.{c}").alias(c) for c in campos_metadata]).alias("_metadata"),
    F.current_timestamp().alias("_ingestion_ts"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Escribir en Bronze
# MAGIC
# MAGIC - `trigger(availableNow=True)`: procesa lo pendiente y termina. Es un batch con la contabilidad de un stream.
# MAGIC - `checkpointLocation`: el estado del stream. **Con el mismo checkpoint, volver a correr no duplica.**
# MAGIC - `mergeSchema`: si Auto Loader agregó una columna nueva, la tabla también la agrega.

# COMMAND ----------

filas_antes = spark.table(tabla_destino).count()

consulta = (bronze.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", ruta_checkpoint)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(tabla_destino))

consulta.awaitTermination()

filas_despues = spark.table(tabla_destino).count()
print(f"Filas agregadas: {filas_despues - filas_antes:,}  (total {filas_despues:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC > Si el stream se detiene con `UNKNOWN_FIELD_EXCEPTION`, Auto Loader encontró una columna nueva:
# MAGIC > vuelve a correr la celda y la incorpora. El job lo hace solo con un reintento.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validar

# COMMAND ----------

# Conteo: Bronze debe tener exactamente las filas de landing (si es la primera carga)
print(f"Landing: {muestra.count():,}   ·   Bronze: {spark.table(tabla_destino).count():,}")

# COMMAND ----------

# Metadata: de qué archivo vino cada fila, cuándo entró, y si algo cayó en _rescued_data
display(spark.sql(f"""
    SELECT _metadata.file_name          AS archivo,
           count(*)                     AS filas,
           count(_rescued_data)         AS filas_rescatadas,
           min(_ingestion_ts)           AS ingestado
    FROM {tabla_destino}
    GROUP BY _metadata.file_name
    ORDER BY archivo
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Idempotencia.** Vuelve a correr la celda del paso 6: debe decir *Filas agregadas: 0*.
# MAGIC Auto Loader ya registró esos archivos en el checkpoint.
# MAGIC
# MAGIC **Incremental.** Genera un delta con `scripts/data_generetor` (ver su README), súbelo a landing y
# MAGIC corre los pasos 4 a 7: solo entran las filas del archivo nuevo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Del notebook al job
# MAGIC
# MAGIC El job (`bronze/<fuente>/src/`) es este mismo código, organizado para correr todas las tablas:
# MAGIC
# MAGIC | En el notebook | En el job |
# MAGIC |---|---|
# MAGIC | Widgets | `argparse` en `main.py` + `parameters` del job en `resources/*.yml` |
# MAGIC | Una tabla | La lista `tablas` de `config/tablas_<fuente>.yml`, en un bucle |
# MAGIC | Pasos 4–6 | Clase `AutoLoaderIngestor` (`ingestion/autoloader.py`) |
# MAGIC | Reintento manual | Reintento automático ante columnas nuevas |
# MAGIC | Tabla `_brz_lab` | Tabla real `_brz` y checkpoint `checkpoint/<fuente>/` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpiar (opcional)
# MAGIC
# MAGIC Para empezar de cero borra la tabla de práctica **y** su checkpoint. Si borras solo la tabla,
# MAGIC el checkpoint cree que ya leyó todo y no vuelve a cargar nada.
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {tabla_destino}")
# MAGIC dbutils.fs.rm(ruta_checkpoint, recurse=True)
# MAGIC dbutils.fs.rm(ruta_schema, recurse=True)
# MAGIC ```
