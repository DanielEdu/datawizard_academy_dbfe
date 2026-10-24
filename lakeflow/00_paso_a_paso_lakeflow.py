# Databricks notebook source

# MAGIC %md
# MAGIC # Lakeflow paso a paso · El Lakehouse de Wizard Bank, reconstruido con un pipeline declarativo
# MAGIC **Data Wizard Academy · Databricks Track**
# MAGIC
# MAGIC Hasta aquí construiste cada capa **a mano**: bundles de ingesta con Auto Loader (Bronze), notebooks con `foreachBatch` + `MERGE`,
# MAGIC SCD2 y watermarks (Silver) y tablas de consumo (Gold). Hoy reconstruyes **todo eso en un solo pipeline declarativo**,
# MAGIC partiendo de los mismos archivos de landing, y comparas cuánto código desaparece.
# MAGIC
# MAGIC **Cómo se trabaja:** este notebook explica cada paso y trae el código en un bloque 📋. Lo **copias a un archivo nuevo del
# MAGIC pipeline**, das **Run pipeline** y vuelves aquí a validar con una celda ▶️. (Los mismos archivos ya están listos en
# MAGIC `lakeflow/transformations/` del repo, por si quieres comparar.)
# MAGIC
# MAGIC | Bloque | Pasos |
# MAGIC |---|---|
# MAGIC | Qué es Lakeflow y los tipos de fuente | Presentación |
# MAGIC | **Bloque 1 · Bronze** | 0 landing · 1 crear el pipeline · 2-5 una fuente por paso |
# MAGIC | **Bloque 2 · Silver** | 6 catálogos · 7 maestras SCD2 · 8 transaccional · 9 tipos de cambio · 10 cobranzas · 11 eventos |
# MAGIC | **Bloque 3 · Gold y operación** | 12 Gold en SQL · 13 llega un lote nuevo · 14 lo que no escribiste · 15 job |
# MAGIC | Cierre | Checklist y limpieza |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Qué es Lakeflow
# MAGIC **Lakeflow** es la familia de Databricks para **ingerir, transformar y orquestar** datos, gobernada por Unity Catalog:
# MAGIC
# MAGIC | Pieza | Qué resuelve | Aquí |
# MAGIC |---|---|---|
# MAGIC | **Lakeflow Connect** | Conectores administrados (SQL Server, Salesforce…) | Se menciona: reemplazaría el extractor de la fuente 1 |
# MAGIC | **Lakeflow Spark Declarative Pipelines** | Declaras **qué** tabla quieres; el pipeline resuelve **cómo** (orden, incrementalidad, checkpoints, reintentos) | El protagonista |
# MAGIC | **Lakeflow Jobs** | Orquestar: programar y encadenar tareas | Paso 15 |
# MAGIC
# MAGIC ### Imperativo (lo que ya hiciste) vs declarativo (lo de hoy)
# MAGIC | En el curso lo escribiste así | En un pipeline |
# MAGIC |---|---|
# MAGIC | `bronze/lending`: `readStream` + `writeStream` + `checkpointLocation` + `schemaLocation` por tabla | Una función que **devuelve** el `readStream` |
# MAGIC | `silver/notebooks/01`: tipar → cuarentena → deduplicar → `MERGE` con guarda de orden en `foreachBatch` | `create_auto_cdc_flow(keys=…, sequence_by=…)` |
# MAGIC | `silver/notebooks/04`: SCD2 con hash, doble source y cierre de versiones (≈300 líneas) | `stored_as_scd_type=2` |
# MAGIC | Reglas de calidad + tabla `_cuarentena` | `@dp.expect_or_drop(...)` con métricas en la UI |
# MAGIC | Un job que corre los notebooks en orden | El pipeline arma el **grafo** leyendo qué tabla lee de cuál |
# MAGIC
# MAGIC ### Los tres tipos de objeto
# MAGIC | Objeto | Cómo lee la fuente | Cuándo | Aquí |
# MAGIC |---|---|---|---|
# MAGIC | **Streaming table** | `spark.readStream` (solo lo nuevo) | Datos que **llegan**: archivos, mensajes, Bronze | Todo Bronze, casi todo Silver |
# MAGIC | **Materialized view** | `spark.read` / `SELECT` (completo) | Catálogos pequeños y agregados de Gold | países, campañas, Gold |
# MAGIC | **Temporary view** | cualquiera | Un paso intermedio que no se guarda | Tipar + expectations antes de AUTO CDC |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Los tipos de fuente de Wizard Bank (lo más importante de hoy)
# MAGIC Antes de escribir una línea, la pregunta es **qué tipo de fuente es** y **cómo cambian sus datos**. Eso decide el objeto Bronze y el patrón Silver.
# MAGIC
# MAGIC | # | Fuente | Llega a landing como | Cómo cambian los datos | Bronze | Silver |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 1 | **Base transaccional** (Azure SQL de Lending) | Un CSV por tabla en `wizard_bank_rdb/<tabla>/`: carga inicial + deltas | Las filas **se actualizan** (una solicitud cambia de estado) | Auto Loader CSV | AUTO CDC tipo 1 (estado vigente) y tipo 2 (historia) |
# MAGIC | 2 | **Proveedor externo** (tipos de cambio) | CSV del tercero, puede venir en subcarpetas y reenviar días | El proveedor **corrige** días ya publicados; no trae fecha de modificación | Auto Loader CSV recursivo | AUTO CDC tipo 1 con clave compuesta; gana el **último archivo** |
# MAGIC | 3 | **Sistema legado on-premise** (cobranzas) | CSV nocturno que copia ADF a `wizard_bank_onp/<tabla>/` | Cuotas cambian de estado; pagos y gestiones son **hechos**, pero el legado **reenvía** | Auto Loader CSV | Cuotas: AUTO CDC · pagos: AUTO CDC como dedup · gestiones: streaming append |
# MAGIC | 4 | **Bus de eventos** (Kafka, telemetría de la app) | `.parquet` con el registro de Kafka (`key`, `value`, `offset`…) en `wizard_bank_kafka/eventos_app/` | Los eventos **no cambian**, pero hay **duplicados**, **atrasados** y **payloads corruptos** | Auto Loader parquet con esquema fijo; `payload` sin interpretar | Streaming + `from_json` + watermark + `dropDuplicatesWithinWatermark`; sesiones con `session_window` |
# MAGIC | — | **Catálogos** (países, campañas) | Dentro de la fuente 1 | Casi nunca cambian; son chicos | Auto Loader CSV | **Materialized view** (lectura completa) |
# MAGIC
# MAGIC ### Lo que vas a construir
# MAGIC ```
# MAGIC  landing                               Bronze (_brz)                    Silver (_slv)                               Gold (_gld)
# MAGIC  wizard_bank_rdb/  ── Auto Loader ──►  paises, campanias        ─batch──► paises, campanias      (materialized view)
# MAGIC   (base transaccional)                 clientes, productos      ─CDC 2──► clientes, productos    (SCD2)          ─► dim_cliente
# MAGIC                                        ofertas, solicitudes,    ─CDC 1──► ofertas, solicitudes,  (último estado)  ─► fact_desembolsos ─► metricas_colocacion
# MAGIC                                        desembolsos                        desembolsos + solicitudes_estados (SCD2)
# MAGIC  wizard_bank_rdb/tipos_cambio ──────►  tipos_cambio             ─CDC 1──► tipos_cambio           (gana el último archivo)
# MAGIC   (proveedor externo)
# MAGIC  wizard_bank_onp/  ── Auto Loader ──►  cuotas, pagos, gestiones ─CDC/append► cuotas, pagos, gestiones                 ─► cartera_creditos
# MAGIC   (legado on-premise)
# MAGIC  wizard_bank_kafka/ ─ Auto Loader ──►  eventos_app (payload)    ─stream──► eventos_app (dedup) ─► sesiones_app        ─► metricas_funnel_app
# MAGIC   (Kafka)
# MAGIC ```

# COMMAND ----------

# Dónde vive todo (lo mismo que configurarás en el pipeline en el Paso 1)
CATALOGO = "datawizard_dev"
ESQUEMA = "lakeflow"
LANDING = "s3://lakehouse-datawizard/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 1 · Bronze
# MAGIC ## Paso 0 — Mirar la landing ▶️
# MAGIC Cuatro carpetas, una por tipo de fuente. Mira los nombres de archivo: carga inicial (`clientes.csv`), una versión con
# MAGIC **columnas nuevas** (`clientes__v2.csv`) y deltas con fecha. Bronze tendrá que aguantar todo eso.

# COMMAND ----------

for fuente in ["wizard_bank_rdb", "wizard_bank_onp", "wizard_bank_kafka"]:
    for carpeta in dbutils.fs.ls(f"{LANDING}/{fuente}"):
        archivos = dbutils.fs.ls(carpeta.path)
        print(f"{fuente}/{carpeta.name:24} {len(archivos):>3} archivos · ej.: {archivos[0].name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Crear el pipeline 🖱️
# MAGIC 1. En **Workspace**, entra a tu carpeta del curso → **Create** → **ETL pipeline**.
# MAGIC 2. Nombre: `pipeline_wizard_bank_lakehouse` (convención del repo: `pipeline_<capa>_<fuente>_<función>`; aquí son todas las capas).
# MAGIC 3. **Settings:** catálogo `datawizard_dev`, esquema `lakeflow`, cómputo **Serverless**.
# MAGIC 4. **Settings → Configuration** → **Add configuration**: clave **`landing`**, valor `s3://lakehouse-datawizard/landing`.
# MAGIC    Cada archivo la lee con `spark.conf.get("landing")`: así el mismo código sirve para otro bucket.
# MAGIC 5. Si el editor trae archivos de ejemplo en `transformations/`, **bórralos**.
# MAGIC
# MAGIC De aquí en adelante: **archivo nuevo en `transformations/`** → pegar el bloque 📋 → **Run pipeline**.
# MAGIC Cada archivo de Bronze tiene su propio `leer_csv` a propósito: así cada archivo se entiende solo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Fuente 1 · Base transaccional → Bronze
# MAGIC El extractor de la base deja un CSV por tabla. **Bronze guarda todo como `STRING`** (no usamos `inferColumnTypes`):
# MAGIC el tipado es responsabilidad de Silver, como en el curso. Se agregan dos columnas de auditoría.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/01_bronze_lending.py`**
# MAGIC ```python
# MAGIC # Paso 2 · Bronze de Lending. FUENTE 1: base de datos transaccional (Azure SQL).
# MAGIC # El extractor deja un CSV por tabla en landing/wizard_bank_rdb/<tabla>/ (carga inicial + deltas).
# MAGIC # Auto Loader los toma tal como llegan: todo STRING, más el archivo de origen y la hora de ingesta.
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")   # Settings → Configuration
# MAGIC
# MAGIC
# MAGIC def leer_csv(carpeta):
# MAGIC     return (spark.readStream.format("cloudFiles")          # Auto Loader: solo archivos nuevos
# MAGIC             .option("cloudFiles.format", "csv")
# MAGIC             .option("header", "true")                        # sin inferColumnTypes: Bronze guarda STRING
# MAGIC             .load(f"{LANDING}/wizard_bank_rdb/{carpeta}")
# MAGIC             .withColumn("_archivo", F.col("_metadata.file_path"))
# MAGIC             .withColumn("_ingestado_en", F.current_timestamp()))
# MAGIC
# MAGIC
# MAGIC @dp.table(name="paises_brz", comment="Fuente transaccional · catálogo de países, crudo")
# MAGIC def paises_brz():
# MAGIC     return leer_csv("paises")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="campanias_brz", comment="Fuente transaccional · campañas de marketing, crudo")
# MAGIC def campanias_brz():
# MAGIC     return leer_csv("campanias")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="productos_prestamo_brz", comment="Fuente transaccional · productos de crédito, crudo")
# MAGIC def productos_prestamo_brz():
# MAGIC     return leer_csv("productos_prestamo")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="clientes_brz", comment="Fuente transaccional · clientes, crudo (cada delta trae nuevos y cambios)")
# MAGIC def clientes_brz():
# MAGIC     return leer_csv("clientes")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="ofertas_preaprobadas_brz", comment="Fuente transaccional · ofertas preaprobadas, crudo")
# MAGIC def ofertas_preaprobadas_brz():
# MAGIC     return leer_csv("ofertas_preaprobadas")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="solicitudes_prestamo_brz", comment="Fuente transaccional · solicitudes, crudo (una solicitud llega varias veces con otro estado)")
# MAGIC def solicitudes_prestamo_brz():
# MAGIC     return leer_csv("solicitudes_prestamo")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="desembolsos_brz", comment="Fuente transaccional · desembolsos, crudo")
# MAGIC def desembolsos_brz():
# MAGIC     return leer_csv("desembolsos")
# MAGIC ```
# MAGIC Fíjate en lo que **no** está: ni `checkpointLocation`, ni `schemaLocation`, ni `writeStream`, ni `saveAsTable`.
# MAGIC Compáralo con `bronze/lending/src/ingestion/autoloader.py`.
# MAGIC
# MAGIC **Columnas nuevas (`clientes__v2.csv`):** el CSV v2 trae `fecha_creacion` y `fecha_actualizacion`, que el v1 no tenía.
# MAGIC Auto Loader detecta el cambio, detiene el flow y **el pipeline lo reinicia solo** con el esquema ampliado
# MAGIC (en el bundle de Bronze esto era un `try/except` y un reintento escritos a mano).
# MAGIC
# MAGIC **Run pipeline.** En el grafo aparecen 7 tablas sueltas: ninguna depende de otra.

# COMMAND ----------

for t in ["paises_brz", "campanias_brz", "productos_prestamo_brz", "clientes_brz",
          "ofertas_preaprobadas_brz", "solicitudes_prestamo_brz", "desembolsos_brz"]:
    print(f"{t:28} {spark.table(f'{CATALOGO}.{ESQUEMA}.{t}').count():>8,} filas")

# COMMAND ----------

# Las filas de la carga inicial (v1) no traen fecha_actualizacion: quedó NULL, no se perdieron
display(spark.sql(f"""
  SELECT regexp_extract(_archivo, '[^/]+$', 0) AS archivo, count(*) AS filas,
         count(fecha_actualizacion) AS con_fecha_actualizacion
  FROM {CATALOGO}.{ESQUEMA}.clientes_brz GROUP BY 1 ORDER BY 1"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Fuente 2 · Proveedor externo → Bronze
# MAGIC Mismo Auto Loader, dos diferencias que nacen del **tipo de fuente**: el proveedor puede organizar sus archivos en
# MAGIC subcarpetas (`recursiveFileLookup`) y **no** trae una fecha de modificación confiable. Guardamos `_ingestado_en`: será el desempate en Silver.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/02_bronze_tipos_cambio.py`**
# MAGIC ```python
# MAGIC # Paso 3 · Bronze de tipos de cambio. FUENTE 2: archivo de un proveedor externo.
# MAGIC # No viene de nuestra base: un tercero publica un CSV con las tasas (a veces en subcarpetas AAAA/MM/DD
# MAGIC # y a veces reenviando días ya publicados). No hay columna confiable de "última modificación":
# MAGIC # el orden lo da el archivo que llegó después.
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="tipos_cambio_brz", comment="Fuente externa (proveedor) · tipos de cambio a USD, crudo")
# MAGIC def tipos_cambio_brz():
# MAGIC     return (spark.readStream.format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "csv")
# MAGIC             .option("header", "true")
# MAGIC             .option("recursiveFileLookup", "true")           # el proveedor puede usar subcarpetas por fecha
# MAGIC             .load(f"{LANDING}/wizard_bank_rdb/tipos_cambio")
# MAGIC             .withColumn("_archivo", F.col("_metadata.file_path"))
# MAGIC             .withColumn("_ingestado_en", F.current_timestamp()))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Fuente 3 · Sistema legado on-premise → Bronze
# MAGIC Para el pipeline es "otra carpeta de CSV". Lo que cambia es el **contrato**: otro sistema, otros nombres
# MAGIC (`fecha_modificacion`), un lote por noche. Por eso va en su propio archivo: si mañana el legado cambia, se toca solo este.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/03_bronze_cobranzas.py`**
# MAGIC ```python
# MAGIC # Paso 4 · Bronze de Cobranzas. FUENTE 3: sistema legado on-premise.
# MAGIC # Azure Data Factory copia cada noche un CSV por tabla a landing/wizard_bank_onp/<tabla>/.
# MAGIC # Para el pipeline es igual que la fuente 1 (archivos), pero el sistema es otro: sus columnas de auditoría
# MAGIC # se llaman fecha_modificacion (no fecha_actualizacion) y el lote llega una vez al día.
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")
# MAGIC
# MAGIC
# MAGIC def leer_csv(carpeta):
# MAGIC     return (spark.readStream.format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "csv")
# MAGIC             .option("header", "true")
# MAGIC             .load(f"{LANDING}/wizard_bank_onp/{carpeta}")
# MAGIC             .withColumn("_archivo", F.col("_metadata.file_path"))
# MAGIC             .withColumn("_ingestado_en", F.current_timestamp()))
# MAGIC
# MAGIC
# MAGIC @dp.table(name="cuotas_brz", comment="Fuente legado on-premise · plan de cuotas, crudo (el estado de una cuota cambia)")
# MAGIC def cuotas_brz():
# MAGIC     return leer_csv("cuotas")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="pagos_brz", comment="Fuente legado on-premise · pagos, crudo (hecho: no cambia, pero puede reenviarse)")
# MAGIC def pagos_brz():
# MAGIC     return leer_csv("pagos")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="gestiones_cobranza_brz", comment="Fuente legado on-premise · gestiones de cobranza, crudo")
# MAGIC def gestiones_cobranza_brz():
# MAGIC     return leer_csv("gestiones_cobranza")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Fuente 4 · Kafka → Bronze
# MAGIC Un bus de mensajes entrega **registros de Kafka**, no tablas: `value` (los bytes del mensaje), `key`, `topic`,
# MAGIC `partition`, `offset`, `timestamp`. En el curso el topic se simula con `.parquet` que tienen **exactamente** ese esquema,
# MAGIC así que el resto del código es el mismo que con Kafka real: solo cambia la lectura (está en el comentario).
# MAGIC
# MAGIC Bronze guarda el mensaje **sin interpretar** (`payload` como texto): si el JSON viene roto, no se pierde; Silver decide.
# MAGIC El esquema del registro es **fijo**, por eso se declara con `.schema(...)` en vez de inferirlo.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/04_bronze_eventos_app.py`**
# MAGIC ```python
# MAGIC # Paso 5 · Bronze de la App. FUENTE 4: eventos en un bus de mensajes (Kafka, topic wizard.lending.eventos-app).
# MAGIC # En el curso el topic se simula: cada archivo .parquet de landing/wizard_bank_kafka/eventos_app/ trae los
# MAGIC # registros con el MISMO esquema que entrega Spark al leer Kafka (key, value, topic, partition, offset, timestamp).
# MAGIC # Bronze guarda el mensaje tal cual: el JSON del evento va como texto en `payload`; se interpreta en Silver.
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")
# MAGIC
# MAGIC ESQUEMA_KAFKA = ("key BINARY, value BINARY, topic STRING, partition INT, offset BIGINT, "
# MAGIC                  "timestamp TIMESTAMP, timestampType INT")
# MAGIC
# MAGIC
# MAGIC @dp.table(name="eventos_app_brz", comment="Fuente Kafka · un mensaje del topic por fila, payload sin interpretar")
# MAGIC def eventos_app_brz():
# MAGIC     # Con Kafka real solo cambia la lectura (las columnas son las mismas):
# MAGIC     #   spark.readStream.format("kafka")
# MAGIC     #        .option("kafka.bootstrap.servers", "...").option("subscribe", "wizard.lending.eventos-app").load()
# MAGIC     mensajes = (spark.readStream.format("cloudFiles")
# MAGIC                 .option("cloudFiles.format", "parquet")
# MAGIC                 .schema(ESQUEMA_KAFKA)                        # el registro de Kafka es fijo: no se infiere
# MAGIC                 .load(f"{LANDING}/wizard_bank_kafka/eventos_app"))
# MAGIC     return mensajes.select(
# MAGIC         F.col("value").cast("string").alias("payload"),
# MAGIC         F.col("key").cast("string").alias("kafka_key"),
# MAGIC         F.col("topic").alias("kafka_topic"),
# MAGIC         F.col("partition").alias("kafka_particion"),
# MAGIC         F.col("offset").alias("kafka_offset"),
# MAGIC         F.col("timestamp").alias("kafka_ts"),
# MAGIC         F.col("_metadata.file_path").alias("_archivo"),
# MAGIC         F.current_timestamp().alias("_ingestado_en"),
# MAGIC     )
# MAGIC ```
# MAGIC **Run pipeline.** Bronze completo: 14 tablas, cuatro fuentes.

# COMMAND ----------

for t in ["tipos_cambio_brz", "cuotas_brz", "pagos_brz", "gestiones_cobranza_brz", "eventos_app_brz"]:
    print(f"{t:28} {spark.table(f'{CATALOGO}.{ESQUEMA}.{t}').count():>8,} filas")
display(spark.table(f"{CATALOGO}.{ESQUEMA}.eventos_app_brz").select("kafka_particion", "kafka_offset", "payload").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿por qué no se parsea el JSON ya en Bronze?
# MAGIC <details><summary>Ver respuesta</summary>Porque Bronze es la copia fiel de la fuente. Si el contrato del evento cambia o un mensaje llega corrupto, en Bronze sigue el texto original y se puede reprocesar. Interpretarlo es una decisión de Silver.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 2 · Silver
# MAGIC El patrón de casi todo Silver es el mismo, y reemplaza los cuatro pasos de `silver/notebooks/01`:
# MAGIC
# MAGIC ```
# MAGIC  Bronze (STRING) ─► temporary view: try_cast + expectations ─► AUTO CDC (keys, sequence_by, scd_type) ─► Silver
# MAGIC ```
# MAGIC
# MAGIC | Expectation | Si la fila no cumple | Reemplaza a… |
# MAGIC |---|---|---|
# MAGIC | `@dp.expect` | Pasa; solo se cuenta | una métrica que nadie miraba |
# MAGIC | `@dp.expect_or_drop` | Se descarta y se cuenta | la tabla `_cuarentena` |
# MAGIC | `@dp.expect_or_fail` | Falla el update | un `assert` |
# MAGIC
# MAGIC ## Paso 6 — Catálogos: materialized view
# MAGIC Países (3 filas) y campañas (30) casi no cambian. Se leen **completos** con `spark.read`, se tipan y se deja la última
# MAGIC versión de cada clave. El pipeline mantiene la vista al día.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/05_silver_catalogos.py`**
# MAGIC ```python
# MAGIC # Paso 6 · Silver de catálogos: países y campañas como MATERIALIZED VIEW.
# MAGIC # Son tablas chicas (3 y 30 filas) que casi no cambian: no tiene sentido leerlas en streaming.
# MAGIC # Se leen COMPLETAS con spark.read (batch), se tipan y se deja la última versión de cada clave.
# MAGIC # El pipeline mantiene la vista al día en cada update.
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC
# MAGIC @dp.materialized_view(name="paises_slv", comment="Catálogo de países: una fila por país")
# MAGIC def paises_slv():
# MAGIC     ultima = Window.partitionBy("id_pais").orderBy(F.col("_ingestado_en").desc())
# MAGIC     return (spark.read.table("paises_brz")                     # spark.read: batch, no readStream
# MAGIC             .selectExpr("try_cast(id_pais AS SMALLINT) AS id_pais", "codigo_iso", "nombre_pais",
# MAGIC                         "moneda_codigo", "_ingestado_en")
# MAGIC             .withColumn("_rn", F.row_number().over(ultima))
# MAGIC             .filter("_rn = 1 AND id_pais IS NOT NULL")
# MAGIC             .drop("_rn", "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC @dp.materialized_view(name="campanias_slv", comment="Catálogo de campañas: una fila por campaña")
# MAGIC @dp.expect("presupuesto_positivo", "presupuesto > 0")          # solo se mide; la fila pasa
# MAGIC def campanias_slv():
# MAGIC     ultima = Window.partitionBy("id_campania").orderBy(F.col("_ingestado_en").desc())
# MAGIC     return (spark.read.table("campanias_brz")
# MAGIC             .selectExpr("try_cast(id_campania AS INT) AS id_campania",
# MAGIC                         "try_cast(id_pais AS SMALLINT) AS id_pais",
# MAGIC                         "nombre_campania", "tipo_campania",
# MAGIC                         "try_cast(fecha_inicio AS DATE) AS fecha_inicio",
# MAGIC                         "try_cast(fecha_fin AS DATE) AS fecha_fin",
# MAGIC                         "try_cast(presupuesto AS DECIMAL(14,2)) AS presupuesto",
# MAGIC                         "_ingestado_en")
# MAGIC             .withColumn("_rn", F.row_number().over(ultima))
# MAGIC             .filter("_rn = 1 AND id_campania IS NOT NULL")
# MAGIC             .drop("_rn", "_ingestado_en"))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Maestras con historia: AUTO CDC tipo 2 (clientes y productos)
# MAGIC Lo que en `silver/notebooks/04` eran ≈300 líneas (hash de atributos, cerrar la versión vigente, abrir otra, llegadas tardías)
# MAGIC aquí son tres parámetros:
# MAGIC
# MAGIC - `keys`: la clave de negocio.
# MAGIC - `sequence_by`: quién es más nuevo. Aquí un `struct(fecha_version, _ingestado_en)`: primero la fecha del cambio en el origen y,
# MAGIC   si empata, el archivo que llegó después. `fecha_version` usa `coalesce` porque la carga v1 no traía `fecha_actualizacion`.
# MAGIC - `track_history_column_list`: **qué cambios abren versión** (Tipo 2). El resto (email, ciudad) se sobrescribe en la versión vigente (Tipo 1).
# MAGIC
# MAGIC AUTO CDC agrega `__START_AT` y `__END_AT`; la versión vigente tiene `__END_AT` vacío. Como `sequence_by` es un `struct`,
# MAGIC esas columnas también lo son: la fecha se lee con `__START_AT.fecha_version`.
# MAGIC
# MAGIC Fíjate en `score_en_rango`: es `@dp.expect` (solo mide). Con `expect_or_drop` se perdían ~800 versiones de clientes por un
# MAGIC score fuera de rango: **descartar una maestra borra al cliente de todo lo que viene después**. En maestras, mide; en hechos, filtra.
# MAGIC De regalo: **`dim_productos`**, que en el curso no tenía notebook de Silver, sale con el mismo patrón.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/06_silver_maestras_scd2.py`**
# MAGIC ```python
# MAGIC # Paso 7 · Silver de maestras con historia: AUTO CDC tipo 2 (clientes y productos).
# MAGIC # Si un cliente cambia su score o un producto su tasa, para riesgo importa la versión vigente CUANDO
# MAGIC # se aprobó el crédito. SCD tipo 2 no sobrescribe: cierra la versión vieja (__END_AT) y abre otra (__START_AT).
# MAGIC #
# MAGIC # Patrón: temporary view (tipar + expectations) → AUTO CDC.
# MAGIC # AUTO CDC no acepta expectations directamente; la vista intermedia filtra y no ocupa almacenamiento.
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # ---------- clientes ----------
# MAGIC
# MAGIC @dp.temporary_view(name="clientes_tipados")
# MAGIC @dp.expect_or_drop("clave_presente", "id_cliente IS NOT NULL")
# MAGIC @dp.expect("score_en_rango", "score_interno BETWEEN 300 AND 900")       # solo mide: un score raro no borra al cliente
# MAGIC @dp.expect_or_drop("riesgo_valido", "nivel_riesgo IN ('A','B','C','D','E')")
# MAGIC def clientes_tipados():
# MAGIC     return (spark.readStream.table("clientes_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_cliente AS BIGINT) AS id_cliente",
# MAGIC                 "try_cast(id_pais AS SMALLINT) AS id_pais",
# MAGIC                 "tipo_documento", "numero_documento", "nombres", "apellidos",
# MAGIC                 "try_cast(fecha_nacimiento AS DATE) AS fecha_nacimiento",
# MAGIC                 "email", "ciudad", "situacion_laboral",
# MAGIC                 "try_cast(ingreso_mensual_declarado AS DECIMAL(14,2)) AS ingreso_mensual_declarado",
# MAGIC                 "try_cast(score_interno AS SMALLINT) AS score_interno",
# MAGIC                 "nivel_riesgo",
# MAGIC                 "try_cast(id_campania_captacion AS INT) AS id_campania_captacion",
# MAGIC                 "try_cast(fecha_registro AS TIMESTAMP) AS fecha_registro",
# MAGIC                 # Orden de versiones: la carga inicial no trae fecha_actualizacion (columna agregada en v2)
# MAGIC                 "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
# MAGIC                 "try_cast(fecha_registro AS TIMESTAMP)) AS fecha_version",
# MAGIC                 "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="clientes_slv", comment="Clientes con historia (SCD tipo 2): una fila por versión",
# MAGIC                           cluster_by=["id_cliente"])
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="clientes_slv",
# MAGIC     source="clientes_tipados",
# MAGIC     keys=["id_cliente"],
# MAGIC     sequence_by=F.struct("fecha_version", "_ingestado_en"),   # a igual fecha, gana el archivo más nuevo
# MAGIC     stored_as_scd_type=2,                                     # agrega __START_AT y __END_AT
# MAGIC     track_history_column_list=["ingreso_mensual_declarado", "situacion_laboral", "score_interno", "nivel_riesgo"],
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC
# MAGIC # ---------- productos ----------
# MAGIC
# MAGIC @dp.temporary_view(name="productos_tipados")
# MAGIC @dp.expect_or_drop("clave_presente", "id_producto IS NOT NULL")
# MAGIC @dp.expect_or_drop("tasa_positiva", "tasa_interes_anual > 0")
# MAGIC def productos_tipados():
# MAGIC     return (spark.readStream.table("productos_prestamo_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_producto AS INT) AS id_producto",
# MAGIC                 "try_cast(id_pais AS SMALLINT) AS id_pais",
# MAGIC                 "nombre_producto", "tipo_producto",
# MAGIC                 "try_cast(monto_minimo AS DECIMAL(14,2)) AS monto_minimo",
# MAGIC                 "try_cast(monto_maximo AS DECIMAL(14,2)) AS monto_maximo",
# MAGIC                 "try_cast(plazo_min_meses AS SMALLINT) AS plazo_min_meses",
# MAGIC                 "try_cast(plazo_max_meses AS SMALLINT) AS plazo_max_meses",
# MAGIC                 "try_cast(tasa_interes_anual AS DECIMAL(6,3)) AS tasa_interes_anual",
# MAGIC                 "try_cast(es_activo AS BOOLEAN) AS es_activo",
# MAGIC                 "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP)) AS fecha_version",
# MAGIC                 "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="productos_slv", comment="Productos con historia de tasa (SCD tipo 2)")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="productos_slv",
# MAGIC     source="productos_tipados",
# MAGIC     keys=["id_producto"],
# MAGIC     sequence_by=F.struct("fecha_version", "_ingestado_en"),
# MAGIC     stored_as_scd_type=2,
# MAGIC     track_history_column_list=["tasa_interes_anual", "monto_minimo", "monto_maximo", "es_activo"],
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC ```
# MAGIC **Run pipeline** y valida:

# COMMAND ----------

display(spark.sql(f"""
  SELECT count(*) AS versiones, count(DISTINCT id_cliente) AS clientes, count_if(__END_AT IS NULL) AS vigentes
  FROM {CATALOGO}.{ESQUEMA}.clientes_slv"""))

# COMMAND ----------

# Un cliente con historia: la versión vieja quedó cerrada
display(spark.sql(f"""
  SELECT id_cliente, ingreso_mensual_declarado, score_interno, nivel_riesgo,
         __START_AT.fecha_version AS vigente_desde, __END_AT.fecha_version AS vigente_hasta
  FROM {CATALOGO}.{ESQUEMA}.clientes_slv
  WHERE id_cliente IN (SELECT id_cliente FROM {CATALOGO}.{ESQUEMA}.clientes_slv GROUP BY id_cliente HAVING count(*) > 1 LIMIT 3)
  ORDER BY id_cliente, __START_AT"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — Transaccional: AUTO CDC tipo 1 (el MERGE sin MERGE)
# MAGIC Ofertas, solicitudes y desembolsos **cambian de estado**. En Silver: una fila por clave con su último estado.
# MAGIC Es el `MERGE` con guarda de orden de `silver/notebooks/01`, sin escribirlo: si llega una versión **vieja** tarde, `sequence_by` impide que pise a la nueva.
# MAGIC
# MAGIC **Dos destinos desde la misma vista:** `solicitudes_tipadas` alimenta el último estado (tipo 1) **y** el historial de estados
# MAGIC (tipo 2 que solo versiona `estado_solicitud`). Es lo que hacía `silver/notebooks/05`.
# MAGIC
# MAGIC `delta.enableChangeDataFeed` deja las tablas listas para que un consumidor lea **solo lo que cambió** (Sesión 18).
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/07_silver_transaccional.py`**
# MAGIC ```python
# MAGIC # Paso 8 · Silver transaccional: AUTO CDC tipo 1 (el MERGE, sin escribir el MERGE).
# MAGIC # Ofertas, solicitudes y desembolsos CAMBIAN de estado (una solicitud pasa de Iniciada a Aprobada).
# MAGIC # En Silver queda una fila por clave con su último estado. Además, de la MISMA vista de solicitudes
# MAGIC # sale un segundo flow: el historial de estados (SCD tipo 2 que solo versiona el estado).
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC CDF = {"delta.enableChangeDataFeed": "true"}   # Gold puede leer solo lo que cambió
# MAGIC
# MAGIC # ---------- ofertas ----------
# MAGIC
# MAGIC @dp.temporary_view(name="ofertas_tipadas")
# MAGIC @dp.expect_or_drop("clave_presente", "id_oferta IS NOT NULL AND id_cliente IS NOT NULL")
# MAGIC @dp.expect_or_drop("monto_positivo", "monto_ofertado > 0")
# MAGIC def ofertas_tipadas():
# MAGIC     return (spark.readStream.table("ofertas_preaprobadas_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_oferta AS BIGINT) AS id_oferta",
# MAGIC                 "try_cast(id_cliente AS BIGINT) AS id_cliente",
# MAGIC                 "try_cast(id_producto AS INT) AS id_producto",
# MAGIC                 "try_cast(id_campania AS INT) AS id_campania",
# MAGIC                 "try_cast(monto_ofertado AS DECIMAL(14,2)) AS monto_ofertado",
# MAGIC                 "try_cast(plazo_meses_ofertado AS SMALLINT) AS plazo_meses_ofertado",
# MAGIC                 "try_cast(tasa_ofertada AS DECIMAL(6,3)) AS tasa_ofertada",
# MAGIC                 "motor_asignacion",
# MAGIC                 "try_cast(fecha_generacion AS TIMESTAMP) AS fecha_generacion",
# MAGIC                 "try_cast(fecha_vigencia_fin AS DATE) AS fecha_vigencia_fin",
# MAGIC                 "estado_oferta",
# MAGIC                 "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
# MAGIC                 "try_cast(fecha_generacion AS TIMESTAMP)) AS fecha_version",
# MAGIC                 "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="ofertas_preaprobadas_slv", comment="Último estado de cada oferta",
# MAGIC                           table_properties=CDF)
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="ofertas_preaprobadas_slv", source="ofertas_tipadas",
# MAGIC     keys=["id_oferta"],                                       # la condición ON del MERGE
# MAGIC     sequence_by=F.struct("fecha_version", "_ingestado_en"),   # si llegan dos versiones, gana la más reciente
# MAGIC     stored_as_scd_type=1,                                     # tipo 1: se sobrescribe, sin historia
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC
# MAGIC # ---------- solicitudes ----------
# MAGIC
# MAGIC @dp.temporary_view(name="solicitudes_tipadas")
# MAGIC @dp.expect_or_drop("clave_presente", "id_solicitud IS NOT NULL AND id_cliente IS NOT NULL")
# MAGIC @dp.expect_or_drop("monto_positivo", "monto_solicitado > 0")
# MAGIC @dp.expect_or_drop("estado_valido", "estado_solicitud IN ('Iniciada','En evaluacion','Aprobada','Rechazada','Desistida')")
# MAGIC def solicitudes_tipadas():
# MAGIC     return (spark.readStream.table("solicitudes_prestamo_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_solicitud AS BIGINT) AS id_solicitud",
# MAGIC                 "try_cast(id_oferta AS BIGINT) AS id_oferta",
# MAGIC                 "try_cast(id_cliente AS BIGINT) AS id_cliente",
# MAGIC                 "try_cast(id_producto AS INT) AS id_producto",
# MAGIC                 "canal",
# MAGIC                 "try_cast(monto_solicitado AS DECIMAL(14,2)) AS monto_solicitado",
# MAGIC                 "try_cast(plazo_meses_solicitado AS SMALLINT) AS plazo_meses_solicitado",
# MAGIC                 "try_cast(fecha_hora_solicitud AS TIMESTAMP) AS fecha_hora_solicitud",
# MAGIC                 "estado_solicitud",
# MAGIC                 "try_cast(score_evaluacion AS SMALLINT) AS score_evaluacion",
# MAGIC                 "decision_motor",
# MAGIC                 "try_cast(monto_aprobado AS DECIMAL(14,2)) AS monto_aprobado",
# MAGIC                 "try_cast(tasa_aprobada AS DECIMAL(6,3)) AS tasa_aprobada",
# MAGIC                 "motivo_rechazo",
# MAGIC                 "try_cast(fecha_hora_resolucion AS TIMESTAMP) AS fecha_hora_resolucion",
# MAGIC                 "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
# MAGIC                 "try_cast(fecha_hora_solicitud AS TIMESTAMP)) AS fecha_version",
# MAGIC                 "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="solicitudes_prestamo_slv", comment="Último estado de cada solicitud",
# MAGIC                           table_properties=CDF, cluster_by=["fecha_hora_solicitud"])
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="solicitudes_prestamo_slv", source="solicitudes_tipadas",
# MAGIC     keys=["id_solicitud"],
# MAGIC     sequence_by=F.struct("fecha_version", "_ingestado_en"),
# MAGIC     stored_as_scd_type=1,
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC
# MAGIC # Mismo origen, otro destino: el historial de estados. SCD tipo 2 que abre versión SOLO si cambia el estado.
# MAGIC dp.create_streaming_table(name="solicitudes_estados_slv", comment="Historial de estados de cada solicitud (SCD tipo 2 sobre estado_solicitud)")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="solicitudes_estados_slv", source="solicitudes_tipadas",
# MAGIC     keys=["id_solicitud"],
# MAGIC     sequence_by=F.struct("fecha_version", "_ingestado_en"),
# MAGIC     stored_as_scd_type=2,
# MAGIC     track_history_column_list=["estado_solicitud"],
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC
# MAGIC # ---------- desembolsos ----------
# MAGIC
# MAGIC @dp.temporary_view(name="desembolsos_tipados")
# MAGIC @dp.expect_or_drop("clave_presente", "id_desembolso IS NOT NULL AND id_solicitud IS NOT NULL")
# MAGIC @dp.expect_or_drop("monto_positivo", "monto_desembolsado > 0")
# MAGIC @dp.expect("moneda_conocida", "moneda IN ('PEN','COP','BOB')")
# MAGIC def desembolsos_tipados():
# MAGIC     return (spark.readStream.table("desembolsos_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_desembolso AS BIGINT) AS id_desembolso",
# MAGIC                 "try_cast(id_solicitud AS BIGINT) AS id_solicitud",
# MAGIC                 "try_cast(id_cliente AS BIGINT) AS id_cliente",
# MAGIC                 "try_cast(monto_desembolsado AS DECIMAL(14,2)) AS monto_desembolsado",
# MAGIC                 "moneda",
# MAGIC                 "try_cast(tasa_aplicada AS DECIMAL(6,3)) AS tasa_aplicada",
# MAGIC                 "try_cast(plazo_meses AS SMALLINT) AS plazo_meses",
# MAGIC                 "try_cast(comision_cobrada AS DECIMAL(12,2)) AS comision_cobrada",
# MAGIC                 "cuenta_destino_masked",
# MAGIC                 "try_cast(fecha_hora_desembolso AS TIMESTAMP) AS fecha_hora_desembolso",
# MAGIC                 "estado_desembolso",
# MAGIC                 "referencia_bancaria",
# MAGIC                 "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
# MAGIC                 "try_cast(fecha_hora_desembolso AS TIMESTAMP)) AS fecha_version",
# MAGIC                 "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="desembolsos_slv", comment="Último estado de cada desembolso", table_properties=CDF)
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="desembolsos_slv", source="desembolsos_tipados",
# MAGIC     keys=["id_desembolso"],
# MAGIC     sequence_by=F.struct("fecha_version", "_ingestado_en"),
# MAGIC     stored_as_scd_type=1,
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC ```

# COMMAND ----------

display(spark.sql(f"""
  SELECT estado_solicitud, count(*) AS solicitudes, count(DISTINCT id_solicitud) AS claves
  FROM {CATALOGO}.{ESQUEMA}.solicitudes_prestamo_slv GROUP BY 1 ORDER BY 2 DESC"""))

# COMMAND ----------

# Historial: una solicitud que pasó por varios estados tiene una fila por estado
display(spark.sql(f"""
  SELECT id_solicitud, estado_solicitud, __START_AT.fecha_version AS desde, __END_AT.fecha_version AS hasta
  FROM {CATALOGO}.{ESQUEMA}.solicitudes_estados_slv
  WHERE id_solicitud IN (SELECT id_solicitud FROM {CATALOGO}.{ESQUEMA}.solicitudes_estados_slv GROUP BY 1 HAVING count(*) > 1 LIMIT 3)
  ORDER BY id_solicitud, __START_AT"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 9 — Proveedor externo: clave compuesta y "gana el último archivo"
# MAGIC El tipo de fuente decide el `sequence_by`. El proveedor no trae fecha de modificación: si reenvía el 15 de agosto corregido,
# MAGIC la versión correcta es **la del archivo que llegó después** → `sequence_by="_ingestado_en"`. La clave es compuesta: día + par de monedas.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/08_silver_tipos_cambio.py`**
# MAGIC ```python
# MAGIC # Paso 9 · Silver de tipos de cambio (fuente externa): AUTO CDC tipo 1 con clave compuesta.
# MAGIC # Un día y un par de monedas = una tasa. Si el proveedor reenvía un día corregido, gana el ARCHIVO más
# MAGIC # nuevo: el orden lo da _ingestado_en, no una columna del proveedor.
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC
# MAGIC @dp.temporary_view(name="tipos_cambio_tipados")
# MAGIC @dp.expect_or_drop("clave_presente", "fecha IS NOT NULL AND moneda_origen IS NOT NULL AND moneda_destino IS NOT NULL")
# MAGIC @dp.expect_or_drop("tasa_positiva", "tasa_venta > 0 AND tasa_compra > 0")
# MAGIC @dp.expect("compra_menor_que_venta", "tasa_compra <= tasa_venta")
# MAGIC def tipos_cambio_tipados():
# MAGIC     return (spark.readStream.table("tipos_cambio_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(fecha AS DATE) AS fecha",
# MAGIC                 "moneda_origen", "moneda_destino",
# MAGIC                 "try_cast(tasa_compra AS DECIMAL(18,6)) AS tasa_compra",
# MAGIC                 "try_cast(tasa_venta AS DECIMAL(18,6)) AS tasa_venta",
# MAGIC                 "_archivo", "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="tipos_cambio_slv", comment="Una tasa por día y par de monedas; gana el último archivo del proveedor")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="tipos_cambio_slv", source="tipos_cambio_tipados",
# MAGIC     keys=["fecha", "moneda_origen", "moneda_destino"],   # clave compuesta
# MAGIC     sequence_by="_ingestado_en",                         # el archivo más nuevo gana
# MAGIC     stored_as_scd_type=1,
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 10 — Legado on-premise: tres tipos de tabla en el mismo sistema
# MAGIC | Tabla | Qué es | Objeto |
# MAGIC |---|---|---|
# MAGIC | `cuotas` | Su estado cambia (Pendiente → Pagada) | AUTO CDC tipo 1 |
# MAGIC | `pagos` | Un **hecho**: no cambia. Pero el legado **reenvía** filas | AUTO CDC tipo 1 por `id_pago`: deduplica los reenvíos |
# MAGIC | `gestiones` | Un hecho que no cambia | **Streaming table simple** (append) + expectations |
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/09_silver_cobranzas.py`**
# MAGIC ```python
# MAGIC # Paso 10 · Silver de Cobranzas (fuente legado). Tres tipos de tabla en el mismo sistema:
# MAGIC #   cuotas    → su estado cambia (Pendiente → Pagada / Vencida): AUTO CDC tipo 1
# MAGIC #   pagos     → hecho que no cambia, pero el legado a veces REENVÍA filas: AUTO CDC tipo 1 como deduplicador por clave
# MAGIC #   gestiones → hecho que no cambia: streaming table simple (append) + expectations
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # ---------- cuotas ----------
# MAGIC
# MAGIC @dp.temporary_view(name="cuotas_tipadas")
# MAGIC @dp.expect_or_drop("clave_presente", "id_cuota IS NOT NULL")
# MAGIC @dp.expect_or_drop("estado_valido", "estado_cuota IN ('Pendiente','Pagada','Vencida')")
# MAGIC @dp.expect("cuota_cuadra", "abs(monto_cuota - (monto_capital + monto_interes)) <= 0.01")
# MAGIC def cuotas_tipadas():
# MAGIC     return (spark.readStream.table("cuotas_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_cuota AS BIGINT) AS id_cuota",
# MAGIC                 "try_cast(numero_credito AS BIGINT) AS numero_credito",
# MAGIC                 "try_cast(numero_cuota AS SMALLINT) AS numero_cuota",
# MAGIC                 "try_cast(fecha_vencimiento AS DATE) AS fecha_vencimiento",
# MAGIC                 "try_cast(monto_cuota AS DECIMAL(12,2)) AS monto_cuota",
# MAGIC                 "try_cast(monto_capital AS DECIMAL(12,2)) AS monto_capital",
# MAGIC                 "try_cast(monto_interes AS DECIMAL(12,2)) AS monto_interes",
# MAGIC                 "estado_cuota",
# MAGIC                 "coalesce(try_cast(fecha_modificacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP)) AS fecha_version",
# MAGIC                 "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="cuotas_slv", comment="Estado vigente de cada cuota", cluster_by=["numero_credito"])
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="cuotas_slv", source="cuotas_tipadas",
# MAGIC     keys=["id_cuota"],
# MAGIC     sequence_by=F.struct("fecha_version", "_ingestado_en"),
# MAGIC     stored_as_scd_type=1,
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC
# MAGIC # ---------- pagos ----------
# MAGIC
# MAGIC @dp.temporary_view(name="pagos_tipados")
# MAGIC @dp.expect_or_drop("clave_presente", "id_pago IS NOT NULL AND numero_credito IS NOT NULL")
# MAGIC @dp.expect_or_drop("monto_positivo", "monto_pagado > 0")
# MAGIC def pagos_tipados():
# MAGIC     return (spark.readStream.table("pagos_brz")
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_pago AS BIGINT) AS id_pago",
# MAGIC                 "try_cast(numero_credito AS BIGINT) AS numero_credito",
# MAGIC                 "try_cast(numero_cuota AS SMALLINT) AS numero_cuota",
# MAGIC                 "try_cast(fecha_pago AS TIMESTAMP) AS fecha_pago",
# MAGIC                 "try_cast(monto_pagado AS DECIMAL(12,2)) AS monto_pagado",
# MAGIC                 "medio_pago",
# MAGIC                 "_ingestado_en"))
# MAGIC
# MAGIC
# MAGIC dp.create_streaming_table(name="pagos_slv", comment="Pagos únicos por id_pago (los reenvíos del legado no se duplican)")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="pagos_slv", source="pagos_tipados",
# MAGIC     keys=["id_pago"],
# MAGIC     sequence_by="_ingestado_en",
# MAGIC     stored_as_scd_type=1,
# MAGIC     except_column_list=["_ingestado_en"],
# MAGIC )
# MAGIC
# MAGIC # ---------- gestiones ----------
# MAGIC
# MAGIC @dp.table(name="gestiones_cobranza_slv", comment="Gestiones de cobranza válidas (append)")
# MAGIC @dp.expect_or_drop("clave_presente", "id_gestion IS NOT NULL AND numero_credito IS NOT NULL")
# MAGIC @dp.expect("resultado_informado", "resultado IS NOT NULL")
# MAGIC def gestiones_cobranza_slv():
# MAGIC     return (spark.readStream.table("gestiones_cobranza_brz")    # streaming simple: cada fila nueva se agrega
# MAGIC             .selectExpr(
# MAGIC                 "try_cast(id_gestion AS BIGINT) AS id_gestion",
# MAGIC                 "try_cast(numero_credito AS BIGINT) AS numero_credito",
# MAGIC                 "try_cast(fecha_gestion AS TIMESTAMP) AS fecha_gestion",
# MAGIC                 "tipo_gestion", "resultado",
# MAGIC                 "try_cast(dias_mora_al_momento AS SMALLINT) AS dias_mora_al_momento",
# MAGIC                 "gestor"))
# MAGIC ```

# COMMAND ----------

# Pagos (AUTO CDC) quedan únicos; gestiones (append) conservan los reenvíos del legado
display(spark.sql(f"""
  SELECT 'pagos_slv' AS tabla, count(*) AS filas, count(DISTINCT id_pago) AS claves FROM {CATALOGO}.{ESQUEMA}.pagos_slv
  UNION ALL
  SELECT 'gestiones_cobranza_slv', count(*), count(DISTINCT id_gestion) FROM {CATALOGO}.{ESQUEMA}.gestiones_cobranza_slv"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** `gestiones_cobranza_slv` tiene más filas que claves. ¿Qué cambiarías?
# MAGIC <details><summary>Ver respuesta</summary>Un append guarda todo lo que llega, incluidos los reenvíos. Si el negocio necesita una fila por gestión, se usa el mismo patrón que pagos (AUTO CDC por <code>id_gestion</code>). Elegir append es correcto solo si la fuente garantiza que no reenvía.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 11 — Kafka: interpretar, deduplicar y armar sesiones
# MAGIC Los eventos no tienen "último estado", así que **no** se usa AUTO CDC. Sus problemas son de streaming (Sesión 17):
# MAGIC
# MAGIC - `from_json` con el contrato del mensaje; lo que no parsea queda con `id_evento` NULL y la expectation lo descarta.
# MAGIC - `withWatermark("ts_evento", "1 hour")` + `dropDuplicatesWithinWatermark(["id_evento"])`: el productor reintenta y duplica;
# MAGIC   se recuerda cada `id_evento` durante una hora **de tiempo del evento**, no de llegada.
# MAGIC - `sesiones_app_slv`: una streaming table que **lee otra del pipeline** y agrega con `session_window` (30 min de silencio cierran la sesión).
# MAGIC   Es estado de streaming: una sesión se escribe cuando el watermark pasa su fin, por eso las más recientes aparecen en el siguiente update.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/10_silver_eventos_app.py`**
# MAGIC ```python
# MAGIC # Paso 11 · Silver de la App (fuente Kafka): interpretar, deduplicar con watermark y armar sesiones.
# MAGIC # Los eventos NO tienen "último estado": cada evento es único. Los problemas son otros:
# MAGIC #   - payloads corruptos (JSON que no parsea)   → expectation: se descartan y se cuentan
# MAGIC #   - el productor reintenta y duplica mensajes  → dropDuplicatesWithinWatermark sobre id_evento
# MAGIC #   - eventos que llegan tarde                   → watermark sobre el tiempo DEL EVENTO, no de llegada
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # Contrato del mensaje (Sesión 11): structs anidados `sesion` y `contexto`
# MAGIC ESQUEMA_EVENTO = (
# MAGIC     "id_evento STRING, tipo_evento STRING, timestamp_evento STRING, id_cliente BIGINT, id_oferta BIGINT, "
# MAGIC     "id_pais INT, canal STRING, "
# MAGIC     "sesion STRUCT<id_sesion: STRING, version_app: STRING, sistema_operativo: STRING, modelo_dispositivo: STRING>, "
# MAGIC     "contexto STRUCT<ubicacion_pantalla: STRING, posicion: INT, tiempo_visible_seg: DOUBLE, "
# MAGIC     "monto_simulado: DOUBLE, plazo_simulado: INT>"
# MAGIC )
# MAGIC
# MAGIC
# MAGIC @dp.table(name="eventos_app_slv", comment="Eventos de la app, interpretados y sin duplicados",
# MAGIC           cluster_by=["fecha_evento"])
# MAGIC @dp.expect_or_drop("payload_valido", "id_evento IS NOT NULL AND ts_evento IS NOT NULL")
# MAGIC @dp.expect_or_drop("tipo_conocido",
# MAGIC                    "tipo_evento IN ('oferta_vista','oferta_click','simulacion_realizada','solicitud_iniciada','banner_cerrado')")
# MAGIC def eventos_app_slv():
# MAGIC     e = F.from_json(F.col("payload"), ESQUEMA_EVENTO)
# MAGIC     return (spark.readStream.table("eventos_app_brz")
# MAGIC             .select(e.alias("e"), "kafka_offset", "kafka_ts")
# MAGIC             .select(
# MAGIC                 "e.id_evento", "e.tipo_evento",
# MAGIC                 F.to_timestamp("e.timestamp_evento").alias("ts_evento"),
# MAGIC                 F.to_date(F.to_timestamp("e.timestamp_evento")).alias("fecha_evento"),
# MAGIC                 "e.id_cliente", "e.id_oferta", "e.id_pais", "e.canal",
# MAGIC                 F.col("e.sesion.id_sesion").alias("id_sesion"),
# MAGIC                 F.col("e.sesion.sistema_operativo").alias("sistema_operativo"),
# MAGIC                 F.col("e.contexto.ubicacion_pantalla").alias("ubicacion_pantalla"),
# MAGIC                 F.col("e.contexto.monto_simulado").alias("monto_simulado"),
# MAGIC                 "kafka_offset", "kafka_ts")
# MAGIC             .withWatermark("ts_evento", "1 hour")                  # cuánto esperamos a un duplicado o un atrasado
# MAGIC             .dropDuplicatesWithinWatermark(["id_evento"]))
# MAGIC
# MAGIC
# MAGIC @dp.table(name="sesiones_app_slv", comment="Sesiones de navegación: eventos del mismo cliente con menos de 30 min de silencio")
# MAGIC def sesiones_app_slv():
# MAGIC     return (spark.readStream.table("eventos_app_slv")
# MAGIC             .withWatermark("ts_evento", "1 hour")
# MAGIC             .groupBy(F.session_window("ts_evento", "30 minutes").alias("sesion"), "id_cliente", "canal")
# MAGIC             .agg(F.count("*").alias("n_eventos"),
# MAGIC                  F.max(F.col("tipo_evento") == "oferta_click").alias("hizo_click"),
# MAGIC                  F.max(F.col("tipo_evento") == "simulacion_realizada").alias("simulo"),
# MAGIC                  F.max(F.col("tipo_evento") == "solicitud_iniciada").alias("inicio_solicitud"))
# MAGIC             .select(F.col("sesion.start").alias("inicio_sesion"), F.col("sesion.end").alias("fin_sesion"),
# MAGIC                     "id_cliente", "canal", "n_eventos", "hizo_click", "simulo", "inicio_solicitud"))
# MAGIC ```
# MAGIC **Run pipeline** y abre **`eventos_app_slv` → Data quality**: verás cuántos payloads corruptos y tipos desconocidos se descartaron.

# COMMAND ----------

display(spark.sql(f"""
  SELECT (SELECT count(*) FROM {CATALOGO}.{ESQUEMA}.eventos_app_brz)              AS mensajes_bronze,
         (SELECT count(*) FROM {CATALOGO}.{ESQUEMA}.eventos_app_slv)              AS eventos_silver,
         (SELECT count(DISTINCT id_evento) FROM {CATALOGO}.{ESQUEMA}.eventos_app_slv) AS eventos_unicos,
         (SELECT count(*) FROM {CATALOGO}.{ESQUEMA}.sesiones_app_slv)             AS sesiones"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 3 · Gold y operación
# MAGIC ## Paso 12 — Gold en SQL: materialized views
# MAGIC Gold une y agrega datasets **completos**: no se lee en streaming. Una **materialized view** en SQL se refresca en cada update
# MAGIC (incremental cuando puede). Un pipeline mezcla `.py` y `.sql` sin problema: el grafo los une por nombre.
# MAGIC Las expectations también existen en SQL (`CONSTRAINT … EXPECT`).
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/11_gold.sql`**
# MAGIC ```sql
# MAGIC -- Paso 12 · Gold: MATERIALIZED VIEWS en SQL sobre las tablas Silver del mismo pipeline.
# MAGIC -- Gold agrega y une datasets completos: se lee en batch (sin STREAM) y el pipeline recalcula lo que cambió.
# MAGIC -- Un pipeline puede mezclar archivos .py y .sql: el grafo los une igual.
# MAGIC
# MAGIC -- Dimensión cliente: la SCD2 de Silver con nombres de negocio y sin PII
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_cliente_gld
# MAGIC COMMENT 'Dimensión cliente con historia: una fila por versión, sin PII'
# MAGIC CLUSTER BY (id_cliente)
# MAGIC AS SELECT
# MAGIC   id_cliente,
# MAGIC   id_pais,
# MAGIC   situacion_laboral,
# MAGIC   ingreso_mensual_declarado,
# MAGIC   score_interno,
# MAGIC   nivel_riesgo,
# MAGIC   ciudad,
# MAGIC   id_campania_captacion,
# MAGIC   __START_AT.fecha_version AS vigente_desde,      -- sequence_by fue un struct: __START_AT/__END_AT también lo son
# MAGIC   __END_AT.fecha_version   AS vigente_hasta,
# MAGIC   __END_AT IS NULL    AS es_vigente
# MAGIC FROM clientes_slv;
# MAGIC
# MAGIC -- Hecho de desembolsos en USD: el tipo de cambio más reciente disponible a la fecha del desembolso
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW fact_desembolsos_gld (
# MAGIC   CONSTRAINT con_tipo_cambio EXPECT (monto_usd IS NOT NULL)          -- solo se mide
# MAGIC )
# MAGIC COMMENT 'Un desembolso por fila, con el monto en moneda local y en USD'
# MAGIC CLUSTER BY (fecha_desembolso)
# MAGIC AS
# MAGIC WITH tc AS (
# MAGIC   SELECT moneda_origen, fecha, tasa_venta,
# MAGIC          lead(fecha) OVER (PARTITION BY moneda_origen ORDER BY fecha) AS fecha_siguiente
# MAGIC   FROM tipos_cambio_slv
# MAGIC   WHERE moneda_destino = 'USD'
# MAGIC )
# MAGIC SELECT
# MAGIC   d.id_desembolso,
# MAGIC   d.id_solicitud,
# MAGIC   d.id_cliente,
# MAGIC   p.id_pais,
# MAGIC   CAST(d.fecha_hora_desembolso AS DATE)                AS fecha_desembolso,
# MAGIC   d.moneda,
# MAGIC   d.estado_desembolso,
# MAGIC   d.monto_desembolsado,
# MAGIC   tc.tasa_venta                                        AS tipo_cambio_usd,
# MAGIC   round(d.monto_desembolsado / tc.tasa_venta, 2)       AS monto_usd,
# MAGIC   d.tasa_aplicada,
# MAGIC   d.plazo_meses
# MAGIC FROM desembolsos_slv d
# MAGIC LEFT JOIN paises_slv p ON p.moneda_codigo = d.moneda
# MAGIC LEFT JOIN tc
# MAGIC   ON  tc.moneda_origen = d.moneda
# MAGIC   AND CAST(d.fecha_hora_desembolso AS DATE) >= tc.fecha
# MAGIC   AND (tc.fecha_siguiente IS NULL OR CAST(d.fecha_hora_desembolso AS DATE) < tc.fecha_siguiente);
# MAGIC
# MAGIC -- Métrica: colocación mensual consolidada en USD por país
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW metricas_colocacion_mensual_gld
# MAGIC COMMENT 'KPI de colocación en USD. Grano: mes x país'
# MAGIC AS SELECT
# MAGIC   trunc(f.fecha_desembolso, 'MM')                          AS mes,
# MAGIC   p.nombre_pais                                            AS pais,
# MAGIC   count(*)                                                 AS desembolsos,
# MAGIC   round(sum(f.monto_usd), 2)                               AS monto_colocado_usd,
# MAGIC   round(try_divide(sum(f.monto_usd), count(*)), 2)         AS ticket_promedio_usd
# MAGIC FROM fact_desembolsos_gld f
# MAGIC JOIN paises_slv p ON p.id_pais = f.id_pais
# MAGIC WHERE f.estado_desembolso = 'Completado'
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC -- Métrica: funnel diario de la app por ubicación en pantalla
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW metricas_funnel_app_diario_gld
# MAGIC COMMENT 'KPI de funnel de la app. Grano: fecha x canal x ubicación'
# MAGIC AS SELECT
# MAGIC   fecha_evento                                             AS fecha,
# MAGIC   coalesce(canal, 'desconocido')                           AS canal,
# MAGIC   coalesce(ubicacion_pantalla, 'desconocida')              AS ubicacion_pantalla,
# MAGIC   count_if(tipo_evento = 'oferta_vista')                   AS vistas,
# MAGIC   count_if(tipo_evento = 'oferta_click')                   AS clicks,
# MAGIC   count_if(tipo_evento = 'simulacion_realizada')           AS simulaciones,
# MAGIC   count_if(tipo_evento = 'solicitud_iniciada')             AS solicitudes_iniciadas,
# MAGIC   round(try_divide(count_if(tipo_evento = 'oferta_click'), count_if(tipo_evento = 'oferta_vista')), 4) AS ctr
# MAGIC FROM eventos_app_slv
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC -- Tabla ancha de cartera: una fila por crédito con features de pago (para el modelo de mora)
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW cartera_creditos_gld
# MAGIC COMMENT 'Una fila por crédito: cuotas, pagos y mora. Target es_mora_30'
# MAGIC CLUSTER BY (numero_credito)
# MAGIC AS
# MAGIC WITH corte AS (SELECT CAST(max(fecha_pago) AS DATE) AS fecha_corte FROM pagos_slv),
# MAGIC cuotas AS (
# MAGIC   SELECT numero_credito,
# MAGIC          count(*)                                                        AS n_cuotas,
# MAGIC          count_if(estado_cuota = 'Vencida')                              AS cuotas_vencidas,
# MAGIC          sum(monto_capital) FILTER (WHERE estado_cuota <> 'Pagada')      AS saldo_capital,
# MAGIC          min(fecha_vencimiento) FILTER (WHERE estado_cuota = 'Vencida')  AS vencida_mas_antigua
# MAGIC   FROM cuotas_slv GROUP BY numero_credito
# MAGIC ),
# MAGIC pagos AS (
# MAGIC   SELECT numero_credito, count(*) AS n_pagos, sum(monto_pagado) AS monto_pagado
# MAGIC   FROM pagos_slv GROUP BY numero_credito
# MAGIC )
# MAGIC SELECT
# MAGIC   c.numero_credito,
# MAGIC   corte.fecha_corte,
# MAGIC   c.n_cuotas,
# MAGIC   c.cuotas_vencidas,
# MAGIC   coalesce(c.saldo_capital, 0)                                     AS saldo_capital,
# MAGIC   coalesce(p.n_pagos, 0)                                           AS n_pagos,
# MAGIC   coalesce(p.monto_pagado, 0)                                      AS monto_pagado,
# MAGIC   coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0)  AS dias_mora,
# MAGIC   CASE WHEN coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0) > 30 THEN 1 ELSE 0 END AS es_mora_30
# MAGIC FROM cuotas c
# MAGIC CROSS JOIN corte
# MAGIC LEFT JOIN pagos p ON p.numero_credito = c.numero_credito;
# MAGIC ```
# MAGIC **Run pipeline.** El grafo queda completo: 4 fuentes → 14 Bronze → Silver → 5 Gold.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOGO}.{ESQUEMA}.metricas_colocacion_mensual_gld ORDER BY mes DESC, pais LIMIT 9"))

# COMMAND ----------

display(spark.sql(f"""
  SELECT ubicacion_pantalla, sum(vistas) AS vistas, sum(clicks) AS clicks,
         round(try_divide(sum(clicks), sum(vistas)), 4) AS ctr
  FROM {CATALOGO}.{ESQUEMA}.metricas_funnel_app_diario_gld GROUP BY 1 ORDER BY ctr DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 13 — Llega un lote nuevo: el pipeline procesa solo lo nuevo
# MAGIC 1. **Run pipeline** otra vez, sin archivos nuevos: en el grafo, cada streaming table procesa **0 filas** (idempotente).
# MAGIC 2. Simula la llegada de datos (elige uno):
# MAGIC    - **Kafka:** corre el job `job_datagen_eventos_app_emit` (bundle `bundle_datagen`) un par de minutos: escribe `.parquet` nuevos con duplicados y atrasados.
# MAGIC    - **Base transaccional / legado:** genera un delta con `scripts/data_generetor` (ver su README) y súbelo a `wizard_bank_rdb/` o `wizard_bank_onp/`.
# MAGIC 3. **Run pipeline**: mira cuántas filas procesó cada tabla **en este update**. Solo las del lote nuevo.
# MAGIC 4. Abre la pestaña **Data quality** de `eventos_app_slv`: los duplicados del lote no pasaron.

# COMMAND ----------

# Historial: AUTO CDC escribe cada update como un MERGE, y el pipeline corre OPTIMIZE por su cuenta
display(spark.sql(f"DESCRIBE HISTORY {CATALOGO}.{ESQUEMA}.solicitudes_prestamo_slv").select("version", "timestamp", "operation"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 14 — Lo que no escribiste
# MAGIC | En el curso lo escribiste tú | En el pipeline |
# MAGIC |---|---|
# MAGIC | `checkpointLocation` y `schemaLocation` por cada stream (`checkpoint/…`, `schemas/…` en S3) | Los administra el pipeline, uno por flow |
# MAGIC | Reintento ante columnas nuevas (`UNKNOWN_FIELD_EXCEPTION`) | El pipeline reinicia el flow solo |
# MAGIC | Orden de ejecución: Bronze → Silver → Gold en un job | El grafo, deducido de las lecturas |
# MAGIC | `foreachBatch` + deduplicar + `MERGE` con guarda | `create_auto_cdc_flow` |
# MAGIC | SCD2: hash, cerrar y abrir versiones | `stored_as_scd_type=2` + `track_history_column_list` |
# MAGIC | Tabla `_cuarentena` y conteos | Expectations + Data quality + event log |
# MAGIC | `OPTIMIZE` periódico | Lo corre el pipeline |
# MAGIC
# MAGIC **El event log** registra cada update, flow y métrica de calidad, y se consulta con SQL:
# MAGIC ```sql
# MAGIC SELECT timestamp, event_type, message
# MAGIC FROM event_log(TABLE(datawizard_dev.lakeflow.eventos_app_slv))
# MAGIC ORDER BY timestamp DESC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC **Lo que sigue siendo tuyo:** decidir el tipo de fuente, la clave, el orden (`sequence_by`), qué columnas son Tipo 2 y qué reglas de calidad aplican.
# MAGIC Eso es diseño, y el pipeline no lo adivina.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 15 — Programarlo con un Lakeflow Job 🖱️
# MAGIC 1. **Jobs & Pipelines** → **Create** → **Job**: `job_lakehouse_wizard_bank_refresh`.
# MAGIC 2. Una tarea tipo **Pipeline** → `pipeline_wizard_bank_lakehouse`.
# MAGIC 3. **Schedules & Triggers** → *File arrival* sobre `s3://lakehouse-datawizard/landing/`: el pipeline corre cuando cae un archivo.
# MAGIC
# MAGIC **Pregunta:** la telemetría de la app llega cada pocos segundos. ¿Usarías el mismo disparador?
# MAGIC <details><summary>Ver respuesta</summary>No necesariamente. Para datos que llegan sin parar, el pipeline puede correr en modo <b>continuo</b> (Settings → Pipeline mode: Continuous). Es la decisión de latencia vs costo de la Sesión 17: un job por archivo o un pipeline siempre encendido.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cierre — Checklist
# MAGIC | Capa | Objetos | Listo si… |
# MAGIC |---|---|---|
# MAGIC | Bronze | 14 streaming tables `_brz` | Todas con filas; `clientes_brz` con las columnas de v2 |
# MAGIC | Silver catálogos | `paises_slv`, `campanias_slv` | Materialized views con una fila por clave |
# MAGIC | Silver maestras | `clientes_slv`, `productos_slv` | Versiones cerradas con `__END_AT` |
# MAGIC | Silver transaccional | `ofertas`, `solicitudes`, `desembolsos`, `solicitudes_estados` | Una fila por clave; historial con varios estados |
# MAGIC | Silver externa / legado | `tipos_cambio_slv`, `cuotas_slv`, `pagos_slv`, `gestiones_cobranza_slv` | Pagos sin duplicados |
# MAGIC | Silver Kafka | `eventos_app_slv`, `sesiones_app_slv` | Eventos únicos; descartes en Data quality |
# MAGIC | Gold | 5 materialized views `_gld` | Colocación en USD y CTR por ubicación |
# MAGIC
# MAGIC - [ ] Sé decir qué tipo de fuente es cada carpeta de landing y por qué eso decide el objeto
# MAGIC - [ ] Sé cuándo usar streaming table, materialized view, temporary view y AUTO CDC (tipo 1 y 2)
# MAGIC - [ ] El segundo update procesó solo lo nuevo

# COMMAND ----------

for t in ["clientes_slv", "productos_slv", "ofertas_preaprobadas_slv", "solicitudes_prestamo_slv", "solicitudes_estados_slv",
          "desembolsos_slv", "tipos_cambio_slv", "cuotas_slv", "pagos_slv", "gestiones_cobranza_slv",
          "eventos_app_slv", "sesiones_app_slv", "dim_cliente_gld", "fact_desembolsos_gld", "cartera_creditos_gld"]:
    print(f"{t:28} {spark.table(f'{CATALOGO}.{ESQUEMA}.{t}').count():>8,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Limpieza
# MAGIC Las tablas del pipeline **le pertenecen al pipeline**: se borran borrándolo.
# MAGIC **Jobs & Pipelines** → `pipeline_wizard_bank_lakehouse` → ⋮ → **Delete**. Se van todas sus tablas de `datawizard_dev.lakeflow`.
# MAGIC Los archivos de landing **no** se tocan: son la fuente de todo el curso.
