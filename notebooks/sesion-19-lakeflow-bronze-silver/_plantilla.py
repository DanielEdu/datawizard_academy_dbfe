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
{{01_bronze_lending.py}}
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
{{02_bronze_tipos_cambio.py}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Fuente 3 · Sistema legado on-premise → Bronze
# MAGIC Para el pipeline es "otra carpeta de CSV". Lo que cambia es el **contrato**: otro sistema, otros nombres
# MAGIC (`fecha_modificacion`), un lote por noche. Por eso va en su propio archivo: si mañana el legado cambia, se toca solo este.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/03_bronze_cobranzas.py`**
{{03_bronze_cobranzas.py}}

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
{{04_bronze_eventos_app.py}}
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
{{05_silver_catalogos.py}}

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
{{06_silver_maestras_scd2.py}}
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
{{07_silver_transaccional.py}}

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
{{08_silver_tipos_cambio.py}}

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
{{09_silver_cobranzas.py}}

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
{{10_silver_eventos_app.py}}
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
{{11_gold.sql}}
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
