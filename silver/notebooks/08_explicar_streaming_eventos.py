# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 17 · Streaming en Silver: eventos de la app, celda por celda
# MAGIC
# MAGIC Vamos a construir el stream de `eventos_app` **de a un paso por celda**, mirando el resultado de cada uno
# MAGIC sobre los datos reales de Bronze. Al final escribimos en la tabla destino.
# MAGIC
# MAGIC ```
# MAGIC productor_eventos.py  →  landing (S3)  →  job_bronze_app_ingest  →  bronze.app.eventos_app_brz  →  este notebook  →  silver.app.eventos_app_lab
# MAGIC ```
# MAGIC
# MAGIC **La gracia está en repetir.** Cada vez que generas un delta y lo ingestas a Bronze, vuelves a correr las
# MAGIC celdas marcadas con 🔁. El checkpoint de cada stream recuerda dónde iba: solo procesa lo nuevo, y ahí se ve
# MAGIC cómo actúan el dedup y el watermark entre un lote y otro.
# MAGIC
# MAGIC | Parte | Qué haces |
# MAGIC |---|---|
# MAGIC | 0 | Cargar la primera tanda de eventos a Bronze |
# MAGIC | 1 | Mirar Bronze: duplicados, retrasos, corruptos |
# MAGIC | 2 | Armar el stream: limpiar → `dropDuplicates` → watermark + `dropDuplicatesWithinWatermark` |
# MAGIC | 3 | Mandar un delta y ver qué hace cada versión |
# MAGIC | 4 | Ventanas de tiempo: tumbling, sliding y session |
# MAGIC | 5 | Escribir en la tabla destino |
# MAGIC
# MAGIC **Compute:** serverless. Todo usa `trigger(availableNow=True)`: procesa lo pendiente y termina.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Cargar la primera tanda de eventos
# MAGIC
# MAGIC En tu terminal, desde `scripts/data_generetor/` (ver su `README.md`):
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Generar un archivo de eventos (2.000 normales + 10 de cada caso raro) y subirlo a landing
# MAGIC python productor_eventos.py --destino parquet --base-csv ./csv_wizard_bank \
# MAGIC     --eventos 2000 --casos-silver 10 --salida ./parquet_eventos_app --s3 $EVENTOS_S3
# MAGIC
# MAGIC # 2. Ingestarlo a Bronze
# MAGIC cd ../../bronze/app && databricks bundle run job_bronze_app_ingest -t dev
# MAGIC ```
# MAGIC
# MAGIC **Alternativa sin laptop:** el bundle `bundle_datagen` tiene el job `job_datagen_eventos_app_emit`, que emite
# MAGIC eventos en vivo (un archivo cada 30 s, con los mismos casos raros) a la misma carpeta de landing. Se despliega
# MAGIC pausado: dale *Resume* en Jobs & Pipelines y, cada vez que quieras un delta, corre `job_bronze_app_ingest`.
# MAGIC
# MAGIC Los casos raros que mete el productor son los del mundo real: **duplicados** (Kafka entrega *al menos una vez*),
# MAGIC eventos que **llegan tarde** (el teléfono estuvo sin señal), **payloads corruptos** y un **tipo de evento nuevo**.

# COMMAND ----------

dbutils.widgets.text("catalogo_bronze", "bronze", "Catálogo Bronze")
dbutils.widgets.text("catalogo_silver", "silver", "Catálogo Silver")
dbutils.widgets.text("watermark", "2 hours", "Watermark")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver/s17", "Raíz de checkpoints")

bronze = f"{dbutils.widgets.get('catalogo_bronze')}.app.eventos_app_brz"
silver = dbutils.widgets.get("catalogo_silver")
lab = f"{silver}.app_lab"                       # aquí van las tablas de cada paso
destino = f"{silver}.app.eventos_app_lab"       # la tabla destino (copia de práctica de silver.app.eventos_app)
watermark = dbutils.widgets.get("watermark")
ckpt = dbutils.widgets.get("checkpoints").rstrip("/")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {lab}")
print(f"Bronze:  {bronze}\nPasos:   {lab}\nDestino: {destino}\nWatermark: {watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Mirar Bronze antes de tocar nada
# MAGIC
# MAGIC Bronze guarda **cada mensaje** de Kafka tal cual llegó: con duplicados, tardíos y corruptos.
# MAGIC Silver es quien los resuelve.

# COMMAND ----------

# 🔁 Cuántos mensajes hay y cuántos son problema
display(spark.sql(f"""
SELECT count(*)                                AS mensajes,
       count(DISTINCT _metadata.file_name)     AS archivos,
       count(DISTINCT id_evento)               AS eventos_unicos,
       count(id_evento) - count(DISTINCT id_evento) AS duplicados,
       count_if(id_evento IS NULL)             AS corruptos
FROM {bronze}
"""))

# COMMAND ----------

# Un duplicado: mismo id_evento, distinto offset de Kafka (el productor reintentó)
display(spark.sql(f"""
WITH repetidos AS (SELECT id_evento FROM {bronze} GROUP BY id_evento HAVING count(*) > 1 LIMIT 3)
SELECT b.id_evento, b.tipo_evento, b.timestamp_evento, b._kafka_particion, b._kafka_offset, b._kafka_timestamp
FROM {bronze} b JOIN repetidos USING (id_evento)
ORDER BY b.id_evento, b._kafka_offset
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tres relojes por evento
# MAGIC
# MAGIC - `timestamp_evento`: cuándo pasó, según el teléfono. **Este es el que importa** (tiempo del evento).
# MAGIC - `_kafka_timestamp`: cuándo llegó al broker.
# MAGIC - `_ingestion_ts`: cuándo lo escribió el job de Bronze.
# MAGIC
# MAGIC El retraso = llegada a Kafka − tiempo del evento. Con esta distribución se elige el watermark.

# COMMAND ----------

# 🔁 ¿Cuánto tardan en llegar los eventos?
display(spark.sql(f"""
SELECT CASE WHEN retraso_min < 1   THEN '1. menos de 1 min'
            WHEN retraso_min < 120 THEN '2. entre 1 min y 2 h'
            ELSE                        '3. más de 2 h' END AS retraso,
       count(*) AS eventos
FROM (SELECT (unix_timestamp(_kafka_timestamp) - unix_timestamp(timestamp_evento)) / 60 AS retraso_min
      FROM {bronze} WHERE id_evento IS NOT NULL)
GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Casi todo llega en segundos. Unos pocos tardan hasta 1,5 h (dentro de un watermark de 2 h) y otros más de 2 h:
# MAGIC esos son los que el watermark va a sacrificar. **Ese es el trato**: esperar hasta 2 h; lo que llegue después,
# MAGIC no cuenta en las operaciones con estado.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Armar el stream, un paso a la vez
# MAGIC
# MAGIC ### 2.1 Leer Bronze como stream y limpiar
# MAGIC
# MAGIC `readStream.table` lee la tabla como una fuente infinita. Tipamos, renombramos y dejamos fuera lo que no tiene
# MAGIC clave ni tiempo (los corruptos se quedan en Bronze con su `_payload_crudo`).

# COMMAND ----------

from pyspark.sql import functions as F

eventos = (spark.readStream.table(bronze)
    .filter("id_evento IS NOT NULL AND timestamp_evento IS NOT NULL")
    .select(
        "id_evento",
        "tipo_evento",
        F.col("timestamp_evento").alias("ts_evento"),
        "id_cliente",
        "canal",
        "monto_simulado",
        (F.unix_timestamp("_kafka_timestamp") - F.unix_timestamp("timestamp_evento")).alias("retraso_seg"),
        F.col("_metadata.file_name").alias("archivo")))

eventos.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC Un DataFrame de streaming no se puede consultar directo: hay que **escribirlo**. Lo mandamos a una tabla del
# MAGIC paso y la consultamos como cualquier tabla. Así vamos a trabajar todo el notebook.

# COMMAND ----------

# 🔁 Paso 1: sin dedup
(eventos.writeStream
    .option("checkpointLocation", f"{ckpt}/paso1_sin_dedup")
    .trigger(availableNow=True)
    .toTable(f"{lab}.paso1_sin_dedup")
    .awaitTermination())

display(spark.sql(f"""
SELECT count(*) AS filas, count(DISTINCT id_evento) AS eventos_unicos,
       count(*) - count(DISTINCT id_evento) AS duplicados
FROM {lab}.paso1_sin_dedup
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Los corruptos ya no están, pero **los duplicados siguen ahí**.
# MAGIC
# MAGIC ### 2.2 `dropDuplicates` sin watermark
# MAGIC
# MAGIC Lo primero que uno prueba. En un stream, `dropDuplicates` guarda en el **estado** (dentro del checkpoint) cada
# MAGIC `id_evento` que vio, para reconocer el próximo duplicado.

# COMMAND ----------

# 🔁 Paso 2: dedup sin watermark
(eventos
    .dropDuplicates(["id_evento"])
    .writeStream
    .option("checkpointLocation", f"{ckpt}/paso2_dedup_sin_watermark")
    .trigger(availableNow=True)
    .toTable(f"{lab}.paso2_dedup_sin_watermark")
    .awaitTermination())

display(spark.sql(f"""
SELECT count(*) AS filas, count(*) - count(DISTINCT id_evento) AS duplicados
FROM {lab}.paso2_dedup_sin_watermark
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Cero duplicados. ¿Problema resuelto? **No**: ese estado recuerda **todos los `id_evento` de la historia** y nunca
# MAGIC suelta ninguno. Con dos millones de eventos al mes crece sin parar, y el stream se cae a los meses. En el lab
# MAGIC funciona perfecto; en producción, no.
# MAGIC
# MAGIC ### 2.3 Watermark + `dropDuplicatesWithinWatermark`
# MAGIC
# MAGIC El watermark le dice al motor cuánto esperar a los rezagados:
# MAGIC
# MAGIC > **watermark = el `ts_evento` más grande que vi − 2 horas**
# MAGIC
# MAGIC Con eso el estado se acota: un `id_evento` se recuerda solo mientras está dentro del watermark.
# MAGIC El precio: un evento **más viejo que el watermark** se descarta.

# COMMAND ----------

# 🔁 Paso 3: dedup con watermark
(eventos
    .withWatermark("ts_evento", watermark)
    .dropDuplicatesWithinWatermark(["id_evento"])
    .writeStream
    .option("checkpointLocation", f"{ckpt}/paso3_dedup_con_watermark")
    .trigger(availableNow=True)
    .toTable(f"{lab}.paso3_dedup_con_watermark")
    .awaitTermination())

display(spark.sql(f"""
SELECT 'paso1 · sin dedup'            AS version, count(*) AS filas, count(*) - count(DISTINCT id_evento) AS duplicados FROM {lab}.paso1_sin_dedup
UNION ALL
SELECT 'paso2 · dedup sin watermark', count(*), count(*) - count(DISTINCT id_evento) FROM {lab}.paso2_dedup_sin_watermark
UNION ALL
SELECT 'paso3 · dedup con watermark', count(*), count(*) - count(DISTINCT id_evento) FROM {lab}.paso3_dedup_con_watermark
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC En la primera corrida **paso 2 y paso 3 tienen las mismas filas**. ¿Por qué no se descartó nada si hay eventos
# MAGIC de más de 2 h de retraso? Porque el watermark **se calcula al terminar cada lote**: mientras se procesa el
# MAGIC primero todavía no hay watermark. A partir de ahora sí:

# COMMAND ----------

# 🔁 El watermark que usará la próxima corrida
display(spark.sql(f"""
SELECT max(ts_evento)                          AS ultimo_ts_evento,
       max(ts_evento) - INTERVAL {watermark}   AS watermark_aprox
FROM {lab}.paso3_dedup_con_watermark
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Mandar un delta 🔁
# MAGIC
# MAGIC En la terminal, otra vez el productor y el job de Bronze. Al encontrar el archivo anterior en `--salida`, el
# MAGIC productor genera un **delta**: tráfico nuevo desde donde terminó el anterior, más los casos raros, incluido el
# MAGIC **reenvío** de eventos del archivo anterior (mismo `id_evento`).
# MAGIC
# MAGIC ```bash
# MAGIC python productor_eventos.py --destino parquet --base-csv ./csv_wizard_bank \
# MAGIC     --eventos 300 --casos-silver 10 --salida ./parquet_eventos_app --s3 $EVENTOS_S3
# MAGIC cd ../../bronze/app && databricks bundle run job_bronze_app_ingest -t dev
# MAGIC ```
# MAGIC
# MAGIC Con `job_datagen_eventos_app_emit` encendido no hace falta el productor: basta correr `job_bronze_app_ingest`.
# MAGIC
# MAGIC Después vuelve a correr las celdas 🔁 de la Parte 1 y la Parte 2 (pasos 1, 2 y 3), y sigue con la celda de abajo.

# COMMAND ----------

# 🔁 ¿Qué pasó con el delta? Eventos que el paso 2 tiene y el paso 3 no
display(spark.sql(f"""
SELECT p2.id_evento, p2.tipo_evento, p2.ts_evento, round(p2.retraso_seg / 3600, 1) AS retraso_horas, p2.archivo
FROM {lab}.paso2_dedup_sin_watermark p2
LEFT ANTI JOIN {lab}.paso3_dedup_con_watermark p3 USING (id_evento)
ORDER BY p2.ts_evento
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Qué deberías ver:
# MAGIC
# MAGIC | Caso del delta | Paso 1 (sin dedup) | Paso 2 (sin watermark) | Paso 3 (con watermark) |
# MAGIC |---|---|---|---|
# MAGIC | Duplicado dentro del archivo | entra 2 veces | 1 vez | 1 vez |
# MAGIC | Reenvío de un evento del archivo anterior | entra 2 veces | 1 vez (el estado lo recuerda) | 1 vez: o el estado lo recuerda, o es más viejo que el watermark y se descarta |
# MAGIC | Llegada tardía (30–90 min) | entra | entra | entra |
# MAGIC | Fuera del watermark (3–6 h) | entra | entra | **se descarta** (es lo que lista la celda de arriba) |
# MAGIC
# MAGIC El paso 2 parece "mejor" porque no pierde nada, pero su estado crece para siempre. El paso 3 pierde los
# MAGIC eventos de más de 2 h de retraso, **a cambio de un estado que no crece**. Ese es el patrón de Silver.
# MAGIC Si perder esos eventos no es aceptable, se agranda el watermark (más estado) o se reprocesan desde Bronze, que los tiene todos.

# COMMAND ----------

# 🔁 Bronze tiene todo: los descartados por el watermark siguen ahí
display(spark.sql(f"""
SELECT count(*) AS en_bronze_pero_no_en_silver
FROM (SELECT DISTINCT id_evento FROM {bronze} WHERE id_evento IS NOT NULL) b
LEFT ANTI JOIN {lab}.paso3_dedup_con_watermark s USING (id_evento)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Ventanas de tiempo
# MAGIC
# MAGIC Ahora agregamos por tiempo. Leemos **la salida deduplicada del paso 3** como stream (igual que en producción
# MAGIC las agregaciones leen `eventos_app`, no Bronze).
# MAGIC
# MAGIC | Ventana | Cómo corta el tiempo | Un evento cae en… | Para qué |
# MAGIC |---|---|---|---|
# MAGIC | **Tumbling** | bloques fijos que no se solapan | una ventana | funnel cada 15 min |
# MAGIC | **Sliding** | bloques fijos que avanzan en pasos más cortos | varias ventanas | "simulaciones de los últimos 30 min", cada 10 min |
# MAGIC | **Session** | se cierra tras N min sin actividad | una sesión | actividad de cada cliente |
# MAGIC
# MAGIC ### 4.1 Tumbling: eventos por tipo cada 15 minutos
# MAGIC
# MAGIC Primero en modo `complete`: en cada corrida reescribe **todas** las ventanas con su valor actual.

# COMMAND ----------

# 🔁
(spark.readStream.table(f"{lab}.paso3_dedup_con_watermark")
    .groupBy(F.window("ts_evento", "15 minutes").alias("ventana"), "tipo_evento")
    .count()
    .select(F.col("ventana.start").alias("inicio"), F.col("ventana.end").alias("fin"), "tipo_evento", "count")
    .writeStream
    .outputMode("complete")
    .option("checkpointLocation", f"{ckpt}/ventana_tumbling")
    .trigger(availableNow=True)
    .toTable(f"{lab}.ventana_tumbling")
    .awaitTermination())

display(spark.table(f"{lab}.ventana_tumbling").orderBy(F.desc("inicio"), "tipo_evento"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Sliding: la misma cuenta, pero ventanas de 30 min que avanzan cada 10
# MAGIC
# MAGIC Cada evento cae en **tres** ventanas (30 / 10). Mira cómo se solapan los `inicio` y `fin`.

# COMMAND ----------

# 🔁
(spark.readStream.table(f"{lab}.paso3_dedup_con_watermark")
    .filter("tipo_evento = 'simulacion_realizada'")
    .groupBy(F.window("ts_evento", "30 minutes", "10 minutes").alias("ventana"))
    .agg(F.count("*").alias("simulaciones"), F.round(F.sum("monto_simulado"), 2).alias("monto_total"))
    .select(F.col("ventana.start").alias("inicio"), F.col("ventana.end").alias("fin"), "simulaciones", "monto_total")
    .writeStream
    .outputMode("complete")
    .option("checkpointLocation", f"{ckpt}/ventana_sliding")
    .trigger(availableNow=True)
    .toTable(f"{lab}.ventana_sliding")
    .awaitTermination())

display(spark.table(f"{lab}.ventana_sliding").orderBy(F.desc("inicio")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Append + watermark: escribir solo las ventanas cerradas
# MAGIC
# MAGIC `complete` sirve para mirar, pero reescribe todo en cada corrida y su estado nunca se limpia.
# MAGIC En Silver se usa **`append` con watermark**: una ventana se escribe **una sola vez, cuando el watermark pasa su
# MAGIC fin** (ya no puede llegarle nada). La fila es definitiva.
# MAGIC
# MAGIC Es la misma tumbling de 4.1, con dos cambios: `withWatermark` y `outputMode("append")`.

# COMMAND ----------

# 🔁
(spark.readStream.table(f"{lab}.paso3_dedup_con_watermark")
    .withWatermark("ts_evento", watermark)
    .groupBy(F.window("ts_evento", "15 minutes").alias("ventana"), "tipo_evento")
    .count()
    .select(F.col("ventana.start").alias("inicio"), F.col("ventana.end").alias("fin"), "tipo_evento", "count")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{ckpt}/ventana_tumbling_append")
    .trigger(availableNow=True)
    .toTable(f"{lab}.ventana_tumbling_append")
    .awaitTermination())

display(spark.sql(f"""
SELECT 'complete' AS modo, count(*) AS ventanas, max(fin) AS ultima_ventana FROM {lab}.ventana_tumbling
UNION ALL
SELECT 'append',           count(*),             max(fin)                   FROM {lab}.ventana_tumbling_append
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC En `append` faltan las ventanas de las **últimas 2 horas**: siguen abiertas en el estado, esperando rezagados.
# MAGIC Saldrán en una corrida posterior, cuando un delta empuje el watermark. Pruébalo: manda otro delta (Parte 3) y
# MAGIC vuelve a correr esta celda. (Si en la primera corrida la tabla `append` sale vacía, es lo mismo: el watermark
# MAGIC recién existe al terminar ese lote. Las ventanas cerradas aparecen en la siguiente.)
# MAGIC
# MAGIC ### 4.4 Session: actividad de cada cliente
# MAGIC
# MAGIC `session_window(ts_evento, "30 minutes")` junta los eventos de un cliente mientras entre uno y otro haya menos
# MAGIC de 30 minutos. A diferencia de las anteriores, **el largo de la ventana lo deciden los datos**.

# COMMAND ----------

# 🔁
(spark.readStream.table(f"{lab}.paso3_dedup_con_watermark")
    .withWatermark("ts_evento", watermark)
    .groupBy("id_cliente", F.session_window("ts_evento", "30 minutes").alias("sesion"))
    .agg(F.count("*").alias("eventos"),
         F.max(F.col("tipo_evento") == "simulacion_realizada").alias("simulo"),
         F.max(F.col("tipo_evento") == "solicitud_iniciada").alias("solicito"))
    .select("id_cliente", F.col("sesion.start").alias("inicio"), F.col("sesion.end").alias("fin"),
            "eventos", "simulo", "solicito")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{ckpt}/ventana_sesion")
    .trigger(availableNow=True)
    .toTable(f"{lab}.ventana_sesion")
    .awaitTermination())

display(spark.table(f"{lab}.ventana_sesion").orderBy(F.desc("eventos"), "inicio"))

# COMMAND ----------

# MAGIC %md
# MAGIC `fin` = último evento de la sesión + 30 min. Un cliente que simuló y no solicitó (`simulo AND NOT solicito`) es
# MAGIC justo el que busca la alerta de abandono.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Escribir en la tabla destino
# MAGIC
# MAGIC Juntamos lo que funcionó: limpiar + watermark + `dropDuplicatesWithinWatermark`, con **todas** las columnas del
# MAGIC DDL de `silver.app.eventos_app`. Escribimos en `eventos_app_lab`, una copia con la misma estructura
# MAGIC (`CREATE TABLE ... LIKE`). Es el mismo código del notebook `06` y del job.

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} LIKE {silver}.app.eventos_app")

eventos_silver = (spark.readStream.table(bronze)
    .filter("id_evento IS NOT NULL AND timestamp_evento IS NOT NULL AND id_cliente IS NOT NULL")
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
        F.current_timestamp().alias("_procesado_ts"))
    .withWatermark("ts_evento", watermark)
    .dropDuplicatesWithinWatermark(["id_evento"]))

# 🔁
(eventos_silver.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{ckpt}/eventos_app")
    .trigger(availableNow=True)
    .toTable(destino)
    .awaitTermination())

# COMMAND ----------

# 🔁 Validar la tabla destino
display(spark.sql(f"""
SELECT (SELECT count(*) FROM {bronze})                               AS mensajes_bronze,
       (SELECT count(DISTINCT id_evento) FROM {bronze})              AS eventos_unicos_bronze,
       (SELECT count(*) FROM {destino})                              AS eventos_silver,
       (SELECT count(*) - count(DISTINCT id_evento) FROM {destino})  AS duplicados_silver
"""))

display(spark.sql(f"SELECT tipo_evento, count(*) AS eventos FROM {destino} GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC `duplicados_silver` debe ser 0. La diferencia entre `eventos_unicos_bronze` y `eventos_silver` son los que el
# MAGIC watermark descartó (si esta es la primera corrida de la celda, todo Bronze entra en un solo lote y no se descarta
# MAGIC nada: manda un delta y vuelve a correrla). Y `banner_cerrado` (el tipo nuevo) **entró**: un tipo desconocido es una señal de que la
# MAGIC app cambió, no un dato inválido.
# MAGIC
# MAGIC En producción esto mismo corre en un job **continuo**: al terminar una corrida `availableNow`, arranca la
# MAGIC siguiente. Es exactamente lo que hiciste a mano con las celdas 🔁.
# MAGIC
# MAGIC ## Empezar de cero (opcional)
# MAGIC
# MAGIC Borra las tablas de los pasos, la tabla destino de práctica y los checkpoints. Bronze no se toca.
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP SCHEMA IF EXISTS {lab} CASCADE")
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {destino}")
# MAGIC dbutils.fs.rm(ckpt, recurse=True)
# MAGIC ```
