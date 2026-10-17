# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 17 · Streaming en Silver: eventos en tiempo real (paso a paso)
# MAGIC
# MAGIC Este notebook recorre la Sesión 17 **celda por celda**: cada concepto de la teoría se demuestra con datos.
# MAGIC Los notebooks `06` y `07` son los patrones listos para empaquetar; este es el que *explica* por qué.
# MAGIC
# MAGIC **Es un laboratorio cerrado.** No toca Bronze ni Silver reales: crea su propio esquema (`<catalogo>.app_lab`)
# MAGIC con una Bronze de juguete, escribe en ella lotes de eventos a mano y ve cómo reacciona el stream.
# MAGIC Como cada lote lo publicas tú, controlas el tiempo y ves el watermark avanzar.
# MAGIC
# MAGIC | Parte | Qué demuestra |
# MAGIC |---|---|
# MAGIC | 1 | ¿Quién actúa en minutos? La prueba del stream y cómo se comporta `availableNow` |
# MAGIC | 2 | Modelar eventos: grano, clave, una tabla con vista por tipo, las tres decisiones del DDL |
# MAGIC | 3 | Tiempo del evento, watermark elegido con datos, dedup con estado acotado y la trampa de `dropDuplicates` |
# MAGIC | 4 | Ventanas tumbling / sliding / session; `sesiones_app`; el evento tardío |
# MAGIC | 5 | Joins: stream-estático (alerta de abandono) y stream-stream (reconciliar app vs base) |
# MAGIC | 6 | Operar: `lastProgress`, el checkpoint que ata el código, lo que lee Gold |
# MAGIC
# MAGIC **Compute:** serverless sirve (todo usa `availableNow`). Re-ejecutar desde arriba reinicia el laboratorio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preparar el laboratorio
# MAGIC
# MAGIC - Un esquema propio (debe terminar en `_lab`, para no borrar por error un esquema real).
# MAGIC - `T0` es el "inicio de la historia": todos los eventos del lab se ubican en minutos relativos a `T0`
# MAGIC   (por ejemplo `mins(5)` = T0 + 5 min), así el tiempo del evento es controlable.
# MAGIC - Dos helpers: `publicar()` agrega un lote a la Bronze de juguete y `correr()` ejecuta un stream con
# MAGIC   `availableNow` y guarda sus métricas.

# COMMAND ----------

from datetime import timedelta

import pandas as pd
from pyspark.sql import functions as F, types as T

dbutils.widgets.text("catalogo", "silver", "Catálogo del laboratorio")
dbutils.widgets.text("esquema", "app_lab", "Esquema del laboratorio (debe terminar en _lab)")
dbutils.widgets.text("checkpoints", "s3://lakehouse-datawizard/checkpoint/notebooks/silver/s17_lab", "Raíz de checkpoints")
dbutils.widgets.text("watermark", "2 hours", "Watermark de eventos y sesiones")
dbutils.widgets.text("watermark_alerta", "10 minutes", "Watermark de la alerta de abandono")
dbutils.widgets.text("inactividad", "30 minutes", "Inactividad que cierra la sesión")
dbutils.widgets.dropdown("reiniciar", "si", ["si", "no"], "Reiniciar el laboratorio")

catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
raiz_ckpt = dbutils.widgets.get("checkpoints").rstrip("/")
watermark = dbutils.widgets.get("watermark")
watermark_alerta = dbutils.widgets.get("watermark_alerta")
inactividad = dbutils.widgets.get("inactividad")

if not esquema.endswith("_lab"):
    raise ValueError(f"El esquema '{esquema}' no termina en _lab: este notebook lo borra al reiniciar.")

ns = f"{catalogo}.{esquema}"
brz = f"{ns}.eventos_app_brz"            # Bronze de juguete (mismas columnas que la real)
slv_ev = f"{ns}.eventos_app"
slv_ses = f"{ns}.sesiones_app"
dim = f"{ns}.dim_clientes"
sol_brz = f"{ns}.solicitudes_prestamo_brz"
alertas = f"{ns}.alertas_abandono"
sin_pareja_tbl = f"{ns}.solicitudes_sin_pareja"
retrasos_tbl = f"{ns}.retrasos_poblacion"
ckpt = lambda nombre: f"{raiz_ckpt}/{nombre}"  # noqa: E731

if dbutils.widgets.get("reiniciar") == "si":
    spark.sql(f"DROP SCHEMA IF EXISTS {ns} CASCADE")
    dbutils.fs.rm(raiz_ckpt, recurse=True)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ns} COMMENT 'Laboratorio de la Sesión 17 (streaming en Silver). Se puede borrar.'")

# T0: hace 8 horas, al minuto. Todo el lab ocurre entre T0 y T0 + 6 h (en el pasado, nada es "futuro").
T0 = spark.sql("SELECT date_trunc('MINUTE', current_timestamp() - INTERVAL 8 HOURS)").first()[0]
mins = lambda m: T0 + timedelta(minutes=m)  # noqa: E731


def verificar(ok, mensaje):
    """Marca cada afirmación de la teoría como confirmada o no, sin romper el notebook."""
    print(("✅ " if ok else "⚠️ ") + mensaje)


PROGRESO = []   # una fila por stream ejecutado: entrada, watermark, tamaño del estado, descartados


def correr(etiqueta, nombre, df, destino=None, modo="append"):
    """Ejecuta un stream con availableNow (procesa lo pendiente y termina) y registra sus métricas.

    Con `destino` escribe a esa tabla; sin `destino` solo mide (foreachBatch que no hace nada).
    """
    w = (df.writeStream.outputMode(modo)
         .option("checkpointLocation", ckpt(nombre))
         .trigger(availableNow=True))
    q = w.toTable(destino) if destino else w.foreachBatch(lambda lote, i: None).start()
    q.awaitTermination()
    prog = q.recentProgress
    estados = [p["stateOperators"] for p in prog if p.get("stateOperators")]
    PROGRESO.append({
        "corrida": etiqueta,
        "filas_entrada": sum(p["numInputRows"] for p in prog),
        "watermark": next((p["eventTime"]["watermark"] for p in reversed(prog)
                           if p.get("eventTime", {}).get("watermark")), None),
        "estado_filas": sum(o.get("numRowsTotal", 0) for o in estados[-1]) if estados else None,
        "descartadas_por_watermark": sum(o.get("numRowsDroppedByWatermark", 0) for e in estados for o in e),
    })
    return q


ESQ_EVENTO = T.StructType([
    T.StructField("id_evento", T.StringType()), T.StructField("tipo_evento", T.StringType()),
    T.StructField("timestamp_evento", T.TimestampType()), T.StructField("id_cliente", T.LongType()),
    T.StructField("id_oferta", T.LongType()), T.StructField("canal", T.StringType()),
    T.StructField("id_sesion", T.StringType()), T.StructField("monto_simulado", T.DoubleType()),
    T.StructField("plazo_simulado", T.IntegerType()), T.StructField("retraso_seg", T.LongType()),
])


def ev(id_evento, tipo, minuto, cliente, sesion, canal, monto=None, plazo=None, retraso=5):
    """Un evento de la app: ocurre en T0+minuto y llega a Bronze `retraso` segundos después."""
    return (id_evento, tipo, mins(minuto), cliente, 10, canal, sesion, monto, plazo, retraso)


def publicar(filas, tabla=None):
    """Agrega un lote a la Bronze de juguete, como si Kafka lo hubiera aterrizado (S11)."""
    tabla = tabla or brz
    df = (spark.createDataFrame(filas, ESQ_EVENTO)
          .withColumn("_ingestion_ts", F.coalesce(
              (F.col("timestamp_evento").cast("long") + F.col("retraso_seg")).cast("timestamp"),
              F.current_timestamp()))
          .withColumn("_kafka_timestamp", F.col("_ingestion_ts"))
          .withColumn("_kafka_offset", F.monotonically_increasing_id())
          .withColumn("_payload_crudo", F.when(F.col("id_evento").isNull(), F.lit('{"id_evento": "'))))
    destino_cols = spark.table(tabla).schema
    for campo in destino_cols:
        if campo.name not in df.columns:
            df = df.withColumn(campo.name, F.lit(None).cast(campo.dataType))
    df.select([c.name for c in destino_cols]).write.mode("append").insertInto(tabla)
    print(f"Publicados {len(filas)} mensajes en {tabla}")


print(f"Laboratorio: {ns}\nT0 = {T0}\nWatermark: {watermark} · alerta: {watermark_alerta} · inactividad: {inactividad}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 1 · ¿Quién actúa en minutos?
# MAGIC
# MAGIC La regla del proyecto (S15): por defecto, `availableNow` con schedule de una hora o menos. Pasar a un stream
# MAGIC encendido exige **nombrar a alguien que actúa sobre el dato en minutos**.
# MAGIC
# MAGIC > *Si el dato llega en 30 segundos pero nadie lo mira hasta mañana, pagaste 24 horas de compute para nada.*
# MAGIC
# MAGIC La tabla de la sesión como datos. La decisión sale de dos preguntas: ¿quién actúa?, ¿cuánto vale el dato
# MAGIC después de esa latencia?

# COMMAND ----------

casos = pd.DataFrame([
    ("Rescate de abandono (simuló y no solicitó en 30 min)", "Motor de campañas (automático)", 30, True),
    ("Monitoreo del funnel (la versión nueva rompió 'solicitar')", "Guardia de producto (alerta)", 10, True),
    ("Comportamiento anómalo (50 simulaciones en 2 min)", "Seguridad (bloquea la sesión)", 5, True),
    ("Reporte diario del funnel", "Comité comercial (personas)", 24 * 60, False),
    ("Features del modelo de propensión (reentreno semanal)", "Nadie en tiempo real", 7 * 24 * 60, False),
], columns=["caso_de_uso", "quien_actua", "latencia_util_min", "actua_automatico_o_en_minutos"])

# La regla: stream continuo solo si alguien actúa en minutos. Si no, availableNow.
casos["modo"] = casos.apply(
    lambda r: "job continuo (stream encendido)" if r.actua_automatico_o_en_minutos and r.latencia_util_min <= 60
    else "availableNow con schedule", axis=1)
display(casos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tres formas de tener un stream encendido
# MAGIC
# MAGIC | Forma | Cómo | Latencia | Serverless |
# MAGIC |---|---|---|---|
# MAGIC | **Job continuo + `availableNow`** | El job es `continuous`: al terminar una corrida arranca la siguiente; cada corrida procesa lo pendiente | 1 a pocos minutos | ✅ |
# MAGIC | `processingTime` en cluster clásico | El stream nunca termina: un micro-lote cada N segundos | segundos | ❌ falla con `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` |
# MAGIC | Pipeline declarativo continuo | Lakeflow Declarative Pipelines gestiona stream, reintentos y compute | segundos | gestionado |
# MAGIC
# MAGIC **El proyecto usa la primera**: los tres casos toleran uno o dos minutos y el código es el mismo patrón de la S15.
# MAGIC Si mañana se necesitan segundos, solo cambia una línea:
# MAGIC
# MAGIC ```python
# MAGIC .trigger(availableNow=True)          # lo que usamos: procesa lo pendiente y termina
# MAGIC .trigger(processingTime="10 seconds") # un micro-lote cada 10 s; el stream no termina (cluster clásico)
# MAGIC ```
# MAGIC
# MAGIC En la Parte 3 verás cómo se comporta `availableNow` corrida tras corrida: eso *es* el job continuo.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 2 · Modelar eventos en Silver
# MAGIC
# MAGIC Un evento es un hecho inmutable (familia *evento* de la S15):
# MAGIC
# MAGIC - **Grano:** una fila por evento. **Clave:** `id_evento`, el UUID que genera la app.
# MAGIC - La clave importa porque Kafka entrega **al menos una vez**: si el productor reintenta, el mismo evento
# MAGIC   llega dos veces con el mismo `id_evento`.
# MAGIC
# MAGIC ### ¿Una tabla o una por tipo?
# MAGIC
# MAGIC | Una tabla con `tipo_evento` | Una tabla por tipo |
# MAGIC |---|---|
# MAGIC | Los tipos comparten casi todas las columnas | Los esquemas divergen mucho |
# MAGIC | Las consultas cruzan tipos (el funnel) | Cada tipo tiene consumidores y reglas distintas |
# MAGIC | Un tipo nuevo no requiere desplegar nada | Un tipo aplasta al resto en volumen |
# MAGIC
# MAGIC En Wizard Bank los cuatro tipos comparten todo salvo `monto_simulado` y `plazo_simulado`, y el uso principal
# MAGIC es el funnel: **una tabla, con vistas por tipo**.
# MAGIC
# MAGIC Primero la Bronze de juguete y las tablas de apoyo (las mismas columnas que la real; la tabla real la creó la S11).

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {brz} (
  id_evento STRING, tipo_evento STRING, timestamp_evento TIMESTAMP, id_cliente BIGINT, id_oferta BIGINT,
  id_pais INT, canal STRING, id_sesion STRING, version_app STRING, sistema_operativo STRING,
  ubicacion_pantalla STRING, posicion INT, tiempo_visible_seg DOUBLE, monto_simulado DOUBLE, plazo_simulado INT,
  _kafka_offset BIGINT, _kafka_timestamp TIMESTAMP, _payload_crudo STRING, _ingestion_ts TIMESTAMP
) USING DELTA
COMMENT 'Bronze de juguete: un mensaje de Kafka por fila, duplicados y corruptos incluidos'
""")

spark.sql(f"""
CREATE TABLE {sol_brz} (id_cliente BIGINT, fecha_hora_solicitud TIMESTAMP) USING DELTA
COMMENT 'Bronze de juguete de solicitudes en la base (llega con el delta horario)'
""")

# dim_clientes mínima (la real es SCD2, S16): una versión vigente por cliente. El 999 NO existe: es un cliente nuevo.
spark.sql(f"""
CREATE TABLE {dim} (id_cliente BIGINT NOT NULL, nivel_riesgo STRING, score_interno SMALLINT, es_vigente BOOLEAN NOT NULL)
USING DELTA COMMENT 'dim_clientes mínima para el join estático'
""")
spark.sql(f"""
INSERT INTO {dim} VALUES
  (101, 'A', 820, true), (102, 'C', 610, false), (102, 'B', 700, true), (103, 'B', 690, true),
  (104, 'C', 640, true), (105, 'A', 800, true), (106, 'B', 710, true)
""")
print("Listo: Bronze de juguete, solicitudes y dim_clientes (el cliente 999 no está en la dimensión a propósito).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### El DDL de `eventos_app`
# MAGIC
# MAGIC Es el mismo de la sesión (el real vive en `bundle_ddl/src/ddls/silver/eventos_app.sql`).
# MAGIC Tres decisiones, que verás funcionando con datos más abajo:
# MAGIC
# MAGIC 1. **`ts_evento` es el tiempo del dispositivo**, no el de Kafka ni el de ingesta. Los otros dos solo sirven para medir el retraso (`retraso_seg`).
# MAGIC 2. **Un `tipo_evento` desconocido no va a cuarentena**: un quinto evento es una señal, no un dato inválido.
# MAGIC 3. **`id_cliente` no se valida contra `dim_clientes`**: un cliente puede generar eventos antes de llegar a la dimensión.
# MAGIC
# MAGIC `CLUSTER BY` (fecha, tipo) acelera el funnel; `delta.enableChangeDataFeed` deja a Gold leer los cambios (Parte 6).

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {slv_ev} (
  id_evento            STRING     NOT NULL COMMENT 'UUID generado por la app: clave de deduplicación',
  tipo_evento          STRING     NOT NULL,
  ts_evento            TIMESTAMP  NOT NULL COMMENT 'Tiempo del EVENTO (reloj del dispositivo, UTC)',
  fecha_evento         DATE       NOT NULL,
  id_cliente           BIGINT     NOT NULL,
  id_oferta            BIGINT,
  id_pais              SMALLINT,
  canal                STRING,
  id_sesion            STRING,
  version_app          STRING,
  sistema_operativo    STRING,
  ubicacion_pantalla   STRING,
  posicion             INT,
  tiempo_visible_seg   DECIMAL(6,2),
  monto_simulado       DECIMAL(14,2) COMMENT 'Solo simulacion_realizada',
  plazo_simulado       SMALLINT      COMMENT 'Solo simulacion_realizada',
  retraso_seg          BIGINT        COMMENT 'Segundos entre el evento y su llegada a Bronze',
  _kafka_offset        BIGINT,
  _bronze_ingestion_ts TIMESTAMP,
  _procesado_ts        TIMESTAMP  NOT NULL
)
USING DELTA
CLUSTER BY (fecha_evento, tipo_evento)
TBLPROPERTIES ('quality' = 'silver', 'delta.enableChangeDataFeed' = 'true')
""")

# La vista por tipo: quien quiera un solo tipo no necesita otra tabla.
spark.sql(f"""
CREATE OR REPLACE VIEW {ns}.v_simulaciones AS
SELECT id_evento, ts_evento, id_cliente, id_oferta, canal, id_sesion, monto_simulado, plazo_simulado
FROM {slv_ev}
WHERE tipo_evento = 'simulacion_realizada'
""")

display(spark.sql(f"DESCRIBE DETAIL {slv_ev}").select("name", "clusteringColumns", "properties"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### El DDL de `sesiones_app`
# MAGIC
# MAGIC Una sesión es una **entidad con su propio grano** (una fila por sesión), así que es Silver, no una métrica de Gold.
# MAGIC La clave es `(id_sesion, inicio_sesion)`: un mismo `id_sesion` puede tener más de una sesión si el cliente
# MAGIC desaparece más de 30 minutos y vuelve (lo verás con el cliente 104 en la Parte 4).

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {slv_ses} (
  id_sesion           STRING NOT NULL      COMMENT 'Clave (1/2): sesión de la app',
  inicio_sesion       TIMESTAMP NOT NULL   COMMENT 'Clave (2/2): inicio de la ventana de sesión (primer evento)',
  fin_sesion          TIMESTAMP NOT NULL   COMMENT 'Fin de la ventana: último evento + inactividad',
  id_cliente          BIGINT               COMMENT 'Cliente de la sesión',
  canal               STRING               COMMENT 'Canal de la sesión',
  n_eventos           BIGINT               COMMENT 'Eventos en la sesión',
  n_vistas            BIGINT               COMMENT 'Ofertas vistas en la sesión',
  hizo_click          BOOLEAN              COMMENT 'Hubo al menos un oferta_click',
  simulo              BOOLEAN              COMMENT 'Hubo al menos una simulacion_realizada',
  inicio_solicitud    BOOLEAN              COMMENT 'Hubo al menos una solicitud_iniciada',
  monto_simulado_max  DECIMAL(14,2)        COMMENT 'Mayor monto simulado en la sesión',
  _procesado_ts       TIMESTAMP NOT NULL   COMMENT 'Momento en que el stream escribió la sesión'
)
USING DELTA
CLUSTER BY (id_cliente, inicio_sesion)
TBLPROPERTIES ('quality' = 'silver', 'delta.enableChangeDataFeed' = 'true')
""")
print("Tablas Silver del lab creadas (vacías todavía).")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 3 · Tiempo del evento, watermarks y duplicados
# MAGIC
# MAGIC Un cliente simula un préstamo en el metro, sin señal. La app envía el evento cuando recupera conexión, dos horas
# MAGIC después: `ts_evento` = 08:10, llega a las 10:10. ¿Cuenta en la ventana de las 08:10? Sí. ¿Cuánto esperar a los
# MAGIC rezagados antes de cerrar la ventana? **Eso decide el watermark.**
# MAGIC
# MAGIC ## 3.1 Medir antes de elegir
# MAGIC
# MAGIC Se elige con la distribución real del retraso en Bronze, no a ojo. Aquí la distribución es sintética (100 000 eventos:
# MAGIC 95 % llega en menos de 30 s, un 4,9 % tarda hasta 30 min y un 0,1 % hasta 90 min), pero la consulta es **exactamente la de la sesión**.

# COMMAND ----------

(spark.range(100000)
    .withColumn("u", F.rand(7))
    .withColumn("retraso_seg", F.when(F.col("u") < 0.95, F.rand(11) * 30)
                .when(F.col("u") < 0.999, 30 + F.rand(13) * 1770)
                .otherwise(1800 + F.rand(17) * 3600).cast("long"))
    .withColumn("timestamp_evento", (F.unix_timestamp(F.current_timestamp()) - (F.col("id") % 86400)).cast("timestamp"))
    .withColumn("_ingestion_ts", (F.col("timestamp_evento").cast("long") + F.col("retraso_seg")).cast("timestamp"))
    .select("timestamp_evento", "_ingestion_ts")
    .write.mode("overwrite").saveAsTable(retrasos_tbl))

# La consulta de la sesión: distribución real del retraso en Bronze
display(spark.sql(f"""
SELECT percentile(retraso, 0.50)  AS p50_seg,
       percentile(retraso, 0.99)  AS p99_seg,
       percentile(retraso, 0.999) AS p999_seg,
       max(retraso)               AS max_seg
FROM (SELECT unix_timestamp(_ingestion_ts) - unix_timestamp(timestamp_evento) AS retraso
      FROM {retrasos_tbl})
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Cuánto se pierde con cada watermark?
# MAGIC
# MAGIC El watermark es un trato explícito: *"espero hasta X de retraso; lo que llegue después, lo pierdo en las operaciones con estado"*.
# MAGIC Esta tabla lo hace concreto. Y muestra por qué **el watermark depende de la pregunta, no de la fuente**:
# MAGIC `sesiones_app` quiere sesiones completas (2 h, pierde casi nada); la alerta de abandono prefiere 10 min porque una alerta tardía no sirve (acepta perder más).

# COMMAND ----------

display(spark.sql(f"""
SELECT w.nombre AS para,
       w.segundos AS watermark_seg,
       round(100 * avg(CASE WHEN r.retraso > w.segundos THEN 1.0 ELSE 0.0 END), 3) AS pct_eventos_perdidos
FROM (SELECT unix_timestamp(_ingestion_ts) - unix_timestamp(timestamp_evento) AS retraso FROM {retrasos_tbl}) r
CROSS JOIN (VALUES ('alerta de abandono (10 min)', 600),
                   ('30 minutos', 1800),
                   ('1,5 horas', 5400),
                   ('sesiones_app (2 horas)', 7200)) AS w(nombre, segundos)
GROUP BY w.nombre, w.segundos
ORDER BY w.segundos
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 El stream de `eventos_app`
# MAGIC
# MAGIC La transformación de Bronze a Silver:
# MAGIC
# MAGIC - **Tipar y renombrar** (`timestamp_evento` → `ts_evento`); `retraso_seg` mide cuánto tardó cada evento.
# MAGIC - **Filtrar lo que no tiene clave ni tiempo**: los payloads corruptos (`id_evento` NULL) se quedan en Bronze con su `_payload_crudo`.
# MAGIC - **`withWatermark` + `dropDuplicatesWithinWatermark`**: dedup con **estado acotado**; recuerda cada `id_evento` solo mientras esté dentro del watermark.
# MAGIC - **Append**: cada evento se escribe una vez. Sin MERGE.

# COMMAND ----------

def eventos_df():
    return (spark.readStream.table(brz)
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
        .filter("id_evento IS NOT NULL AND ts_evento IS NOT NULL AND id_cliente IS NOT NULL")
        # Dedup con estado ACOTADO: recuerda cada id_evento solo mientras esté dentro del watermark.
        # Un duplicado que llega mucho después ya no se detecta.
        .withWatermark("ts_evento", watermark)
        .dropDuplicatesWithinWatermark(["id_evento"]))


def estado_comparado(etiqueta):
    """Corre dos dedups sobre la misma Bronze solo para medir su estado: sin watermark y con watermark."""
    base = lambda: (spark.readStream.table(brz)  # noqa: E731
                    .filter("id_evento IS NOT NULL")
                    .select("id_evento", F.col("timestamp_evento").alias("ts_evento")))
    correr(f"{etiqueta} · dropDuplicates SIN watermark", "cmp_sin_watermark", base().dropDuplicates(["id_evento"]))
    correr(f"{etiqueta} · dropDuplicatesWithinWatermark", "cmp_con_watermark",
           base().withWatermark("ts_evento", watermark).dropDuplicatesWithinWatermark(["id_evento"]))


print("Funciones listas: eventos_df(), estado_comparado()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Lote 1: el caso base, con todos los problemas
# MAGIC
# MAGIC Siete sesiones de la app con los problemas típicos del mundo real:
# MAGIC
# MAGIC | Qué | Dónde |
# MAGIC |---|---|
# MAGIC | **Duplicado** (Kafka at-least-once) | `e03` y `e07` llegan dos veces en el mismo lote |
# MAGIC | **Llegada tardía** | `e14` (cliente 999) llega 90 min tarde, `e12` 30 min |
# MAGIC | **Payload corrupto** | un mensaje sin `id_evento` |
# MAGIC | **Tipo desconocido** | `oferta_compartida` (la app agregó un evento nuevo) |
# MAGIC | **Cliente que no está en la dimensión** | el 999 |
# MAGIC | **Sesión que se parte** | el cliente 104 vuelve 58 min después con el mismo `id_sesion` |
# MAGIC
# MAGIC Los números de minuto son relativos a `T0`.

# COMMAND ----------

A, I, W = "APP_ANDROID", "APP_IOS", "WEB"

lote_1 = [
    # S1 · cliente 101: el funnel completo (vista → click → simulación → solicitud)
    ev("e01", "oferta_vista", 0, 101, "S1", A),
    ev("e02", "oferta_click", 2, 101, "S1", A),
    ev("e03", "simulacion_realizada", 5, 101, "S1", A, monto=5000, plazo=12),
    ev("e04", "solicitud_iniciada", 9, 101, "S1", A),
    # S2 · cliente 102: simula y NO solicita (candidato a abandono)
    ev("e05", "oferta_vista", 1, 102, "S2", I),
    ev("e06", "oferta_click", 3, 102, "S2", I),
    ev("e07", "simulacion_realizada", 6, 102, "S2", I, monto=8000, plazo=24),
    # S3 · cliente 103: solo mira, y un tipo de evento que no conocíamos
    ev("e08", "oferta_vista", 2, 103, "S3", W),
    ev("e09", "oferta_compartida", 3, 103, "S3", W),
    # S4 · cliente 104: dos visitas con el mismo id_sesion, 58 minutos de silencio entre medio
    ev("e10", "oferta_vista", 10, 104, "S4", A),
    ev("e11", "oferta_vista", 12, 104, "S4", A),
    ev("e12", "oferta_vista", 70, 104, "S4", A, retraso=1800),       # sin señal 30 min
    # S5 · cliente 999: NO está en dim_clientes; su evento llega 90 min tarde
    ev("e13", "oferta_vista", 18, 999, "S5", W),
    ev("e14", "simulacion_realizada", 20, 999, "S5", W, monto=3000, plazo=6, retraso=5400),
    # S7 · cliente 106: inicia solicitud en la app (la base nunca la verá: Parte 5)
    ev("e15", "oferta_vista", 14, 106, "S7", I),
    ev("e16", "solicitud_iniciada", 18, 106, "S7", I),
    # Duplicados: el productor reintentó (mismo id_evento, mismo ts, llega 40 s después)
    ev("e03", "simulacion_realizada", 5, 101, "S1", A, monto=5000, plazo=12, retraso=40),
    ev("e07", "simulacion_realizada", 6, 102, "S2", I, monto=8000, plazo=24, retraso=40),
    # Payload corrupto: no parseó, id_evento NULL (se queda en Bronze con _payload_crudo)
    (None, None, None, None, None, None, None, None, None, None),
]
publicar(lote_1)

display(spark.sql(f"""
SELECT count(*) AS mensajes, count(id_evento) AS con_id, count(DISTINCT id_evento) AS ids_unicos,
       count_if(id_evento IS NULL) AS corruptos FROM {brz}
"""))

# COMMAND ----------

q = correr("eventos_app · lote 1", "eventos_app", eventos_df(), destino=slv_ev)
print(f"Silver: {spark.table(slv_ev).count()} eventos")

mensajes = spark.table(brz).count()
unicos = spark.table(brz).filter("id_evento IS NOT NULL").select("id_evento").distinct().count()
silver_n = spark.table(slv_ev).count()
dups = spark.sql(f"SELECT count(*) - count(DISTINCT id_evento) FROM {slv_ev}").first()[0]
verificar(silver_n == unicos, f"{mensajes} mensajes en Bronze → {unicos} eventos únicos → {silver_n} en Silver")
verificar(dups == 0, "duplicados dentro del lote: eliminados (e03 y e07 aparecen una sola vez)")
verificar(spark.table(slv_ev).filter("id_evento IS NULL").count() == 0, "el payload corrupto no llegó a Silver (queda en Bronze)")

# Mide el estado de los dos tipos de dedup tras este lote (se compara en 3.6)
estado_comparado("tras lote 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Las tres decisiones del DDL, con datos
# MAGIC
# MAGIC 1. Tres relojes por evento: `ts_evento` (el dispositivo), `_bronze_ingestion_ts` (cuándo llegó) y `retraso_seg` (la diferencia). Mira `e12` y `e14`.
# MAGIC 2. `oferta_compartida` **entró**: un tipo desconocido es una señal. Se monitorea el conteo por tipo.
# MAGIC 3. El cliente 999 **entró** aunque no está en `dim_clientes`.

# COMMAND ----------

display(spark.sql(f"""
SELECT id_evento, tipo_evento, ts_evento, _bronze_ingestion_ts, retraso_seg
FROM {slv_ev} WHERE id_evento IN ('e01', 'e12', 'e14') ORDER BY id_evento
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT tipo_evento, count(*) AS eventos,
       count_if(id_cliente NOT IN (SELECT id_cliente FROM {dim})) AS de_clientes_fuera_de_dim
FROM {slv_ev} GROUP BY 1 ORDER BY 2 DESC
"""))

# COMMAND ----------

# La vista por tipo: lo mismo que una tabla aparte, sin mantener otra tabla
display(spark.sql(f"SELECT * FROM {ns}.v_simulaciones ORDER BY ts_evento"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Volver a correr sin datos nuevos: así se comporta el job continuo
# MAGIC
# MAGIC `availableNow` procesa **lo pendiente** y termina. El job continuo lo lanza otra vez en cuanto termina.
# MAGIC Cuando no hay nada nuevo, la corrida es casi gratis y no cambia nada: el checkpoint recuerda dónde iba.

# COMMAND ----------

q = correr("eventos_app · sin datos nuevos", "eventos_app", eventos_df(), destino=slv_ev)
display(pd.DataFrame(PROGRESO[-1:]))
verificar(PROGRESO[-1]["filas_entrada"] == 0, "la segunda corrida no leyó nada (el checkpoint evita reprocesar)")
verificar(spark.table(slv_ev).count() == silver_n, "Silver sigue igual")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Lote 2: reenvío del lote y tráfico nuevo
# MAGIC
# MAGIC El productor reintenta y **republica** tres eventos del lote 1 (mismos `id_evento`). Además llega tráfico nuevo
# MAGIC cinco horas "más tarde" (cliente 105, ya casi en el presente del lab). Ese tráfico hace avanzar el watermark.
# MAGIC
# MAGIC Prueba de la sesión: **republicar un lote con los mismos `id_evento` y verificar que Silver no cambia**.

# COMMAND ----------

lote_2 = [
    # Reenvío: mismos id_evento que el lote 1 (el estado de dedup todavía los recuerda)
    ev("e03", "simulacion_realizada", 5, 101, "S1", A, monto=5000, plazo=12, retraso=300),
    ev("e07", "simulacion_realizada", 6, 102, "S2", I, monto=8000, plazo=24, retraso=300),
    ev("e04", "solicitud_iniciada", 9, 101, "S1", A, retraso=300),
    # Tráfico nuevo: cliente 105, cinco horas después. Hace avanzar el watermark.
    ev("e17", "oferta_vista", 300, 105, "S6", I),
    ev("e18", "oferta_click", 302, 105, "S6", I),
    ev("e19", "solicitud_iniciada", 304, 105, "S6", I),
]
publicar(lote_2)
q = correr("eventos_app · lote 2", "eventos_app", eventos_df(), destino=slv_ev)

dups = spark.sql(f"SELECT count(*) - count(DISTINCT id_evento) FROM {slv_ev}").first()[0]
verificar(spark.table(slv_ev).count() == silver_n + 3, f"Silver pasó de {silver_n} a {spark.table(slv_ev).count()}: solo entraron los 3 eventos nuevos")
verificar(dups == 0, "los 3 reenvíos se detectaron como duplicados: Silver no cambió")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 La trampa: `dropDuplicates` sin watermark
# MAGIC
# MAGIC `dropDuplicates(["id_evento"])` en un stream recuerda **todos los `id_evento` que vio en su vida**: con dos millones de
# MAGIC eventos al mes el estado crece sin parar y el stream se cae a los tres meses en producción (en el lab funciona perfecto).
# MAGIC
# MAGIC Se miden los dos sobre la misma Bronze. Compara el tamaño del estado (`estado_filas`) tras cada lote.

# COMMAND ----------

# Segunda medición, con el lote 2 ya procesado (la primera fue tras el lote 1)
estado_comparado("tras lote 2")
display(pd.DataFrame([p for p in PROGRESO if "dropDuplicates" in p["corrida"]]))

# COMMAND ----------

# MAGIC %md
# MAGIC Con el watermark avanzando (el tráfico de las 5 h empujó el reloj del evento), el estado de los ids viejos
# MAGIC **se libera**; sin watermark no. Hoy son pocas filas de diferencia; con dos millones de eventos al mes es la diferencia
# MAGIC entre un stream estable y uno que se cae.
# MAGIC
# MAGIC El costo de un estado acotado es que **un duplicado que llega después del watermark ya no se detecta**.
# MAGIC Eso se prueba en la Parte 4, después de construir las sesiones.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 4 · Ventanas
# MAGIC
# MAGIC | Ventana | Cómo se cierra | Un evento cae en… | Ejemplo |
# MAGIC |---|---|---|---|
# MAGIC | **Tumbling** | bloques contiguos, sin solapar | una sola ventana | funnel cada 5 minutos |
# MAGIC | **Sliding** | largo fijo, avanza en pasos más cortos | varias ventanas | "clics en los últimos 10 min", cada minuto |
# MAGIC | **Session** | tras N minutos sin actividad; largo variable | una sesión | sesiones de la app |
# MAGIC
# MAGIC Tumbling y sliding sobre **el mismo evento** (`e07`, la simulación del cliente 102), con una consulta batch
# MAGIC (la forma de la ventana es la misma en batch y en streaming):

# COMMAND ----------

e07 = spark.table(slv_ev).filter("id_evento = 'e07'")

print("TUMBLING de 30 minutos: el evento cae en UNA ventana")
display(e07.groupBy(F.window("ts_evento", "30 minutes").alias("ventana")).count())

print("SLIDING de 30 minutos cada 5: el mismo evento cae en SEIS ventanas")
display(e07.groupBy(F.window("ts_evento", "30 minutes", "5 minutes").alias("ventana")).count().orderBy("ventana"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Session window: construir `sesiones_app`
# MAGIC
# MAGIC `session_window(ts_evento, "30 minutes")` agrupa los eventos de un mismo `id_sesion` mientras entre uno y otro haya menos de
# MAGIC 30 minutos. A diferencia de una ventana fija, **su largo depende de los datos**.
# MAGIC
# MAGIC **Append vs update.** En *append* una sesión se escribe **una sola vez, cuando el watermark confirma que cerró**: la fila es
# MAGIC definitiva (Silver prefiere eso), pero llega tarde: fin de sesión + 30 min de inactividad + 2 h de watermark.
# MAGIC En *update* la ventana se emite en cada lote con valor parcial y hay que escribirla con `foreachBatch` + MERGE.

# COMMAND ----------

def sesiones_df():
    es = lambda tipo: F.col("tipo_evento") == tipo  # noqa: E731
    return (spark.readStream.table(slv_ev)
        .withWatermark("ts_evento", watermark)
        .groupBy("id_sesion", "id_cliente", "canal",
                 # La sesión se cierra tras `inactividad` sin eventos de ese id_sesion
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


q = correr("sesiones_app · tras lote 2", "sesiones_app", sesiones_df(), destino=slv_ses)
display(spark.table(slv_ses).orderBy("id_sesion", "inicio_sesion"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qué mirar en el resultado
# MAGIC
# MAGIC - **`S4` aparece dos veces**: el silencio de 58 minutos partió la sesión. Por eso la clave es `(id_sesion, inicio_sesion)`.
# MAGIC - **`S6` (el tráfico de las 5 h) no aparece**: todavía puede recibir eventos, el watermark no confirma que cerró. Sale en una corrida posterior.
# MAGIC - `fin_sesion` = último evento + 30 minutos.

# COMMAND ----------

ses = spark.table(slv_ses)
repetidas = ses.groupBy("id_sesion", "inicio_sesion").count().filter("count > 1").count()
s4 = ses.filter("id_sesion = 'S4'").count()
verificar(repetidas == 0, "cada (id_sesion, inicio_sesion) aparece una sola vez")
verificar(s4 == 2, "S4 quedó en 2 sesiones (la ventana de sesión se parte por inactividad)")
verificar(ses.filter("id_sesion = 'S6'").count() == 0, "S6 sigue abierta: en append solo se escribe lo que el watermark cerró")
display(spark.sql(f"""
SELECT (SELECT count(DISTINCT id_sesion) FROM {slv_ev}) AS sesiones_con_eventos,
       (SELECT count(DISTINCT id_sesion) FROM {slv_ses}) AS sesiones_cerradas,
       (SELECT max(ts_evento) FROM {slv_ev})             AS ultimo_evento
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Lote 3: el evento tardío y el reenvío después del watermark
# MAGIC
# MAGIC Dos pruebas de la sesión, juntas:
# MAGIC
# MAGIC 1. **Evento tardío**: `e20`, un click del cliente 102 con `ts_evento` en el minuto 30 (hace cuatro horas y media), id **nuevo**.
# MAGIC    En `eventos_app` el dedup no tiene estado de ese id; en `sesiones_app` el watermark ya pasó por ahí.
# MAGIC 2. **Reenvío tras olvidar**: `e01` otra vez, pero el estado de dedup **ya olvidó** ese id (el watermark pasó el minuto 0).
# MAGIC    Es el costo de un estado acotado: el duplicado se cuela.

# COMMAND ----------

antes = {r["id_sesion"]: r["n_eventos"] for r in spark.table(slv_ses).filter("id_sesion = 'S2'").collect()}

lote_3 = [
    ev("e20", "oferta_click", 30, 102, "S2", I, retraso=16200),     # tardío: id nuevo, 4,5 h de retraso
    ev("e01", "oferta_vista", 0, 101, "S1", A, retraso=17000),      # reenvío de un id que el estado ya olvidó
    ev("e21", "oferta_vista", 310, 105, "S6", I),                   # tráfico normal, para que haya algo vigente
]
publicar(lote_3)
q = correr("eventos_app · lote 3", "eventos_app", eventos_df(), destino=slv_ev)

en_silver = spark.table(slv_ev).filter("id_evento = 'e20'").count()
dups = spark.sql(f"SELECT count(*) - count(DISTINCT id_evento) FROM {slv_ev}").first()[0]
print(f"descartadas por watermark en eventos_app: {PROGRESO[-1]['descartadas_por_watermark']}")
verificar(en_silver == 1, "e20 (tardío, id nuevo) entró a eventos_app: nada recordaba ese id")
verificar(dups == 1, f"e01 se coló: ahora hay {dups} duplicado en Silver (el estado acotado ya lo había olvidado)")

# COMMAND ----------

q = correr("sesiones_app · tras lote 3", "sesiones_app", sesiones_df(), destino=slv_ses)

despues = {r["id_sesion"]: r["n_eventos"] for r in spark.table(slv_ses).filter("id_sesion = 'S2'").collect()}
print(f"S2 antes: {antes} · después: {despues} · descartadas por watermark: {PROGRESO[-1]['descartadas_por_watermark']}")
verificar(antes == despues, "e20 NO cambió la sesión S2: el watermark ya cerró esa ventana (llegó tarde)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lo que enseña esta prueba
# MAGIC
# MAGIC - El evento tardío **está en `eventos_app`** (el hecho existe, no se pierde) pero **no cuenta en `sesiones_app`**: la sesión ya se cerró y se escribió.
# MAGIC - La contabilidad no es perfecta: el trato del watermark es explícito y se mide (columna `descartadas_por_watermark`, Parte 6).
# MAGIC - Si el tardío importa, se reprocesa desde Bronze (que tiene todo) con otro checkpoint.
# MAGIC
# MAGIC > Si algún `⚠️` salió distinto en tu workspace, no es un error del notebook: mira el contador de descartados
# MAGIC > y la fila de la corrida en la tabla `PROGRESO` (Parte 6). Es justo lo que hay que aprender a leer.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 5 · Joins en streaming
# MAGIC
# MAGIC ## 5.1 Stream-estático: la alerta de abandono
# MAGIC
# MAGIC El rescate de abandono no puede esperar las 2 h + 30 min de `sesiones_app`. Se resuelve con una **ventana deslizante**:
# MAGIC clientes que simularon y no iniciaron solicitud, ventana de 30 min evaluada cada 5.
# MAGIC
# MAGIC - **Dos watermarks para la misma fuente**: `sesiones_app` espera 2 h porque quiere sesiones completas; la alerta espera 10 min porque
# MAGIC   una alerta tardía no sirve. El watermark no es una propiedad de la fuente sino **de la pregunta**.
# MAGIC - La alerta es lógica de un consumidor (campañas): su resultado va a Gold (S18), no a Silver. Aquí queda en `alertas_abandono` del lab.
# MAGIC - El motor de campañas necesita el nivel de riesgo: **join con `dim_clientes`**, leída con `read` (no `readStream`) y filtrando `es_vigente`.
# MAGIC - **`left`**: un cliente nuevo que aún no está en la dimensión **no pierde su alerta** (el 999).
# MAGIC - El join estático **no guarda estado**: no necesita watermark.

# COMMAND ----------

def alertas_df():
    abandono = (spark.readStream.table(slv_ev)
        .filter("tipo_evento IN ('simulacion_realizada', 'solicitud_iniciada')")
        .withWatermark("ts_evento", watermark_alerta)     # para una alerta, esperar más no sirve
        .groupBy("id_cliente", F.window("ts_evento", "30 minutes", "5 minutes").alias("v"))
        .agg(F.max(F.col("tipo_evento") == "simulacion_realizada").alias("simulo"),
             F.max(F.col("tipo_evento") == "solicitud_iniciada").alias("solicito"),
             F.max("monto_simulado").alias("monto_simulado"))
        .filter("simulo AND NOT solicito"))

    clientes = (spark.read.table(dim)                  # read, no readStream
        .filter("es_vigente")
        .select("id_cliente", "nivel_riesgo", "score_interno"))

    return (abandono
        .join(clientes, "id_cliente", "left")          # left: el cliente nuevo no pierde su alerta
        .withColumn("cliente_en_dimension", F.col("nivel_riesgo").isNotNull())
        .select("id_cliente",
                F.col("v.start").alias("ventana_inicio"), F.col("v.end").alias("ventana_fin"),
                "monto_simulado", "nivel_riesgo", "score_interno", "cliente_en_dimension",
                F.current_timestamp().alias("_procesado_ts")))


q = correr("alertas_abandono · tras lote 3", "alertas_abandono", alertas_df(), destino=alertas)
display(spark.table(alertas).orderBy("id_cliente", "ventana_inicio"))

# COMMAND ----------

# MAGIC %md
# MAGIC Qué mirar:
# MAGIC
# MAGIC - **Cliente 102** simuló y no solicitó: aparece. **El 101 no**: sí inició solicitud, ninguna ventana lo marca como abandono.
# MAGIC - **Cliente 999** aparece con `cliente_en_dimension = false`: el `left` lo protegió. La integridad se resuelve en Gold.
# MAGIC - Cada cliente sale **varias veces**: la ventana deslizante solapa (cada evento cae en seis ventanas). El consumidor se queda con una por cliente, p. ej. la última.

# COMMAND ----------

display(spark.sql(f"""
SELECT id_cliente, max(ventana_fin) AS ultima_ventana, max(monto_simulado) AS monto_simulado,
       first(nivel_riesgo) AS nivel_riesgo, first(cliente_en_dimension) AS cliente_en_dimension,
       count(*) AS ventanas_marcadas
FROM {alertas} GROUP BY id_cliente ORDER BY id_cliente
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### El lado estático se relee
# MAGIC
# MAGIC Si el lado estático es una tabla Delta, el stream ve **la última versión** de `dim_clientes` en cada corrida. Prueba:
# MAGIC cambio el riesgo del cliente 102 (de `B` a `D`) y publico una simulación nueva sin reiniciar nada. La alerta nueva trae el riesgo nuevo.

# COMMAND ----------

spark.sql(f"UPDATE {dim} SET nivel_riesgo = 'D', score_interno = 520 WHERE id_cliente = 102 AND es_vigente")

lote_4 = [
    ev("e22", "simulacion_realizada", 320, 102, "S8", I, monto=12000, plazo=36),
    ev("e23", "oferta_vista", 345, 105, "S6", I),      # empuja el watermark de 10 min para que la ventana cierre
]
publicar(lote_4)
q = correr("eventos_app · lote 4", "eventos_app", eventos_df(), destino=slv_ev)
q = correr("alertas_abandono · tras lote 4", "alertas_abandono", alertas_df(), destino=alertas)

display(spark.sql(f"""
SELECT id_cliente, ventana_inicio, nivel_riesgo, score_interno, monto_simulado
FROM {alertas} WHERE id_cliente = 102 ORDER BY ventana_inicio
"""))
nuevas = spark.table(alertas).filter("id_cliente = 102 AND monto_simulado = 12000 AND nivel_riesgo = 'D'").count()
verificar(nuevas > 0, "las alertas nuevas del 102 traen el nivel 'D': el stream releyó la dimensión sin reiniciarse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Stream-stream: reconciliar la app con la base (opcional)
# MAGIC
# MAGIC `solicitud_iniciada` llega por **dos caminos**: el evento de la app y la fila en la base (que llega a Bronze con el delta horario).
# MAGIC Si la app dice que el cliente inició una solicitud y la base no tiene ninguna en la hora siguiente, algo se rompió entre la app y el backend.
# MAGIC
# MAGIC - **Ambos lados con watermark** y una **condición de TIEMPO** en el join: sin ella el motor guardaría para siempre cada evento esperando su pareja.
# MAGIC - El watermark de la base es más holgado (3 h) porque llega con el delta horario.
# MAGIC - `leftOuter`: una fila sin pareja solo se emite cuando el watermark confirma que ya no llegará (1 h de rango + el watermark más lento de los dos lados).
# MAGIC
# MAGIC En el lab, la base tiene la solicitud del 101 (minuto 10, hay pareja) y la del 105 (minuto 303). **El 106 inició solicitud en la app y la base no la tiene.**

# COMMAND ----------

spark.sql(f"""
INSERT INTO {sol_brz} VALUES (101, TIMESTAMP'{mins(10)}'), (105, TIMESTAMP'{mins(303)}')
""")

app = (spark.readStream.table(slv_ev)
    .filter("tipo_evento = 'solicitud_iniciada'")
    .withWatermark("ts_evento", watermark)
    .select("id_cliente", "id_oferta", F.col("ts_evento").alias("ts_app")))

# Bronze de solicitudes es append-only: se puede leer como stream.
base = (spark.readStream.table(sol_brz)
    .select(F.col("id_cliente").cast("bigint").alias("cli_base"),
            F.col("fecha_hora_solicitud").cast("timestamp").alias("ts_base"))
    .withWatermark("ts_base", "3 hours"))

sin_pareja = (app.join(base,
        F.expr("cli_base = id_cliente AND ts_base BETWEEN ts_app - INTERVAL 5 MINUTES AND ts_app + INTERVAL 1 HOUR"),
        "leftOuter")
    .filter("ts_base IS NULL")
    .select("id_cliente", "id_oferta", "ts_app"))

q = correr("solicitudes_sin_pareja", "solicitudes_sin_pareja", sin_pareja, destino=sin_pareja_tbl)
display(spark.table(sin_pareja_tbl))
verificar(spark.table(sin_pareja_tbl).filter("id_cliente = 106").count() == 1,
          "el 106 aparece: la app dijo que inició solicitud y la base no tiene ninguna")
verificar(spark.table(sin_pareja_tbl).filter("id_cliente IN (101, 105)").count() == 0,
          "el 101 y el 105 no aparecen: tienen pareja en la base")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 6 · Operar un stream encendido
# MAGIC
# MAGIC ## 6.1 Las métricas que hay que vigilar
# MAGIC
# MAGIC En el job real se lee `q.lastProgress` / `q.recentProgress`. En este notebook `correr()` ya las guardó en `PROGRESO`, una fila por corrida:
# MAGIC
# MAGIC | Métrica | Qué dice | Señal de alarma |
# MAGIC |---|---|---|
# MAGIC | **Retraso del stream** | ahora − `ts_evento` más reciente procesado | crece: el stream no da abasto |
# MAGIC | **`estado_filas`** (`numRowsTotal`) | tamaño del estado | crece sin estabilizarse: falta watermark o es demasiado grande |
# MAGIC | **`descartadas_por_watermark`** (`numRowsDroppedByWatermark`) | eventos que llegaron demasiado tarde | sube: el watermark quedó corto; mide de nuevo (3.1) |

# COMMAND ----------

display(pd.DataFrame(PROGRESO))

# COMMAND ----------

# Retraso del stream: ahora − ts_evento más reciente procesado.
# En el lab es alto porque los datos son estáticos (no hay productor en vivo); en producción debe ser de segundos o minutos.
display(spark.sql(f"""
SELECT max(ts_evento) AS ultimo_evento_procesado, current_timestamp() AS ahora,
       current_timestamp() - max(ts_evento) AS retraso_del_stream
FROM {slv_ev}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 El checkpoint ata el código
# MAGIC
# MAGIC El checkpoint guarda el estado **con la forma de la query que lo creó**. Cambiar las columnas del `groupBy`, el largo de una ventana
# MAGIC o agregar un `dropDuplicates` puede dejarlo incompatible. En ese caso: **nueva ruta de checkpoint y reprocesar desde Bronze**.
# MAGIC **Nunca borrar el checkpoint de un stream que está corriendo.**
# MAGIC
# MAGIC Prueba segura: se **copia** el checkpoint de `sesiones_app` y se corre sobre la copia la misma query **sin `canal` en el `groupBy`**.

# COMMAND ----------

copia = ckpt("sesiones_app_copia")
dbutils.fs.rm(copia, recurse=True)
dbutils.fs.cp(ckpt("sesiones_app"), copia, recurse=True)

query_cambiada = (spark.readStream.table(slv_ev)
    .withWatermark("ts_evento", watermark)
    .groupBy("id_sesion", "id_cliente",                       # ← sin "canal": cambió la forma del estado
             F.session_window("ts_evento", inactividad).alias("ventana"))
    .agg(F.count("*").alias("n_eventos")))

try:
    qc = (query_cambiada.writeStream.outputMode("append")
          .option("checkpointLocation", copia).trigger(availableNow=True)
          .foreachBatch(lambda lote, i: None).start())
    qc.awaitTermination()
    print("⚠️ No falló en este caso. Aun así, esa query ya no es la que creó el estado: no la despliegues sobre ese checkpoint.")
except Exception as e:  # noqa: BLE001
    print("✅ Falló, como dice la teoría: el estado guardado no encaja con la query nueva.")
    print(str(e)[:500])
finally:
    dbutils.fs.rm(copia, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Lo que recibe Gold: el Change Data Feed
# MAGIC
# MAGIC Las tablas que Gold lee de forma incremental (eventos, transaccionales, maestras SCD2, historiales) tienen **CDF activo**.
# MAGIC Gold lee solo lo que cambió desde la última vez con `readChangeFeed`:

# COMMAND ----------

cdf = (spark.read.option("readChangeFeed", "true").option("startingVersion", 0).table(slv_ev))
display(cdf.groupBy("_change_type").count())
display(cdf.select("_commit_version", "_change_type", "id_evento", "tipo_evento", "ts_evento")
        .orderBy("_commit_version", "ts_evento").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC | Silver | Cómo lo lee Gold |
# MAGIC |---|---|
# MAGIC | Transaccionales, maestras SCD2, historiales, eventos (CDF activo) | Incremental con `readChangeFeed` |
# MAGIC | Catálogos (`paises`, `campanias`, `tipos_cambio`) | Lectura completa: son pequeñas |
# MAGIC | Todas | Con un contrato: tipos fijos, clave única probada, columnas técnicas y cuarentena |
# MAGIC
# MAGIC ## 6.4 Del notebook al job
# MAGIC
# MAGIC Lo que acabas de correr a mano es lo que el job automatiza. Dos jobs de Silver, cada uno con su ritmo:
# MAGIC
# MAGIC | Job | Tablas | Ejecución |
# MAGIC |---|---|---|
# MAGIC | `job_silver_lending_hourly` | las 9 de la S15, las maestras y el historial de la S16 | schedule horario · `availableNow` |
# MAGIC | `job_silver_eventos_stream` | `eventos_app`, `sesiones_app` | **continuo** · `availableNow` en bucle |
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     job_silver_eventos_stream:
# MAGIC       continuous:
# MAGIC         pause_status: UNPAUSED     # al terminar una corrida arranca la siguiente; si falla, se reintenta solo
# MAGIC       tasks:
# MAGIC         - task_key: eventos_app    # lo de la 3.2
# MAGIC         - task_key: sesiones_app   # lo de la 4.1
# MAGIC           depends_on: [{ task_key: eventos_app }]
# MAGIC ```
# MAGIC
# MAGIC `sesiones_app` depende de `eventos_app` (lee su salida), igual que el orden de estos notebooks: `06` antes que `07`.
# MAGIC
# MAGIC ## Cierre
# MAGIC
# MAGIC | Concepto | Dónde lo viste |
# MAGIC |---|---|
# MAGIC | Stream continuo solo con un consumidor que actúa en minutos | Parte 1 |
# MAGIC | Grano, clave, una tabla con vistas por tipo; tipo desconocido y cliente sin dimensión se aceptan | Parte 2 · 3.3 |
# MAGIC | Watermark elegido midiendo el retraso; depende de la pregunta | 3.1 · 5.1 |
# MAGIC | Dedup con estado acotado; nunca `dropDuplicates` sin watermark | 3.5 · 3.6 · 4.2 |
# MAGIC | Tumbling, sliding, session | Parte 4 |
# MAGIC | Joins estático y stream-stream sin estado infinito | Parte 5 |
# MAGIC | Operar: retraso, estado, descartados; el checkpoint ata el código | Parte 6 |
# MAGIC
# MAGIC **Siguiente:** Sesión 18 abre Gold: el modelo dimensional, los hechos construidos desde el CDF de Silver y la conversión a USD con `tipos_cambio`.
# MAGIC
# MAGIC ## Limpiar (opcional)
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"DROP SCHEMA IF EXISTS {ns} CASCADE")
# MAGIC dbutils.fs.rm(raiz_ckpt, recurse=True)
# MAGIC ```
