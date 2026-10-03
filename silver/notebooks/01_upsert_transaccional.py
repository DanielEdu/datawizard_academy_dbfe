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
# MAGIC versión es más nueva; si viene vacía se usa la siguiente), **reglas** de calidad (SQL que debe ser verdadero),
# MAGIC **columnas** (el `try_cast` de cada columna al tipo del DDL) y la **condición** y los **valores** del MERGE,
# MAGIC escritos columna por columna.

# COMMAND ----------

CONFIG = {
    "paises": {
        "schema": "lending", "clave": ["id_pais"],
        "orden": ["fecha_actualizacion", "fecha_creacion"],
        "reglas": {"clave": "id_pais IS NOT NULL",
                   "iso_valido": "codigo_iso RLIKE '^[A-Z]{2}$'"},
        # Paso 1 · los tipos del DDL de Silver
        "columnas": [
            "try_cast(id_pais AS SMALLINT) AS id_pais",
            "try_cast(codigo_iso AS STRING) AS codigo_iso",
            "try_cast(nombre_pais AS STRING) AS nombre_pais",
            "try_cast(moneda_codigo AS STRING) AS moneda_codigo",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_actualizacion AS TIMESTAMP) AS fecha_actualizacion",
        ],
        # Paso 4 · MERGE: condición y valores columna por columna
        "condicion": "t.id_pais = s.id_pais",
        "valores": {
            "id_pais": "s.id_pais",
            "codigo_iso": "s.codigo_iso",
            "nombre_pais": "s.nombre_pais",
            "moneda_codigo": "s.moneda_codigo",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_actualizacion": "s.fecha_actualizacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
    },
    "campanias": {
        "schema": "lending", "clave": ["id_campania"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_inicio"],
        "reglas": {"clave": "id_campania IS NOT NULL"},
        "columnas": [
            "try_cast(id_campania AS INT) AS id_campania",
            "try_cast(id_pais AS SMALLINT) AS id_pais",
            "try_cast(nombre_campania AS STRING) AS nombre_campania",
            "try_cast(tipo_campania AS STRING) AS tipo_campania",
            "try_cast(fecha_inicio AS DATE) AS fecha_inicio",
            "try_cast(fecha_fin AS DATE) AS fecha_fin",
            "try_cast(presupuesto AS DECIMAL(14,2)) AS presupuesto",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_actualizacion AS TIMESTAMP) AS fecha_actualizacion",
        ],
        "condicion": "t.id_campania = s.id_campania",
        "valores": {
            "id_campania": "s.id_campania",
            "id_pais": "s.id_pais",
            "nombre_campania": "s.nombre_campania",
            "tipo_campania": "s.tipo_campania",
            "fecha_inicio": "s.fecha_inicio",
            "fecha_fin": "s.fecha_fin",
            "presupuesto": "s.presupuesto",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_actualizacion": "s.fecha_actualizacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
    },
    "ofertas_preaprobadas": {
        "schema": "lending", "clave": ["id_oferta"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_generacion"],
        "reglas": {"clave": "id_oferta IS NOT NULL AND id_cliente IS NOT NULL",
                   "monto_positivo": "monto_ofertado > 0"},
        "columnas": [
            "try_cast(id_oferta AS BIGINT) AS id_oferta",
            "try_cast(id_cliente AS BIGINT) AS id_cliente",
            "try_cast(id_producto AS INT) AS id_producto",
            "try_cast(id_campania AS INT) AS id_campania",
            "try_cast(monto_ofertado AS DECIMAL(14,2)) AS monto_ofertado",
            "try_cast(plazo_meses_ofertado AS SMALLINT) AS plazo_meses_ofertado",
            "try_cast(tasa_ofertada AS DECIMAL(6,3)) AS tasa_ofertada",
            "try_cast(motor_asignacion AS STRING) AS motor_asignacion",
            "try_cast(fecha_generacion AS TIMESTAMP) AS fecha_generacion",
            "try_cast(fecha_vigencia_fin AS DATE) AS fecha_vigencia_fin",
            "try_cast(estado_oferta AS STRING) AS estado_oferta",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_actualizacion AS TIMESTAMP) AS fecha_actualizacion",
        ],
        "condicion": "t.id_oferta = s.id_oferta",
        "valores": {
            "id_oferta": "s.id_oferta",
            "id_cliente": "s.id_cliente",
            "id_producto": "s.id_producto",
            "id_campania": "s.id_campania",
            "monto_ofertado": "s.monto_ofertado",
            "plazo_meses_ofertado": "s.plazo_meses_ofertado",
            "tasa_ofertada": "s.tasa_ofertada",
            "motor_asignacion": "s.motor_asignacion",
            "fecha_generacion": "s.fecha_generacion",
            "fecha_vigencia_fin": "s.fecha_vigencia_fin",
            "estado_oferta": "s.estado_oferta",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_actualizacion": "s.fecha_actualizacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
    },
    "solicitudes_prestamo": {
        "schema": "lending", "clave": ["id_solicitud"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_hora_solicitud"],
        "reglas": {"clave": "id_solicitud IS NOT NULL AND id_cliente IS NOT NULL",
                   "monto_positivo": "monto_solicitado > 0",
                   "estado_valido": "estado_solicitud IN ('Iniciada','En evaluacion','Aprobada','Rechazada','Desistida')"},
        "columnas": [
            "try_cast(id_solicitud AS BIGINT) AS id_solicitud",
            "try_cast(id_oferta AS BIGINT) AS id_oferta",
            "try_cast(id_cliente AS BIGINT) AS id_cliente",
            "try_cast(id_producto AS INT) AS id_producto",
            "try_cast(canal AS STRING) AS canal",
            "try_cast(monto_solicitado AS DECIMAL(14,2)) AS monto_solicitado",
            "try_cast(plazo_meses_solicitado AS SMALLINT) AS plazo_meses_solicitado",
            "try_cast(fecha_hora_solicitud AS TIMESTAMP) AS fecha_hora_solicitud",
            "try_cast(estado_solicitud AS STRING) AS estado_solicitud",
            "try_cast(score_evaluacion AS SMALLINT) AS score_evaluacion",
            "try_cast(decision_motor AS STRING) AS decision_motor",
            "try_cast(monto_aprobado AS DECIMAL(14,2)) AS monto_aprobado",
            "try_cast(tasa_aprobada AS DECIMAL(6,3)) AS tasa_aprobada",
            "try_cast(motivo_rechazo AS STRING) AS motivo_rechazo",
            "try_cast(fecha_hora_resolucion AS TIMESTAMP) AS fecha_hora_resolucion",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_actualizacion AS TIMESTAMP) AS fecha_actualizacion",
        ],
        "condicion": "t.id_solicitud = s.id_solicitud",
        "valores": {
            "id_solicitud": "s.id_solicitud",
            "id_oferta": "s.id_oferta",
            "id_cliente": "s.id_cliente",
            "id_producto": "s.id_producto",
            "canal": "s.canal",
            "monto_solicitado": "s.monto_solicitado",
            "plazo_meses_solicitado": "s.plazo_meses_solicitado",
            "fecha_hora_solicitud": "s.fecha_hora_solicitud",
            "estado_solicitud": "s.estado_solicitud",
            "score_evaluacion": "s.score_evaluacion",
            "decision_motor": "s.decision_motor",
            "monto_aprobado": "s.monto_aprobado",
            "tasa_aprobada": "s.tasa_aprobada",
            "motivo_rechazo": "s.motivo_rechazo",
            "fecha_hora_resolucion": "s.fecha_hora_resolucion",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_actualizacion": "s.fecha_actualizacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
    },
    "desembolsos": {
        "schema": "lending", "clave": ["id_desembolso"],
        "orden": ["fecha_actualizacion", "fecha_creacion", "fecha_hora_desembolso"],
        "reglas": {"clave": "id_desembolso IS NOT NULL",
                   "monto_positivo": "monto_desembolsado > 0"},
        "columnas": [
            "try_cast(id_desembolso AS BIGINT) AS id_desembolso",
            "try_cast(id_solicitud AS BIGINT) AS id_solicitud",
            "try_cast(id_cliente AS BIGINT) AS id_cliente",
            "try_cast(monto_desembolsado AS DECIMAL(14,2)) AS monto_desembolsado",
            "try_cast(moneda AS STRING) AS moneda",
            "try_cast(tasa_aplicada AS DECIMAL(6,3)) AS tasa_aplicada",
            "try_cast(plazo_meses AS SMALLINT) AS plazo_meses",
            "try_cast(comision_cobrada AS DECIMAL(12,2)) AS comision_cobrada",
            "try_cast(cuenta_destino_masked AS STRING) AS cuenta_destino_masked",
            "try_cast(fecha_hora_desembolso AS TIMESTAMP) AS fecha_hora_desembolso",
            "try_cast(estado_desembolso AS STRING) AS estado_desembolso",
            "try_cast(referencia_bancaria AS STRING) AS referencia_bancaria",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_actualizacion AS TIMESTAMP) AS fecha_actualizacion",
        ],
        "condicion": "t.id_desembolso = s.id_desembolso",
        "valores": {
            "id_desembolso": "s.id_desembolso",
            "id_solicitud": "s.id_solicitud",
            "id_cliente": "s.id_cliente",
            "monto_desembolsado": "s.monto_desembolsado",
            "moneda": "s.moneda",
            "tasa_aplicada": "s.tasa_aplicada",
            "plazo_meses": "s.plazo_meses",
            "comision_cobrada": "s.comision_cobrada",
            "cuenta_destino_masked": "s.cuenta_destino_masked",
            "fecha_hora_desembolso": "s.fecha_hora_desembolso",
            "estado_desembolso": "s.estado_desembolso",
            "referencia_bancaria": "s.referencia_bancaria",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_actualizacion": "s.fecha_actualizacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
    },
    "cuotas": {
        "schema": "cobranzas", "clave": ["id_cuota"],
        "orden": ["fecha_modificacion", "fecha_creacion"],
        "reglas": {"clave": "id_cuota IS NOT NULL",
                   "estado_valido": "estado_cuota IN ('Pendiente','Pagada','Vencida')",
                   "cuota_cuadra": "abs(monto_cuota - (monto_capital + monto_interes)) <= 0.01"},
        "columnas": [
            "try_cast(id_cuota AS BIGINT) AS id_cuota",
            "try_cast(numero_credito AS BIGINT) AS numero_credito",
            "try_cast(numero_cuota AS SMALLINT) AS numero_cuota",
            "try_cast(fecha_vencimiento AS DATE) AS fecha_vencimiento",
            "try_cast(monto_cuota AS DECIMAL(12,2)) AS monto_cuota",
            "try_cast(monto_capital AS DECIMAL(12,2)) AS monto_capital",
            "try_cast(monto_interes AS DECIMAL(12,2)) AS monto_interes",
            "try_cast(estado_cuota AS STRING) AS estado_cuota",
            "try_cast(fecha_creacion AS TIMESTAMP) AS fecha_creacion",
            "try_cast(fecha_modificacion AS TIMESTAMP) AS fecha_modificacion",
        ],
        "condicion": "t.id_cuota = s.id_cuota",
        "valores": {
            "id_cuota": "s.id_cuota",
            "numero_credito": "s.numero_credito",
            "numero_cuota": "s.numero_cuota",
            "fecha_vencimiento": "s.fecha_vencimiento",
            "monto_cuota": "s.monto_cuota",
            "monto_capital": "s.monto_capital",
            "monto_interes": "s.monto_interes",
            "estado_cuota": "s.estado_cuota",
            "fecha_creacion": "s.fecha_creacion",
            "fecha_modificacion": "s.fecha_modificacion",
            "_origen_archivo": "s._origen_archivo",
            "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
            "_procesado_ts": "s._procesado_ts",
        },
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

def tipar(lote, columnas):
    """Paso 1 · Castea cada columna al tipo del DDL de Silver (lista "columnas" de CONFIG).
    try_cast da NULL si un valor no convierte, en vez de romper el lote."""
    return lote.selectExpr(
        *columnas,
        "_metadata.file_path AS _origen_archivo",
        "_ingestion_ts AS _bronze_ingestion_ts",
        "current_timestamp() AS _procesado_ts",
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
        F.to_json(F.struct(*invalidas.drop("_motivos").columns)).alias("fila"),
        "_origen_archivo",
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_procesado_ts"))
     .write.mode("append").saveAsTable(cuarentena))


def ultima_version(df, clave, orden):
    """Paso 3 · Una fila por clave: la de mayor orden (si empatan, la última ingestada).
    Sin esto el MERGE falla: dos filas del lote coincidirían con la misma fila destino."""
    df = df.withColumn("_orden", F.coalesce(*orden, "_bronze_ingestion_ts"))
    w = Window.partitionBy(*clave).orderBy(F.col("_orden").desc(), F.col("_bronze_ingestion_ts").desc())
    return (df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
              .withColumn(orden[0], F.col("_orden"))   # la columna de orden queda siempre llena
              .drop("_rn", "_orden"))


def mergear(df, condicion, valores, columna_orden):
    """Paso 4 · MERGE idempotente con guarda: una versión vieja que llega tarde no pisa a una nueva.
    condicion y valores vienen de CONFIG, escritos columna por columna."""
    (DeltaTable.forName(spark, destino).alias("t")
        .merge(df.alias("s"), condicion)
        .whenMatchedUpdate(condition=f"s.{columna_orden} > t.{columna_orden}", set=valores)
        .whenNotMatchedInsert(values=valores)
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Probar los pasos sobre un pedazo de Bronze (sin escribir)

# COMMAND ----------

muestra = spark.table(origen).limit(2000)
tipado = tipar(muestra, cfg["columnas"])
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
    validas, invalidas = separar_invalidas(tipar(lote, cfg["columnas"]), cfg["reglas"])
    enviar_a_cuarentena(invalidas, batch_id)
    mergear(ultima_version(validas, cfg["clave"], cfg["orden"]),
            cfg["condicion"], cfg["valores"], cfg["orden"][0])


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
