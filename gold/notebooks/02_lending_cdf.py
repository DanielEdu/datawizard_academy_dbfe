# Databricks notebook source
# MAGIC %md
# MAGIC # Gold paso a paso · 02 · Lending: Change Data Feed (CDF)
# MAGIC
# MAGIC **Escenario:** Lending · **Sesión 18** · **Consumidor:** otros sistemas (CRM, caché de una app, otro Lakehouse).
# MAGIC
# MAGIC El CRM quiere saber **qué clientes cambiaron de riesgo o de score**. Sin CDF solo tiene dos caminos malos:
# MAGIC releer los 50.000 clientes cada día, o comparar dos copias de la tabla. Con **Change Data Feed** Delta
# MAGIC registra cada cambio y el consumidor pide *«dame solo lo que cambió desde la versión N»*.
# MAGIC
# MAGIC | Paso | Qué haces | Feature |
# MAGIC |---|---|---|
# MAGIC | 1 | Crear `dim_cliente_vigente` (un registro por cliente) | CTAS |
# MAGIC | 2 | Activar CDF | `delta.enableChangeDataFeed` |
# MAGIC | 3 | Llega un lote nuevo y se aplica | `MERGE` con condición de cambio + `DELETE` |
# MAGIC | 4 | Ver la historia de versiones | `DESCRIBE HISTORY` |
# MAGIC | 5 | Leer los cambios | `table_changes()` y `_change_type` |
# MAGIC | 6 | Consumirlos de forma incremental | `readChangeFeed` + checkpoint |
# MAGIC
# MAGIC > Aquí la tabla es la **versión vigente** del cliente (no la SCD2 del notebook 01): es lo que consume el CRM.
# MAGIC > Y CDF luce con `MERGE`: solo se registran las filas que **realmente cambian**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Empezar de cero
# MAGIC
# MAGIC Las versiones de la tabla se numeran desde 0 al crearla. Para que el notebook se pueda repetir y los números
# MAGIC de versión de abajo sean siempre los mismos, borramos la tabla, la cola de salida y el checkpoint del consumidor.

# COMMAND ----------

checkpoint = "s3://lakehouse-datawizard/checkpoint/notebooks/gold/cdf_sync_crm"

spark.sql("DROP TABLE IF EXISTS gold.lending_lab.dim_cliente_vigente")
spark.sql("DROP TABLE IF EXISTS gold.lending_lab.cola_sync_crm")
dbutils.fs.rm(checkpoint, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear la tabla Gold (versión 0)
# MAGIC
# MAGIC Un registro por cliente, solo lo que el CRM necesita.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.lending_lab.dim_cliente_vigente
# MAGIC COMMENT 'Un registro por cliente con su estado vigente. Consumo: CRM y campañas'
# MAGIC AS
# MAGIC SELECT
# MAGIC   id_cliente,
# MAGIC   nivel_riesgo,
# MAGIC   score_interno,
# MAGIC   ingreso_mensual_declarado,
# MAGIC   situacion_laboral,
# MAGIC   ciudad,
# MAGIC   id_pais,
# MAGIC   fecha_inicio_vigencia AS actualizado_desde
# MAGIC FROM silver.lending.dim_clientes_lab
# MAGIC WHERE es_vigente;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Activar CDF (versión 1)
# MAGIC
# MAGIC CDF es una **propiedad de la tabla**. Desde que se activa, cada escritura deja, además de los datos,
# MAGIC los registros de cambio (en la carpeta `_change_data` de la tabla). Lo anterior **no** queda en el feed:
# MAGIC por eso se activa **antes** de que haya consumidores.
# MAGIC
# MAGIC Para una tabla nueva puede ir en el `CREATE`: `TBLPROPERTIES (delta.enableChangeDataFeed = true)`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gold.lending_lab.dim_cliente_vigente
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC SHOW TBLPROPERTIES gold.lending_lab.dim_cliente_vigente (delta.enableChangeDataFeed);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Llega un lote nuevo desde Silver (versión 2)
# MAGIC
# MAGIC Se simula con una vista temporal que arma el lote del día:
# MAGIC
# MAGIC - **3 clientes** bajan a riesgo `E` y pierden 150 puntos de score → deben salir como `update`.
# MAGIC - **3 clientes** llegan **sin cambios** → no deben generar nada en el feed.
# MAGIC - **1 cliente nuevo** → `insert`.
# MAGIC
# MAGIC El `MERGE` solo actualiza si **algo cambió realmente** (`<=>` compara tratando NULL como un valor).
# MAGIC Sin esa condición, un `UPDATE SET *` reescribiría las 3 filas sin cambios y el feed las reportaría como cambio.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW lote_silver AS
# MAGIC -- 3 clientes que cambian
# MAGIC SELECT id_cliente, 'E' AS nivel_riesgo, CAST(score_interno - 150 AS SMALLINT) AS score_interno,
# MAGIC        ingreso_mensual_declarado, situacion_laboral, ciudad, id_pais, current_timestamp() AS actualizado_desde
# MAGIC FROM (SELECT * FROM silver.lending.dim_clientes_lab WHERE es_vigente ORDER BY id_cliente LIMIT 3)
# MAGIC UNION ALL
# MAGIC -- 3 clientes que llegan iguales
# MAGIC SELECT id_cliente, nivel_riesgo, score_interno, ingreso_mensual_declarado, situacion_laboral, ciudad, id_pais, fecha_inicio_vigencia
# MAGIC FROM (SELECT * FROM silver.lending.dim_clientes_lab WHERE es_vigente ORDER BY id_cliente LIMIT 3 OFFSET 3)
# MAGIC UNION ALL
# MAGIC -- 1 cliente nuevo
# MAGIC SELECT 9000001, 'B', CAST(720 AS SMALLINT), CAST(5000 AS DECIMAL(14,2)), 'Dependiente', 'Lima', CAST(1 AS SMALLINT), current_timestamp();

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.lending_lab.dim_cliente_vigente AS t
# MAGIC USING lote_silver AS s
# MAGIC ON t.id_cliente = s.id_cliente
# MAGIC WHEN MATCHED AND (NOT (t.nivel_riesgo <=> s.nivel_riesgo) OR NOT (t.score_interno <=> s.score_interno)) THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Una baja (versión 3)
# MAGIC
# MAGIC Un cliente pide eliminar su cuenta. El `DELETE` también queda en el feed: el CRM necesita enterarse.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.lending_lab.dim_cliente_vigente
# MAGIC WHERE id_cliente = (SELECT id_cliente FROM gold.lending_lab.dim_cliente_vigente ORDER BY id_cliente LIMIT 1 OFFSET 20);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. La historia de versiones: `DESCRIBE HISTORY`
# MAGIC
# MAGIC Cada operación es una **versión**: 0 = crear, 1 = activar CDF, 2 = `MERGE`, 3 = `DELETE`.
# MAGIC `operationMetrics` dice cuántas filas tocó cada una. Revisa el `MERGE`: reporta 3 filas actualizadas y 1 insertada,
# MAGIC **no 7**: las 3 sin cambios no se tocaron.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, timestamp, operation, operationMetrics
# MAGIC FROM (DESCRIBE HISTORY gold.lending_lab.dim_cliente_vigente)
# MAGIC ORDER BY version;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Leer los cambios: `table_changes()`
# MAGIC
# MAGIC `table_changes(tabla, versión_inicial)` devuelve las filas modificadas más tres columnas de control:
# MAGIC
# MAGIC | `_change_type` | Significa |
# MAGIC |---|---|
# MAGIC | `insert` | Fila nueva |
# MAGIC | `update_preimage` | La fila **antes** del cambio |
# MAGIC | `update_postimage` | La fila **después** del cambio |
# MAGIC | `delete` | Fila eliminada |
# MAGIC
# MAGIC Más `_commit_version` y `_commit_timestamp` (cuándo ocurrió). Se empieza en la versión 2: antes de eso no había CDF.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _change_type, count(*) AS filas
# MAGIC FROM table_changes('gold.lending_lab.dim_cliente_vigente', 2)
# MAGIC GROUP BY _change_type
# MAGIC ORDER BY _change_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El antes y el después de cada cliente que cambió: pre y post imagen en una sola fila
# MAGIC SELECT a.id_cliente,
# MAGIC        a.nivel_riesgo AS riesgo_antes, b.nivel_riesgo AS riesgo_despues,
# MAGIC        a.score_interno AS score_antes, b.score_interno AS score_despues,
# MAGIC        b._commit_version
# MAGIC FROM table_changes('gold.lending_lab.dim_cliente_vigente', 2) a
# MAGIC JOIN table_changes('gold.lending_lab.dim_cliente_vigente', 2) b
# MAGIC   ON  a.id_cliente = b.id_cliente
# MAGIC   AND a._commit_version = b._commit_version
# MAGIC WHERE a._change_type = 'update_preimage'
# MAGIC   AND b._change_type = 'update_postimage';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. El consumidor incremental
# MAGIC
# MAGIC El CRM no quiere correr SQL a mano: quiere un proceso que **recuerde dónde se quedó**. Es el mismo mecanismo
# MAGIC de Bronze y Silver: leer como *stream* con un **checkpoint**, `trigger(availableNow=True)` y terminar.
# MAGIC Cada ejecución procesa **solo los cambios nuevos**.
# MAGIC
# MAGIC - `readChangeFeed = true` activa la lectura del feed (en vez de la tabla).
# MAGIC - Se descarta `update_preimage`: al CRM le sirve lo que quedó, no lo que había.
# MAGIC - La salida (`cola_sync_crm`) hace de cola de sincronización; en producción sería una llamada a la API del CRM.

# COMMAND ----------

(spark.readStream
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .table("gold.lending_lab.dim_cliente_vigente")
    .filter("_change_type != 'update_preimage'")
    .writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable("gold.lending_lab.cola_sync_crm")
    .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _commit_version, _change_type, count(*) AS filas
# MAGIC FROM gold.lending_lab.cola_sync_crm
# MAGIC GROUP BY _commit_version, _change_type
# MAGIC ORDER BY _commit_version, _change_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Un cambio más (versión 4) y el consumidor vuelve a correr
# MAGIC
# MAGIC Un cliente mejora su riesgo (a `A`, score 800). Aquí hacemos un `UPDATE` directo: el consumidor lo ve igual,
# MAGIC porque CDF registra **cualquier** escritura, no solo los `MERGE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.lending_lab.dim_cliente_vigente
# MAGIC SET nivel_riesgo = 'A', score_interno = 800
# MAGIC WHERE id_cliente = (SELECT id_cliente FROM gold.lending_lab.dim_cliente_vigente ORDER BY id_cliente LIMIT 1 OFFSET 30);

# COMMAND ----------

# Mismo código y mismo checkpoint: solo procesa la versión 4
(spark.readStream
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .table("gold.lending_lab.dim_cliente_vigente")
    .filter("_change_type != 'update_preimage'")
    .writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable("gold.lending_lab.cola_sync_crm")
    .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Debe aparecer la versión 4 con 1 fila; las versiones 2 y 3 no se repiten
# MAGIC SELECT _commit_version, _change_type, count(*) AS filas
# MAGIC FROM gold.lending_lab.cola_sync_crm
# MAGIC GROUP BY _commit_version, _change_type
# MAGIC ORDER BY _commit_version, _change_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuándo usar CDF (y cuándo no)
# MAGIC
# MAGIC | Caso | Sin CDF | Con CDF |
# MAGIC |---|---|---|
# MAGIC | Sincronizar Gold con un sistema externo (CRM, DWH) | Releer toda la tabla | Solo los cambios |
# MAGIC | Refrescar una caché de aplicación | Invalidar todo | Solo las filas modificadas |
# MAGIC | Auditar *quién cambió de qué a qué* | Comparar snapshots | `update_preimage` / `update_postimage` |
# MAGIC | Tabla de referencia pequeña que casi no cambia | — | **No vale la pena**: un `OVERWRITE` completo es más simple |
# MAGIC
# MAGIC **Costo:** el feed ocupa almacenamiento y se limpia con `VACUUM` (sesión 19). Actívalo solo en tablas que
# MAGIC tienen un consumidor incremental real. `CREATE OR REPLACE TABLE` (como en el notebook 01) **no** genera un feed útil:
# MAGIC reemplaza todo y el feed lo reporta como borrar todo e insertar todo. CDF está pensado para `MERGE`, `UPDATE` y `DELETE`.
# MAGIC
# MAGIC ## Resumen: features de Databricks usadas
# MAGIC
# MAGIC `delta.enableChangeDataFeed` · `MERGE … WHEN MATCHED AND …` · `DELETE` · `DESCRIBE HISTORY` · `table_changes()` ·
# MAGIC `_change_type` · `readChangeFeed` + `startingVersion` · checkpoint + `trigger(availableNow=True)`
