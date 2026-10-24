# Databricks notebook source
# MAGIC %md
# MAGIC # Gold paso a paso · 05 · Cobranzas: de una tabla lenta a una rápida
# MAGIC
# MAGIC **Escenario:** Cobranzas · **Sesión 19** · **Consumidor:** cualquiera que consulte Gold (BI, apps, analistas).
# MAGIC
# MAGIC Una tabla Gold que tarda minutos no está terminada. El problema casi siempre es el mismo: miles de **archivos
# MAGIC pequeños** (cada carga crea archivos nuevos) y datos **desordenados** respecto de lo que la gente filtra.
# MAGIC
# MAGIC | Paso | Qué haces | Feature |
# MAGIC |---|---|---|
# MAGIC | 1 | Fabricar el problema: 20 cargas incrementales | `INSERT INTO` en bucle |
# MAGIC | 2 | Medir: archivos y tiempo | `DESCRIBE DETAIL`, `_metadata.file_path` |
# MAGIC | 3 | Compactar | `OPTIMIZE` |
# MAGIC | 4 | Co-localizar por la columna de filtro | **Liquid Clustering**: `CLUSTER BY` + `OPTIMIZE` |
# MAGIC | 5 | Cambiar el criterio sin reescribir | `ALTER TABLE … CLUSTER BY` (y `AUTO`) |
# MAGIC | 6 | Estadísticas para el optimizador | `ANALYZE TABLE … COMPUTE STATISTICS` |
# MAGIC | 7 | Liberar almacenamiento | `VACUUM` |
# MAGIC
# MAGIC **La tabla:** `fact_cuotas`, una fila por cuota de crédito (~560.000 filas), desde `silver.cobranzas.cuotas_lab`.
# MAGIC **La consulta típica de cobranza:** *cuotas vencidas de un trimestre*, filtrando por `fecha_vencimiento` y `estado_cuota`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold.cobranzas_lab
# MAGIC COMMENT 'Gold de práctica de Cobranzas (sesiones 18 a 20): tabla ancha de cartera, snapshots y optimización';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fabricar el problema
# MAGIC
# MAGIC Se crean **dos tablas idénticas en columnas**, cargadas con el mismo proceso: 20 `INSERT` separados, como 20 días de carga incremental.
# MAGIC Cada `INSERT` agrega archivos nuevos y no toca los anteriores.
# MAGIC
# MAGIC | Tabla | Diseño |
# MAGIC |---|---|
# MAGIC | `fact_cuotas_lenta` | Sin nada especial (el punto de partida típico) |
# MAGIC | `fact_cuotas_lc` | Con **`CLUSTER BY (fecha_vencimiento, estado_cuota)`**: las columnas por las que se filtra |
# MAGIC
# MAGIC `delta.targetFileSize = 1mb` solo existe **para este laboratorio**: la tabla es chica (unos 6 MB) y con el tamaño
# MAGIC por defecto `OPTIMIZE` la dejaría en un solo archivo, y no se vería el efecto del clustering. Con 1 MB
# MAGIC la tabla se comporta como una de cientos de millones de filas. **En producción no se toca.**
# MAGIC
# MAGIC Las cargas reparten las filas con `pmod(id_cuota, 20)`: simula que las cuotas llegan **sin orden por fecha**, que es lo habitual.

# COMMAND ----------

# Empezar de cero (el notebook se puede repetir)
spark.sql("DROP TABLE IF EXISTS gold.cobranzas_lab.fact_cuotas_lenta")
spark.sql("DROP TABLE IF EXISTS gold.cobranzas_lab.fact_cuotas_lc")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.cobranzas_lab.fact_cuotas_lenta
# MAGIC COMMENT 'Hecho de cuotas SIN optimizar: punto de partida del laboratorio'
# MAGIC AS
# MAGIC SELECT id_cuota, numero_credito, numero_cuota, fecha_vencimiento, estado_cuota, monto_cuota, monto_capital, monto_interes
# MAGIC FROM silver.cobranzas.cuotas_lab
# MAGIC WHERE 1 = 0;   -- solo la estructura; los datos entran en el bucle

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.cobranzas_lab.fact_cuotas_lc
# MAGIC COMMENT 'Hecho de cuotas con Liquid Clustering por las columnas de filtro'
# MAGIC CLUSTER BY (fecha_vencimiento, estado_cuota)
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '1mb')   -- solo laboratorio
# MAGIC AS
# MAGIC SELECT id_cuota, numero_credito, numero_cuota, fecha_vencimiento, estado_cuota, monto_cuota, monto_capital, monto_interes
# MAGIC FROM silver.cobranzas.cuotas_lab
# MAGIC WHERE 1 = 0;

# COMMAND ----------

# 20 cargas incrementales en cada tabla
for i in range(20):
    for tabla in ["fact_cuotas_lenta", "fact_cuotas_lc"]:
        spark.sql(f"""
            INSERT INTO gold.cobranzas_lab.{tabla}
            SELECT id_cuota, numero_credito, numero_cuota, fecha_vencimiento, estado_cuota, monto_cuota, monto_capital, monto_interes
            FROM silver.cobranzas.cuotas_lab
            WHERE pmod(id_cuota, 20) = {i}
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Medir el estado inicial
# MAGIC
# MAGIC Una optimización **se mide**, no se supone. Tres números:
# MAGIC
# MAGIC 1. **`numFiles`** de `DESCRIBE DETAIL`: cuántos archivos tiene la tabla.
# MAGIC 2. **Archivos con filas del filtro**: cuántos archivos contienen datos de la consulta. Es lo mínimo que hay que leer;
# MAGIC    si el dato está disperso, son casi todos. `_metadata.file_path` es la columna oculta con el archivo de cada fila.
# MAGIC 3. **Tiempo** de la consulta (indicativo: en una tabla chica y un cómputo compartido varía; los archivos son lo concluyente).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mira numFiles (cuántos archivos), sizeInBytes y clusteringColumns
# MAGIC DESCRIBE DETAIL gold.cobranzas_lab.fact_cuotas_lenta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- La consulta de cobranza: cuotas vencidas del primer trimestre de 2026
# MAGIC SELECT count(DISTINCT _metadata.file_path) AS archivos_con_filas_del_filtro,
# MAGIC        count(*) AS cuotas, round(sum(monto_cuota)) AS monto
# MAGIC FROM gold.cobranzas_lab.fact_cuotas_lenta
# MAGIC WHERE fecha_vencimiento BETWEEN '2026-01-01' AND '2026-03-31'
# MAGIC   AND estado_cuota = 'Vencida';

# COMMAND ----------

import time

consulta = """
    SELECT estado_cuota, count(*) AS cuotas, round(sum(monto_cuota)) AS monto
    FROM {tabla}
    WHERE fecha_vencimiento BETWEEN '2026-01-01' AND '2026-03-31' AND estado_cuota = 'Vencida'
    GROUP BY estado_cuota
"""

tiempos = []
for _ in range(3):
    t0 = time.time()
    spark.sql(consulta.format(tabla="gold.cobranzas_lab.fact_cuotas_lenta")).collect()
    tiempos.append(time.time() - t0)
print(f"fact_cuotas_lenta (20 archivos): mejor de 3 corridas = {min(tiempos):.2f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `OPTIMIZE`: compactar
# MAGIC
# MAGIC `OPTIMIZE` reescribe muchos archivos pequeños en pocos archivos del tamaño ideal. No cambia los datos: es una
# MAGIC transacción nueva en el log (los archivos viejos quedan marcados como eliminados).
# MAGIC El resultado trae las métricas: `numFilesRemoved` mucho mayor que `numFilesAdded` = compactación exitosa.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.cobranzas_lab.fact_cuotas_lenta;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL gold.cobranzas_lab.fact_cuotas_lenta;

# COMMAND ----------

# MAGIC %md
# MAGIC Pasaron de 20 archivos a 1: menos *overhead* (abrir archivos, leer pies de página). Pero ojo: **ahora la consulta
# MAGIC lee el único archivo completo** (~6 MB) para quedarse con unas pocas filas. Compactar arregla el tamaño de los
# MAGIC archivos, no el orden de los datos. Eso lo hace el clustering.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Liquid Clustering: ordenar por lo que se filtra
# MAGIC
# MAGIC `fact_cuotas_lc` se creó con `CLUSTER BY (fecha_vencimiento, estado_cuota)`. Hoy, antes de `OPTIMIZE`, está igual de
# MAGIC desordenada que la primera. Cada archivo guarda en su pie de página el **mínimo y máximo de cada columna**
# MAGIC (estadísticas); con los datos ordenados, la mayoría de archivos queda **descartada por completo** (*data skipping*).
# MAGIC
# MAGIC **`OPTIMIZE` aplica el clustering.** Es el mismo comando; Databricks lee el `CLUSTER BY` de la tabla.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANTES de OPTIMIZE: ~20 archivos, con las fechas dispersas en todos
# MAGIC SELECT count(DISTINCT _metadata.file_path) AS archivos_con_filas_del_filtro
# MAGIC FROM gold.cobranzas_lab.fact_cuotas_lc
# MAGIC WHERE fecha_vencimiento BETWEEN '2026-01-01' AND '2026-03-31' AND estado_cuota = 'Vencida';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL gold.cobranzas_lab.fact_cuotas_lc;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.cobranzas_lab.fact_cuotas_lc;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL gold.cobranzas_lab.fact_cuotas_lc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESPUÉS de OPTIMIZE: las filas del filtro están concentradas en pocos archivos de los totales.
# MAGIC -- (Referencia de la corrida de validación: 20 de 20 archivos antes; 1 de 11 después. Las mismas 4.667 cuotas)
# MAGIC SELECT count(DISTINCT _metadata.file_path) AS archivos_con_filas_del_filtro
# MAGIC FROM gold.cobranzas_lab.fact_cuotas_lc
# MAGIC WHERE fecha_vencimiento BETWEEN '2026-01-01' AND '2026-03-31' AND estado_cuota = 'Vencida';

# COMMAND ----------

tiempos = []
for _ in range(3):
    t0 = time.time()
    spark.sql(consulta.format(tabla="gold.cobranzas_lab.fact_cuotas_lc")).collect()
    tiempos.append(time.time() - t0)
print(f"fact_cuotas_lc (clustered): mejor de 3 corridas = {min(tiempos):.2f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC **Dónde ver el *data skipping* de verdad:** ejecuta la consulta de arriba y abre el **Query Profile** (en
# MAGIC *Historial de consultas*, o en el icono del resultado). En el nodo de *scan* de la tabla aparecen
# MAGIC **«Files read»** y **«Files pruned»**: con clustering, casi todos los archivos salen como *pruned*.
# MAGIC
# MAGIC ### ¿Por qué Liquid Clustering reemplazó a `ZORDER` y al particionado?
# MAGIC
# MAGIC | | `PARTITION BY` / `ZORDER` (legacy) | **Liquid Clustering** |
# MAGIC |---|---|---|
# MAGIC | Se define | En cada `OPTIMIZE … ZORDER BY` / al crear la tabla | **Una vez**, en `CLUSTER BY` |
# MAGIC | Cada `OPTIMIZE` | Reordena **toda** la tabla | **Incremental**: solo archivos nuevos o sin ordenar |
# MAGIC | Cambiar el criterio | Reescribir la tabla | `ALTER TABLE … CLUSTER BY` |
# MAGIC | Columnas de baja/alta cardinalidad | Particionar mal es un error caro | Sin problema |
# MAGIC | Streaming y `MERGE` | Incómodo | Compatible |
# MAGIC
# MAGIC `ZORDER` solo se explica para **reconocerlo en código heredado** (`OPTIMIZE tabla ZORDER BY (col)`). En tablas nuevas: `CLUSTER BY`.
# MAGIC
# MAGIC **Qué columnas elegir:** las que más aparecen en `WHERE` y `JOIN ON` (máximo 4).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cambiar el criterio sin reescribir la tabla
# MAGIC
# MAGIC Cobranza ahora también consulta por crédito. Cambiar las columnas de clustering es un `ALTER`: **no reescribe nada**; el nuevo criterio
# MAGIC se aplica en el siguiente `OPTIMIZE`, y los archivos ya ordenados se reorganizan solo cuando conviene.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gold.cobranzas_lab.fact_cuotas_lc CLUSTER BY (numero_credito, fecha_vencimiento);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL gold.cobranzas_lab.fact_cuotas_lc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### `CLUSTER BY AUTO`: que Databricks elija
# MAGIC
# MAGIC Con **Predictive Optimization** activo en el catálogo, `CLUSTER BY AUTO` deja que Databricks escoja las columnas
# MAGIC según las consultas que realmente se ejecutan en la tabla, y corra `OPTIMIZE` por su cuenta. Es lo recomendable cuando no se conocen aún
# MAGIC los patrones de consulta. Para volver a un criterio manual: `ALTER TABLE … CLUSTER BY (col1, col2)`; para quitarlo: `CLUSTER BY NONE`.
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE gold.cobranzas_lab.fact_cuotas_lc CLUSTER BY AUTO;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `ANALYZE TABLE`: estadísticas para el optimizador
# MAGIC
# MAGIC Las **estadísticas de archivo** (mínimo/máximo, usadas para *data skipping*) se calculan solas en cada escritura.
# MAGIC Las **estadísticas de columna** del optimizador (valores distintos, nulos, tamaño) **no**: se piden con `ANALYZE`.
# MAGIC Con ellas, Spark decide mejor el orden de los `JOIN` y cuándo hacer un `broadcast` de una dimensión chica.
# MAGIC Se vuelve a correr cuando la distribución cambia mucho (un año nuevo, una fuente nueva).

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE gold.cobranzas_lab.fact_cuotas_lc
# MAGIC COMPUTE STATISTICS FOR COLUMNS fecha_vencimiento, estado_cuota, numero_credito, monto_cuota;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Estadísticas de una columna: mínimo, máximo, nulos, valores distintos
# MAGIC DESCRIBE EXTENDED gold.cobranzas_lab.fact_cuotas_lc estado_cuota;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `VACUUM`: liberar almacenamiento
# MAGIC
# MAGIC `OPTIMIZE` **no borra** los archivos viejos: quedan en el almacenamiento para el *Time Travel* y para que las
# MAGIC consultas que estaban corriendo terminen. `VACUUM` borra físicamente los archivos que ya no pertenecen a ninguna versión
# MAGIC dentro del período de retención (por defecto **7 días**).
# MAGIC
# MAGIC - `DRY RUN` lista qué borraría, sin borrar nada.
# MAGIC - Como la compactación se hizo hace segundos, con la retención de 7 días **todavía no hay nada que borrar**: es lo correcto.
# MAGIC - **Nunca `RETAIN 0 HOURS` en producción:** elimina el historial y el Time Travel deja de funcionar.
# MAGIC - Orden correcto: cargar → `OPTIMIZE` → `ANALYZE` → (semanal) `VACUUM`. Nunca `VACUUM` antes de `OPTIMIZE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM gold.cobranzas_lab.fact_cuotas_lenta DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM gold.cobranzas_lab.fact_cuotas_lenta;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resultado: antes y después
# MAGIC
# MAGIC La historia de la tabla cuenta lo que pasó: 20 `INSERT` (`WRITE`) y luego el `OPTIMIZE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationMetrics['numRemovedFiles'] AS archivos_removidos, operationMetrics['numAddedFiles'] AS archivos_agregados
# MAGIC FROM (DESCRIBE HISTORY gold.cobranzas_lab.fact_cuotas_lc)
# MAGIC WHERE operation IN ('OPTIMIZE', 'CLUSTER BY')
# MAGIC ORDER BY version;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen: el job de optimización de Gold
# MAGIC
# MAGIC | Orden | Comando | Frecuencia |
# MAGIC |---|---|---|
# MAGIC | 1 | Cargar (`MERGE`, `INSERT … REPLACE WHERE`) | Cada carga |
# MAGIC | 2 | `OPTIMIZE tabla` (aplica el `CLUSTER BY`) | Después de cada carga, o diario |
# MAGIC | 3 | `ANALYZE TABLE tabla COMPUTE STATISTICS FOR COLUMNS …` | Cuando cambia la distribución |
# MAGIC | 4 | `VACUUM tabla` | Semanal, retención ≥ 7 días |
# MAGIC
# MAGIC | Feature de Databricks | Para qué |
# MAGIC |---|---|
# MAGIC | `DESCRIBE DETAIL` / `DESCRIBE HISTORY` | Medir archivos y ver qué operaciones se hicieron |
# MAGIC | `OPTIMIZE` | Compactar y aplicar clustering |
# MAGIC | `CLUSTER BY` / `ALTER TABLE … CLUSTER BY` / `AUTO` | Liquid Clustering |
# MAGIC | `ANALYZE TABLE` | Estadísticas de columna |
# MAGIC | `VACUUM` | Borrar archivos obsoletos |
# MAGIC | `ZORDER` | Legacy: solo reconocerlo |
