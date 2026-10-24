# Databricks notebook source
# MAGIC %md
# MAGIC # Gold paso a paso · 06 · App: métricas de telemetría
# MAGIC
# MAGIC **Escenario:** App (eventos de Kafka) · **Sesión 20** · **Consumidor:** producto y marketing (dashboards de la app).
# MAGIC **Requiere:** `01_lending_modelo_estrella` (se reutiliza `dim_cliente` como dimensión conforme).
# MAGIC
# MAGIC Silver de la App tiene **un evento por fila** (36.000 en 2 semanas) y **una fila por sesión**. Nadie quiere leer eventos:
# MAGIC producto quiere saber **cómo rinde cada pantalla**. Preguntas del negocio:
# MAGIC
# MAGIC - ¿Cuál es el CTR (clics / vistas) de cada ubicación en pantalla?
# MAGIC - ¿Dónde se cae el funnel de la app: vista → clic → simulación → solicitud?
# MAGIC - ¿Convierten distinto los clientes según su nivel de riesgo?
# MAGIC
# MAGIC | Paso | Tabla Gold | Feature de Databricks |
# MAGIC |---|---|---|
# MAGIC | 1 | `metricas_funnel_diario` | `count_if`, `GROUP BY ALL`, `try_divide` |
# MAGIC | 2 | `metricas_sesiones_segmento` | **Dimensión conforme**: se reutiliza `dim_cliente` de Lending |
# MAGIC | 3 | `mv_ctr_ubicacion` (archivo `06b_…sql`) | **Materialized View** + `REFRESH` (en SQL warehouse) |
# MAGIC
# MAGIC > **Origen:** tablas `_lab` de Silver. **Destino:** `gold.app_lab`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold.app_lab
# MAGIC COMMENT 'Gold de práctica de la App (sesión 20): métricas de embudo y de sesiones';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. De dónde partimos
# MAGIC
# MAGIC Cuatro tipos de evento del funnel (`oferta_vista`, `oferta_click`, `simulacion_realizada`, `solicitud_iniciada`) más
# MAGIC `banner_cerrado`. Cada fila trae la **ubicación en pantalla** donde ocurrió.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo_evento, count(*) AS eventos, count(DISTINCT id_cliente) AS clientes, min(fecha_evento) AS desde, max(fecha_evento) AS hasta
# MAGIC FROM silver.app.eventos_app_lab
# MAGIC GROUP BY tipo_evento
# MAGIC ORDER BY eventos DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `metricas_funnel_diario`: pivotear tipos de evento a columnas
# MAGIC
# MAGIC **Grano:** día × canal × ubicación en pantalla.
# MAGIC
# MAGIC El patrón es **convertir una columna de tipo en columnas de conteo** con `count_if`: cada paso del funnel pasa a ser una medida.
# MAGIC
# MAGIC - **Bases aditivas:** `vistas`, `clicks`, `simulaciones`, `solicitudes_iniciadas`, `banners_cerrados`. Se pueden sumar por cualquier dimensión.
# MAGIC - **Semi-aditiva:** `clientes_unicos`. Un mismo cliente puede aparecer en varias ubicaciones: **sumar `clientes_unicos` entre filas lo cuenta dos veces**.
# MAGIC   Para el total real hay que volver a contar `DISTINCT` sobre Silver.
# MAGIC - **Ratios** (`ctr`, tasas de paso): salen de las bases con `try_divide`. No se suman ni se promedian al re-agregar.
# MAGIC - **Sin NULLs en dimensiones:** `coalesce(canal, 'desconocido')`. Una dimensión NULL rompe filtros y slicers de BI.
# MAGIC - `GROUP BY ALL` agrupa por todas las columnas que no son agregaciones: no hay que repetir la lista.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.app_lab.metricas_funnel_diario
# MAGIC COMMENT 'KPI de funnel de la app. Grano: fecha x canal x ubicacion_pantalla. Bases aditivas; clientes_unicos es semi-aditiva'
# MAGIC CLUSTER BY (fecha)
# MAGIC AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     fecha_evento                                  AS fecha,
# MAGIC     coalesce(canal, 'desconocido')                AS canal,
# MAGIC     coalesce(ubicacion_pantalla, 'desconocida')   AS ubicacion_pantalla,
# MAGIC     count_if(tipo_evento = 'oferta_vista')        AS vistas,
# MAGIC     count_if(tipo_evento = 'oferta_click')        AS clicks,
# MAGIC     count_if(tipo_evento = 'simulacion_realizada') AS simulaciones,
# MAGIC     count_if(tipo_evento = 'solicitud_iniciada')  AS solicitudes_iniciadas,
# MAGIC     count_if(tipo_evento = 'banner_cerrado')      AS banners_cerrados,
# MAGIC     count(DISTINCT id_cliente)                    AS clientes_unicos
# MAGIC   FROM silver.app.eventos_app_lab
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   round(try_divide(clicks, vistas), 4)                       AS ctr,
# MAGIC   round(try_divide(simulaciones, clicks), 4)                 AS tasa_click_a_simulacion,
# MAGIC   round(try_divide(solicitudes_iniciadas, simulaciones), 4)  AS tasa_simulacion_a_solicitud
# MAGIC FROM base;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.app_lab.metricas_funnel_diario ORDER BY fecha DESC, canal, ubicacion_pantalla LIMIT 8;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validar: re-agregar bien, y el error de sumar un ratio
# MAGIC
# MAGIC Para el CTR de toda la app se suman las **bases** y se divide. El promedio de los CTR diarios pesa igual un día con
# MAGIC 50 vistas que uno con 5.000, y da otro número.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count_if(tipo_evento = 'oferta_vista') FROM silver.app.eventos_app_lab)  AS vistas_silver,
# MAGIC   sum(vistas)                                                  AS vistas_gold,
# MAGIC   round(try_divide(sum(clicks), sum(vistas)), 4)               AS ctr_BIEN_suma_bases_y_divide,
# MAGIC   round(avg(ctr), 4)                                           AS ctr_MAL_promedio_de_ratios,
# MAGIC   sum(clientes_unicos)                                         AS clientes_MAL_suma_de_distintos,
# MAGIC   (SELECT count(DISTINCT id_cliente) FROM silver.app.eventos_app_lab) AS clientes_BIEN_distinct_real
# MAGIC FROM gold.app_lab.metricas_funnel_diario;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `metricas_sesiones_segmento`: la dimensión conforme
# MAGIC
# MAGIC Producto quiere cortar la app por **segmento de cliente**, pero la app no sabe quién es el cliente: ese dato vive en
# MAGIC Lending. Como en el notebook 01 se publicó `gold.lending_lab.dim_cliente`, la **misma dimensión** sirve a los dos dominios.
# MAGIC Eso es una **dimensión conforme**: un solo lugar donde se define qué es el «nivel de riesgo» de un cliente.
# MAGIC
# MAGIC **Grano:** día × canal × nivel de riesgo. Silver de sesiones trae banderas por sesión (`hizo_click`, `simulo`, `inicio_solicitud`);
# MAGIC `count_if` las convierte en conteos aditivos.
# MAGIC
# MAGIC Se usa la versión **vigente** del cliente (`es_vigente`): la pregunta es *«cómo se comportan hoy los clientes de riesgo E»*.
# MAGIC Un cliente que la app no logra identificar queda en `Sin segmento`, no en NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.app_lab.metricas_sesiones_segmento
# MAGIC COMMENT 'KPI de sesiones de la app por segmento de riesgo del cliente. Grano: fecha x canal x nivel_riesgo'
# MAGIC CLUSTER BY (fecha)
# MAGIC AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     CAST(s.inicio_sesion AS DATE)                    AS fecha,
# MAGIC     coalesce(s.canal, 'desconocido')                 AS canal,
# MAGIC     coalesce(c.nivel_riesgo, 'Sin segmento')         AS nivel_riesgo,
# MAGIC     count(*)                                         AS sesiones,
# MAGIC     count_if(s.hizo_click)                           AS sesiones_con_click,
# MAGIC     count_if(s.simulo)                               AS sesiones_con_simulacion,
# MAGIC     count_if(s.inicio_solicitud)                     AS sesiones_con_solicitud
# MAGIC   FROM silver.app.sesiones_app_lab s
# MAGIC   LEFT JOIN gold.lending_lab.dim_cliente c
# MAGIC     ON c.id_cliente = s.id_cliente AND c.es_vigente
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   round(try_divide(sesiones_con_solicitud, sesiones), 4) AS tasa_sesion_a_solicitud
# MAGIC FROM base;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Convierten distinto los clientes según su riesgo? Se suman las bases por segmento y se recalcula la tasa
# MAGIC SELECT nivel_riesgo,
# MAGIC        sum(sesiones)                                              AS sesiones,
# MAGIC        sum(sesiones_con_solicitud)                                AS con_solicitud,
# MAGIC        round(100 * try_divide(sum(sesiones_con_solicitud), sum(sesiones)), 2) AS pct_sesion_a_solicitud
# MAGIC FROM gold.app_lab.metricas_sesiones_segmento
# MAGIC GROUP BY nivel_riesgo
# MAGIC ORDER BY nivel_riesgo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Materialized View: la métrica que se mantiene sola
# MAGIC
# MAGIC Hasta ahora cada tabla Gold la **refrescamos nosotros** (un job que corre un `INSERT`, `MERGE` o `CREATE OR REPLACE`).
# MAGIC Una **Materialized View** (MV) es una tabla cuyo contenido es el resultado de una consulta y que **Databricks sabe refrescar**:
# MAGIC
# MAGIC - Se escribe solo la consulta (`SELECT`); no hay `MERGE` ni lógica de refresco.
# MAGIC - `REFRESH MATERIALIZED VIEW` la actualiza, **de forma incremental cuando puede** (solo recalcula lo que cambió en la fuente).
# MAGIC - Se puede programar con `SCHEDULE` en la propia definición.
# MAGIC - Dashboards y consultas la leen como una tabla: el cómputo ya está hecho.
# MAGIC
# MAGIC Es ideal para agregados que **se recalculan completos sobre una fuente que crece** (como el CTR por ubicación).
# MAGIC Para lógica con control fino de ventanas e historia (snapshots, `REPLACE WHERE`) siguen siendo mejores las tablas de los notebooks 03 y 04.
# MAGIC
# MAGIC > **Dónde se ejecuta:** una MV se crea y refresca en un **SQL warehouse** (SQL Editor), no en el cómputo de un notebook.
# MAGIC > Por eso esta parte está en el archivo **`06b_app_mv_ctr_ubicacion.sql`**: ábrelo en el *SQL Editor* y ejecútalo ahí.
# MAGIC > Parte de `metricas_funnel_diario` (paso 2), así que corre este notebook primero.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW gold.app_lab.mv_ctr_ubicacion
# MAGIC COMMENT 'CTR y funnel por ubicación en pantalla'
# MAGIC AS
# MAGIC SELECT ubicacion_pantalla,
# MAGIC        sum(vistas) AS vistas, sum(clicks) AS clicks,
# MAGIC        round(try_divide(sum(clicks), sum(vistas)), 4) AS ctr
# MAGIC FROM gold.app_lab.metricas_funnel_diario
# MAGIC GROUP BY ubicacion_pantalla;
# MAGIC
# MAGIC REFRESH MATERIALIZED VIEW gold.app_lab.mv_ctr_ubicacion;   -- cuando la fuente cambie
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen: features de Databricks usadas
# MAGIC
# MAGIC | Feature | Para qué en Gold |
# MAGIC |---|---|
# MAGIC | `count_if` + `GROUP BY ALL` | Pivotear tipos de evento a medidas sin `CASE WHEN` |
# MAGIC | `try_divide` | Ratios que no fallan |
# MAGIC | `coalesce` en dimensiones | BI sin NULLs |
# MAGIC | Dimensión conforme (`dim_cliente`) | Un solo significado de «segmento» entre dominios |
# MAGIC | `MATERIALIZED VIEW` + `REFRESH` | Agregado que Databricks mantiene |
# MAGIC
# MAGIC **Limitación de los datos actuales:** en `sesiones_app` cada sesión tiene un solo evento, por eso no se calcula el *tiempo del flujo*
# MAGIC (de vista a solicitud). Cuando el productor emita varios eventos por sesión se agrega a `metricas_sesiones_segmento` con
# MAGIC `max(ts_evento) - min(ts_evento)`.
