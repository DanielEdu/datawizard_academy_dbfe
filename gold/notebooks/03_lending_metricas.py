# Databricks notebook source
# MAGIC %md
# MAGIC # Gold paso a paso · 03 · Lending: capa de métricas
# MAGIC
# MAGIC **Escenario:** Lending · **Sesión 20** · **Consumidor:** dashboards, Genie y la gerencia comercial.
# MAGIC **Requiere:** haber corrido `01_lending_modelo_estrella` (usa sus hechos y dimensiones).
# MAGIC
# MAGIC Las preguntas de negocio de Lending, ya respondidas y materializadas:
# MAGIC
# MAGIC | Pregunta | Tabla de métricas |
# MAGIC |---|---|
# MAGIC | ¿Dónde se cae el funnel? ¿Qué motor de asignación convierte mejor? | `metricas_originacion_mensual` |
# MAGIC | ¿Cuánto colocamos, consolidado en USD? | `metricas_colocacion_mensual` |
# MAGIC | ¿Qué campaña rinde más por dólar invertido? | `metricas_roi_campania` |
# MAGIC | La misma métrica, definida una sola vez para todos | `mv_colocacion` (Metric View) |
# MAGIC
# MAGIC **La regla de la capa de métricas:** las **bases** (conteos, sumas) son *aditivas* y se guardan;
# MAGIC los **ratios** (conversión, ticket promedio) *no se suman* y siempre se recalculan desde las bases.
# MAGIC
# MAGIC | Feature de Databricks | Dónde |
# MAGIC |---|---|
# MAGIC | `CREATE VIEW` como definición única del KPI | Pasos 1 y 2 |
# MAGIC | `try_divide`, `count_if` | Ratios sin errores de división por cero |
# MAGIC | `INSERT INTO … REPLACE WHERE` | Refresco incremental de una tabla (paso 1) |
# MAGIC | `MERGE … WHEN NOT MATCHED BY SOURCE` | Refresco incremental con borrado (paso 2) |
# MAGIC | `LEFT JOIN LATERAL` | Tipo de cambio de la fecha de inicio (paso 3) |
# MAGIC | **Metric View** + `MEASURE()` | Definir el KPI una vez y consultarlo con cualquier corte (paso 4) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `metricas_originacion_mensual`: el funnel por mes, país y motor
# MAGIC
# MAGIC **Grano:** mes de la oferta × país × motor de asignación. Funnel: oferta → solicitud → aprobada → desembolsada.
# MAGIC
# MAGIC ### 1.1 Primero, la definición del KPI como vista
# MAGIC
# MAGIC La lógica se escribe **una sola vez** en una vista. La tabla se construye de ella (carga completa) y se
# MAGIC refresca de ella (carga incremental): así la definición no se duplica ni diverge.
# MAGIC
# MAGIC - Las **bases** son aditivas: `ofertas`, `solicitudes`, `desembolsos`, `monto_colocado_usd`.
# MAGIC - Los **ratios** salen de las bases del mismo grano con `try_divide` (devuelve NULL en vez de fallar si el divisor es 0).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.lending_lab.v_originacion_mensual AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     trunc(t.fecha, 'MM')                                  AS mes,
# MAGIC     o.id_pais,
# MAGIC     o.motor_asignacion,
# MAGIC     count(*)                                              AS ofertas,
# MAGIC     sum(o.es_aceptada)                                    AS ofertas_aceptadas,
# MAGIC     count(s.id_solicitud)                                 AS solicitudes,
# MAGIC     coalesce(sum(s.es_aprobada), 0)                       AS solicitudes_aprobadas,
# MAGIC     count_if(d.es_completado = 1)                         AS desembolsos,
# MAGIC     coalesce(sum(CASE WHEN d.es_completado = 1 THEN d.monto_usd END), 0) AS monto_colocado_usd
# MAGIC   FROM gold.lending_lab.fact_ofertas o
# MAGIC   JOIN gold.lending_lab.dim_tiempo t            ON t.id_fecha = o.id_fecha
# MAGIC   LEFT JOIN gold.lending_lab.fact_solicitudes s ON s.id_oferta = o.id_oferta
# MAGIC   LEFT JOIN gold.lending_lab.fact_desembolsos d ON d.id_solicitud = s.id_solicitud
# MAGIC   GROUP BY trunc(t.fecha, 'MM'), o.id_pais, o.motor_asignacion
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   round(try_divide(solicitudes, ofertas), 4)             AS tasa_oferta_a_solicitud,
# MAGIC   round(try_divide(solicitudes_aprobadas, solicitudes), 4) AS tasa_aprobacion,
# MAGIC   round(try_divide(desembolsos, ofertas), 4)             AS conversion_end_to_end,
# MAGIC   round(try_divide(monto_colocado_usd, desembolsos), 2)  AS ticket_promedio_usd
# MAGIC FROM base;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Materializar: carga completa
# MAGIC
# MAGIC Una consulta de varios joins sobre tres hechos es cara de repetir en cada refresh de un dashboard.
# MAGIC Se **materializa** en una tabla pequeña que el dashboard solo lee. `CLUSTER BY` sobre las columnas por las que se filtra.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.metricas_originacion_mensual
# MAGIC COMMENT 'KPI de originación: funnel oferta → solicitud → desembolso por mes, país y motor. Grano: mes x pais x motor'
# MAGIC CLUSTER BY (mes, id_pais)
# MAGIC AS
# MAGIC SELECT * FROM gold.lending_lab.v_originacion_mensual;
# MAGIC
# MAGIC SELECT * FROM gold.lending_lab.metricas_originacion_mensual ORDER BY mes DESC, id_pais, motor_asignacion LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Refresco incremental con `INSERT INTO … REPLACE WHERE`
# MAGIC
# MAGIC Los meses cerrados no cambian; los **últimos dos** sí (una solicitud abierta se aprueba, un desembolso se revierte).
# MAGIC En vez de recalcular todo, se reemplazan **solo esos meses**:
# MAGIC
# MAGIC - `REPLACE WHERE <condición>` borra las filas que cumplen la condición e inserta las nuevas, **en una sola transacción**.
# MAGIC - Es **idempotente**: correrlo dos veces deja el mismo resultado (un `INSERT INTO` normal duplicaría las filas).
# MAGIC - Las filas que cumplen la condición pero ya no existen en la fuente también desaparecen.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.lending_lab.metricas_originacion_mensual
# MAGIC REPLACE WHERE mes >= add_months(trunc(current_date(), 'MM'), -2)
# MAGIC SELECT * FROM gold.lending_lab.v_originacion_mensual
# MAGIC WHERE mes >= add_months(trunc(current_date(), 'MM'), -2);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- La operación reemplazó solo los meses recientes: borró (numDeletedRows) y volvió a escribir esas filas
# MAGIC SELECT version, operation, operationParameters['predicate'] AS predicado,
# MAGIC        operationMetrics['numDeletedRows'] AS filas_reemplazadas,
# MAGIC        operationMetrics['numCopiedRows']  AS filas_intactas_copiadas
# MAGIC FROM (DESCRIBE HISTORY gold.lending_lab.metricas_originacion_mensual)
# MAGIC ORDER BY version;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Responder las preguntas
# MAGIC
# MAGIC **Qué motor convierte mejor.** Se suman las **bases** de cada motor y se recalcula el ratio. No se promedian los ratios mensuales.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT motor_asignacion,
# MAGIC        sum(ofertas)                                           AS ofertas,
# MAGIC        sum(solicitudes)                                       AS solicitudes,
# MAGIC        sum(desembolsos)                                       AS desembolsos,
# MAGIC        round(100 * try_divide(sum(solicitudes), sum(ofertas)), 1)  AS pct_oferta_a_solicitud,
# MAGIC        round(100 * try_divide(sum(desembolsos), sum(ofertas)), 1)  AS pct_end_to_end,
# MAGIC        round(sum(monto_colocado_usd))                         AS monto_colocado_usd
# MAGIC FROM gold.lending_lab.metricas_originacion_mensual
# MAGIC GROUP BY motor_asignacion
# MAGIC ORDER BY motor_asignacion;

# COMMAND ----------

# MAGIC %md
# MAGIC **Dónde se cae el funnel.** Un `UNION` de los pasos pivotea las columnas de la tabla en filas: listo para un gráfico de embudo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT paso, total, round(100 * total / first_value(total) OVER (ORDER BY orden), 1) AS pct_de_ofertas
# MAGIC FROM (
# MAGIC   SELECT 1 AS orden, '1. Ofertas asignadas'   AS paso, sum(ofertas)               AS total FROM gold.lending_lab.metricas_originacion_mensual
# MAGIC   UNION ALL SELECT 2, '2. Solicitudes',        sum(solicitudes)           FROM gold.lending_lab.metricas_originacion_mensual
# MAGIC   UNION ALL SELECT 3, '3. Solicitudes aprobadas', sum(solicitudes_aprobadas) FROM gold.lending_lab.metricas_originacion_mensual
# MAGIC   UNION ALL SELECT 4, '4. Desembolsos completados', sum(desembolsos)         FROM gold.lending_lab.metricas_originacion_mensual
# MAGIC )
# MAGIC ORDER BY orden;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `metricas_colocacion_mensual`: monto colocado en USD
# MAGIC
# MAGIC **Grano:** mes × país. Sale de `fact_desembolsos` (solo `Completado`).
# MAGIC
# MAGIC Otra técnica de refresco: **`MERGE`**. Es la opción cuando la tabla es más grande o cuando necesitas
# MAGIC control por fila. `WHEN NOT MATCHED BY SOURCE` borra las filas del rango recalculado que ya no existen en la fuente
# MAGIC (por ejemplo, un país/mes cuyos desembolsos fueron todos reversados).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.lending_lab.v_colocacion_mensual AS
# MAGIC SELECT
# MAGIC   trunc(t.fecha, 'MM')                                        AS mes,
# MAGIC   d.id_pais,
# MAGIC   count(*)                                                    AS desembolsos,
# MAGIC   round(sum(d.monto_usd), 2)                                  AS monto_colocado_usd,
# MAGIC   round(sum(d.comision_usd), 2)                               AS comision_usd,
# MAGIC   round(try_divide(sum(d.monto_usd), count(*)), 2)            AS ticket_promedio_usd,
# MAGIC   round(try_divide(sum(d.tasa_aplicada * d.monto_usd), sum(d.monto_usd)), 3) AS tasa_promedio_ponderada
# MAGIC FROM gold.lending_lab.fact_desembolsos d
# MAGIC JOIN gold.lending_lab.dim_tiempo t ON t.id_fecha = d.id_fecha
# MAGIC WHERE d.es_completado = 1
# MAGIC GROUP BY trunc(t.fecha, 'MM'), d.id_pais;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.metricas_colocacion_mensual
# MAGIC COMMENT 'KPI de colocación consolidada en USD. Grano: mes x pais'
# MAGIC CLUSTER BY (mes)
# MAGIC AS
# MAGIC SELECT * FROM gold.lending_lab.v_colocacion_mensual;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Refresco incremental: recalcula los últimos 2 meses y los aplica con MERGE
# MAGIC MERGE INTO gold.lending_lab.metricas_colocacion_mensual AS t
# MAGIC USING (
# MAGIC   SELECT * FROM gold.lending_lab.v_colocacion_mensual
# MAGIC   WHERE mes >= add_months(trunc(current_date(), 'MM'), -2)
# MAGIC ) AS s
# MAGIC ON t.mes = s.mes AND t.id_pais = s.id_pais
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE AND t.mes >= add_months(trunc(current_date(), 'MM'), -2) THEN DELETE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Colocado en USD por país (consolidado): se suman las bases y se recalcula el ticket
# MAGIC SELECT p.pais,
# MAGIC        sum(m.desembolsos)                                   AS desembolsos,
# MAGIC        round(sum(m.monto_colocado_usd))                     AS monto_colocado_usd,
# MAGIC        round(try_divide(sum(m.monto_colocado_usd), sum(m.desembolsos)), 2) AS ticket_promedio_usd
# MAGIC FROM gold.lending_lab.metricas_colocacion_mensual m
# MAGIC JOIN gold.lending_lab.dim_pais p ON p.id_pais = m.id_pais
# MAGIC GROUP BY p.pais
# MAGIC ORDER BY monto_colocado_usd DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `metricas_roi_campania`: cuánto se coloca por cada dólar invertido
# MAGIC
# MAGIC **Grano:** una fila por campaña. Une dos mundos: lo que **cuesta** la campaña (`dim_campania`) con lo que **colocaron**
# MAGIC los clientes que captó (`id_campania_captacion` de `dim_cliente`).
# MAGIC
# MAGIC El presupuesto está en moneda local. Se lleva a USD con el tipo de cambio **vigente el día de inicio** de la campaña
# MAGIC (`LEFT JOIN LATERAL`, igual que en el notebook 01).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.metricas_roi_campania
# MAGIC COMMENT 'KPI de retorno por campaña: USD colocado por USD invertido. Grano: campaña'
# MAGIC AS
# MAGIC WITH colocado AS (
# MAGIC   SELECT c.id_campania_captacion AS id_campania,
# MAGIC          count(DISTINCT d.id_cliente) AS clientes_con_desembolso,
# MAGIC          count(*)                     AS desembolsos,
# MAGIC          sum(d.monto_usd)             AS monto_colocado_usd
# MAGIC   FROM gold.lending_lab.fact_desembolsos d
# MAGIC   JOIN gold.lending_lab.dim_cliente c ON c.id_cliente = d.id_cliente AND c.es_vigente
# MAGIC   WHERE d.es_completado = 1
# MAGIC   GROUP BY c.id_campania_captacion
# MAGIC )
# MAGIC SELECT
# MAGIC   ca.id_campania,
# MAGIC   ca.nombre_campania,
# MAGIC   ca.tipo_campania,
# MAGIC   p.pais,
# MAGIC   round(ca.presupuesto_local / tc.tasa_venta, 2)                AS presupuesto_usd,
# MAGIC   coalesce(co.clientes_con_desembolso, 0)                       AS clientes_con_desembolso,
# MAGIC   coalesce(co.desembolsos, 0)                                   AS desembolsos,
# MAGIC   round(coalesce(co.monto_colocado_usd, 0), 2)                  AS monto_colocado_usd,
# MAGIC   round(try_divide(co.monto_colocado_usd, ca.presupuesto_local / tc.tasa_venta), 2) AS usd_colocado_por_usd_invertido,
# MAGIC   rank() OVER (ORDER BY try_divide(co.monto_colocado_usd, ca.presupuesto_local / tc.tasa_venta) DESC) AS ranking_roi
# MAGIC FROM gold.lending_lab.dim_campania ca
# MAGIC JOIN gold.lending_lab.dim_pais p ON p.id_pais = ca.id_pais
# MAGIC LEFT JOIN colocado co ON co.id_campania = ca.id_campania
# MAGIC LEFT JOIN LATERAL (
# MAGIC   SELECT t.tasa_venta
# MAGIC   FROM silver.lending.tipos_cambio_lab t
# MAGIC   WHERE t.moneda_origen = p.moneda AND t.moneda_destino = 'USD' AND t.fecha <= ca.fecha_inicio
# MAGIC   ORDER BY t.fecha DESC LIMIT 1
# MAGIC ) tc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ranking_roi, nombre_campania, tipo_campania, pais, presupuesto_usd, monto_colocado_usd, usd_colocado_por_usd_invertido
# MAGIC FROM gold.lending_lab.metricas_roi_campania
# MAGIC ORDER BY ranking_roi
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Metric View: el KPI se define una vez y se consulta con cualquier corte
# MAGIC
# MAGIC Las tablas de arriba responden las preguntas **que ya pensamos**. ¿Y la que llega mañana («colocación por ciudad y
# MAGIC nivel de riesgo»)? Una **Metric View** de Unity Catalog separa:
# MAGIC
# MAGIC - las **dimensiones** (cómo se corta): mes, país, ciudad…
# MAGIC - las **medidas** (qué se calcula): monto colocado, ticket promedio, % de fallidos…
# MAGIC
# MAGIC La definición vive **una sola vez**, en YAML, dentro de Unity Catalog. Quien consulta elige el corte con
# MAGIC `GROUP BY` y las medidas con `MEASURE()`: Databricks recalcula **bien** cada ratio para ese corte
# MAGIC (suma las bases y divide al final). Dashboards, Genie y SQL ven el mismo número.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.lending_lab.mv_colocacion
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 1.1
# MAGIC comment: "KPIs de colocación de Wizard Bank. Una sola definición para dashboards y Genie"
# MAGIC source: gold.lending_lab.fact_desembolsos
# MAGIC joins:
# MAGIC   - name: dim_pais
# MAGIC     source: gold.lending_lab.dim_pais
# MAGIC     'on': source.id_pais = dim_pais.id_pais
# MAGIC   - name: dim_tiempo
# MAGIC     source: gold.lending_lab.dim_tiempo
# MAGIC     'on': source.id_fecha = dim_tiempo.id_fecha
# MAGIC   - name: dim_cliente
# MAGIC     source: gold.lending_lab.dim_cliente
# MAGIC     'on': source.sk_cliente = dim_cliente.sk_cliente
# MAGIC dimensions:
# MAGIC   - name: mes
# MAGIC     expr: dim_tiempo.anio_mes
# MAGIC   - name: pais
# MAGIC     expr: dim_pais.pais
# MAGIC   - name: nivel_riesgo
# MAGIC     expr: dim_cliente.nivel_riesgo
# MAGIC   - name: ciudad
# MAGIC     expr: dim_cliente.ciudad
# MAGIC measures:
# MAGIC   - name: desembolsos
# MAGIC     expr: COUNT(1) FILTER (WHERE source.es_completado = 1)
# MAGIC   - name: monto_colocado_usd
# MAGIC     expr: SUM(source.monto_usd) FILTER (WHERE source.es_completado = 1)
# MAGIC   - name: ticket_promedio_usd
# MAGIC     expr: MEASURE(monto_colocado_usd) / MEASURE(desembolsos)
# MAGIC   - name: pct_fallidos
# MAGIC     expr: COUNT(1) FILTER (WHERE source.estado_desembolso = 'Fallido') / COUNT(1)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Corte 1: por país
# MAGIC SELECT pais,
# MAGIC        MEASURE(desembolsos)         AS desembolsos,
# MAGIC        MEASURE(monto_colocado_usd)  AS monto_colocado_usd,
# MAGIC        MEASURE(ticket_promedio_usd) AS ticket_promedio_usd,
# MAGIC        MEASURE(pct_fallidos)        AS pct_fallidos
# MAGIC FROM gold.lending_lab.mv_colocacion
# MAGIC GROUP BY pais
# MAGIC ORDER BY monto_colocado_usd DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Corte 2, que nadie había previsto: por nivel de riesgo del cliente AL desembolsar. Mismas medidas, sin tocar la definición
# MAGIC SELECT nivel_riesgo,
# MAGIC        MEASURE(desembolsos)         AS desembolsos,
# MAGIC        MEASURE(monto_colocado_usd)  AS monto_colocado_usd,
# MAGIC        MEASURE(ticket_promedio_usd) AS ticket_promedio_usd,
# MAGIC        MEASURE(pct_fallidos)        AS pct_fallidos
# MAGIC FROM gold.lending_lab.mv_colocacion
# MAGIC GROUP BY nivel_riesgo
# MAGIC ORDER BY nivel_riesgo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### El error que la Metric View evita: promediar promedios
# MAGIC
# MAGIC El ticket promedio **no es aditivo**. Si lo calculas como el promedio de los tickets mensuales, cada mes pesa igual
# MAGIC aunque tenga muy pocos desembolsos, y el resultado se desvía del real. Compara las dos columnas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   round(avg(ticket_promedio_usd), 2)                                  AS ticket_MAL_promedio_de_promedios,
# MAGIC   round(try_divide(sum(monto_colocado_usd), sum(desembolsos)), 2)     AS ticket_BIEN_suma_bases_y_divide
# MAGIC FROM gold.lending_lab.metricas_colocacion_mensual;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen: la capa de métricas
# MAGIC
# MAGIC | Decisión | Qué hiciste |
# MAGIC |---|---|
# MAGIC | Definir el KPI una vez | Vistas (`v_…`) y Metric View (`mv_colocacion`) |
# MAGIC | Bases aditivas, ratios recalculados | `try_divide` desde las bases; al re-agregar, `SUM` de bases |
# MAGIC | Materializar lo caro | `CREATE OR REPLACE TABLE … AS SELECT` + `CLUSTER BY` |
# MAGIC | Refrescar solo lo que cambia | `INSERT … REPLACE WHERE` (reemplazo por rango) y `MERGE … NOT MATCHED BY SOURCE` |
# MAGIC | Cortes imprevistos | Metric View + `MEASURE()` |
