# Databricks notebook source
# MAGIC %md
# MAGIC # Gold paso a paso · 04 · Cobranzas: tabla ancha (wide table)
# MAGIC
# MAGIC **Escenario:** Cobranzas · **Sesiones 18 y 20** · **Consumidor:** ciencia de datos (modelo de riesgo de mora) y cobranza.
# MAGIC
# MAGIC El modelo estrella (notebook 01) es para BI. Para **machine learning** y exploración rápida el
# MAGIC arquetipo es otro: la **tabla ancha**, una sola tabla con **una fila por entidad** y todas las variables ya
# MAGIC calculadas. Sin joins para el consumidor, sin NULLs, una columna objetivo (*target*).
# MAGIC
# MAGIC **Entidad:** el crédito (`numero_credito`). Silver de Cobranzas lo tiene repartido en tres tablas:
# MAGIC
# MAGIC | Silver | Aporta al crédito |
# MAGIC |---|---|
# MAGIC | `cuotas` | Plan de pagos: cuántas cuotas, cuáles vencidas, saldo pendiente |
# MAGIC | `pagos` | Comportamiento de pago: cuánto pagó, cuándo, por qué medio |
# MAGIC | `gestiones_cobranza` | Esfuerzo de cobranza: cuántas gestiones, cuál fue la última |
# MAGIC
# MAGIC | Paso | Qué construyes | Feature de Databricks |
# MAGIC |---|---|---|
# MAGIC | 1 | `cartera_creditos` (tabla ancha) | CTEs + `count_if`, `FILTER`, `max_by`, `mode` |
# MAGIC | 2 | Sin NULLs y con *target* | `coalesce`, alias de columna reutilizable |
# MAGIC | 3 | `snapshot_cartera_tramos` (foto por fecha de corte) | `REPLACE WHERE`: snapshot idempotente |
# MAGIC
# MAGIC > **Origen:** tablas `_lab` de Silver. **Destino:** `gold.cobranzas_lab`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold.cobranzas_lab
# MAGIC COMMENT 'Gold de práctica de Cobranzas (sesiones 18 a 20): tabla ancha de cartera y snapshots';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. De dónde partimos
# MAGIC
# MAGIC Tres tablas con **grano distinto**: una fila por cuota, por pago y por gestión. Un modelo necesita una fila por crédito:
# MAGIC hay que **agregar cada tabla al grano del crédito antes de unirlas**. Si se unieran directo, cada cuota se
# MAGIC repetiría por cada pago y todas las sumas saldrían infladas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'cuotas' AS tabla, count(*) AS filas, count(DISTINCT numero_credito) AS creditos FROM silver.cobranzas.cuotas_lab
# MAGIC UNION ALL SELECT 'pagos',     count(*), count(DISTINCT numero_credito) FROM silver.cobranzas.pagos_lab
# MAGIC UNION ALL SELECT 'gestiones', count(*), count(DISTINCT numero_credito) FROM silver.cobranzas.gestiones_cobranza_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Un aggregate por tabla: las variables (*features*)
# MAGIC
# MAGIC Primero se prueba cada bloque por separado. Funciones de agregación de Databricks que simplifican las features:
# MAGIC
# MAGIC | Función | Qué hace | Ejemplo |
# MAGIC |---|---|---|
# MAGIC | `count_if(cond)` | Cuenta las filas que cumplen | cuotas vencidas |
# MAGIC | `agg(...) FILTER (WHERE cond)` | Agrega solo las filas que cumplen | saldo de capital de cuotas no pagadas |
# MAGIC | `max_by(col, orden)` | El valor de `col` en la fila de mayor `orden` | resultado de la **última** gestión |
# MAGIC | `mode(col)` | El valor más frecuente | medio de pago habitual |
# MAGIC
# MAGIC ### 2.1 Cuotas: el plan de pagos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   numero_credito,
# MAGIC   count(*)                                              AS n_cuotas,
# MAGIC   count_if(estado_cuota = 'Pagada')                     AS cuotas_pagadas,
# MAGIC   count_if(estado_cuota = 'Vencida')                    AS cuotas_vencidas,
# MAGIC   sum(monto_capital) FILTER (WHERE estado_cuota <> 'Pagada') AS saldo_capital,
# MAGIC   min(fecha_vencimiento) FILTER (WHERE estado_cuota = 'Vencida') AS vencida_mas_antigua
# MAGIC FROM silver.cobranzas.cuotas_lab
# MAGIC GROUP BY numero_credito
# MAGIC HAVING count_if(estado_cuota = 'Vencida') > 0
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Pagos y gestiones

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   numero_credito,
# MAGIC   count(*)                            AS n_pagos,
# MAGIC   sum(monto_pagado)                   AS monto_pagado,
# MAGIC   max(fecha_pago)                     AS fecha_ultimo_pago,
# MAGIC   mode(medio_pago)                    AS medio_pago_frecuente
# MAGIC FROM silver.cobranzas.pagos_lab
# MAGIC GROUP BY numero_credito
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   numero_credito,
# MAGIC   count(*)                            AS n_gestiones,
# MAGIC   max(fecha_gestion)                  AS fecha_ultima_gestion,
# MAGIC   max_by(resultado, fecha_gestion)    AS resultado_ultima_gestion,
# MAGIC   max_by(tipo_gestion, fecha_gestion) AS tipo_ultima_gestion
# MAGIC FROM silver.cobranzas.gestiones_cobranza_lab
# MAGIC GROUP BY numero_credito
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unirlo todo: `cartera_creditos`
# MAGIC
# MAGIC Cada bloque anterior se convierte en una CTE y se unen con `LEFT JOIN` sobre las cuotas (todo crédito tiene cuotas;
# MAGIC no todos tienen pagos ni gestiones).
# MAGIC
# MAGIC Decisiones de diseño para ML:
# MAGIC
# MAGIC - **Fecha de corte:** la tabla es una **foto a una fecha**. Se toma de los datos (último pago registrado), no de `current_date()`,
# MAGIC   para que la tabla sea reproducible.
# MAGIC - **Sin NULLs:** un crédito sin pagos tiene `n_pagos = 0`, no NULL. `coalesce` rellena con un valor que *significa* algo.
# MAGIC   Para las columnas categóricas se usa `'Sin ...'`.
# MAGIC - **Features ya calculadas:** `dias_mora`, `pct_cuotas_pagadas`, `ratio_pagado`.
# MAGIC - **Target:** `es_mora_30` = 1 si el crédito tiene una cuota vencida hace más de 30 días.
# MAGIC - **Alias reutilizables:** Databricks permite usar en el mismo `SELECT` un alias definido antes (`dias_mora`).
# MAGIC - `CLUSTER BY (numero_credito)`: el acceso por clave es el patrón típico de apps y de *feature lookup*.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.cobranzas_lab.cartera_creditos
# MAGIC COMMENT 'Tabla ancha de cartera: una fila por crédito con features de pago y cobranza y el target es_mora_30. Foto a fecha_corte'
# MAGIC CLUSTER BY (numero_credito)
# MAGIC AS
# MAGIC WITH corte AS (
# MAGIC   SELECT CAST(max(fecha_pago) AS DATE) AS fecha_corte FROM silver.cobranzas.pagos_lab
# MAGIC ),
# MAGIC cuotas AS (
# MAGIC   SELECT
# MAGIC     numero_credito,
# MAGIC     count(*)                                                   AS n_cuotas,
# MAGIC     min(fecha_vencimiento)                                     AS fecha_primera_cuota,
# MAGIC     max(fecha_vencimiento)                                     AS fecha_ultima_cuota,
# MAGIC     sum(monto_cuota)                                           AS monto_total_cuotas,
# MAGIC     count_if(estado_cuota = 'Pagada')                          AS cuotas_pagadas,
# MAGIC     count_if(estado_cuota = 'Vencida')                         AS cuotas_vencidas,
# MAGIC     count_if(estado_cuota = 'Pendiente')                       AS cuotas_pendientes,
# MAGIC     sum(monto_capital) FILTER (WHERE estado_cuota <> 'Pagada') AS saldo_capital,
# MAGIC     sum(monto_cuota)   FILTER (WHERE estado_cuota = 'Vencida') AS monto_vencido,
# MAGIC     min(fecha_vencimiento) FILTER (WHERE estado_cuota = 'Vencida') AS vencida_mas_antigua
# MAGIC   FROM silver.cobranzas.cuotas_lab
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC pagos AS (
# MAGIC   SELECT
# MAGIC     numero_credito,
# MAGIC     count(*)                 AS n_pagos,
# MAGIC     sum(monto_pagado)        AS monto_pagado,
# MAGIC     max(fecha_pago)          AS fecha_ultimo_pago,
# MAGIC     mode(medio_pago)         AS medio_pago_frecuente
# MAGIC   FROM silver.cobranzas.pagos_lab
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC gestiones AS (
# MAGIC   SELECT
# MAGIC     numero_credito,
# MAGIC     count(*)                              AS n_gestiones,
# MAGIC     max_by(resultado, fecha_gestion)      AS resultado_ultima_gestion
# MAGIC   FROM silver.cobranzas.gestiones_cobranza_lab
# MAGIC   GROUP BY numero_credito
# MAGIC )
# MAGIC SELECT
# MAGIC   c.numero_credito,
# MAGIC   corte.fecha_corte,
# MAGIC   -- plan de pagos
# MAGIC   c.n_cuotas,
# MAGIC   c.fecha_primera_cuota,
# MAGIC   c.fecha_ultima_cuota,
# MAGIC   c.monto_total_cuotas,
# MAGIC   c.cuotas_pagadas,
# MAGIC   c.cuotas_vencidas,
# MAGIC   c.cuotas_pendientes,
# MAGIC   round(c.cuotas_pagadas / c.n_cuotas, 4)                         AS pct_cuotas_pagadas,
# MAGIC   coalesce(c.saldo_capital, 0)                                    AS saldo_capital,
# MAGIC   coalesce(c.monto_vencido, 0)                                    AS monto_vencido,
# MAGIC   -- mora
# MAGIC   coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0) AS dias_mora,
# MAGIC   CASE WHEN dias_mora = 0   THEN 'Al dia'
# MAGIC        WHEN dias_mora <= 30 THEN '1-30'
# MAGIC        WHEN dias_mora <= 60 THEN '31-60'
# MAGIC        WHEN dias_mora <= 90 THEN '61-90'
# MAGIC        ELSE 'Mas de 90' END                                       AS tramo_mora,
# MAGIC   -- comportamiento de pago
# MAGIC   coalesce(p.n_pagos, 0)                                          AS n_pagos,
# MAGIC   coalesce(p.monto_pagado, 0)                                     AS monto_pagado,
# MAGIC   round(coalesce(p.monto_pagado, 0) / c.monto_total_cuotas, 4)    AS ratio_pagado,
# MAGIC   p.fecha_ultimo_pago,
# MAGIC   coalesce(datediff(corte.fecha_corte, CAST(p.fecha_ultimo_pago AS DATE)), -1) AS dias_desde_ultimo_pago,
# MAGIC   coalesce(p.medio_pago_frecuente, 'Sin pagos')                   AS medio_pago_frecuente,
# MAGIC   -- cobranza
# MAGIC   coalesce(g.n_gestiones, 0)                                      AS n_gestiones,
# MAGIC   coalesce(g.resultado_ultima_gestion, 'Sin gestion')             AS resultado_ultima_gestion,
# MAGIC   -- target para el modelo
# MAGIC   CASE WHEN dias_mora > 30 THEN 1 ELSE 0 END                      AS es_mora_30
# MAGIC FROM cuotas c
# MAGIC CROSS JOIN corte
# MAGIC LEFT JOIN pagos p      ON p.numero_credito = c.numero_credito
# MAGIC LEFT JOIN gestiones g  ON g.numero_credito = c.numero_credito;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.cobranzas_lab.cartera_creditos WHERE cuotas_vencidas > 0 ORDER BY dias_mora DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validar la tabla ancha
# MAGIC
# MAGIC Lo que debe cumplir una tabla lista para ML:
# MAGIC
# MAGIC 1. **Una fila por crédito** (el grano que prometimos).
# MAGIC 2. **Los totales cuadran con Silver**: si los joins inflaran filas, las sumas no coincidirían.
# MAGIC 3. **Cero NULLs** en las columnas de features.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count(DISTINCT numero_credito) FROM silver.cobranzas.cuotas_lab)   AS creditos_silver,
# MAGIC   (SELECT count(*) FROM gold.cobranzas_lab.cartera_creditos)                 AS filas_gold,
# MAGIC   (SELECT round(sum(monto_pagado)) FROM silver.cobranzas.pagos_lab)          AS pagado_silver,
# MAGIC   (SELECT round(sum(monto_pagado)) FROM gold.cobranzas_lab.cartera_creditos) AS pagado_gold,
# MAGIC   (SELECT round(sum(monto_capital)) FROM silver.cobranzas.cuotas_lab WHERE estado_cuota <> 'Pagada') AS saldo_silver,
# MAGIC   (SELECT round(sum(saldo_capital)) FROM gold.cobranzas_lab.cartera_creditos) AS saldo_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NULLs por columna: todo debe dar 0 (salvo fechas que legítimamente no existen)
# MAGIC SELECT
# MAGIC   count_if(saldo_capital IS NULL) AS saldo_capital, count_if(monto_vencido IS NULL) AS monto_vencido,
# MAGIC   count_if(dias_mora IS NULL) AS dias_mora, count_if(n_pagos IS NULL) AS n_pagos,
# MAGIC   count_if(medio_pago_frecuente IS NULL) AS medio_pago, count_if(resultado_ultima_gestion IS NULL) AS resultado_gestion,
# MAGIC   count_if(es_mora_30 IS NULL) AS target
# MAGIC FROM gold.cobranzas_lab.cartera_creditos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El target: ¿cuántos créditos están en mora de más de 30 días?
# MAGIC SELECT es_mora_30, count(*) AS creditos, round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct
# MAGIC FROM gold.cobranzas_lab.cartera_creditos
# MAGIC GROUP BY es_mora_30;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. De tabla ancha a métrica: `snapshot_cartera_tramos`
# MAGIC
# MAGIC Cobranza no quiere 27.000 filas: quiere saber **cuánta cartera hay en cada tramo de mora**. Es una métrica
# MAGIC **semi-aditiva**: el saldo se puede sumar entre tramos o créditos, pero **no entre fechas** (el saldo de hoy no es la
# MAGIC suma de los saldos de cada día del mes). Por eso se guarda como **snapshot**: una foto por fecha de corte.
# MAGIC
# MAGIC Cada corrida añade la foto del día. Si el job se reintenta, la foto de ese día **no debe duplicarse**:
# MAGIC se usa `INSERT INTO … REPLACE WHERE fecha_corte = …` (reemplaza solo esa fecha, en una transacción).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.cobranzas_lab.snapshot_cartera_tramos (
# MAGIC   fecha_corte   DATE          COMMENT 'Fecha de la foto',
# MAGIC   tramo_mora    STRING        COMMENT 'Al dia, 1-30, 31-60, 61-90, Mas de 90',
# MAGIC   creditos      BIGINT        COMMENT 'Aditiva: creditos en el tramo',
# MAGIC   saldo_capital DECIMAL(18,2) COMMENT 'Semi-aditiva: suma entre tramos, NO entre fechas',
# MAGIC   monto_vencido DECIMAL(18,2) COMMENT 'Semi-aditiva'
# MAGIC )
# MAGIC COMMENT 'Foto diaria de la cartera por tramo de mora. Grano: fecha_corte x tramo_mora'
# MAGIC CLUSTER BY (fecha_corte);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REPLACE WHERE no admite subconsultas: la fecha de corte se guarda antes en una variable de sesión SQL
# MAGIC DECLARE OR REPLACE VARIABLE v_corte DATE;
# MAGIC SET VAR v_corte = (SELECT max(fecha_corte) FROM gold.cobranzas_lab.cartera_creditos);
# MAGIC
# MAGIC INSERT INTO gold.cobranzas_lab.snapshot_cartera_tramos
# MAGIC REPLACE WHERE fecha_corte = v_corte
# MAGIC SELECT
# MAGIC   fecha_corte,
# MAGIC   tramo_mora,
# MAGIC   count(*)           AS creditos,
# MAGIC   sum(saldo_capital) AS saldo_capital,
# MAGIC   sum(monto_vencido) AS monto_vencido
# MAGIC FROM gold.cobranzas_lab.cartera_creditos
# MAGIC GROUP BY fecha_corte, tramo_mora;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vuelve a correr la celda anterior: la tabla debe quedar igual (idempotente), no duplicada
# MAGIC SELECT fecha_corte, tramo_mora, creditos, saldo_capital, monto_vencido
# MAGIC FROM gold.cobranzas_lab.snapshot_cartera_tramos
# MAGIC ORDER BY fecha_corte, CASE tramo_mora WHEN 'Al dia' THEN 0 WHEN '1-30' THEN 1 WHEN '31-60' THEN 2 WHEN '61-90' THEN 3 ELSE 4 END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen: features de Databricks usadas
# MAGIC
# MAGIC | Feature | Para qué en Gold |
# MAGIC |---|---|
# MAGIC | CTEs por grano + `LEFT JOIN` | Llevar tablas de granos distintos al grano de la entidad sin inflar sumas |
# MAGIC | `count_if`, `agg … FILTER (WHERE …)` | Features condicionales sin `CASE WHEN` repetidos |
# MAGIC | `max_by`, `mode` | «Último valor» y «valor más frecuente» sin ventanas |
# MAGIC | Alias reutilizable en el `SELECT` | Features derivadas legibles |
# MAGIC | `coalesce` | Cero NULLs para el modelo |
# MAGIC | `CREATE TABLE … COMMENT` por columna | Documentar la semántica (aditiva / semi-aditiva) |
# MAGIC | `INSERT … REPLACE WHERE` + variable de sesión (`DECLARE VARIABLE`) | Snapshots idempotentes |
# MAGIC
# MAGIC **Siguiente:** `05_cobranzas_optimizacion` toma una tabla de hechos de Cobranzas y la hace rápida (Sesión 19).
