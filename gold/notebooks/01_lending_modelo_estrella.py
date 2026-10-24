# Databricks notebook source
# MAGIC %md
# MAGIC # Gold paso a paso · 01 · Lending: modelo estrella
# MAGIC
# MAGIC **Escenario:** Lending (Wizard Bank) · **Sesión 18** · **Consumidor:** BI (Power BI, dashboards).
# MAGIC
# MAGIC Silver es correcto pero técnico. Gold se diseña para quien consume: el analista de BI quiere
# MAGIC **hechos** (lo que pasó, con números) y **dimensiones** (por qué lo cortan: cliente, país, fecha),
# MAGIC con los joins ya resueltos. Es el **esquema estrella**.
# MAGIC
# MAGIC ```
# MAGIC                 dim_tiempo     dim_pais     dim_campania
# MAGIC                      \            |            /
# MAGIC   dim_cliente ——  fact_ofertas · fact_solicitudes · fact_desembolsos
# MAGIC ```
# MAGIC
# MAGIC | Paso | Tabla Gold | Sale de (Silver) | Feature de Databricks |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `dim_pais`, `dim_campania` | `paises`, `campanias` | `CREATE TABLE AS SELECT` + `COMMENT` |
# MAGIC | 2 | `dim_tiempo` | (se genera) | `sequence()` + `explode()` |
# MAGIC | 3 | `dim_cliente` | `dim_clientes` (SCD2) | `CLUSTER BY` (Liquid Clustering) |
# MAGIC | 4 | `fact_solicitudes` | `solicitudes_prestamo` | **join punto-en-el-tiempo** contra la SCD2 |
# MAGIC | 5 | `fact_ofertas` | `ofertas_preaprobadas` | el mismo patrón |
# MAGIC | 6 | `fact_desembolsos` | `desembolsos` + `tipos_cambio` | `LEFT JOIN LATERAL` (tipo de cambio vigente ese día) |
# MAGIC | 7 | llaves | — | `PRIMARY KEY` / `FOREIGN KEY` informativas de Unity Catalog |
# MAGIC
# MAGIC > **Origen:** las tablas `_lab` de Silver (las que llenan los notebooks de `silver/notebooks`).
# MAGIC > **Destino:** schema de práctica `gold.lending_lab`. Cuando Silver real esté poblado basta quitar el `_lab`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. El schema de Gold
# MAGIC
# MAGIC Gold vive en su propio catálogo (`gold`). Un schema por dominio, igual que en Silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold.lending_lab
# MAGIC COMMENT 'Gold de práctica de Lending (sesiones 18 a 20): modelo estrella y métricas';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. De dónde partimos: Silver
# MAGIC
# MAGIC Todo lo que sigue sale de estas tablas, ya tipadas y deduplicadas. Fíjate en `dim_clientes`:
# MAGIC tiene **más filas que clientes** porque guarda la historia (una fila por versión del cliente).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'paises'               AS tabla, count(*) AS filas FROM silver.lending.paises_lab
# MAGIC UNION ALL SELECT 'campanias',            count(*) FROM silver.lending.campanias_lab
# MAGIC UNION ALL SELECT 'dim_clientes (versiones)', count(*) FROM silver.lending.dim_clientes_lab
# MAGIC UNION ALL SELECT 'ofertas_preaprobadas', count(*) FROM silver.lending.ofertas_preaprobadas_lab
# MAGIC UNION ALL SELECT 'solicitudes_prestamo', count(*) FROM silver.lending.solicitudes_prestamo_lab
# MAGIC UNION ALL SELECT 'desembolsos',          count(*) FROM silver.lending.desembolsos_lab
# MAGIC UNION ALL SELECT 'tipos_cambio',         count(*) FROM silver.lending.tipos_cambio_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dimensiones pequeñas: `dim_pais` y `dim_campania`
# MAGIC
# MAGIC El patrón de Gold es siempre el mismo: **`CREATE OR REPLACE TABLE … AS SELECT`** (CTAS).
# MAGIC Se recalcula completa: para una dimensión de 3 filas es lo más simple y siempre consistente.
# MAGIC
# MAGIC - Los nombres van en el **idioma del negocio** (`pais`, no `nombre_pais`).
# MAGIC - `COMMENT` documenta la tabla en Unity Catalog: lo ven Catalog Explorer, Genie y Power BI.
# MAGIC - Las columnas técnicas de Silver (`_origen_archivo`, `_procesado_ts`…) **no se exponen** en Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.dim_pais
# MAGIC COMMENT 'Dimensión país: una fila por país y su moneda local'
# MAGIC AS
# MAGIC SELECT
# MAGIC   id_pais,
# MAGIC   codigo_iso,
# MAGIC   nombre_pais   AS pais,
# MAGIC   moneda_codigo AS moneda
# MAGIC FROM silver.lending.paises_lab;
# MAGIC
# MAGIC SELECT * FROM gold.lending_lab.dim_pais;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.dim_campania
# MAGIC COMMENT 'Dimensión campaña: una fila por campaña de marketing'
# MAGIC AS
# MAGIC SELECT
# MAGIC   id_campania,
# MAGIC   id_pais,
# MAGIC   nombre_campania,
# MAGIC   tipo_campania,
# MAGIC   fecha_inicio,
# MAGIC   fecha_fin,
# MAGIC   presupuesto AS presupuesto_local   -- en la moneda del país de la campaña
# MAGIC FROM silver.lending.campanias_lab;
# MAGIC
# MAGIC SELECT tipo_campania, count(*) AS campanias FROM gold.lending_lab.dim_campania GROUP BY tipo_campania;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `dim_tiempo`: la dimensión que no viene de ninguna fuente
# MAGIC
# MAGIC BI siempre filtra por mes, trimestre o día de la semana. En vez de que cada reporte los calcule,
# MAGIC se publica **una tabla calendario**. `sequence()` genera un día por fila y `explode()` lo convierte en filas.
# MAGIC
# MAGIC La clave `id_fecha` es un entero `yyyyMMdd` (20260928): barata de unir y legible.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.dim_tiempo
# MAGIC COMMENT 'Dimensión calendario: un día por fila, 2024 a 2027'
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(date_format(fecha, 'yyyyMMdd') AS INT) AS id_fecha,
# MAGIC   fecha,
# MAGIC   year(fecha)                  AS anio,
# MAGIC   quarter(fecha)               AS trimestre,
# MAGIC   month(fecha)                 AS mes,
# MAGIC   date_format(fecha, 'yyyy-MM') AS anio_mes,
# MAGIC   date_format(fecha, 'MMMM')   AS nombre_mes,
# MAGIC   date_format(fecha, 'EEEE')   AS nombre_dia,
# MAGIC   dayofweek(fecha) IN (1, 7)   AS es_fin_de_semana
# MAGIC FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS fecha);
# MAGIC
# MAGIC SELECT * FROM gold.lending_lab.dim_tiempo WHERE fecha BETWEEN '2026-09-25' AND '2026-09-28';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `dim_cliente`: la historia (SCD2) pasa a Gold
# MAGIC
# MAGIC En Silver cada cliente tiene **una fila por versión** (si cambia su score o su ingreso, se abre otra fila
# MAGIC con su vigencia). En Gold **se conserva esa historia**: así un hecho puede apuntar a la versión
# MAGIC del cliente *que existía cuando pasó*, no a la de hoy.
# MAGIC
# MAGIC Decisiones de diseño de Gold:
# MAGIC
# MAGIC - Se reutiliza la **clave subrogada** `sk_cliente` de Silver (única por versión). `id_cliente` es la clave de negocio.
# MAGIC - **PII fuera**: nombres, email, documento y fecha de nacimiento no se publican (en la sesión 21 se protegen con máscaras).
# MAGIC - Lógica de negocio ya aplicada: `rango_score` (BI no debería re-implementar el corte de score en cada reporte).
# MAGIC - La **primera versión** de cada cliente arranca en `1900-01-01`: así una solicitud anterior a la carga del
# MAGIC   cliente igual encuentra versión (si no, quedaría sin cliente).
# MAGIC - **`CLUSTER BY (id_cliente)`**: Liquid Clustering declarado al crear la tabla. Reemplaza al particionado y a `ZORDER`
# MAGIC   (se explica en la sesión 19); aquí solo se declara.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.dim_cliente
# MAGIC COMMENT 'Dimensión cliente con historia (SCD2): una fila por versión del cliente'
# MAGIC CLUSTER BY (id_cliente)
# MAGIC AS
# MAGIC SELECT
# MAGIC   sk_cliente,
# MAGIC   id_cliente,
# MAGIC   ingreso_mensual_declarado,
# MAGIC   situacion_laboral,
# MAGIC   score_interno,
# MAGIC   nivel_riesgo,
# MAGIC   CASE WHEN score_interno >= 700 THEN 'Alto'
# MAGIC        WHEN score_interno >= 600 THEN 'Medio'
# MAGIC        ELSE 'Bajo' END AS rango_score,
# MAGIC   ciudad,
# MAGIC   id_pais,
# MAGIC   id_campania_captacion,
# MAGIC   fecha_registro,
# MAGIC   CASE WHEN fecha_inicio_vigencia = min(fecha_inicio_vigencia) OVER (PARTITION BY id_cliente)
# MAGIC        THEN TIMESTAMP'1900-01-01'
# MAGIC        ELSE fecha_inicio_vigencia END AS vigente_desde,
# MAGIC   fecha_fin_vigencia AS vigente_hasta,      -- NULL = versión vigente
# MAGIC   es_vigente
# MAGIC FROM silver.lending.dim_clientes_lab;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Un cliente con dos versiones: la vieja (cerrada) y la vigente
# MAGIC SELECT sk_cliente, id_cliente, nivel_riesgo, score_interno, rango_score, vigente_desde, vigente_hasta, es_vigente
# MAGIC FROM gold.lending_lab.dim_cliente
# MAGIC WHERE id_cliente IN (SELECT id_cliente FROM gold.lending_lab.dim_cliente WHERE NOT es_vigente LIMIT 2)
# MAGIC ORDER BY id_cliente, vigente_desde;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `fact_solicitudes`: el hecho y el join punto-en-el-tiempo
# MAGIC
# MAGIC **Grano:** una fila por solicitud de préstamo. Es lo primero que se decide en un hecho.
# MAGIC
# MAGIC El hecho guarda **claves a las dimensiones** + **métricas numéricas**:
# MAGIC
# MAGIC - `id_fecha` → `dim_tiempo`, `id_pais` → `dim_pais`.
# MAGIC - `sk_cliente` → `dim_cliente`: **la versión del cliente vigente en el instante de la solicitud**. Se resuelve con un join
# MAGIC   por rango: `fecha_hora_solicitud` entre `vigente_desde` y `vigente_hasta`.
# MAGIC   Así la pregunta del regulador *«¿con qué score se aprobó esto?»* tiene respuesta aunque el cliente haya cambiado después.
# MAGIC - Métricas **aditivas** listas para `SUM`: `es_aprobada` (0/1), `monto_solicitado`, `monto_aprobado`.
# MAGIC - Los montos están en moneda local: **no se suman entre países** (por eso en `fact_desembolsos` se agrega USD).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.fact_solicitudes
# MAGIC COMMENT 'Hecho: una fila por solicitud de préstamo, con el cliente tal como era al solicitar'
# MAGIC CLUSTER BY (id_fecha)
# MAGIC AS
# MAGIC SELECT
# MAGIC   s.id_solicitud,
# MAGIC   CAST(date_format(s.fecha_hora_solicitud, 'yyyyMMdd') AS INT) AS id_fecha,
# MAGIC   c.sk_cliente,                                  -- la versión del cliente EN ESE MOMENTO
# MAGIC   s.id_cliente,
# MAGIC   c.id_pais,
# MAGIC   s.id_oferta,
# MAGIC   s.id_producto,
# MAGIC   s.canal,
# MAGIC   s.estado_solicitud,
# MAGIC   CASE WHEN s.estado_solicitud = 'Aprobada' THEN 1 ELSE 0 END AS es_aprobada,
# MAGIC   s.monto_solicitado,
# MAGIC   s.monto_aprobado,
# MAGIC   s.plazo_meses_solicitado,
# MAGIC   s.tasa_aprobada,
# MAGIC   s.score_evaluacion,
# MAGIC   s.motivo_rechazo,
# MAGIC   round((unix_timestamp(s.fecha_hora_resolucion) - unix_timestamp(s.fecha_hora_solicitud)) / 3600, 1) AS horas_a_resolucion
# MAGIC FROM silver.lending.solicitudes_prestamo_lab s
# MAGIC LEFT JOIN gold.lending_lab.dim_cliente c
# MAGIC   ON  c.id_cliente = s.id_cliente
# MAGIC   AND s.fecha_hora_solicitud >= c.vigente_desde
# MAGIC   AND s.fecha_hora_solicitud <  coalesce(c.vigente_hasta, TIMESTAMP'9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.lending_lab.fact_solicitudes LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `fact_ofertas`: el mismo patrón
# MAGIC
# MAGIC **Grano:** una fila por oferta preaprobada. Es el primer paso del funnel de originación.
# MAGIC Misma receta: claves a dimensiones (con el cliente punto-en-el-tiempo) + métricas aditivas.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.fact_ofertas
# MAGIC COMMENT 'Hecho: una fila por oferta preaprobada asignada a un cliente'
# MAGIC CLUSTER BY (id_fecha)
# MAGIC AS
# MAGIC SELECT
# MAGIC   o.id_oferta,
# MAGIC   CAST(date_format(o.fecha_generacion, 'yyyyMMdd') AS INT) AS id_fecha,
# MAGIC   c.sk_cliente,
# MAGIC   o.id_cliente,
# MAGIC   c.id_pais,
# MAGIC   o.id_producto,
# MAGIC   o.id_campania,
# MAGIC   o.motor_asignacion,
# MAGIC   o.estado_oferta,
# MAGIC   CASE WHEN o.estado_oferta = 'Aceptada' THEN 1 ELSE 0 END AS es_aceptada,
# MAGIC   o.monto_ofertado,
# MAGIC   o.plazo_meses_ofertado,
# MAGIC   o.tasa_ofertada
# MAGIC FROM silver.lending.ofertas_preaprobadas_lab o
# MAGIC LEFT JOIN gold.lending_lab.dim_cliente c
# MAGIC   ON  c.id_cliente = o.id_cliente
# MAGIC   AND o.fecha_generacion >= c.vigente_desde
# MAGIC   AND o.fecha_generacion <  coalesce(c.vigente_hasta, TIMESTAMP'9999-12-31');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `fact_desembolsos`: monto en USD con el tipo de cambio de ese día
# MAGIC
# MAGIC Wizard Bank opera en PEN, COP y BOB. Para **consolidar** el monto colocado hay que llevarlo a USD, y el
# MAGIC tipo de cambio **cambia cada día**. Dos detalles:
# MAGIC
# MAGIC - El proveedor publica tipos de cambio hasta cierta fecha: para un desembolso posterior hay que usar **el último disponible**.
# MAGIC   Se resuelve con **`LEFT JOIN LATERAL`**: la subconsulta se ejecuta por cada fila y trae el cambio más reciente `<=` a la fecha del desembolso.
# MAGIC - `tasa_venta` está expresada en moneda local por 1 USD (COP ≈ 4.100), por eso `monto_usd = monto / tasa`.
# MAGIC
# MAGIC El hecho conserva el monto local **y** el USD, más el tipo de cambio usado (auditable).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.lending_lab.fact_desembolsos
# MAGIC COMMENT 'Hecho: una fila por desembolso, con el monto en moneda local y en USD'
# MAGIC CLUSTER BY (id_fecha)
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.id_desembolso,
# MAGIC   d.id_solicitud,
# MAGIC   CAST(date_format(d.fecha_hora_desembolso, 'yyyyMMdd') AS INT) AS id_fecha,
# MAGIC   c.sk_cliente,
# MAGIC   d.id_cliente,
# MAGIC   p.id_pais,
# MAGIC   d.moneda,
# MAGIC   d.estado_desembolso,
# MAGIC   CASE WHEN d.estado_desembolso = 'Completado' THEN 1 ELSE 0 END AS es_completado,
# MAGIC   d.monto_desembolsado,
# MAGIC   tc.tasa_venta                                    AS tipo_cambio_usd,
# MAGIC   round(d.monto_desembolsado / tc.tasa_venta, 2)   AS monto_usd,
# MAGIC   d.comision_cobrada,
# MAGIC   round(d.comision_cobrada / tc.tasa_venta, 2)     AS comision_usd,
# MAGIC   d.tasa_aplicada,
# MAGIC   d.plazo_meses
# MAGIC FROM silver.lending.desembolsos_lab d
# MAGIC LEFT JOIN gold.lending_lab.dim_pais p
# MAGIC   ON p.moneda = d.moneda
# MAGIC LEFT JOIN gold.lending_lab.dim_cliente c
# MAGIC   ON  c.id_cliente = d.id_cliente
# MAGIC   AND d.fecha_hora_desembolso >= c.vigente_desde
# MAGIC   AND d.fecha_hora_desembolso <  coalesce(c.vigente_hasta, TIMESTAMP'9999-12-31')
# MAGIC LEFT JOIN LATERAL (
# MAGIC   SELECT t.tasa_venta
# MAGIC   FROM silver.lending.tipos_cambio_lab t
# MAGIC   WHERE t.moneda_origen  = d.moneda
# MAGIC     AND t.moneda_destino = 'USD'
# MAGIC     AND t.fecha <= CAST(d.fecha_hora_desembolso AS DATE)
# MAGIC   ORDER BY t.fecha DESC
# MAGIC   LIMIT 1
# MAGIC ) tc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consolidado: aquí el USD sí se puede sumar entre países (el monto local no)
# MAGIC SELECT moneda, count(*) AS desembolsos,
# MAGIC        round(sum(monto_desembolsado)) AS monto_local,
# MAGIC        round(sum(monto_usd))          AS monto_usd
# MAGIC FROM gold.lending_lab.fact_desembolsos
# MAGIC WHERE es_completado = 1
# MAGIC GROUP BY moneda;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Declarar las llaves: `PRIMARY KEY` y `FOREIGN KEY`
# MAGIC
# MAGIC Unity Catalog permite declarar las llaves del modelo estrella. Son **informativas** (Delta no las hace cumplir),
# MAGIC pero:
# MAGIC
# MAGIC - Power BI y otras herramientas **leen el modelo** (las relaciones se arman solas al conectar).
# MAGIC - Catalog Explorer dibuja el diagrama de relaciones.
# MAGIC - `RELY` autoriza al optimizador a **eliminar joins innecesarios** confiando en que la llave es única.
# MAGIC
# MAGIC Una `PRIMARY KEY` exige columnas `NOT NULL`. Como no se hacen cumplir, **las validamos nosotros** en el paso 9.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dimensiones: NOT NULL + PRIMARY KEY
# MAGIC ALTER TABLE gold.lending_lab.dim_pais    ALTER COLUMN id_pais    SET NOT NULL;
# MAGIC ALTER TABLE gold.lending_lab.dim_tiempo  ALTER COLUMN id_fecha   SET NOT NULL;
# MAGIC ALTER TABLE gold.lending_lab.dim_cliente ALTER COLUMN sk_cliente SET NOT NULL;
# MAGIC ALTER TABLE gold.lending_lab.dim_pais    ADD CONSTRAINT pk_dim_pais    PRIMARY KEY (id_pais)    RELY;
# MAGIC ALTER TABLE gold.lending_lab.dim_tiempo  ADD CONSTRAINT pk_dim_tiempo  PRIMARY KEY (id_fecha)   RELY;
# MAGIC ALTER TABLE gold.lending_lab.dim_cliente ADD CONSTRAINT pk_dim_cliente PRIMARY KEY (sk_cliente) RELY;
# MAGIC
# MAGIC -- Hechos: PRIMARY KEY propia + FOREIGN KEY hacia cada dimensión
# MAGIC ALTER TABLE gold.lending_lab.fact_solicitudes  ALTER COLUMN id_solicitud  SET NOT NULL;
# MAGIC ALTER TABLE gold.lending_lab.fact_desembolsos  ALTER COLUMN id_desembolso SET NOT NULL;
# MAGIC ALTER TABLE gold.lending_lab.fact_ofertas      ALTER COLUMN id_oferta     SET NOT NULL;
# MAGIC ALTER TABLE gold.lending_lab.fact_solicitudes  ADD CONSTRAINT pk_fact_solicitudes  PRIMARY KEY (id_solicitud);
# MAGIC ALTER TABLE gold.lending_lab.fact_desembolsos  ADD CONSTRAINT pk_fact_desembolsos  PRIMARY KEY (id_desembolso);
# MAGIC ALTER TABLE gold.lending_lab.fact_ofertas      ADD CONSTRAINT pk_fact_ofertas      PRIMARY KEY (id_oferta);
# MAGIC
# MAGIC ALTER TABLE gold.lending_lab.fact_solicitudes ADD CONSTRAINT fk_sol_cliente FOREIGN KEY (sk_cliente) REFERENCES gold.lending_lab.dim_cliente (sk_cliente);
# MAGIC ALTER TABLE gold.lending_lab.fact_solicitudes ADD CONSTRAINT fk_sol_fecha   FOREIGN KEY (id_fecha)   REFERENCES gold.lending_lab.dim_tiempo (id_fecha);
# MAGIC ALTER TABLE gold.lending_lab.fact_solicitudes ADD CONSTRAINT fk_sol_pais    FOREIGN KEY (id_pais)    REFERENCES gold.lending_lab.dim_pais (id_pais);
# MAGIC ALTER TABLE gold.lending_lab.fact_ofertas     ADD CONSTRAINT fk_ofe_cliente FOREIGN KEY (sk_cliente) REFERENCES gold.lending_lab.dim_cliente (sk_cliente);
# MAGIC ALTER TABLE gold.lending_lab.fact_ofertas     ADD CONSTRAINT fk_ofe_fecha   FOREIGN KEY (id_fecha)   REFERENCES gold.lending_lab.dim_tiempo (id_fecha);
# MAGIC ALTER TABLE gold.lending_lab.fact_desembolsos ADD CONSTRAINT fk_des_cliente FOREIGN KEY (sk_cliente) REFERENCES gold.lending_lab.dim_cliente (sk_cliente);
# MAGIC ALTER TABLE gold.lending_lab.fact_desembolsos ADD CONSTRAINT fk_des_fecha   FOREIGN KEY (id_fecha)   REFERENCES gold.lending_lab.dim_tiempo (id_fecha);
# MAGIC ALTER TABLE gold.lending_lab.fact_desembolsos ADD CONSTRAINT fk_des_pais    FOREIGN KEY (id_pais)    REFERENCES gold.lending_lab.dim_pais (id_pais);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Las llaves quedan registradas en Unity Catalog
# MAGIC SELECT table_name, constraint_name, constraint_type
# MAGIC FROM gold.information_schema.table_constraints
# MAGIC WHERE table_schema = 'lending_lab'
# MAGIC ORDER BY table_name, constraint_type DESC, constraint_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validar el modelo
# MAGIC
# MAGIC Como las llaves no se hacen cumplir, el criterio de «Gold terminado» se comprueba con consultas:
# MAGIC
# MAGIC 1. **Cuadre de filas:** cada hecho tiene tantas filas como su tabla de Silver (los joins no duplicaron ni perdieron nada).
# MAGIC 2. **Llaves únicas:** la PK declarada realmente es única.
# MAGIC 3. **Sin huérfanos:** toda clave foránea encuentra su dimensión.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'solicitudes' AS hecho,
# MAGIC        (SELECT count(*) FROM silver.lending.solicitudes_prestamo_lab)  AS filas_silver,
# MAGIC        (SELECT count(*) FROM gold.lending_lab.fact_solicitudes)        AS filas_gold,
# MAGIC        (SELECT count(*) - count(DISTINCT id_solicitud) FROM gold.lending_lab.fact_solicitudes) AS pk_repetidas
# MAGIC UNION ALL
# MAGIC SELECT 'ofertas',
# MAGIC        (SELECT count(*) FROM silver.lending.ofertas_preaprobadas_lab),
# MAGIC        (SELECT count(*) FROM gold.lending_lab.fact_ofertas),
# MAGIC        (SELECT count(*) - count(DISTINCT id_oferta) FROM gold.lending_lab.fact_ofertas)
# MAGIC UNION ALL
# MAGIC SELECT 'desembolsos',
# MAGIC        (SELECT count(*) FROM silver.lending.desembolsos_lab),
# MAGIC        (SELECT count(*) FROM gold.lending_lab.fact_desembolsos),
# MAGIC        (SELECT count(*) - count(DISTINCT id_desembolso) FROM gold.lending_lab.fact_desembolsos);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Huérfanos: todo debe dar 0
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM gold.lending_lab.fact_solicitudes f ANTI JOIN gold.lending_lab.dim_cliente c ON c.sk_cliente = f.sk_cliente) AS solicitudes_sin_cliente,
# MAGIC   (SELECT count(*) FROM gold.lending_lab.fact_ofertas     f ANTI JOIN gold.lending_lab.dim_cliente c ON c.sk_cliente = f.sk_cliente) AS ofertas_sin_cliente,
# MAGIC   (SELECT count(*) FROM gold.lending_lab.fact_desembolsos  f ANTI JOIN gold.lending_lab.dim_cliente c ON c.sk_cliente = f.sk_cliente) AS desembolsos_sin_cliente,
# MAGIC   (SELECT count(*) FROM gold.lending_lab.fact_desembolsos  WHERE monto_usd IS NULL) AS desembolsos_sin_usd,
# MAGIC   (SELECT count(*) FROM gold.lending_lab.fact_solicitudes  f ANTI JOIN gold.lending_lab.dim_tiempo t ON t.id_fecha = f.id_fecha) AS solicitudes_sin_fecha;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. La prueba de fuego: una pregunta de negocio con `SELECT` + `GROUP BY`
# MAGIC
# MAGIC Un modelo Gold está bien diseñado si el analista responde **sin limpiar nada y sin lógica extra**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Cómo evolucionó la aprobación por país y trimestre?
# MAGIC SELECT t.anio, t.trimestre, p.pais,
# MAGIC        count(*)                                           AS solicitudes,
# MAGIC        sum(f.es_aprobada)                                 AS aprobadas,
# MAGIC        round(100 * sum(f.es_aprobada) / count(*), 1)      AS pct_aprobacion
# MAGIC FROM gold.lending_lab.fact_solicitudes f
# MAGIC JOIN gold.lending_lab.dim_tiempo t ON t.id_fecha = f.id_fecha
# MAGIC JOIN gold.lending_lab.dim_pais   p ON p.id_pais  = f.id_pais
# MAGIC GROUP BY t.anio, t.trimestre, p.pais
# MAGIC ORDER BY t.anio, t.trimestre, p.pais;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por qué importa `sk_cliente`: el riesgo «al solicitar» vs. el riesgo «hoy»
# MAGIC
# MAGIC Dos consultas casi idénticas. La primera une por `sk_cliente` (la foto del cliente al solicitar).
# MAGIC La segunda une por `id_cliente` solo con la versión vigente (el cliente de hoy). **Compara la columna `solicitudes`: no coincide por nivel.**
# MAGIC La segunda reescribe el pasado: un cliente que hoy es `E` pero entró siendo `B` se cuenta como `E` en sus solicitudes viejas.
# MAGIC (En estos datos son pocos los clientes con cambio de versión, por eso la diferencia es pequeña; en una cartera real crece con el tiempo.)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Riesgo del cliente AL MOMENTO de solicitar (punto en el tiempo)
# MAGIC SELECT c.nivel_riesgo, count(*) AS solicitudes, round(100 * avg(f.es_aprobada), 1) AS pct_aprobacion
# MAGIC FROM gold.lending_lab.fact_solicitudes f
# MAGIC JOIN gold.lending_lab.dim_cliente c ON c.sk_cliente = f.sk_cliente
# MAGIC GROUP BY c.nivel_riesgo ORDER BY c.nivel_riesgo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Riesgo del cliente HOY (la versión vigente aplicada al pasado)
# MAGIC SELECT c.nivel_riesgo, count(*) AS solicitudes, round(100 * avg(f.es_aprobada), 1) AS pct_aprobacion
# MAGIC FROM gold.lending_lab.fact_solicitudes f
# MAGIC JOIN gold.lending_lab.dim_cliente c ON c.id_cliente = f.id_cliente AND c.es_vigente
# MAGIC GROUP BY c.nivel_riesgo ORDER BY c.nivel_riesgo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen: qué features de Databricks usaste
# MAGIC
# MAGIC | Feature | Para qué en Gold |
# MAGIC |---|---|
# MAGIC | `CREATE OR REPLACE TABLE … AS SELECT` | Construir y reconstruir una tabla Gold de forma idempotente |
# MAGIC | `COMMENT` | Documentar para BI, Genie y Catalog Explorer |
# MAGIC | `CLUSTER BY` | Liquid Clustering declarado al crear (sesión 19 lo aprovecha) |
# MAGIC | `sequence()` + `explode()` | Generar una dimensión calendario |
# MAGIC | Join por rango contra SCD2 | Congelar la foto de la dimensión en el hecho |
# MAGIC | `LEFT JOIN LATERAL` | Tipo de cambio más reciente por fila |
# MAGIC | `PRIMARY KEY` / `FOREIGN KEY` (+ `RELY`) | Publicar el modelo estrella a Power BI y al optimizador |
# MAGIC | `information_schema` | Auditar llaves y objetos |
# MAGIC
# MAGIC **Siguiente:** `02_lending_cdf` expone a otros sistemas *qué cambió* de una dimensión Gold.
