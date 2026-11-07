# Databricks notebook source

# MAGIC %md
# MAGIC # Sesión 20 · Gold paso a paso: modelo estrella, capa de métricas y feature tables en el pipeline
# MAGIC **Data Wizard Academy · Databricks Track · Módulo 4**
# MAGIC `v1.0 · creada 2026-11-06 · actualizada 2026-11-06`
# MAGIC
# MAGIC **Continúa la Sesión 19.** Tu `pipeline_wizard_bank_lakehouse` ya tiene Bronze y Silver. Hoy le agregas **Gold**, en el mismo
# MAGIC pipeline y leyendo esas Silver, para cuatro consumidores distintos:
# MAGIC
# MAGIC | Consumidor | Qué necesita | Lo que construyes |
# MAGIC |---|---|---|
# MAGIC | **BI y Power BI** | Cortar por cualquier dimensión | Modelo estrella: 6 dimensiones y 3 hechos |
# MAGIC | **El negocio** (dashboards, Genie) | El KPI ya calculado, con **una sola** definición | Capa de métricas: 4 tablas materializadas + 2 **metric views** |
# MAGIC | **Cobranzas** | Una fila por crédito con todo lo que se sabe | Tabla ancha de cartera |
# MAGIC | **Data Science** | Features sin fuga de información | 2 **feature tables** con llave `TIMESERIES` |
# MAGIC
# MAGIC **Cómo se trabaja (igual que la Sesión 19):** cada bloque 📋 va a un archivo nuevo en `transformations/` de tu pipeline →
# MAGIC **Run pipeline** → validas aquí con una celda ▶️. Las celdas de las metric views se ejecutan aquí mismo.
# MAGIC
# MAGIC | Bloque | Pasos | Min |
# MAGIC |---|---|---|
# MAGIC | Introducción | Gold para el consumidor | 10 |
# MAGIC | **Bloque 1 · Modelo estrella** | 0 punto de partida · 1 diseñar · 2 dimensiones · 3 hechos | 45 |
# MAGIC | **Bloque 2 · Capa de métricas** | 4 métricas materializadas · 5 metric views · 6 consistencia y refresco | 55 |
# MAGIC | **Bloque 3 · Cartera y features** | 7 tabla ancha · 8 feature tables · 9 point-in-time · 10 cómo la usa Data Science | 60 |
# MAGIC | Cierre | Checklist | 10 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Introducción
# MAGIC ### Gold se diseña desde el consumidor, no desde Silver
# MAGIC En la Sesión 18 viste las tres formas de Gold. Hoy las construyes **declarativas**: cada tabla es una **materialized view** del
# MAGIC pipeline. No escribes `CREATE OR REPLACE TABLE`, ni `MERGE`, ni `REPLACE WHERE`, ni el job que las refresca: el pipeline
# MAGIC recalcula cada vista cuando cambia lo que lee, **incremental cuando puede**.
# MAGIC
# MAGIC | En la Sesión 18 (notebooks `gold/notebooks/`) | Hoy, en el pipeline |
# MAGIC |---|---|
# MAGIC | `CREATE OR REPLACE TABLE … AS SELECT` en orden: primero dims, después hechos | `CREATE OR REFRESH MATERIALIZED VIEW`: el orden lo saca el grafo |
# MAGIC | Refresco incremental a mano: `INSERT … REPLACE WHERE`, `MERGE … NOT MATCHED BY SOURCE` | El pipeline decide: incremental o recálculo completo |
# MAGIC | Leer Silver `_lab` de otro esquema | Leer las Silver del mismo pipeline por su nombre corto |
# MAGIC | Un job para encadenar todo | El mismo job de la Sesión 19 refresca Bronze → Silver → Gold |
# MAGIC
# MAGIC ### Lo que vas a construir
# MAGIC ```
# MAGIC  Silver (Sesión 19)              Gold · estrella                      Gold · métricas                      Consumo
# MAGIC  paises, campanias, productos ─► dim_pais, dim_campania, dim_producto
# MAGIC  clientes (SCD2)             ─► dim_cliente (una fila por versión)
# MAGIC  tipos_cambio                ─► dim_tipo_cambio, dim_tiempo
# MAGIC  ofertas, solicitudes,       ─► fact_ofertas, fact_solicitudes,  ─► metricas_originacion_mensual    ─► kpi_colocacion (metric view)
# MAGIC  desembolsos                     fact_desembolsos                ─► metricas_colocacion_mensual     ─► Genie (Sesión 20.2)
# MAGIC                                                                   ─► metricas_roi_campania
# MAGIC  eventos_app                 ──────────────────────────────────► metricas_funnel_app_diario      ─► kpi_funnel_app (metric view)
# MAGIC  cuotas, pagos, gestiones    ─► cartera_creditos (tabla ancha)
# MAGIC  pagos, cuotas, gestiones,   ─► features_cliente_mes            (PK id_cliente + disponible_desde TIMESERIES) ─► modelo de mora
# MAGIC  solicitudes, sesiones, clientes
# MAGIC  eventos_app                 ─► features_app_cliente_dia        (PK id_cliente + disponible_desde TIMESERIES) ─► modelo de propensión
# MAGIC ```
# MAGIC Todas las tablas Gold llevan el sufijo `_gld` y viven en el mismo esquema del pipeline.

# COMMAND ----------

# Dónde vive todo: lo mismo que configuraste en el pipeline en la Sesión 19
CATALOGO = "datawizard_dev"
ESQUEMA = "lakeflow"

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def t(nombre):
    """Nombre completo de una tabla del pipeline."""
    return f"{CATALOGO}.{ESQUEMA}.{nombre}"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 1 · Modelo estrella
# MAGIC ## Paso 0 — Punto de partida ▶️
# MAGIC Deben existir las Silver de la Sesión 19 que lee Gold, con datos. Si falta alguna, vuelve a la Sesión 19 antes de seguir.

# COMMAND ----------

SILVER = ["paises_slv", "campanias_slv", "clientes_slv", "productos_slv", "ofertas_preaprobadas_slv", "solicitudes_prestamo_slv",
          "desembolsos_slv", "tipos_cambio_slv", "cuotas_slv", "pagos_slv", "gestiones_cobranza_slv", "eventos_app_slv", "sesiones_app_slv"]
for nombre in SILVER:
    assert spark.catalog.tableExists(t(nombre)), f"Falta {t(nombre)}: termina la Sesión 19 y vuelve"
    print(f"{nombre:28} {spark.table(t(nombre)).count():>9,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Diseñar Gold desde la pregunta 🗣️
# MAGIC | Pregunta del negocio | Grano | Tabla |
# MAGIC |---|---|---|
# MAGIC | ¿Cuánto colocamos por mes y país, en una sola moneda? | mes × país | `metricas_colocacion_mensual_gld` |
# MAGIC | ¿Dónde se cae el funnel de ofertas y qué motor convierte mejor? | mes de la oferta × país × motor | `metricas_originacion_mensual_gld` |
# MAGIC | ¿Qué campaña devuelve más por dólar invertido? | campaña | `metricas_roi_campania_gld` |
# MAGIC | ¿Qué ubicación de la app tiene mejor CTR? | día × canal × ubicación | `metricas_funnel_app_diario_gld` |
# MAGIC | ¿Cómo está la cartera hoy, crédito por crédito? | crédito | `cartera_creditos_gld` |
# MAGIC | Una pregunta nueva que nadie pensó (colocación por nivel de riesgo y producto) | cualquiera | estrella + **metric view** |
# MAGIC
# MAGIC **Pregunta:** el gerente pide "colocación por nivel de riesgo". El cliente 1234 era nivel C cuando se le desembolsó en 2025 y hoy es A.
# MAGIC ¿En qué nivel debe contar ese desembolso?
# MAGIC <details><summary>Ver respuesta</summary>En <b>C</b>: la pregunta es sobre la decisión de crédito que se tomó, y se tomó con el cliente en C. Si el hecho se une a la versión <b>de hoy</b> del cliente, la colocación de 2025 "se mueve" de nivel cada vez que un cliente mejora o empeora, y los reportes de meses cerrados cambian solos. Por eso el hecho guarda <code>sk_cliente</code>: la versión vigente cuando ocurrió (paso 3).</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Dimensiones
# MAGIC Seis materialized views en SQL. Tres cosas para fijarse:
# MAGIC - **`dim_cliente_gld` tiene una fila por versión** (sale de la SCD2 de la Sesión 19) y una llave por versión, `sk_cliente`.
# MAGIC   La primera versión arranca en 1900 para que ningún hecho quede sin cliente.
# MAGIC - **`dim_tiempo_gld` no viene de ninguna fuente**: se genera con `sequence()` + `explode()`.
# MAGIC - **`dim_tipo_cambio_gld`** convierte "una tasa por día publicado" en **rangos de vigencia** con `lead()`. Un fin de semana
# MAGIC   sin publicación usa la última tasa conocida. La usan los hechos, el ROI y la cartera: la regla de conversión vive en un solo lugar.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/11_gold_dimensiones.sql`**
# MAGIC ```sql
# MAGIC -- Sesión 20 · Paso 2 · Gold: las dimensiones del modelo estrella, como MATERIALIZED VIEWS en SQL.
# MAGIC -- Gold une y agrega datasets completos: se lee en batch (sin STREAM) y el pipeline recalcula lo que cambió.
# MAGIC -- Un pipeline mezcla .py y .sql: estas vistas leen las Silver de la Sesión 19 por su nombre corto.
# MAGIC -- Nombres del negocio (pais, no nombre_pais) y sin columnas técnicas ni datos personales.
# MAGIC
# MAGIC -- País: 3 filas. La moneda une a los desembolsos con su tipo de cambio
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_pais_gld
# MAGIC COMMENT 'Dimensión país: una fila por país (Perú, Colombia, Bolivia) con su moneda local (PEN, COP, BOB)'
# MAGIC AS SELECT
# MAGIC   id_pais,
# MAGIC   codigo_iso,
# MAGIC   nombre_pais   AS pais,
# MAGIC   moneda_codigo AS moneda
# MAGIC FROM paises_slv;
# MAGIC
# MAGIC -- Campaña de marketing. El presupuesto queda en moneda local: la conversión a USD se hace en la métrica de ROI
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_campania_gld
# MAGIC COMMENT 'Dimensión campaña: una fila por campaña de marketing. presupuesto_local está en la moneda del país de la campaña'
# MAGIC AS SELECT
# MAGIC   id_campania,
# MAGIC   id_pais,
# MAGIC   nombre_campania,
# MAGIC   tipo_campania,
# MAGIC   fecha_inicio,
# MAGIC   fecha_fin,
# MAGIC   presupuesto AS presupuesto_local
# MAGIC FROM campanias_slv;
# MAGIC
# MAGIC -- Producto: de la SCD2 de Silver se publica solo la versión vigente (las condiciones de hoy)
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_producto_gld
# MAGIC COMMENT 'Dimensión producto: una fila por producto de crédito con sus condiciones vigentes hoy (montos en moneda local)'
# MAGIC AS SELECT
# MAGIC   id_producto,
# MAGIC   id_pais,
# MAGIC   nombre_producto,
# MAGIC   tipo_producto,
# MAGIC   monto_minimo,
# MAGIC   monto_maximo,
# MAGIC   plazo_min_meses,
# MAGIC   plazo_max_meses,
# MAGIC   tasa_interes_anual,
# MAGIC   es_activo
# MAGIC FROM productos_slv
# MAGIC WHERE __END_AT IS NULL;
# MAGIC
# MAGIC -- Calendario: no viene de ninguna fuente, se genera. id_fecha (AAAAMMDD) es la llave que usan los hechos
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_tiempo_gld
# MAGIC COMMENT 'Dimensión calendario: un día por fila, de 2024 a 2027'
# MAGIC AS SELECT
# MAGIC   CAST(date_format(fecha, 'yyyyMMdd') AS INT) AS id_fecha,
# MAGIC   fecha,
# MAGIC   year(fecha)                   AS anio,
# MAGIC   quarter(fecha)                AS trimestre,
# MAGIC   month(fecha)                  AS mes,
# MAGIC   trunc(fecha, 'MM')            AS primer_dia_mes,
# MAGIC   date_format(fecha, 'yyyy-MM') AS anio_mes,
# MAGIC   dayofweek(fecha) IN (1, 7)    AS es_fin_de_semana
# MAGIC FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS fecha);
# MAGIC
# MAGIC -- Cliente con historia: una fila por VERSIÓN (la SCD2 de clientes_slv), con una llave por versión: sk_cliente.
# MAGIC -- Los hechos guardan el sk_cliente vigente cuando ocurrieron: así "colocación por nivel de riesgo" usa el riesgo
# MAGIC -- que tenía el cliente al desembolsar, no el de hoy.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_cliente_gld
# MAGIC COMMENT 'Dimensión cliente con historia (SCD2): una fila por versión del cliente, sin datos personales. Para el estado de hoy filtra es_vigente'
# MAGIC CLUSTER BY (id_cliente)
# MAGIC AS SELECT
# MAGIC   xxhash64(id_cliente, __START_AT.fecha_version, __START_AT._ingestado_en) AS sk_cliente,
# MAGIC   id_cliente,
# MAGIC   id_pais,
# MAGIC   ciudad,
# MAGIC   situacion_laboral,
# MAGIC   ingreso_mensual_declarado,
# MAGIC   score_interno,
# MAGIC   nivel_riesgo,
# MAGIC   CASE WHEN score_interno >= 700 THEN 'Alto'
# MAGIC        WHEN score_interno >= 600 THEN 'Medio'
# MAGIC        ELSE 'Bajo' END                                   AS rango_score,
# MAGIC   id_campania_captacion,
# MAGIC   fecha_registro,
# MAGIC   -- La primera versión cubre desde siempre: un hecho anterior a la primera fecha_version igual encuentra cliente
# MAGIC   CASE WHEN row_number() OVER (PARTITION BY id_cliente ORDER BY __START_AT) = 1 THEN TIMESTAMP'1900-01-01'
# MAGIC        ELSE __START_AT.fecha_version END                 AS vigente_desde,
# MAGIC   __END_AT.fecha_version                                 AS vigente_hasta,      -- NULL = versión vigente
# MAGIC   __END_AT IS NULL                                       AS es_vigente
# MAGIC FROM clientes_slv;
# MAGIC
# MAGIC -- Tipo de cambio a USD con su rango de vigencia: un día sin publicación usa la última tasa conocida.
# MAGIC -- Lo usan los hechos (monto_usd), el ROI de campañas y la cartera.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_tipo_cambio_gld
# MAGIC COMMENT 'Tipo de cambio de cada moneda a USD con su rango de vigencia [vigente_desde, vigente_hasta). Dividir un monto local por tasa_venta da USD'
# MAGIC AS SELECT
# MAGIC   moneda_origen                                                       AS moneda,
# MAGIC   fecha                                                               AS vigente_desde,
# MAGIC   lead(fecha) OVER (PARTITION BY moneda_origen ORDER BY fecha)        AS vigente_hasta,   -- NULL = tasa vigente hoy
# MAGIC   tasa_venta
# MAGIC FROM tipos_cambio_slv
# MAGIC WHERE moneda_destino = 'USD';
# MAGIC ```
# MAGIC **Run pipeline.** En el grafo aparecen las 6 dimensiones colgando de sus Silver.

# COMMAND ----------

display(spark.sql(f"""
  SELECT count(*) AS versiones, count(DISTINCT id_cliente) AS clientes, count(DISTINCT sk_cliente) AS sk_distintos,
         count_if(es_vigente) AS vigentes
  FROM {t('dim_cliente_gld')}"""))
# Esperado: sk_distintos = versiones (la llave no se repite) y vigentes = clientes (una versión vigente por cliente)

# COMMAND ----------

# Los rangos del tipo de cambio no se pisan: cada día cae en una sola tasa por moneda
display(spark.sql(f"""
  SELECT moneda, count(*) AS rangos, min(vigente_desde) AS desde, max(vigente_desde) AS ultima_publicacion,
         round(avg(tasa_venta), 4) AS tasa_promedio
  FROM {t('dim_tipo_cambio_gld')} GROUP BY moneda ORDER BY moneda"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Hechos: el join punto en el tiempo
# MAGIC Cada hecho busca la versión del cliente vigente **cuando ocurrió**:
# MAGIC ```sql
# MAGIC ON c.id_cliente = d.id_cliente
# MAGIC AND d.fecha_hora_desembolso >= c.vigente_desde
# MAGIC AND (c.vigente_hasta IS NULL OR d.fecha_hora_desembolso < c.vigente_hasta)     -- rango [desde, hasta)
# MAGIC ```
# MAGIC El mismo patrón, contra `dim_tipo_cambio_gld`, lleva cada desembolso a USD con la tasa **de ese día**. Es el `LEFT JOIN LATERAL` de
# MAGIC la Sesión 18 escrito como join por rango, que en una materialized view se refresca mejor.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/12_gold_hechos.sql`**
# MAGIC ```sql
# MAGIC -- Sesión 20 · Paso 3 · Gold: los hechos del modelo estrella.
# MAGIC -- Cada hecho es un evento del negocio (una oferta, una solicitud, un desembolso) con:
# MAGIC --   - id_fecha       → dim_tiempo_gld
# MAGIC --   - sk_cliente     → la VERSIÓN de dim_cliente_gld vigente cuando ocurrió el hecho (join punto-en-el-tiempo)
# MAGIC --   - las medidas    → montos, tasas, banderas 0/1 que se suman (es_aprobada, es_completado)
# MAGIC -- Las Silver de solicitudes y desembolsos son SCD tipo 1: el hecho trae su ÚLTIMO estado.
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW fact_ofertas_gld
# MAGIC COMMENT 'Hecho: una fila por oferta preaprobada asignada a un cliente, con el cliente tal como era al recibirla'
# MAGIC CLUSTER BY (id_fecha)
# MAGIC AS SELECT
# MAGIC   o.id_oferta,
# MAGIC   CAST(date_format(o.fecha_generacion, 'yyyyMMdd') AS INT)    AS id_fecha,
# MAGIC   c.sk_cliente,
# MAGIC   o.id_cliente,
# MAGIC   c.id_pais,
# MAGIC   o.id_producto,
# MAGIC   o.id_campania,
# MAGIC   o.motor_asignacion,
# MAGIC   o.estado_oferta,
# MAGIC   CASE WHEN o.estado_oferta = 'Aceptada' THEN 1 ELSE 0 END    AS es_aceptada,
# MAGIC   o.monto_ofertado,
# MAGIC   o.plazo_meses_ofertado,
# MAGIC   o.tasa_ofertada
# MAGIC FROM ofertas_preaprobadas_slv o
# MAGIC LEFT JOIN dim_cliente_gld c
# MAGIC   ON  c.id_cliente = o.id_cliente
# MAGIC   AND o.fecha_generacion >= c.vigente_desde
# MAGIC   AND (c.vigente_hasta IS NULL OR o.fecha_generacion < c.vigente_hasta);
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW fact_solicitudes_gld
# MAGIC COMMENT 'Hecho: una fila por solicitud de préstamo con su último estado, y el cliente tal como era al solicitar'
# MAGIC CLUSTER BY (id_fecha)
# MAGIC AS SELECT
# MAGIC   s.id_solicitud,
# MAGIC   CAST(date_format(s.fecha_hora_solicitud, 'yyyyMMdd') AS INT) AS id_fecha,
# MAGIC   c.sk_cliente,
# MAGIC   s.id_cliente,
# MAGIC   c.id_pais,
# MAGIC   s.id_oferta,
# MAGIC   s.id_producto,
# MAGIC   s.canal,
# MAGIC   s.estado_solicitud,
# MAGIC   CASE WHEN s.estado_solicitud = 'Aprobada' THEN 1 ELSE 0 END  AS es_aprobada,
# MAGIC   s.monto_solicitado,
# MAGIC   s.monto_aprobado,
# MAGIC   s.plazo_meses_solicitado,
# MAGIC   s.tasa_aprobada,
# MAGIC   s.score_evaluacion,
# MAGIC   s.motivo_rechazo,
# MAGIC   round((unix_timestamp(s.fecha_hora_resolucion) - unix_timestamp(s.fecha_hora_solicitud)) / 3600, 1) AS horas_a_resolucion
# MAGIC FROM solicitudes_prestamo_slv s
# MAGIC LEFT JOIN dim_cliente_gld c
# MAGIC   ON  c.id_cliente = s.id_cliente
# MAGIC   AND s.fecha_hora_solicitud >= c.vigente_desde
# MAGIC   AND (c.vigente_hasta IS NULL OR s.fecha_hora_solicitud < c.vigente_hasta);
# MAGIC
# MAGIC -- El desembolso en USD: se divide por la tasa vigente ESE día (dim_tipo_cambio_gld tiene el rango de vigencia).
# MAGIC -- La expectation solo mide: un desembolso sin tasa sigue contando en moneda local.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW fact_desembolsos_gld (
# MAGIC   CONSTRAINT con_tipo_cambio EXPECT (monto_usd IS NOT NULL)
# MAGIC )
# MAGIC COMMENT 'Hecho: una fila por desembolso con el monto en moneda local y en USD. Solo los de estado Completado son colocación'
# MAGIC CLUSTER BY (id_fecha)
# MAGIC AS SELECT
# MAGIC   d.id_desembolso,
# MAGIC   d.id_solicitud,
# MAGIC   CAST(date_format(d.fecha_hora_desembolso, 'yyyyMMdd') AS INT) AS id_fecha,
# MAGIC   CAST(d.fecha_hora_desembolso AS DATE)                          AS fecha_desembolso,
# MAGIC   c.sk_cliente,
# MAGIC   d.id_cliente,
# MAGIC   p.id_pais,
# MAGIC   s.id_producto,
# MAGIC   d.moneda,
# MAGIC   d.estado_desembolso,
# MAGIC   CASE WHEN d.estado_desembolso = 'Completado' THEN 1 ELSE 0 END AS es_completado,
# MAGIC   d.monto_desembolsado,
# MAGIC   tc.tasa_venta                                                  AS tipo_cambio_usd,
# MAGIC   round(d.monto_desembolsado / tc.tasa_venta, 2)                 AS monto_usd,
# MAGIC   d.comision_cobrada,
# MAGIC   round(d.comision_cobrada / tc.tasa_venta, 2)                   AS comision_usd,
# MAGIC   d.tasa_aplicada,
# MAGIC   d.plazo_meses
# MAGIC FROM desembolsos_slv d
# MAGIC LEFT JOIN dim_pais_gld p                ON p.moneda = d.moneda
# MAGIC LEFT JOIN solicitudes_prestamo_slv s    ON s.id_solicitud = d.id_solicitud
# MAGIC LEFT JOIN dim_cliente_gld c
# MAGIC   ON  c.id_cliente = d.id_cliente
# MAGIC   AND d.fecha_hora_desembolso >= c.vigente_desde
# MAGIC   AND (c.vigente_hasta IS NULL OR d.fecha_hora_desembolso < c.vigente_hasta)
# MAGIC LEFT JOIN dim_tipo_cambio_gld tc
# MAGIC   ON  tc.moneda = d.moneda
# MAGIC   AND CAST(d.fecha_hora_desembolso AS DATE) >= tc.vigente_desde
# MAGIC   AND (tc.vigente_hasta IS NULL OR CAST(d.fecha_hora_desembolso AS DATE) < tc.vigente_hasta);
# MAGIC ```
# MAGIC **Run pipeline** y abre `fact_desembolsos_gld` → **Data quality**: `con_tipo_cambio` cuenta los desembolsos sin tasa.

# COMMAND ----------

# Cada hecho encontró exactamente una versión del cliente (sin perder ni duplicar filas en el join por rango)
display(spark.sql(f"""
  SELECT 'fact_desembolsos_gld' AS hecho, (SELECT count(*) FROM {t('desembolsos_slv')}) AS filas_silver,
         count(*) AS filas_gold, count_if(sk_cliente IS NULL) AS sin_cliente, count_if(monto_usd IS NULL) AS sin_usd
  FROM {t('fact_desembolsos_gld')}
  UNION ALL
  SELECT 'fact_solicitudes_gld', (SELECT count(*) FROM {t('solicitudes_prestamo_slv')}), count(*), count_if(sk_cliente IS NULL), NULL
  FROM {t('fact_solicitudes_gld')}"""))

# COMMAND ----------

# El riesgo AL DESEMBOLSAR vs el riesgo de HOY: los desembolsos donde no coinciden son los que un join ingenuo mueve de nivel
display(spark.sql(f"""
  SELECT al_desembolso.nivel_riesgo AS riesgo_al_desembolsar, hoy.nivel_riesgo AS riesgo_hoy, count(*) AS desembolsos
  FROM {t('fact_desembolsos_gld')} d
  JOIN {t('dim_cliente_gld')} al_desembolso ON al_desembolso.sk_cliente = d.sk_cliente
  JOIN {t('dim_cliente_gld')} hoy           ON hoy.id_cliente = d.id_cliente AND hoy.es_vigente
  WHERE al_desembolso.nivel_riesgo <> hoy.nivel_riesgo
  GROUP BY ALL ORDER BY desembolsos DESC LIMIT 10"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿por qué `fact_desembolsos_gld` guarda `es_completado` (0/1) si ya tiene `estado_desembolso`?
# MAGIC <details><summary>Ver respuesta</summary>Porque una bandera 0/1 <b>se suma</b>: <code>sum(es_completado)</code> cuenta desembolsos completados a cualquier grano, sin repetir el <code>CASE</code> en cada consulta. Es la misma idea de <code>es_aprobada</code> y <code>es_aceptada</code>: el hecho trae las medidas listas para agregar.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 2 · Capa de métricas
# MAGIC ## Paso 4 — Métricas materializadas
# MAGIC Cada tabla responde una pregunta del paso 1 a un **grano fijo**. Regla: se guardan las **bases aditivas** (conteos, sumas) y
# MAGIC los ratios se calculan de ellas.
# MAGIC
# MAGIC | Tipo de métrica | Ejemplo | Cómo se agrega a otro grano |
# MAGIC |---|---|---|
# MAGIC | Aditiva | `desembolsos`, `monto_colocado_usd` | `SUM` |
# MAGIC | Semi-aditiva | `clientes_con_desembolso` (distintos) | **No** se suma entre meses: un cliente que repite cuenta dos veces |
# MAGIC | No aditiva | `ticket_promedio_usd`, `ctr`, `tasa_aprobacion` | Se recalcula: `sum(monto) / sum(desembolsos)`, nunca `avg(ticket)` |
# MAGIC
# MAGIC Fíjate en dos granos que parecen iguales y no lo son: `metricas_originacion_mensual_gld` atribuye la colocación al **mes de la
# MAGIC oferta**; `metricas_colocacion_mensual_gld`, al **mes del desembolso**. Las dos están bien; responden preguntas distintas.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/13_gold_metricas.sql`**
# MAGIC ```sql
# MAGIC -- Sesión 20 · Paso 4 · Gold: la capa de métricas materializada.
# MAGIC -- Cada tabla responde una pregunta del negocio a un GRANO fijo y la deja precalculada: un dashboard solo la lee.
# MAGIC -- Regla de diseño: se guardan las BASES aditivas (conteos, sumas) y los ratios se calculan de ellas.
# MAGIC -- Quien agregue a otro grano debe sumar las bases y recalcular el ratio: nunca promediar ratios.
# MAGIC
# MAGIC -- Funnel de originación: oferta → solicitud → aprobada → desembolso. Grano: mes de la OFERTA x país x motor.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW metricas_originacion_mensual_gld
# MAGIC COMMENT 'KPI de originación: funnel oferta → solicitud → aprobación → desembolso. Grano: mes de la oferta x país x motor de asignación'
# MAGIC AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     trunc(t.fecha, 'MM')                                              AS mes,
# MAGIC     coalesce(p.pais, 'desconocido')                       AS pais,
# MAGIC     coalesce(o.motor_asignacion, 'desconocido')           AS motor_asignacion,
# MAGIC     count(*)                                                          AS ofertas,
# MAGIC     sum(o.es_aceptada)                                                AS ofertas_aceptadas,
# MAGIC     count(s.id_solicitud)                                             AS solicitudes,
# MAGIC     coalesce(sum(s.es_aprobada), 0)                                   AS solicitudes_aprobadas,
# MAGIC     count_if(d.es_completado = 1)                                     AS desembolsos,
# MAGIC     coalesce(sum(d.monto_usd) FILTER (WHERE d.es_completado = 1), 0)  AS monto_colocado_usd
# MAGIC   FROM fact_ofertas_gld o
# MAGIC   JOIN dim_tiempo_gld t                ON t.id_fecha = o.id_fecha
# MAGIC   LEFT JOIN dim_pais_gld p             ON p.id_pais = o.id_pais
# MAGIC   LEFT JOIN fact_solicitudes_gld s     ON s.id_oferta = o.id_oferta
# MAGIC   LEFT JOIN fact_desembolsos_gld d     ON d.id_solicitud = s.id_solicitud
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   round(try_divide(solicitudes, ofertas), 4)                AS tasa_oferta_a_solicitud,
# MAGIC   round(try_divide(solicitudes_aprobadas, solicitudes), 4)  AS tasa_aprobacion,
# MAGIC   round(try_divide(desembolsos, ofertas), 4)                AS conversion_end_to_end
# MAGIC FROM base;
# MAGIC
# MAGIC -- Colocación: lo desembolsado (solo Completado) en USD. Grano: mes del DESEMBOLSO x país.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW metricas_colocacion_mensual_gld
# MAGIC COMMENT 'KPI de colocación consolidada en USD (solo desembolsos Completados). Grano: mes del desembolso x país'
# MAGIC AS SELECT
# MAGIC   trunc(d.fecha_desembolso, 'MM')                                          AS mes,
# MAGIC   p.pais,
# MAGIC   count(*)                                                                 AS desembolsos,
# MAGIC   round(sum(d.monto_usd), 2)                                               AS monto_colocado_usd,
# MAGIC   round(sum(d.comision_usd), 2)                                            AS comision_usd,
# MAGIC   round(try_divide(sum(d.monto_usd), count(*)), 2)                         AS ticket_promedio_usd,
# MAGIC   round(try_divide(sum(d.tasa_aplicada * d.monto_usd), sum(d.monto_usd)), 3) AS tasa_promedio_ponderada
# MAGIC FROM fact_desembolsos_gld d
# MAGIC JOIN dim_pais_gld p ON p.id_pais = d.id_pais
# MAGIC WHERE d.es_completado = 1
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC -- Retorno de campañas: USD colocado por los clientes que captó cada campaña, por cada USD de presupuesto.
# MAGIC -- El presupuesto se convierte con la tasa vigente el día de inicio de la campaña. Grano: una fila por campaña.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW metricas_roi_campania_gld
# MAGIC COMMENT 'KPI de retorno por campaña: USD colocado por los clientes captados por cada USD invertido. Grano: campaña'
# MAGIC AS
# MAGIC WITH colocado AS (
# MAGIC   SELECT c.id_campania_captacion          AS id_campania,
# MAGIC          count(DISTINCT d.id_cliente)     AS clientes_con_desembolso,
# MAGIC          count(*)                         AS desembolsos,
# MAGIC          sum(d.monto_usd)                 AS monto_colocado_usd
# MAGIC   FROM fact_desembolsos_gld d
# MAGIC   JOIN dim_cliente_gld c ON c.sk_cliente = d.sk_cliente
# MAGIC   WHERE d.es_completado = 1
# MAGIC   GROUP BY ALL
# MAGIC ),
# MAGIC presupuesto AS (
# MAGIC   SELECT ca.id_campania, ca.nombre_campania, ca.tipo_campania, p.pais, ca.fecha_inicio,
# MAGIC          round(ca.presupuesto_local / tc.tasa_venta, 2) AS presupuesto_usd
# MAGIC   FROM dim_campania_gld ca
# MAGIC   JOIN dim_pais_gld p ON p.id_pais = ca.id_pais
# MAGIC   LEFT JOIN dim_tipo_cambio_gld tc
# MAGIC     ON  tc.moneda = p.moneda
# MAGIC     AND ca.fecha_inicio >= tc.vigente_desde
# MAGIC     AND (tc.vigente_hasta IS NULL OR ca.fecha_inicio < tc.vigente_hasta)
# MAGIC )
# MAGIC SELECT
# MAGIC   pr.id_campania,
# MAGIC   pr.nombre_campania,
# MAGIC   pr.tipo_campania,
# MAGIC   pr.pais,
# MAGIC   pr.fecha_inicio,
# MAGIC   pr.presupuesto_usd,
# MAGIC   coalesce(co.clientes_con_desembolso, 0)                         AS clientes_con_desembolso,
# MAGIC   coalesce(co.desembolsos, 0)                                     AS desembolsos,
# MAGIC   round(coalesce(co.monto_colocado_usd, 0), 2)                    AS monto_colocado_usd,
# MAGIC   round(try_divide(co.monto_colocado_usd, pr.presupuesto_usd), 2) AS usd_colocado_por_usd_invertido,
# MAGIC   rank() OVER (ORDER BY try_divide(co.monto_colocado_usd, pr.presupuesto_usd) DESC NULLS LAST) AS ranking_roi
# MAGIC FROM presupuesto pr
# MAGIC LEFT JOIN colocado co ON co.id_campania = pr.id_campania;
# MAGIC
# MAGIC -- Funnel de la app: eventos de Kafka por día, canal y ubicación en pantalla.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW metricas_funnel_app_diario_gld
# MAGIC COMMENT 'KPI de funnel de la app: vistas, clicks, simulaciones y solicitudes. Grano: fecha x canal x ubicación en pantalla'
# MAGIC CLUSTER BY (fecha)
# MAGIC AS SELECT
# MAGIC   fecha_evento                                                AS fecha,
# MAGIC   coalesce(canal, 'desconocido')                              AS canal,
# MAGIC   coalesce(ubicacion_pantalla, 'desconocida')                 AS ubicacion_pantalla,
# MAGIC   count_if(tipo_evento = 'oferta_vista')                      AS vistas,
# MAGIC   count_if(tipo_evento = 'oferta_click')                      AS clicks,
# MAGIC   count_if(tipo_evento = 'simulacion_realizada')              AS simulaciones,
# MAGIC   count_if(tipo_evento = 'solicitud_iniciada')                AS solicitudes_iniciadas,
# MAGIC   count_if(tipo_evento = 'banner_cerrado')                    AS banners_cerrados,
# MAGIC   round(try_divide(count_if(tipo_evento = 'oferta_click'), count_if(tipo_evento = 'oferta_vista')), 4) AS ctr
# MAGIC FROM eventos_app_slv
# MAGIC GROUP BY ALL;
# MAGIC ```
# MAGIC **Run pipeline.**

# COMMAND ----------

# Prueba de consistencia: la métrica suma lo mismo que el hecho. Si no cuadra, el KPI está mal definido
display(spark.sql(f"""
  SELECT (SELECT round(sum(monto_colocado_usd), 2) FROM {t('metricas_colocacion_mensual_gld')})              AS metrica,
         (SELECT round(sum(monto_usd), 2) FROM {t('fact_desembolsos_gld')} WHERE es_completado = 1)          AS hecho,
         (SELECT round(sum(monto_colocado_usd), 2) FROM {t('metricas_originacion_mensual_gld')})             AS originacion"""))
# metrica = hecho. originacion casi cuadra en el total, pero mes a mes NO: pone cada desembolso en el mes de su OFERTA
# (y deja fuera las solicitudes que no vinieron de una oferta). Mismo dato, otra pregunta: por eso cada tabla dice su grano

# COMMAND ----------

# El error que la capa de métricas debe evitar: promediar promedios
display(spark.sql(f"""
  SELECT pais,
         round(avg(ticket_promedio_usd), 2)                                AS avg_de_tickets_mensuales,   -- ❌
         round(sum(monto_colocado_usd) / sum(desembolsos), 2)              AS ticket_real                 -- ✅
  FROM {t('metricas_colocacion_mensual_gld')} GROUP BY pais ORDER BY pais"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT ranking_roi, nombre_campania, tipo_campania, pais, presupuesto_usd, monto_colocado_usd, usd_colocado_por_usd_invertido
  FROM {t('metricas_roi_campania_gld')} ORDER BY ranking_roi LIMIT 10"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Metric views: el KPI se define una vez y se consulta con cualquier corte
# MAGIC Las tablas del paso 4 responden las preguntas **que ya pensamos**. ¿Y "colocación por producto y nivel de riesgo en 2026"?
# MAGIC Una **metric view** de Unity Catalog separa:
# MAGIC - las **dimensiones** (cómo se corta): mes, país, producto, nivel de riesgo…
# MAGIC - las **medidas** (qué se calcula): monto colocado, ticket, tasa ponderada…
# MAGIC
# MAGIC Se consulta con `MEASURE(<medida>)` y `GROUP BY` por las dimensiones que quieras, y Databricks recalcula **bien** cada ratio para ese corte.
# MAGIC Es la definición oficial del KPI: la usan el SQL editor, los dashboards AI/BI y **Genie** (Sesión 20.2).
# MAGIC
# MAGIC **No es parte del pipeline:** el pipeline publica las tablas, y la metric view se crea con SQL encima de ellas. El código vive en
# MAGIC `lakeflow/metric_views/` del repo, versionado como cualquier otro. Las celdas de abajo lo ejecutan con tu catálogo y esquema.
# MAGIC
# MAGIC | Archivo | Fuente | Qué enseña |
# MAGIC |---|---|---|
# MAGIC | `kpi_colocacion.sql` | `fact_desembolsos_gld` + joins a 3 dimensiones | Modelo estrella dentro de la metric view; medidas con `FILTER`; ratios con `MEASURE()` |
# MAGIC | `kpi_funnel_app.sql` | `metricas_funnel_app_diario_gld` | Metric view **sobre una métrica materializada**: suma las bases, divide al final |
# MAGIC
# MAGIC > Requiere cómputo serverless o un SQL warehouse (DBR 17.2+). Si la celda falla por versión, ejecuta el mismo SQL en el **SQL editor**.

# COMMAND ----------

# Fuente: lakeflow/metric_views/kpi_colocacion.sql (generado: no editar aquí)
sql = """
-- Sesión 20 · Paso 5 · Metric View: los KPIs de colocación, definidos una vez y consultables con cualquier corte.
-- No es parte del pipeline: se crea con SQL (SQL editor o notebook en serverless) sobre sus tablas Gold.
-- Las MEDIDAS son agregados (SUM, COUNT); los ratios se componen con MEASURE() y Databricks los recalcula bien
-- para cada corte. Así nadie promedia tickets ni mezcla monedas.
-- Para consultarla:  SELECT pais, MEASURE(monto_colocado_usd) FROM datawizard_dev.lakeflow.kpi_colocacion GROUP BY pais
CREATE OR REPLACE VIEW datawizard_dev.lakeflow.kpi_colocacion
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "KPIs de colocación de créditos de Wizard Bank (datos sintéticos). Un desembolso es colocación solo si está Completado. Montos en USD con la tasa del día del desembolso. El nivel de riesgo es el que tenía el cliente al desembolsar."
source: datawizard_dev.lakeflow.fact_desembolsos_gld
joins:
  - name: dim_pais
    source: datawizard_dev.lakeflow.dim_pais_gld
    'on': source.id_pais = dim_pais.id_pais
  - name: dim_cliente
    source: datawizard_dev.lakeflow.dim_cliente_gld
    'on': source.sk_cliente = dim_cliente.sk_cliente
  - name: dim_producto
    source: datawizard_dev.lakeflow.dim_producto_gld
    'on': source.id_producto = dim_producto.id_producto
dimensions:
  - name: mes
    expr: "trunc(source.fecha_desembolso, 'MM')"
    comment: "Mes del desembolso, como su día 1"
  - name: anio
    expr: year(source.fecha_desembolso)
    comment: "Año del desembolso"
  - name: pais
    expr: dim_pais.pais
    comment: "País del desembolso: Perú, Colombia o Bolivia"
  - name: producto
    expr: dim_producto.nombre_producto
    comment: "Producto de crédito"
  - name: tipo_producto
    expr: dim_producto.tipo_producto
    comment: "Libre disponibilidad, Consumo o Educativo"
  - name: nivel_riesgo
    expr: dim_cliente.nivel_riesgo
    comment: "Nivel de riesgo A (mejor) a E del cliente al momento del desembolso"
  - name: rango_score
    expr: dim_cliente.rango_score
    comment: "Rango del score interno al desembolsar: Alto (700+), Medio (600-699), Bajo"
  - name: situacion_laboral
    expr: dim_cliente.situacion_laboral
    comment: "Situación laboral del cliente al desembolsar"
  - name: estado_desembolso
    expr: source.estado_desembolso
    comment: "Completado, Fallido o Reversado. Las medidas de colocación ya filtran Completado"
measures:
  - name: desembolsos
    expr: COUNT(1) FILTER (WHERE source.es_completado = 1)
    comment: "Cantidad de desembolsos completados"
  - name: monto_colocado_usd
    expr: SUM(source.monto_usd) FILTER (WHERE source.es_completado = 1)
    comment: "Monto colocado en USD: suma de los desembolsos completados"
  - name: comision_usd
    expr: SUM(source.comision_usd) FILTER (WHERE source.es_completado = 1)
    comment: "Comisiones cobradas en USD en los desembolsos completados"
  - name: clientes_con_desembolso
    expr: COUNT(DISTINCT source.id_cliente) FILTER (WHERE source.es_completado = 1)
    comment: "Clientes distintos con al menos un desembolso completado"
  - name: tasa_x_monto
    expr: SUM(source.tasa_aplicada * source.monto_usd) FILTER (WHERE source.es_completado = 1)
    comment: "Auxiliar para la tasa ponderada: tasa anual por monto en USD"
  - name: ticket_promedio_usd
    expr: MEASURE(monto_colocado_usd) / MEASURE(desembolsos)
    comment: "Monto promedio por desembolso completado, en USD"
  - name: tasa_promedio_ponderada
    expr: MEASURE(tasa_x_monto) / MEASURE(monto_colocado_usd)
    comment: "Tasa anual promedio ponderada por monto colocado (porcentaje, ej. 28.5)"
  - name: pct_no_completados
    expr: COUNT(1) FILTER (WHERE source.es_completado = 0) / COUNT(1)
    comment: "Proporción de desembolsos Fallidos o Reversados sobre todos los intentos (0 a 1)"
$$
"""
spark.sql(sql.replace("datawizard_dev.lakeflow", f"{CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

# Fuente: lakeflow/metric_views/kpi_funnel_app.sql (generado: no editar aquí)
sql = """
-- Sesión 20 · Paso 5 · Metric View sobre una métrica materializada: el funnel de la app.
-- metricas_funnel_app_diario_gld ya guarda las BASES por día (vistas, clicks…). La metric view las suma al corte que
-- se pida y recién ahí divide: el CTR de un mes sale de sum(clicks)/sum(vistas), no del promedio de los CTR diarios.
CREATE OR REPLACE VIEW datawizard_dev.lakeflow.kpi_funnel_app
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "KPIs del funnel de la app de Wizard Bank (datos sintéticos): de ver una oferta a iniciar una solicitud, por día, canal y ubicación en pantalla."
source: datawizard_dev.lakeflow.metricas_funnel_app_diario_gld
dimensions:
  - name: fecha
    expr: source.fecha
    comment: "Día del evento"
  - name: mes
    expr: "trunc(source.fecha, 'MM')"
    comment: "Mes del evento, como su día 1"
  - name: canal
    expr: source.canal
    comment: "APP_ANDROID, APP_IOS o WEB"
  - name: ubicacion_pantalla
    expr: source.ubicacion_pantalla
    comment: "Dónde se mostró la oferta: home_banner, home_card, push o detalle_oferta"
measures:
  - name: vistas
    expr: SUM(source.vistas)
    comment: "Ofertas vistas"
  - name: clicks
    expr: SUM(source.clicks)
    comment: "Clicks en ofertas"
  - name: simulaciones
    expr: SUM(source.simulaciones)
    comment: "Simulaciones de crédito"
  - name: solicitudes_iniciadas
    expr: SUM(source.solicitudes_iniciadas)
    comment: "Solicitudes iniciadas desde la app"
  - name: ctr
    expr: MEASURE(clicks) / MEASURE(vistas)
    comment: "Click-through rate: clicks / vistas (0 a 1)"
  - name: tasa_click_a_simulacion
    expr: MEASURE(simulaciones) / MEASURE(clicks)
    comment: "Simulaciones por click (0 a 1)"
  - name: conversion_vista_a_solicitud
    expr: MEASURE(solicitudes_iniciadas) / MEASURE(vistas)
    comment: "Solicitudes iniciadas por oferta vista (0 a 1)"
$$
"""
spark.sql(sql.replace("datawizard_dev.lakeflow", f"{CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

# Una pregunta que ninguna tabla del paso 4 responde: colocación 2026 por producto y nivel de riesgo AL DESEMBOLSAR
display(spark.sql(f"""
  SELECT producto, nivel_riesgo,
         MEASURE(desembolsos)                       AS desembolsos,
         round(MEASURE(monto_colocado_usd))         AS monto_colocado_usd,
         round(MEASURE(ticket_promedio_usd), 2)     AS ticket_promedio_usd,
         round(MEASURE(tasa_promedio_ponderada), 2) AS tasa_ponderada
  FROM {t('kpi_colocacion')}
  WHERE anio = 2026
  GROUP BY producto, nivel_riesgo
  ORDER BY producto, nivel_riesgo"""))

# COMMAND ----------

# El CTR de cada ubicación, calculado como sum(clicks)/sum(vistas) del período, sin escribir la división
display(spark.sql(f"""
  SELECT ubicacion_pantalla, MEASURE(vistas) AS vistas, MEASURE(clicks) AS clicks,
         round(MEASURE(ctr), 4) AS ctr, round(MEASURE(conversion_vista_a_solicitud), 4) AS conversion
  FROM {t('kpi_funnel_app')}
  GROUP BY ubicacion_pantalla ORDER BY ctr DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ **Míralo en la UI:** Catalog Explorer → `kpi_colocacion` → pestaña **Overview**: aparece como *Metric view*, con sus
# MAGIC dimensiones, medidas y comentarios. **Lineage** muestra las tablas Gold que lee.
# MAGIC
# MAGIC **Pregunta:** ¿por qué `kpi_funnel_app` lee la tabla diaria y no `eventos_app_slv` directamente?
# MAGIC <details><summary>Ver respuesta</summary>Porque la tabla diaria ya resolvió lo caro (contar millones de eventos) y la metric view solo suma unas pocas filas por día. Es el reparto de trabajo de la capa de métricas: el pipeline <b>materializa las bases</b>; la metric view <b>define cómo se combinan</b>. Si leyera eventos, cada pregunta de un dashboard o de Genie recorrería todos los eventos.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Consistencia y refresco de la capa de métricas ▶️
# MAGIC Dos caminos al mismo número deben dar lo mismo. Si la tabla materializada y la metric view no cuadran, hay **dos definiciones** del KPI.

# COMMAND ----------

display(spark.sql(f"""
  WITH tabla AS (SELECT pais, sum(desembolsos) AS desembolsos, round(sum(monto_colocado_usd), 2) AS monto
                 FROM {t('metricas_colocacion_mensual_gld')} GROUP BY pais),
       vista AS (SELECT pais, MEASURE(desembolsos) AS desembolsos, round(MEASURE(monto_colocado_usd), 2) AS monto
                 FROM {t('kpi_colocacion')} GROUP BY pais)
  SELECT tabla.pais, tabla.desembolsos, vista.desembolsos AS desembolsos_mv, tabla.monto, vista.monto AS monto_mv,
         tabla.desembolsos = vista.desembolsos AND abs(tabla.monto - vista.monto) < 1 AS cuadra
  FROM tabla FULL JOIN vista USING (pais) ORDER BY pais"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Cómo se refrescó cada materialized view?** El pipeline decide en cada update si la recalcula entera o solo lo que cambió, y lo
# MAGIC deja en el event log (`planning_information`). Corre el pipeline otra vez sin datos nuevos y vuelve a mirar: no hay nada que recalcular.

# COMMAND ----------

display(spark.sql(f"""
  SELECT timestamp, origin.flow_name AS tabla,
         details:planning_information:incrementalization_status::string AS resultado,
         filter(from_json(details:planning_information:technique_information,
                          'array<struct<maintenance_type: string, is_chosen: boolean>>'), x -> x.is_chosen)[0].maintenance_type AS tecnica_elegida
  FROM event_log(TABLE({t('metricas_colocacion_mensual_gld')}))
  WHERE event_type = 'planning_information'
  ORDER BY timestamp DESC LIMIT 10"""))
# El primer update dice COMPLETE_RECOMPUTE (FIRST_COMPUTATION): no hay nada previo sobre qué incrementar.
# En los siguientes verás NO_OP si la fuente no cambió, o una técnica incremental si cambió poco.

# COMMAND ----------

# MAGIC %md
# MAGIC | En la Sesión 18 lo escribiste tú | En el pipeline |
# MAGIC |---|---|
# MAGIC | `INSERT INTO … REPLACE WHERE mes >= …` para los últimos meses | El pipeline elige incremental o recálculo completo |
# MAGIC | `OPTIMIZE` + `ANALYZE` después de cada carga | Mantenimiento automático del pipeline; `CLUSTER BY` en la definición |
# MAGIC | `REFRESH MATERIALIZED VIEW` en un job | El update del pipeline (el mismo job de la Sesión 19) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 3 · Cartera y features
# MAGIC ## Paso 7 — La tabla ancha de cartera
# MAGIC Una fila por crédito con todo lo que se sabe de él. Une dos sistemas: el legado de cobranzas y Lending, porque
# MAGIC **`numero_credito` del legado es el `id_desembolso` de Lending** (así quedó definido en la Sesión 10).
# MAGIC Con eso la cartera conoce al cliente, su país, su moneda y su riesgo al desembolsar, y el saldo se puede llevar a USD.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/14_gold_cartera.sql`**
# MAGIC ```sql
# MAGIC -- Sesión 20 · Paso 7 · Gold: tabla ancha de cartera (Cobranzas + Lending).
# MAGIC -- Tercera forma de Gold (Sesión 18): ni estrella ni métrica, sino UNA FILA POR CRÉDITO con todo lo que se sabe de él,
# MAGIC -- lista para un analista de cobranzas o para un dashboard de cartera.
# MAGIC -- numero_credito del legado = id_desembolso de Lending: por ahí la cartera conoce al cliente, su país y su moneda.
# MAGIC -- Es una FOTO a fecha_corte (el último pago registrado): sirve para "cómo está la cartera hoy", no para entrenar un modelo
# MAGIC -- (para eso están las feature tables del paso 8, que guardan desde cuándo se conoce cada dato).
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW cartera_creditos_gld
# MAGIC COMMENT 'Cartera: una fila por crédito con cuotas, pagos, gestiones y mora a fecha_corte. Montos en moneda local y saldo también en USD'
# MAGIC CLUSTER BY (numero_credito)
# MAGIC AS
# MAGIC WITH corte AS (
# MAGIC   SELECT CAST(max(fecha_pago) AS DATE) AS fecha_corte FROM pagos_slv
# MAGIC ),
# MAGIC cuotas AS (
# MAGIC   SELECT numero_credito,
# MAGIC          count(*)                                                        AS n_cuotas,
# MAGIC          count_if(estado_cuota = 'Pagada')                               AS cuotas_pagadas,
# MAGIC          count_if(estado_cuota = 'Vencida')                              AS cuotas_vencidas,
# MAGIC          sum(monto_capital) FILTER (WHERE estado_cuota <> 'Pagada')      AS saldo_capital,
# MAGIC          sum(monto_cuota)   FILTER (WHERE estado_cuota = 'Vencida')      AS monto_vencido,
# MAGIC          min(fecha_vencimiento) FILTER (WHERE estado_cuota = 'Vencida')  AS vencida_mas_antigua
# MAGIC   FROM cuotas_slv
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC pagos AS (
# MAGIC   SELECT numero_credito, count(*) AS n_pagos, sum(monto_pagado) AS monto_pagado, max(fecha_pago) AS fecha_ultimo_pago
# MAGIC   FROM pagos_slv
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC gestiones AS (
# MAGIC   SELECT numero_credito, count(*) AS n_gestiones, max_by(resultado, fecha_gestion) AS resultado_ultima_gestion
# MAGIC   FROM gestiones_cobranza_slv
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC cartera AS (
# MAGIC   SELECT
# MAGIC     c.numero_credito,
# MAGIC     d.id_cliente,
# MAGIC     d.id_pais,
# MAGIC     pa.pais,
# MAGIC     d.moneda,
# MAGIC     cl.nivel_riesgo                                                   AS nivel_riesgo_al_desembolso,
# MAGIC     d.fecha_desembolso,
# MAGIC     d.monto_desembolsado,
# MAGIC     corte.fecha_corte,
# MAGIC     c.n_cuotas,
# MAGIC     c.cuotas_pagadas,
# MAGIC     c.cuotas_vencidas,
# MAGIC     coalesce(c.saldo_capital, 0)                                      AS saldo_capital,
# MAGIC     coalesce(c.monto_vencido, 0)                                      AS monto_vencido,
# MAGIC     greatest(coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0), 0) AS dias_mora,   -- una cuota marcada Vencida después del corte no da mora negativa
# MAGIC     coalesce(p.n_pagos, 0)                                            AS n_pagos,
# MAGIC     coalesce(p.monto_pagado, 0)                                       AS monto_pagado,
# MAGIC     p.fecha_ultimo_pago,
# MAGIC     coalesce(g.n_gestiones, 0)                                        AS n_gestiones,
# MAGIC     coalesce(g.resultado_ultima_gestion, 'Sin gestion')               AS resultado_ultima_gestion
# MAGIC   FROM cuotas c
# MAGIC   CROSS JOIN corte
# MAGIC   LEFT JOIN pagos p                   ON p.numero_credito = c.numero_credito
# MAGIC   LEFT JOIN gestiones g               ON g.numero_credito = c.numero_credito
# MAGIC   LEFT JOIN fact_desembolsos_gld d    ON d.id_desembolso = c.numero_credito
# MAGIC   LEFT JOIN dim_cliente_gld cl        ON cl.sk_cliente = d.sk_cliente
# MAGIC   LEFT JOIN dim_pais_gld pa           ON pa.id_pais = d.id_pais
# MAGIC )
# MAGIC SELECT
# MAGIC   ca.*,
# MAGIC   round(ca.saldo_capital / tc.tasa_venta, 2)                          AS saldo_capital_usd,
# MAGIC   CASE WHEN ca.dias_mora = 0   THEN 'Al dia'
# MAGIC        WHEN ca.dias_mora <= 30 THEN '1-30'
# MAGIC        WHEN ca.dias_mora <= 60 THEN '31-60'
# MAGIC        WHEN ca.dias_mora <= 90 THEN '61-90'
# MAGIC        ELSE 'Mas de 90' END                                           AS tramo_mora,
# MAGIC   CASE WHEN ca.dias_mora > 30 THEN 1 ELSE 0 END                       AS es_mora_30
# MAGIC FROM cartera ca
# MAGIC LEFT JOIN dim_tipo_cambio_gld tc
# MAGIC   ON  tc.moneda = ca.moneda
# MAGIC   AND ca.fecha_corte >= tc.vigente_desde
# MAGIC   AND (tc.vigente_hasta IS NULL OR ca.fecha_corte < tc.vigente_hasta);
# MAGIC ```
# MAGIC **Run pipeline.**

# COMMAND ----------

display(spark.sql(f"""
  SELECT tramo_mora, count(*) AS creditos, round(sum(saldo_capital_usd)) AS saldo_usd,
         round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct_creditos
  FROM {t('cartera_creditos_gld')} GROUP BY tramo_mora
  ORDER BY CASE tramo_mora WHEN 'Al dia' THEN 0 WHEN '1-30' THEN 1 WHEN '31-60' THEN 2 WHEN '61-90' THEN 3 ELSE 4 END"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** `cartera_creditos_gld` trae `es_mora_30`. ¿Por qué no entrenar el modelo de mora directo con esta tabla?
# MAGIC <details><summary>Ver respuesta</summary>Porque es una <b>foto de hoy</b>: <code>cuotas_vencidas</code>, <code>saldo_capital</code> y <code>dias_mora</code> salen de <code>cuotas_slv</code>, que es SCD tipo 1 (solo el último estado de cada cuota). Si entrenas "¿caerá en mora?" con esas columnas, el modelo ve el resultado que tenía que predecir: <b>fuga de información</b>. Brilla en entrenamiento y falla en producción. Para entrenar se necesita saber <b>qué se conocía en cada fecha</b>: eso es una feature table con llave <code>TIMESERIES</code> (paso 8).</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — Feature tables, dentro del pipeline
# MAGIC Una **feature** es una columna calculada para un modelo ("cuotas impagas en los últimos 3 meses"). El viejo *Workspace Feature
# MAGIC Store* está deprecado: hoy **una tabla de Unity Catalog con llave primaria es una feature table** (Databricks Feature Store),
# MAGIC con los mismos permisos y linaje que cualquier tabla. Si la llave incluye una columna **`TIMESERIES`**, Data Science puede hacer
# MAGIC joins *point-in-time*.
# MAGIC
# MAGIC Son materialized views del pipeline con dos cosas nuevas:
# MAGIC - **`schema=`**: declaras cada columna con su tipo y su **comentario**, y la **llave primaria** con `TIMESERIES`.
# MAGIC - **`disponible_desde`**: desde cuándo se conoce la fila. Los pagos de marzo solo se conocen completos cuando marzo cierra: 1-abr 00:00.
# MAGIC
# MAGIC | Feature table | Grano | Modelo | Features |
# MAGIC |---|---|---|---|
# MAGIC | `features_cliente_mes_gld` | cliente × mes cerrado | Mora | Cuotas que vencían, a tiempo, impagas al cierre, atraso; pagos, gestiones, solicitudes, sesiones; ventanas de 3 meses; perfil al cierre |
# MAGIC | `features_app_cliente_dia_gld` | cliente × día con actividad | Propensión a solicitar | Vistas, clicks, simulaciones; ventanas de 7 y 30 días; CTR de 30 días |
# MAGIC
# MAGIC **Cómo se evita la fuga en las cuotas:** `estado_cuota` es el estado de **hoy**. La feature table no lo usa: toma cada cuota con la
# MAGIC **fecha de su primer pago** y pregunta "¿estaba pagada al cierre del mes en que vencía?". Eso sí se sabía en ese momento.
# MAGIC
# MAGIC 📋 Archivo nuevo: **`transformations/15_gold_features.py`**
# MAGIC ```python
# MAGIC # Sesión 20 · Paso 8 · Feature tables para Data Science, dentro del pipeline.
# MAGIC # Una tabla de Unity Catalog con PRIMARY KEY es una feature table; si la llave incluye una columna TIMESERIES,
# MAGIC # Data Science puede unirla "punto en el tiempo": cada etiqueta ve solo lo que se sabía en su fecha.
# MAGIC #
# MAGIC #   features_cliente_mes_gld     → modelo de MORA: comportamiento de pago, cobranza y uso, una fila por cliente y mes
# MAGIC #   features_app_cliente_dia_gld → modelo de PROPENSIÓN a solicitar: uso de la app, una fila por cliente y día
# MAGIC #
# MAGIC # Regla de oro: las features salen de datos que guardan HISTORIA (eventos con fecha y dimensiones SCD2), nunca de
# MAGIC # una foto de hoy. estado_cuota de cuotas_slv es SCD tipo 1 (solo el último estado): una cuota que hoy figura
# MAGIC # "Pagada" pudo estar impaga al cierre de marzo. Por eso el atraso se reconstruye con la FECHA de cada pago.
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC # ---------- features_cliente_mes_gld ----------
# MAGIC
# MAGIC ESQUEMA_FEATURES_MES = """
# MAGIC   id_cliente                    BIGINT        NOT NULL COMMENT 'Llave: cliente',
# MAGIC   disponible_desde              TIMESTAMP     NOT NULL COMMENT 'Desde cuándo se conoce la fila: el cierre del mes (día 1 del mes siguiente, 00:00)',
# MAGIC   mes                           DATE                   COMMENT 'Mes que resume la fila, como su día 1',
# MAGIC   id_pais                       SMALLINT               COMMENT 'País del cliente al cierre del mes: 1 Perú, 2 Colombia, 3 Bolivia',
# MAGIC   nivel_riesgo                  STRING                 COMMENT 'Nivel de riesgo A (mejor) a E vigente al cierre del mes (clientes_slv, SCD2)',
# MAGIC   score_interno                 SMALLINT               COMMENT 'Score interno vigente al cierre del mes',
# MAGIC   situacion_laboral             STRING                 COMMENT 'Situación laboral vigente al cierre del mes',
# MAGIC   ingreso_mensual_declarado     DECIMAL(14,2)          COMMENT 'Ingreso declarado vigente al cierre del mes, en moneda local',
# MAGIC   cuotas_vencen_mes             BIGINT                 COMMENT 'Cuotas de sus créditos que vencían en el mes',
# MAGIC   cuotas_a_tiempo_mes           BIGINT                 COMMENT 'De esas, las pagadas hasta su fecha de vencimiento',
# MAGIC   cuotas_impagas_al_cierre_mes  BIGINT                 COMMENT 'De esas, las que seguían sin pagar al cierre del mes',
# MAGIC   max_dias_atraso_mes           INT                    COMMENT 'Mayor atraso en días de las cuotas del mes: hasta el pago, o hasta el cierre si seguía impaga',
# MAGIC   n_pagos_mes                   BIGINT                 COMMENT 'Pagos registrados en el mes',
# MAGIC   n_gestiones_mes               BIGINT                 COMMENT 'Gestiones de cobranza recibidas en el mes',
# MAGIC   n_solicitudes_mes             BIGINT                 COMMENT 'Solicitudes de préstamo hechas en el mes',
# MAGIC   n_sesiones_app_mes            BIGINT                 COMMENT 'Sesiones en la app en el mes',
# MAGIC   cuotas_impagas_3m             BIGINT                 COMMENT 'Cuotas impagas al cierre, sumadas en los 3 meses calendario que terminan en mes',
# MAGIC   pct_cuotas_a_tiempo_3m        DOUBLE                 COMMENT 'Cuotas a tiempo / cuotas que vencían, en los 3 meses calendario que terminan en mes. NULL si no vencía ninguna',
# MAGIC   n_gestiones_3m                BIGINT                 COMMENT 'Gestiones de cobranza en los 3 meses calendario que terminan en mes',
# MAGIC   n_sesiones_app_3m             BIGINT                 COMMENT 'Sesiones en la app en los 3 meses calendario que terminan en mes',
# MAGIC   CONSTRAINT pk_features_cliente_mes_gld PRIMARY KEY (id_cliente, disponible_desde TIMESERIES)
# MAGIC """
# MAGIC
# MAGIC
# MAGIC @dp.materialized_view(
# MAGIC     name="features_cliente_mes_gld",
# MAGIC     comment=("Feature table para el modelo de mora: comportamiento de pago, cobranza y uso de cada cliente, una fila por "
# MAGIC              "cliente y mes cerrado. Llave (id_cliente, disponible_desde TIMESERIES) para joins punto en el tiempo"),
# MAGIC     schema=ESQUEMA_FEATURES_MES,
# MAGIC     cluster_by=["id_cliente"],
# MAGIC )
# MAGIC @dp.expect_or_fail("impagas_coherentes", "cuotas_impagas_al_cierre_mes <= cuotas_vencen_mes")
# MAGIC @dp.expect("cliente_con_perfil", "nivel_riesgo IS NOT NULL")
# MAGIC def features_cliente_mes_gld():
# MAGIC     # numero_credito del legado = id_desembolso de Lending: así cada cuota, pago y gestión encuentra a su cliente
# MAGIC     creditos = spark.read.table("desembolsos_slv").select(F.col("id_desembolso").alias("numero_credito"), "id_cliente")
# MAGIC     pagos = spark.read.table("pagos_slv")
# MAGIC     mes_de = lambda c: F.trunc(c, "month")
# MAGIC
# MAGIC     # 1) Cada cuota con su PRIMER pago: ¿estaba pagada al cierre del mes en que vencía? ¿a tiempo? ¿cuánto atraso?
# MAGIC     primer_pago = pagos.groupBy("numero_credito", "numero_cuota").agg(F.min("fecha_pago").alias("fecha_pago"))
# MAGIC     cuotas = (spark.read.table("cuotas_slv").select("numero_credito", "numero_cuota", "fecha_vencimiento")
# MAGIC               .join(creditos, "numero_credito")
# MAGIC               .join(primer_pago, ["numero_credito", "numero_cuota"], "left")
# MAGIC               .withColumn("mes", mes_de("fecha_vencimiento"))
# MAGIC               .withColumn("cierre", F.add_months("mes", 1).cast("timestamp"))
# MAGIC               .withColumn("pagada_al_cierre", F.col("fecha_pago").isNotNull() & (F.col("fecha_pago") < F.col("cierre")))
# MAGIC               .withColumn("a_tiempo", F.col("fecha_pago").isNotNull() & (F.to_date("fecha_pago") <= F.col("fecha_vencimiento")))
# MAGIC               .withColumn("dias_atraso", F.greatest(F.lit(0), F.datediff(
# MAGIC                   F.when(F.col("pagada_al_cierre"), F.col("fecha_pago")).otherwise(F.col("cierre")), F.col("fecha_vencimiento")))))
# MAGIC     cuotas_mes = cuotas.groupBy("id_cliente", "mes").agg(
# MAGIC         F.count("*").alias("cuotas_vencen_mes"),
# MAGIC         F.count_if("a_tiempo").alias("cuotas_a_tiempo_mes"),
# MAGIC         F.count_if(~F.col("pagada_al_cierre")).alias("cuotas_impagas_al_cierre_mes"),
# MAGIC         F.max("dias_atraso").alias("max_dias_atraso_mes"))
# MAGIC
# MAGIC     # 2) Eventos con fecha: pagos, gestiones, solicitudes y sesiones de la app, contados por mes
# MAGIC     def por_mes(df, col_fecha, nombre):
# MAGIC         return df.groupBy("id_cliente", mes_de(col_fecha).alias("mes")).agg(F.count("*").alias(nombre))
# MAGIC
# MAGIC     pagos_mes = por_mes(pagos.join(creditos, "numero_credito"), "fecha_pago", "n_pagos_mes")
# MAGIC     gestiones_mes = por_mes(spark.read.table("gestiones_cobranza_slv").join(creditos, "numero_credito"), "fecha_gestion", "n_gestiones_mes")
# MAGIC     solicitudes_mes = por_mes(spark.read.table("solicitudes_prestamo_slv"), "fecha_hora_solicitud", "n_solicitudes_mes")
# MAGIC     sesiones_mes = por_mes(spark.read.table("sesiones_app_slv").filter("id_cliente IS NOT NULL"), "inicio_sesion", "n_sesiones_app_mes")
# MAGIC
# MAGIC     # 3) Una fila por cliente y mes con actividad, SOLO de meses cerrados: el mes en curso todavía no se conoce completo
# MAGIC     corte = pagos.agg(F.max("fecha_pago").alias("fecha_corte"))
# MAGIC     partes = [cuotas_mes, pagos_mes, gestiones_mes, solicitudes_mes, sesiones_mes]
# MAGIC     base = partes[0].select("id_cliente", "mes")
# MAGIC     for p in partes[1:]:
# MAGIC         base = base.unionByName(p.select("id_cliente", "mes"))
# MAGIC     mensual = base.distinct()
# MAGIC     for p in partes:
# MAGIC         mensual = mensual.join(p, ["id_cliente", "mes"], "left")
# MAGIC     mensual = (mensual.fillna(0)
# MAGIC                .withColumn("disponible_desde", F.add_months("mes", 1).cast("timestamp"))
# MAGIC                .crossJoin(corte).filter(F.col("disponible_desde") <= F.col("fecha_corte"))
# MAGIC                .withColumn("_mes_num", F.year("mes") * 12 + F.month("mes")))
# MAGIC
# MAGIC     # 4) Ventanas de 3 meses CALENDARIO (rangeBetween sobre el número de mes, no sobre filas)
# MAGIC     w_3m = Window.partitionBy("id_cliente").orderBy("_mes_num").rangeBetween(-2, 0)
# MAGIC     mensual = (mensual
# MAGIC                .withColumn("cuotas_impagas_3m", F.sum("cuotas_impagas_al_cierre_mes").over(w_3m))
# MAGIC                .withColumn("pct_cuotas_a_tiempo_3m", F.sum("cuotas_a_tiempo_mes").over(w_3m)
# MAGIC                            / F.nullif(F.sum("cuotas_vencen_mes").over(w_3m), F.lit(0)))
# MAGIC                .withColumn("n_gestiones_3m", F.sum("n_gestiones_mes").over(w_3m))
# MAGIC                .withColumn("n_sesiones_app_3m", F.sum("n_sesiones_app_mes").over(w_3m)))
# MAGIC
# MAGIC     # 5) Perfil del cliente VIGENTE AL CIERRE del mes: join punto en el tiempo contra la SCD2 de Silver
# MAGIC     clientes = spark.read.table("clientes_slv").select(
# MAGIC         F.col("id_cliente").alias("c_id"), "id_pais", "nivel_riesgo", "score_interno", "situacion_laboral",
# MAGIC         "ingreso_mensual_declarado",
# MAGIC         F.col("__START_AT.fecha_version").alias("c_desde"), F.col("__END_AT.fecha_version").alias("c_hasta"),
# MAGIC         F.row_number().over(Window.partitionBy("id_cliente").orderBy("__START_AT")).alias("_n_version"))
# MAGIC     # Igual que dim_cliente_gld: la PRIMERA versión cubre desde siempre. Sin esto, un mes con actividad anterior a la
# MAGIC     # primera fecha_version del cliente queda sin perfil (pasa con los datos sintéticos de cobranzas)
# MAGIC     clientes = clientes.withColumn("c_desde", F.when(F.col("_n_version") == 1, F.lit("1900-01-01").cast("timestamp"))
# MAGIC                                               .otherwise(F.col("c_desde"))).drop("_n_version")
# MAGIC     vigente_al_cierre = ((F.col("id_cliente") == F.col("c_id"))
# MAGIC                          & (F.col("c_desde") < F.col("disponible_desde"))
# MAGIC                          & (F.col("c_hasta").isNull() | (F.col("c_hasta") >= F.col("disponible_desde"))))
# MAGIC     return (mensual.join(clientes, vigente_al_cierre, "left")
# MAGIC             .select(
# MAGIC                 F.col("id_cliente").cast("bigint"), "disponible_desde", "mes",
# MAGIC                 F.col("id_pais").cast("smallint"), "nivel_riesgo", F.col("score_interno").cast("smallint"),
# MAGIC                 "situacion_laboral", F.col("ingreso_mensual_declarado").cast("decimal(14,2)"),
# MAGIC                 F.col("cuotas_vencen_mes").cast("bigint"), F.col("cuotas_a_tiempo_mes").cast("bigint"),
# MAGIC                 F.col("cuotas_impagas_al_cierre_mes").cast("bigint"), F.col("max_dias_atraso_mes").cast("int"),
# MAGIC                 F.col("n_pagos_mes").cast("bigint"), F.col("n_gestiones_mes").cast("bigint"),
# MAGIC                 F.col("n_solicitudes_mes").cast("bigint"), F.col("n_sesiones_app_mes").cast("bigint"),
# MAGIC                 F.col("cuotas_impagas_3m").cast("bigint"), F.col("pct_cuotas_a_tiempo_3m").cast("double"),
# MAGIC                 F.col("n_gestiones_3m").cast("bigint"), F.col("n_sesiones_app_3m").cast("bigint")))
# MAGIC
# MAGIC
# MAGIC # ---------- features_app_cliente_dia_gld ----------
# MAGIC
# MAGIC ESQUEMA_FEATURES_APP = """
# MAGIC   id_cliente                  BIGINT    NOT NULL COMMENT 'Llave: cliente',
# MAGIC   disponible_desde            TIMESTAMP NOT NULL COMMENT 'Desde cuándo se conoce la fila: el fin del día (00:00 del día siguiente)',
# MAGIC   fecha                       DATE               COMMENT 'Día que resume la fila',
# MAGIC   vistas_dia                  BIGINT             COMMENT 'Ofertas vistas en el día',
# MAGIC   clicks_dia                  BIGINT             COMMENT 'Clicks en ofertas en el día',
# MAGIC   simulaciones_dia            BIGINT             COMMENT 'Simulaciones de crédito en el día',
# MAGIC   solicitudes_iniciadas_dia   BIGINT             COMMENT 'Solicitudes iniciadas desde la app en el día',
# MAGIC   vistas_7d                   BIGINT             COMMENT 'Ofertas vistas en los 7 días que terminan en fecha',
# MAGIC   clicks_7d                   BIGINT             COMMENT 'Clicks en los 7 días que terminan en fecha',
# MAGIC   simulaciones_7d             BIGINT             COMMENT 'Simulaciones en los 7 días que terminan en fecha',
# MAGIC   dias_activo_30d             BIGINT             COMMENT 'Días con al menos un evento en los 30 días que terminan en fecha',
# MAGIC   ctr_30d                     DOUBLE             COMMENT 'Clicks / vistas en los 30 días que terminan en fecha. NULL si no vio ofertas',
# MAGIC   monto_simulado_max_30d      DOUBLE             COMMENT 'Mayor monto simulado en los 30 días que terminan en fecha, en moneda local',
# MAGIC   CONSTRAINT pk_features_app_cliente_dia_gld PRIMARY KEY (id_cliente, disponible_desde TIMESERIES)
# MAGIC """
# MAGIC
# MAGIC
# MAGIC @dp.materialized_view(
# MAGIC     name="features_app_cliente_dia_gld",
# MAGIC     comment=("Feature table para el modelo de propensión a solicitar: uso de la app de cada cliente, una fila por cliente "
# MAGIC              "y día con actividad, con ventanas de 7 y 30 días. Llave (id_cliente, disponible_desde TIMESERIES)"),
# MAGIC     schema=ESQUEMA_FEATURES_APP,
# MAGIC     cluster_by=["id_cliente"],
# MAGIC )
# MAGIC @dp.expect_or_fail("ventana_coherente", "dias_activo_30d BETWEEN 1 AND 30")   # si falla, la ventana está mal armada
# MAGIC def features_app_cliente_dia_gld():
# MAGIC     diario = (spark.read.table("eventos_app_slv").filter("id_cliente IS NOT NULL")
# MAGIC               .groupBy("id_cliente", F.col("fecha_evento").alias("fecha"))
# MAGIC               .agg(F.count_if(F.col("tipo_evento") == "oferta_vista").alias("vistas_dia"),
# MAGIC                    F.count_if(F.col("tipo_evento") == "oferta_click").alias("clicks_dia"),
# MAGIC                    F.count_if(F.col("tipo_evento") == "simulacion_realizada").alias("simulaciones_dia"),
# MAGIC                    F.count_if(F.col("tipo_evento") == "solicitud_iniciada").alias("solicitudes_iniciadas_dia"),
# MAGIC                    F.max("monto_simulado").alias("_monto_simulado_max"))
# MAGIC               .withColumn("_dia_num", F.datediff("fecha", F.lit("1970-01-01"))))
# MAGIC
# MAGIC     # Ventanas de días CALENDARIO: rangeBetween sobre el número de día (un día sin actividad no es una fila)
# MAGIC     w_7d = Window.partitionBy("id_cliente").orderBy("_dia_num").rangeBetween(-6, 0)
# MAGIC     w_30d = Window.partitionBy("id_cliente").orderBy("_dia_num").rangeBetween(-29, 0)
# MAGIC     return (diario
# MAGIC             .withColumn("disponible_desde", F.date_add("fecha", 1).cast("timestamp"))
# MAGIC             .withColumn("vistas_7d", F.sum("vistas_dia").over(w_7d))
# MAGIC             .withColumn("clicks_7d", F.sum("clicks_dia").over(w_7d))
# MAGIC             .withColumn("simulaciones_7d", F.sum("simulaciones_dia").over(w_7d))
# MAGIC             .withColumn("dias_activo_30d", F.count("*").over(w_30d))
# MAGIC             .withColumn("ctr_30d", F.sum("clicks_dia").over(w_30d) / F.nullif(F.sum("vistas_dia").over(w_30d), F.lit(0)))
# MAGIC             .withColumn("monto_simulado_max_30d", F.max("_monto_simulado_max").over(w_30d))
# MAGIC             .select(F.col("id_cliente").cast("bigint"), "disponible_desde", "fecha",
# MAGIC                     *[F.col(c).cast("bigint") for c in ["vistas_dia", "clicks_dia", "simulaciones_dia", "solicitudes_iniciadas_dia",
# MAGIC                                                          "vistas_7d", "clicks_7d", "simulaciones_7d", "dias_activo_30d"]],
# MAGIC                     F.col("ctr_30d").cast("double"), F.col("monto_simulado_max_30d").cast("double")))
# MAGIC ```
# MAGIC **Run pipeline.** En el grafo, las feature tables leen **Silver** (eventos con fecha y la SCD2 de clientes), no la cartera.
# MAGIC
# MAGIC > **Plan B si el update falla por la llave primaria** (un mensaje sobre `CONSTRAINT` o `TIMESERIES`): borra la línea `CONSTRAINT …`
# MAGIC > de los dos esquemas **y la coma de la línea anterior**, corre el pipeline y usa la celda **Plan B** de abajo: crea una copia con la
# MAGIC > llave desde el notebook. La mecánica es la misma; la copia no se refresca sola.

# COMMAND ----------

# La llave se consulta en information_schema (DESCRIBE TABLE EXTENDED no lista las constraints de una materialized view)
def llaves(nombre):
    return spark.sql(f"""
      SELECT tc.constraint_name, kcu.column_name, kcu.ordinal_position
      FROM {CATALOGO}.information_schema.table_constraints tc
      JOIN {CATALOGO}.information_schema.key_column_usage kcu
        ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
      WHERE tc.table_schema = '{ESQUEMA}' AND tc.table_name = '{nombre}' AND tc.constraint_type = 'PRIMARY KEY'
      ORDER BY kcu.ordinal_position""")

FEATURES_MES, FEATURES_APP = "features_cliente_mes_gld", "features_app_cliente_dia_gld"
for nombre in [FEATURES_MES, FEATURES_APP]:
    print(f"{nombre:30} llave primaria: {[r.column_name for r in llaves(nombre).collect()] or '❌ sin llave (usa el plan B)'}")

# COMMAND ----------

# Plan B: solo si la celda de arriba dice "sin llave"
USAR_PLAN_B = False

if USAR_PLAN_B:
    for nombre in [FEATURES_MES, FEATURES_APP]:
        copia = f"{nombre}_nb"
        spark.sql(f"CREATE OR REPLACE TABLE {t(copia)} CLUSTER BY (id_cliente) AS SELECT * FROM {t(nombre)}")
        spark.sql(f"ALTER TABLE {t(copia)} ALTER COLUMN id_cliente SET NOT NULL")
        spark.sql(f"ALTER TABLE {t(copia)} ALTER COLUMN disponible_desde SET NOT NULL")
        spark.sql(f"ALTER TABLE {t(copia)} ADD CONSTRAINT pk_{copia} PRIMARY KEY (id_cliente, disponible_desde TIMESERIES)")
    FEATURES_MES, FEATURES_APP = f"{FEATURES_MES}_nb", f"{FEATURES_APP}_nb"
    print("Desde aquí se usan", FEATURES_MES, "y", FEATURES_APP)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Controles de calidad ▶️
# MAGIC La llave primaria en Databricks es **informativa**: Unity Catalog **no rechaza duplicados**. Garantizarla es trabajo tuyo.

# COMMAND ----------

fm = spark.table(t(FEATURES_MES))
controles = [
    ("Llave única (id_cliente, disponible_desde)",     fm.groupBy("id_cliente", "disponible_desde").count().filter("count > 1").count() == 0),
    ("disponible_desde = mes + 1 mes",                 fm.filter(F.col("disponible_desde") != F.add_months("mes", 1).cast("timestamp")).count() == 0),
    ("Solo meses cerrados (≤ último pago)",            fm.agg(F.max("disponible_desde")).first()[0] <= spark.table(t("pagos_slv")).agg(F.max("fecha_pago")).first()[0]),
    ("cuotas_impagas_3m ≥ impagas del mes",            fm.filter(F.col("cuotas_impagas_3m") < F.col("cuotas_impagas_al_cierre_mes")).count() == 0),
    ("Cada fila encontró el perfil del cliente",       fm.filter("nivel_riesgo IS NULL").count() == 0),
]
for nombre, ok in controles:
    print(f"{'✅' if ok else '❌'} {nombre}")

# COMMAND ----------

# Cómo queda armado: un cliente que tuvo cuotas impagas. Compara la fila del mes con su historia en Silver
cliente = fm.filter("cuotas_impagas_al_cierre_mes > 0").select("id_cliente").first()[0]
display(fm.filter(F.col("id_cliente") == cliente).orderBy("mes")
        .select("mes", "disponible_desde", "nivel_riesgo", "cuotas_vencen_mes", "cuotas_a_tiempo_mes",
                "cuotas_impagas_al_cierre_mes", "max_dias_atraso_mes", "cuotas_impagas_3m", "pct_cuotas_a_tiempo_3m", "n_gestiones_3m"))
display(spark.sql(f"""
  SELECT id_cliente, nivel_riesgo, score_interno, __START_AT.fecha_version AS desde, __END_AT.fecha_version AS hasta
  FROM {t('clientes_slv')} WHERE id_cliente = {cliente} ORDER BY __START_AT"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** una cuota venció el 10-mar y se pagó el 5-abr. ¿Cómo aparece en la fila de marzo? ¿Y qué diría `estado_cuota` hoy?
# MAGIC <details><summary>Ver respuesta</summary>En marzo cuenta en <code>cuotas_vencen_mes</code> y en <code>cuotas_impagas_al_cierre_mes</code> (al 1-abr seguía sin pagar), con <code>max_dias_atraso_mes</code> = 22 (del 10 al 1-abr). Hoy <code>estado_cuota</code> dice <b>Pagada</b>: si la feature saliera de ahí, marzo se vería limpio y el modelo aprendería que ese cliente "nunca se atrasa". La fecha del pago es lo que permite reconstruir lo que se sabía al cierre.</details>

# COMMAND ----------

fa = spark.table(t(FEATURES_APP))
print("Llave única:", fa.groupBy("id_cliente", "disponible_desde").count().filter("count > 1").count() == 0)
display(fa.filter("simulaciones_7d > 0").orderBy("id_cliente", "fecha").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 9 — Point-in-time: así se ve la fuga de información ▶️
# MAGIC Data Science arma su dataset con **etiquetas**: "este cliente, a esta fecha, ¿tuvo cuotas impagas el mes siguiente?". Para cada
# MAGIC etiqueta, el modelo solo puede ver features **conocidas antes** de esa fecha.
# MAGIC
# MAGIC Aquí la etiqueta sale de la misma tabla: para el cliente en el mes *m*, `cae_en_mora = 1` si en *m + 1* tuvo cuotas impagas al cierre.
# MAGIC La `fecha_referencia` es el inicio de *m + 1*: el momento en que habría que predecir.

# COMMAND ----------

etiquetas = (fm.select("id_cliente", "mes").alias("a")
             .join(fm.select("id_cliente", F.col("mes").alias("mes_sig"), F.col("cuotas_impagas_al_cierre_mes").alias("impagas_sig")).alias("b"),
                   (F.col("a.id_cliente") == F.col("b.id_cliente")) & (F.col("b.mes_sig") == F.add_months("a.mes", 1)))
             .select(F.col("a.id_cliente").alias("id_cliente"), F.col("b.mes_sig").cast("timestamp").alias("fecha_referencia"),
                     (F.col("b.impagas_sig") > 0).cast("int").alias("cae_en_mora")))
print("Etiquetas:", etiquetas.count(), "· positivas:", etiquetas.filter("cae_en_mora = 1").count())

f = fm.select("id_cliente", "disponible_desde", "cuotas_impagas_3m", "pct_cuotas_a_tiempo_3m")

# ❌ Ingenuo: la última fila conocida HOY de cada cliente
w_hoy = Window.partitionBy("id_cliente").orderBy(F.desc("disponible_desde"))
ingenuo = (etiquetas.join(f.withColumn("_rn", F.row_number().over(w_hoy)).filter("_rn = 1").drop("_rn"), "id_cliente")
           .withColumn("criterio", F.lit("ingenuo (último valor)")))

# ✅ Point-in-time: la última fila con disponible_desde <= fecha_referencia de CADA etiqueta
w_pit = Window.partitionBy("e.id_cliente", "e.fecha_referencia").orderBy(F.desc("f.disponible_desde"))
pit = (etiquetas.alias("e")
       .join(f.alias("f"), (F.col("e.id_cliente") == F.col("f.id_cliente")) & (F.col("f.disponible_desde") <= F.col("e.fecha_referencia")))
       .withColumn("_rn", F.row_number().over(w_pit)).filter("_rn = 1")
       .select("e.id_cliente", "e.fecha_referencia", "e.cae_en_mora", "f.disponible_desde", "f.cuotas_impagas_3m", "f.pct_cuotas_a_tiempo_3m")
       .withColumn("criterio", F.lit("point-in-time")))

comparado = ingenuo.unionByName(pit).withColumn("usa_dato_del_futuro", F.col("disponible_desde") > F.col("fecha_referencia"))
display(comparado.groupBy("criterio").agg(
    F.count("*").alias("filas"),
    F.sum(F.col("usa_dato_del_futuro").cast("int")).alias("filas_con_dato_del_futuro"),
    F.round(F.corr("cuotas_impagas_3m", "cae_en_mora"), 3).alias("correlacion_feature_etiqueta")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Lee las dos últimas columnas:** el join ingenuo usa un dato del futuro en **todas** sus filas, y por eso la feature suele
# MAGIC "predecir" mejor (correlación más alta): trae información que ya contiene la respuesta. Es el modelo que brilla en entrenamiento
# MAGIC y falla en producción, donde ese dato todavía no existe.
# MAGIC
# MAGIC **Error común:** creer que el point-in-time "rellena" meses sin actividad. Si un cliente no tuvo cuotas ni pagos en 3 meses, el
# MAGIC lookup devuelve su última fila, con datos viejos que parecen actuales. Cuando el negocio lo necesite, se publica una fila por cliente
# MAGIC y mes aunque sea en cero (una "columna vertebral" de meses en vez de los meses con actividad).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 10 — Cómo la consume Data Science 📖
# MAGIC Esto lo escribe Data Science, no tú, y **no se ejecuta hoy**. Solo pasa la tabla de etiquetas y los **nombres** de las llaves: el
# MAGIC join point-in-time de la celda anterior lo hace la librería, gracias a la llave `TIMESERIES` que declaraste.
# MAGIC ```python
# MAGIC from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
# MAGIC fe = FeatureEngineeringClient()
# MAGIC
# MAGIC training_set = fe.create_training_set(
# MAGIC     df=etiquetas,                                     # id_cliente, fecha_referencia, cae_en_mora
# MAGIC     feature_lookups=[
# MAGIC         FeatureLookup(table_name="datawizard_dev.lakeflow.features_cliente_mes_gld",
# MAGIC                       lookup_key="id_cliente", timestamp_lookup_key="fecha_referencia",
# MAGIC                       feature_names=["cuotas_impagas_3m", "pct_cuotas_a_tiempo_3m", "n_gestiones_3m", "nivel_riesgo"]),
# MAGIC         FeatureLookup(table_name="datawizard_dev.lakeflow.features_app_cliente_dia_gld",
# MAGIC                       lookup_key="id_cliente", timestamp_lookup_key="fecha_referencia",
# MAGIC                       feature_names=["dias_activo_30d", "ctr_30d"]),
# MAGIC     ],
# MAGIC     label="cae_en_mora",
# MAGIC )
# MAGIC df_entrenamiento = training_set.load_df()
# MAGIC # ... entrena y registra con fe.log_model(...): el modelo guarda de qué feature tables salió (linaje)
# MAGIC ```
# MAGIC Dos feature tables de distinto grano (mes y día) se combinan en el mismo training set: cada lookup busca la última fila conocida
# MAGIC a la `fecha_referencia`.
# MAGIC
# MAGIC **Servir en línea:** para el modelo de propensión, que decide en la app qué oferta mostrar, las features se publican al
# MAGIC **Online Feature Store** y el endpoint del modelo las busca en milisegundos. La tabla que publica es esta misma: el Data Engineer la
# MAGIC mantiene fresca con el pipeline; plataforma configura la publicación.
# MAGIC
# MAGIC Lo que te toca a ti: la llave con `TIMESERIES`, el comentario de cada columna, que la tabla se refresque a tiempo (el job de la
# MAGIC Sesión 19 corre todo el pipeline) y el `GRANT SELECT` al grupo de Data Science.
# MAGIC
# MAGIC 🖱️ **Míralo en la UI:** Catalog Explorer → `features_cliente_mes_gld` → **Overview**: `id_cliente` lleva la etiqueta **PK** y
# MAGIC `disponible_desde` **PK (TS)**. *Lineage* muestra de qué Silver sale.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cierre — Checklist
# MAGIC | Entregable | Listo si… |
# MAGIC |---|---|
# MAGIC | 6 dimensiones `dim_*_gld` | `sk_cliente` único; una versión vigente por cliente |
# MAGIC | 3 hechos `fact_*_gld` | Mismas filas que Silver; cada hecho con su versión del cliente y su monto en USD |
# MAGIC | 4 métricas `metricas_*_gld` | La colocación cuadra con el hecho |
# MAGIC | 2 metric views `kpi_colocacion`, `kpi_funnel_app` | Cuadran con las tablas materializadas |
# MAGIC | `cartera_creditos_gld` | Una fila por crédito, con tramo de mora y saldo en USD |
# MAGIC | 2 feature tables `features_*_gld` | Llave `(id_cliente, disponible_desde TIMESERIES)` y controles en ✅ |
# MAGIC
# MAGIC - [ ] Sé elegir la forma de Gold según el consumidor: estrella, métrica, tabla ancha o feature table
# MAGIC - [ ] Sé por qué un hecho guarda la versión del cliente vigente cuando ocurrió
# MAGIC - [ ] Sé qué es aditivo, semi-aditivo y no aditivo, y por qué la metric view divide al final
# MAGIC - [ ] Sé mostrar con datos qué es una fuga de información y cómo la evita el point-in-time
# MAGIC
# MAGIC **Siguiente: Sesión 20.2.** Le das Gold y las metric views a un **Genie Agent**, mides cuántas preguntas del negocio responde bien,
# MAGIC mejoras los metadatos y lo encuentras en **Genie One**. No borres nada: se construye sobre lo de hoy.

# COMMAND ----------

GOLD = ["dim_pais_gld", "dim_campania_gld", "dim_producto_gld", "dim_tiempo_gld", "dim_cliente_gld", "dim_tipo_cambio_gld",
        "fact_ofertas_gld", "fact_solicitudes_gld", "fact_desembolsos_gld",
        "metricas_originacion_mensual_gld", "metricas_colocacion_mensual_gld", "metricas_roi_campania_gld", "metricas_funnel_app_diario_gld",
        "cartera_creditos_gld", FEATURES_MES, FEATURES_APP]
for nombre in GOLD:
    print(f"{nombre:34} {spark.table(t(nombre)).count():>9,} filas")
for mv in ["kpi_colocacion", "kpi_funnel_app"]:
    print(f"{mv:34} {'✅ metric view' if spark.catalog.tableExists(t(mv)) else '❌ falta: corre el paso 5'}")
