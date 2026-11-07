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
ESQUEMA = "lending"

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
{{transformations/11_gold_dimensiones.sql}}
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
{{transformations/12_gold_hechos.sql}}
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
{{transformations/13_gold_metricas.sql}}
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

{{ejecutar:metric_views/kpi_colocacion.sql}}

# COMMAND ----------

{{ejecutar:metric_views/kpi_funnel_app.sql}}

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
{{transformations/14_gold_cartera.sql}}
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
{{transformations/15_gold_features.py}}
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
# MAGIC         FeatureLookup(table_name="datawizard_dev.lending.features_cliente_mes_gld",
# MAGIC                       lookup_key="id_cliente", timestamp_lookup_key="fecha_referencia",
# MAGIC                       feature_names=["cuotas_impagas_3m", "pct_cuotas_a_tiempo_3m", "n_gestiones_3m", "nivel_riesgo"]),
# MAGIC         FeatureLookup(table_name="datawizard_dev.lending.features_app_cliente_dia_gld",
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
