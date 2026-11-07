-- Sesión 20.2 · Paso 6a · Capa de métricas — VERSIÓN MEJORADA PARA GENIE.
-- Reemplaza transformations/13_gold_metricas.sql. La lógica es la misma de la Sesión 20; cambia esto:
--   - lista de columnas con TIPO y COMMENT: qué significa, en qué unidad y cuándo NO usarla
--   - llave primaria (el grano de la tabla) y un comentario de tabla que dice para qué preguntas sirve y para cuáles no
--   - un CAST por columna en el SELECT, para que el tipo coincida exactamente con el declarado
-- Genie no lee este archivo: lee lo que Unity Catalog dice de cada tabla. Escribirlo aquí, y no con un ALTER a mano,
-- hace que sobreviva a un Full refresh y que el código y la tabla digan lo mismo.

CREATE OR REFRESH MATERIALIZED VIEW metricas_originacion_mensual_gld (
  mes                      DATE          NOT NULL COMMENT 'Mes en que se GENERÓ LA OFERTA, como su día 1 (2026-03-01 = marzo 2026). No es el mes del desembolso',
  pais                     STRING        NOT NULL COMMENT 'País: Perú, Colombia o Bolivia (con tilde)',
  motor_asignacion         STRING        NOT NULL COMMENT 'Modelo que asignó la oferta: modelo_v1 o modelo_v2',
  ofertas                  BIGINT                 COMMENT 'Ofertas preaprobadas generadas',
  ofertas_aceptadas        BIGINT                 COMMENT 'Ofertas con estado Aceptada',
  solicitudes              BIGINT                 COMMENT 'Solicitudes que nacieron de esas ofertas',
  solicitudes_aprobadas    BIGINT                 COMMENT 'De esas solicitudes, las aprobadas',
  desembolsos              BIGINT                 COMMENT 'Desembolsos completados que vienen de esas ofertas',
  monto_colocado_usd       DECIMAL(18,2)          COMMENT 'USD desembolsado de esas ofertas, atribuido al mes de la oferta. Para colocación por mes de desembolso usar kpi_colocacion',
  tasa_oferta_a_solicitud  DOUBLE                 COMMENT 'solicitudes / ofertas de la fila (0 a 1). Para otro grano recalcular sum(solicitudes)/sum(ofertas): no promediar',
  tasa_aprobacion          DOUBLE                 COMMENT 'solicitudes_aprobadas / solicitudes de la fila (0 a 1). Para otro grano recalcular con las sumas: no promediar',
  conversion_end_to_end    DOUBLE                 COMMENT 'desembolsos / ofertas de la fila (0 a 1). Para comparar motores o países usar sum(desembolsos)/sum(ofertas)',
  CONSTRAINT pk_metricas_originacion_mensual_gld PRIMARY KEY (mes, pais, motor_asignacion)
)
COMMENT 'Funnel de originación: ofertas → solicitudes → aprobadas → desembolsos, por mes de la OFERTA, país y motor de asignación. Úsala para conversión, tasa de aprobación y comparar motores. NO la uses para el monto colocado de un mes: atribuye el desembolso al mes de la oferta y deja fuera las solicitudes sin oferta (para eso está kpi_colocacion). Datos sintéticos.'
AS
WITH base AS (
  SELECT
    trunc(t.fecha, 'MM')                                              AS mes,
    coalesce(p.pais, 'desconocido')                       AS pais,
    coalesce(o.motor_asignacion, 'desconocido')           AS motor_asignacion,
    count(*)                                                          AS ofertas,
    sum(o.es_aceptada)                                                AS ofertas_aceptadas,
    count(s.id_solicitud)                                             AS solicitudes,
    coalesce(sum(s.es_aprobada), 0)                                   AS solicitudes_aprobadas,
    count_if(d.es_completado = 1)                                     AS desembolsos,
    coalesce(sum(d.monto_usd) FILTER (WHERE d.es_completado = 1), 0)  AS monto_colocado_usd
  FROM fact_ofertas_gld o
  JOIN dim_tiempo_gld t                ON t.id_fecha = o.id_fecha
  LEFT JOIN dim_pais_gld p             ON p.id_pais = o.id_pais
  LEFT JOIN fact_solicitudes_gld s     ON s.id_oferta = o.id_oferta
  LEFT JOIN fact_desembolsos_gld d     ON d.id_solicitud = s.id_solicitud
  GROUP BY ALL
)
SELECT
  CAST(mes AS DATE)                                                AS mes,
  CAST(pais AS STRING)                                             AS pais,
  CAST(motor_asignacion AS STRING)                                 AS motor_asignacion,
  CAST(ofertas AS BIGINT)                                          AS ofertas,
  CAST(ofertas_aceptadas AS BIGINT)                                AS ofertas_aceptadas,
  CAST(solicitudes AS BIGINT)                                      AS solicitudes,
  CAST(solicitudes_aprobadas AS BIGINT)                            AS solicitudes_aprobadas,
  CAST(desembolsos AS BIGINT)                                      AS desembolsos,
  CAST(monto_colocado_usd AS DECIMAL(18,2))                        AS monto_colocado_usd,
  CAST(round(try_divide(solicitudes, ofertas), 4) AS DOUBLE)       AS tasa_oferta_a_solicitud,
  CAST(round(try_divide(solicitudes_aprobadas, solicitudes), 4) AS DOUBLE) AS tasa_aprobacion,
  CAST(round(try_divide(desembolsos, ofertas), 4) AS DOUBLE)       AS conversion_end_to_end
FROM base;

CREATE OR REFRESH MATERIALIZED VIEW metricas_colocacion_mensual_gld (
  mes                      DATE          NOT NULL COMMENT 'Mes del DESEMBOLSO, como su día 1 (2026-03-01 = marzo 2026)',
  pais                     STRING        NOT NULL COMMENT 'País: Perú, Colombia o Bolivia (con tilde)',
  desembolsos              BIGINT                 COMMENT 'Desembolsos completados (Fallidos y Reversados no cuentan)',
  monto_colocado_usd       DECIMAL(18,2)          COMMENT 'Monto colocado en USD con la tasa del día de cada desembolso',
  comision_usd             DECIMAL(18,2)          COMMENT 'Comisiones cobradas en USD',
  ticket_promedio_usd      DECIMAL(18,2)          COMMENT 'monto_colocado_usd / desembolsos de la fila. Para otro grano recalcular sum(monto)/sum(desembolsos): no promediar',
  tasa_promedio_ponderada  DECIMAL(10,3)          COMMENT 'Tasa anual promedio ponderada por monto (porcentaje, ej. 28.5). No promediar entre filas',
  CONSTRAINT pk_metricas_colocacion_mensual_gld PRIMARY KEY (mes, pais)
)
COMMENT 'Colocación mensual consolidada en USD por país (solo desembolsos Completados). Es la versión materializada de kpi_colocacion: para cortes por producto, nivel de riesgo o año usar kpi_colocacion. Datos sintéticos.'
AS SELECT
  CAST(trunc(d.fecha_desembolso, 'MM') AS DATE)                                              AS mes,
  CAST(p.pais AS STRING)                                                                     AS pais,
  CAST(count(*) AS BIGINT)                                                                   AS desembolsos,
  CAST(round(sum(d.monto_usd), 2) AS DECIMAL(18,2))                                          AS monto_colocado_usd,
  CAST(round(sum(d.comision_usd), 2) AS DECIMAL(18,2))                                       AS comision_usd,
  CAST(round(try_divide(sum(d.monto_usd), count(*)), 2) AS DECIMAL(18,2))                    AS ticket_promedio_usd,
  CAST(round(try_divide(sum(d.tasa_aplicada * d.monto_usd), sum(d.monto_usd)), 3) AS DECIMAL(10,3)) AS tasa_promedio_ponderada
FROM fact_desembolsos_gld d
JOIN dim_pais_gld p ON p.id_pais = d.id_pais
WHERE d.es_completado = 1
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW metricas_roi_campania_gld (
  id_campania                     INT           NOT NULL COMMENT 'Llave: campaña de marketing',
  nombre_campania                 STRING                 COMMENT 'Nombre de la campaña',
  tipo_campania                   STRING                 COMMENT 'Captacion, Reactivacion o Cross-sell',
  pais                            STRING                 COMMENT 'País de la campaña: Perú, Colombia o Bolivia',
  fecha_inicio                    DATE                   COMMENT 'Inicio de la campaña',
  presupuesto_usd                 DECIMAL(18,2)          COMMENT 'Presupuesto en USD (convertido con la tasa del día de inicio)',
  clientes_con_desembolso         BIGINT                 COMMENT 'Clientes captados por la campaña que tuvieron al menos un desembolso completado. No sumar entre campañas de distinto país sin revisar',
  desembolsos                     BIGINT                 COMMENT 'Desembolsos completados de los clientes captados por la campaña',
  monto_colocado_usd              DECIMAL(18,2)          COMMENT 'USD colocado a los clientes captados por la campaña',
  usd_colocado_por_usd_invertido  DECIMAL(18,2)          COMMENT 'RETORNO: monto_colocado_usd / presupuesto_usd. Es la medida para "mejor campaña" o "mayor retorno"',
  ranking_roi                     INT                    COMMENT '1 = la campaña con mayor retorno (usd_colocado_por_usd_invertido). No es el ranking por monto colocado',
  CONSTRAINT pk_metricas_roi_campania_gld PRIMARY KEY (id_campania)
)
COMMENT 'Retorno de cada campaña de marketing: USD colocado por los clientes que captó por cada USD invertido. Una fila por campaña. "Mejor campaña" o "mayor retorno" se responde con usd_colocado_por_usd_invertido o ranking_roi, no con el monto colocado. Datos sintéticos.'
AS
WITH colocado AS (
  SELECT c.id_campania_captacion          AS id_campania,
         count(DISTINCT d.id_cliente)     AS clientes_con_desembolso,
         count(*)                         AS desembolsos,
         sum(d.monto_usd)                 AS monto_colocado_usd
  FROM fact_desembolsos_gld d
  JOIN dim_cliente_gld c ON c.sk_cliente = d.sk_cliente
  WHERE d.es_completado = 1
  GROUP BY ALL
),
presupuesto AS (
  SELECT ca.id_campania, ca.nombre_campania, ca.tipo_campania, p.pais, ca.fecha_inicio,
         round(ca.presupuesto_local / tc.tasa_venta, 2) AS presupuesto_usd
  FROM dim_campania_gld ca
  JOIN dim_pais_gld p ON p.id_pais = ca.id_pais
  LEFT JOIN dim_tipo_cambio_gld tc
    ON  tc.moneda = p.moneda
    AND ca.fecha_inicio >= tc.vigente_desde
    AND (tc.vigente_hasta IS NULL OR ca.fecha_inicio < tc.vigente_hasta)
)
SELECT
  CAST(pr.id_campania AS INT)                                                  AS id_campania,
  CAST(pr.nombre_campania AS STRING)                                           AS nombre_campania,
  CAST(pr.tipo_campania AS STRING)                                             AS tipo_campania,
  CAST(pr.pais AS STRING)                                                      AS pais,
  CAST(pr.fecha_inicio AS DATE)                                                AS fecha_inicio,
  CAST(pr.presupuesto_usd AS DECIMAL(18,2))                                    AS presupuesto_usd,
  CAST(coalesce(co.clientes_con_desembolso, 0) AS BIGINT)                      AS clientes_con_desembolso,
  CAST(coalesce(co.desembolsos, 0) AS BIGINT)                                  AS desembolsos,
  CAST(round(coalesce(co.monto_colocado_usd, 0), 2) AS DECIMAL(18,2))          AS monto_colocado_usd,
  CAST(round(try_divide(co.monto_colocado_usd, pr.presupuesto_usd), 2) AS DECIMAL(18,2)) AS usd_colocado_por_usd_invertido,
  CAST(rank() OVER (ORDER BY try_divide(co.monto_colocado_usd, pr.presupuesto_usd) DESC NULLS LAST) AS INT) AS ranking_roi
FROM presupuesto pr
LEFT JOIN colocado co ON co.id_campania = pr.id_campania;

CREATE OR REFRESH MATERIALIZED VIEW metricas_funnel_app_diario_gld (
  fecha                  DATE    NOT NULL COMMENT 'Día del evento',
  canal                  STRING  NOT NULL COMMENT 'APP_ANDROID, APP_IOS o WEB',
  ubicacion_pantalla     STRING  NOT NULL COMMENT 'Dónde se mostró la oferta: home_banner, home_card, push o detalle_oferta',
  vistas                 BIGINT           COMMENT 'Ofertas vistas',
  clicks                 BIGINT           COMMENT 'Clicks en ofertas',
  simulaciones           BIGINT           COMMENT 'Simulaciones de crédito',
  solicitudes_iniciadas  BIGINT           COMMENT 'Solicitudes iniciadas desde la app',
  banners_cerrados       BIGINT           COMMENT 'Banners cerrados sin interactuar',
  ctr                    DOUBLE           COMMENT 'clicks / vistas del día (0 a 1). Para un período usar sum(clicks)/sum(vistas) o la medida ctr de kpi_funnel_app: no promediar',
  CONSTRAINT pk_metricas_funnel_app_diario_gld PRIMARY KEY (fecha, canal, ubicacion_pantalla)
)
COMMENT 'Funnel diario de la app (bases por día, canal y ubicación). Para CTR o conversión de un período preferir la metric view kpi_funnel_app, que suma las bases y divide al final. Datos sintéticos.'
CLUSTER BY (fecha)
AS SELECT
  CAST(fecha_evento AS DATE)                                  AS fecha,
  CAST(coalesce(canal, 'desconocido') AS STRING)              AS canal,
  CAST(coalesce(ubicacion_pantalla, 'desconocida') AS STRING) AS ubicacion_pantalla,
  CAST(count_if(tipo_evento = 'oferta_vista') AS BIGINT)          AS vistas,
  CAST(count_if(tipo_evento = 'oferta_click') AS BIGINT)          AS clicks,
  CAST(count_if(tipo_evento = 'simulacion_realizada') AS BIGINT)  AS simulaciones,
  CAST(count_if(tipo_evento = 'solicitud_iniciada') AS BIGINT)    AS solicitudes_iniciadas,
  CAST(count_if(tipo_evento = 'banner_cerrado') AS BIGINT)        AS banners_cerrados,
  CAST(round(try_divide(count_if(tipo_evento = 'oferta_click'), count_if(tipo_evento = 'oferta_vista')), 4) AS DOUBLE) AS ctr
FROM eventos_app_slv
GROUP BY ALL;
