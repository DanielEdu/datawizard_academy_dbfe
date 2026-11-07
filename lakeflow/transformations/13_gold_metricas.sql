-- Sesión 20 · Paso 4 · Gold: la capa de métricas materializada.
-- Cada tabla responde una pregunta del negocio a un GRANO fijo y la deja precalculada: un dashboard solo la lee.
-- Regla de diseño: se guardan las BASES aditivas (conteos, sumas) y los ratios se calculan de ellas.
-- Quien agregue a otro grano debe sumar las bases y recalcular el ratio: nunca promediar ratios.

-- Funnel de originación: oferta → solicitud → aprobada → desembolso. Grano: mes de la OFERTA x país x motor.
CREATE OR REFRESH MATERIALIZED VIEW metricas_originacion_mensual_gld
COMMENT 'KPI de originación: funnel oferta → solicitud → aprobación → desembolso. Grano: mes de la oferta x país x motor de asignación'
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
  *,
  round(try_divide(solicitudes, ofertas), 4)                AS tasa_oferta_a_solicitud,
  round(try_divide(solicitudes_aprobadas, solicitudes), 4)  AS tasa_aprobacion,
  round(try_divide(desembolsos, ofertas), 4)                AS conversion_end_to_end
FROM base;

-- Colocación: lo desembolsado (solo Completado) en USD. Grano: mes del DESEMBOLSO x país.
CREATE OR REFRESH MATERIALIZED VIEW metricas_colocacion_mensual_gld
COMMENT 'KPI de colocación consolidada en USD (solo desembolsos Completados). Grano: mes del desembolso x país'
AS SELECT
  trunc(d.fecha_desembolso, 'MM')                                          AS mes,
  p.pais,
  count(*)                                                                 AS desembolsos,
  round(sum(d.monto_usd), 2)                                               AS monto_colocado_usd,
  round(sum(d.comision_usd), 2)                                            AS comision_usd,
  round(try_divide(sum(d.monto_usd), count(*)), 2)                         AS ticket_promedio_usd,
  round(try_divide(sum(d.tasa_aplicada * d.monto_usd), sum(d.monto_usd)), 3) AS tasa_promedio_ponderada
FROM fact_desembolsos_gld d
JOIN dim_pais_gld p ON p.id_pais = d.id_pais
WHERE d.es_completado = 1
GROUP BY ALL;

-- Retorno de campañas: USD colocado por los clientes que captó cada campaña, por cada USD de presupuesto.
-- El presupuesto se convierte con la tasa vigente el día de inicio de la campaña. Grano: una fila por campaña.
CREATE OR REFRESH MATERIALIZED VIEW metricas_roi_campania_gld
COMMENT 'KPI de retorno por campaña: USD colocado por los clientes captados por cada USD invertido. Grano: campaña'
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
  pr.id_campania,
  pr.nombre_campania,
  pr.tipo_campania,
  pr.pais,
  pr.fecha_inicio,
  pr.presupuesto_usd,
  coalesce(co.clientes_con_desembolso, 0)                         AS clientes_con_desembolso,
  coalesce(co.desembolsos, 0)                                     AS desembolsos,
  round(coalesce(co.monto_colocado_usd, 0), 2)                    AS monto_colocado_usd,
  round(try_divide(co.monto_colocado_usd, pr.presupuesto_usd), 2) AS usd_colocado_por_usd_invertido,
  rank() OVER (ORDER BY try_divide(co.monto_colocado_usd, pr.presupuesto_usd) DESC NULLS LAST) AS ranking_roi
FROM presupuesto pr
LEFT JOIN colocado co ON co.id_campania = pr.id_campania;

-- Funnel de la app: eventos de Kafka por día, canal y ubicación en pantalla.
CREATE OR REFRESH MATERIALIZED VIEW metricas_funnel_app_diario_gld
COMMENT 'KPI de funnel de la app: vistas, clicks, simulaciones y solicitudes. Grano: fecha x canal x ubicación en pantalla'
CLUSTER BY (fecha)
AS SELECT
  fecha_evento                                                AS fecha,
  coalesce(canal, 'desconocido')                              AS canal,
  coalesce(ubicacion_pantalla, 'desconocida')                 AS ubicacion_pantalla,
  count_if(tipo_evento = 'oferta_vista')                      AS vistas,
  count_if(tipo_evento = 'oferta_click')                      AS clicks,
  count_if(tipo_evento = 'simulacion_realizada')              AS simulaciones,
  count_if(tipo_evento = 'solicitud_iniciada')                AS solicitudes_iniciadas,
  count_if(tipo_evento = 'banner_cerrado')                    AS banners_cerrados,
  round(try_divide(count_if(tipo_evento = 'oferta_click'), count_if(tipo_evento = 'oferta_vista')), 4) AS ctr
FROM eventos_app_slv
GROUP BY ALL;
