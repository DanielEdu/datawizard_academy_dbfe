-- Paso 12 · Gold: MATERIALIZED VIEWS en SQL sobre las tablas Silver del mismo pipeline.
-- Gold agrega y une datasets completos: se lee en batch (sin STREAM) y el pipeline recalcula lo que cambió.
-- Un pipeline puede mezclar archivos .py y .sql: el grafo los une igual.

-- Dimensión cliente: la SCD2 de Silver con nombres de negocio y sin PII
CREATE OR REFRESH MATERIALIZED VIEW dim_cliente_gld
COMMENT 'Dimensión cliente con historia: una fila por versión, sin PII'
CLUSTER BY (id_cliente)
AS SELECT
  id_cliente,
  id_pais,
  situacion_laboral,
  ingreso_mensual_declarado,
  score_interno,
  nivel_riesgo,
  ciudad,
  id_campania_captacion,
  __START_AT.fecha_version AS vigente_desde,      -- sequence_by fue un struct: __START_AT/__END_AT también lo son
  __END_AT.fecha_version   AS vigente_hasta,
  __END_AT IS NULL    AS es_vigente
FROM clientes_slv;

-- Hecho de desembolsos en USD: el tipo de cambio más reciente disponible a la fecha del desembolso
CREATE OR REFRESH MATERIALIZED VIEW fact_desembolsos_gld (
  CONSTRAINT con_tipo_cambio EXPECT (monto_usd IS NOT NULL)          -- solo se mide
)
COMMENT 'Un desembolso por fila, con el monto en moneda local y en USD'
CLUSTER BY (fecha_desembolso)
AS
WITH tc AS (
  SELECT moneda_origen, fecha, tasa_venta,
         lead(fecha) OVER (PARTITION BY moneda_origen ORDER BY fecha) AS fecha_siguiente
  FROM tipos_cambio_slv
  WHERE moneda_destino = 'USD'
)
SELECT
  d.id_desembolso,
  d.id_solicitud,
  d.id_cliente,
  p.id_pais,
  CAST(d.fecha_hora_desembolso AS DATE)                AS fecha_desembolso,
  d.moneda,
  d.estado_desembolso,
  d.monto_desembolsado,
  tc.tasa_venta                                        AS tipo_cambio_usd,
  round(d.monto_desembolsado / tc.tasa_venta, 2)       AS monto_usd,
  d.tasa_aplicada,
  d.plazo_meses
FROM desembolsos_slv d
LEFT JOIN paises_slv p ON p.moneda_codigo = d.moneda
LEFT JOIN tc
  ON  tc.moneda_origen = d.moneda
  AND CAST(d.fecha_hora_desembolso AS DATE) >= tc.fecha
  AND (tc.fecha_siguiente IS NULL OR CAST(d.fecha_hora_desembolso AS DATE) < tc.fecha_siguiente);

-- Métrica: colocación mensual consolidada en USD por país
CREATE OR REFRESH MATERIALIZED VIEW metricas_colocacion_mensual_gld
COMMENT 'KPI de colocación en USD. Grano: mes x país'
AS SELECT
  trunc(f.fecha_desembolso, 'MM')                          AS mes,
  p.nombre_pais                                            AS pais,
  count(*)                                                 AS desembolsos,
  round(sum(f.monto_usd), 2)                               AS monto_colocado_usd,
  round(try_divide(sum(f.monto_usd), count(*)), 2)         AS ticket_promedio_usd
FROM fact_desembolsos_gld f
JOIN paises_slv p ON p.id_pais = f.id_pais
WHERE f.estado_desembolso = 'Completado'
GROUP BY ALL;

-- Métrica: funnel diario de la app por ubicación en pantalla
CREATE OR REFRESH MATERIALIZED VIEW metricas_funnel_app_diario_gld
COMMENT 'KPI de funnel de la app. Grano: fecha x canal x ubicación'
AS SELECT
  fecha_evento                                             AS fecha,
  coalesce(canal, 'desconocido')                           AS canal,
  coalesce(ubicacion_pantalla, 'desconocida')              AS ubicacion_pantalla,
  count_if(tipo_evento = 'oferta_vista')                   AS vistas,
  count_if(tipo_evento = 'oferta_click')                   AS clicks,
  count_if(tipo_evento = 'simulacion_realizada')           AS simulaciones,
  count_if(tipo_evento = 'solicitud_iniciada')             AS solicitudes_iniciadas,
  round(try_divide(count_if(tipo_evento = 'oferta_click'), count_if(tipo_evento = 'oferta_vista')), 4) AS ctr
FROM eventos_app_slv
GROUP BY ALL;

-- Tabla ancha de cartera: una fila por crédito con features de pago (para el modelo de mora)
CREATE OR REFRESH MATERIALIZED VIEW cartera_creditos_gld
COMMENT 'Una fila por crédito: cuotas, pagos y mora. Target es_mora_30'
CLUSTER BY (numero_credito)
AS
WITH corte AS (SELECT CAST(max(fecha_pago) AS DATE) AS fecha_corte FROM pagos_slv),
cuotas AS (
  SELECT numero_credito,
         count(*)                                                        AS n_cuotas,
         count_if(estado_cuota = 'Vencida')                              AS cuotas_vencidas,
         sum(monto_capital) FILTER (WHERE estado_cuota <> 'Pagada')      AS saldo_capital,
         min(fecha_vencimiento) FILTER (WHERE estado_cuota = 'Vencida')  AS vencida_mas_antigua
  FROM cuotas_slv GROUP BY numero_credito
),
pagos AS (
  SELECT numero_credito, count(*) AS n_pagos, sum(monto_pagado) AS monto_pagado
  FROM pagos_slv GROUP BY numero_credito
)
SELECT
  c.numero_credito,
  corte.fecha_corte,
  c.n_cuotas,
  c.cuotas_vencidas,
  coalesce(c.saldo_capital, 0)                                     AS saldo_capital,
  coalesce(p.n_pagos, 0)                                           AS n_pagos,
  coalesce(p.monto_pagado, 0)                                      AS monto_pagado,
  coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0)  AS dias_mora,
  CASE WHEN coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0) > 30 THEN 1 ELSE 0 END AS es_mora_30
FROM cuotas c
CROSS JOIN corte
LEFT JOIN pagos p ON p.numero_credito = c.numero_credito;
