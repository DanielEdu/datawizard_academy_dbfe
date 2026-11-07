-- Sesión 20 · Paso 3 · Gold: los hechos del modelo estrella.
-- Cada hecho es un evento del negocio (una oferta, una solicitud, un desembolso) con:
--   - id_fecha       → dim_tiempo_gld
--   - sk_cliente     → la VERSIÓN de dim_cliente_gld vigente cuando ocurrió el hecho (join punto-en-el-tiempo)
--   - las medidas    → montos, tasas, banderas 0/1 que se suman (es_aprobada, es_completado)
-- Las Silver de solicitudes y desembolsos son SCD tipo 1: el hecho trae su ÚLTIMO estado.

CREATE OR REFRESH MATERIALIZED VIEW fact_ofertas_gld
COMMENT 'Hecho: una fila por oferta preaprobada asignada a un cliente, con el cliente tal como era al recibirla'
CLUSTER BY (id_fecha)
AS SELECT
  o.id_oferta,
  CAST(date_format(o.fecha_generacion, 'yyyyMMdd') AS INT)    AS id_fecha,
  c.sk_cliente,
  o.id_cliente,
  c.id_pais,
  o.id_producto,
  o.id_campania,
  o.motor_asignacion,
  o.estado_oferta,
  CASE WHEN o.estado_oferta = 'Aceptada' THEN 1 ELSE 0 END    AS es_aceptada,
  o.monto_ofertado,
  o.plazo_meses_ofertado,
  o.tasa_ofertada
FROM ofertas_preaprobadas_slv o
LEFT JOIN dim_cliente_gld c
  ON  c.id_cliente = o.id_cliente
  AND o.fecha_generacion >= c.vigente_desde
  AND (c.vigente_hasta IS NULL OR o.fecha_generacion < c.vigente_hasta);

CREATE OR REFRESH MATERIALIZED VIEW fact_solicitudes_gld
COMMENT 'Hecho: una fila por solicitud de préstamo con su último estado, y el cliente tal como era al solicitar'
CLUSTER BY (id_fecha)
AS SELECT
  s.id_solicitud,
  CAST(date_format(s.fecha_hora_solicitud, 'yyyyMMdd') AS INT) AS id_fecha,
  c.sk_cliente,
  s.id_cliente,
  c.id_pais,
  s.id_oferta,
  s.id_producto,
  s.canal,
  s.estado_solicitud,
  CASE WHEN s.estado_solicitud = 'Aprobada' THEN 1 ELSE 0 END  AS es_aprobada,
  s.monto_solicitado,
  s.monto_aprobado,
  s.plazo_meses_solicitado,
  s.tasa_aprobada,
  s.score_evaluacion,
  s.motivo_rechazo,
  round((unix_timestamp(s.fecha_hora_resolucion) - unix_timestamp(s.fecha_hora_solicitud)) / 3600, 1) AS horas_a_resolucion
FROM solicitudes_prestamo_slv s
LEFT JOIN dim_cliente_gld c
  ON  c.id_cliente = s.id_cliente
  AND s.fecha_hora_solicitud >= c.vigente_desde
  AND (c.vigente_hasta IS NULL OR s.fecha_hora_solicitud < c.vigente_hasta);

-- El desembolso en USD: se divide por la tasa vigente ESE día (dim_tipo_cambio_gld tiene el rango de vigencia).
-- La expectation solo mide: un desembolso sin tasa sigue contando en moneda local.
CREATE OR REFRESH MATERIALIZED VIEW fact_desembolsos_gld (
  CONSTRAINT con_tipo_cambio EXPECT (monto_usd IS NOT NULL)
)
COMMENT 'Hecho: una fila por desembolso con el monto en moneda local y en USD. Solo los de estado Completado son colocación'
CLUSTER BY (id_fecha)
AS SELECT
  d.id_desembolso,
  d.id_solicitud,
  CAST(date_format(d.fecha_hora_desembolso, 'yyyyMMdd') AS INT) AS id_fecha,
  CAST(d.fecha_hora_desembolso AS DATE)                          AS fecha_desembolso,
  c.sk_cliente,
  d.id_cliente,
  p.id_pais,
  s.id_producto,
  d.moneda,
  d.estado_desembolso,
  CASE WHEN d.estado_desembolso = 'Completado' THEN 1 ELSE 0 END AS es_completado,
  d.monto_desembolsado,
  tc.tasa_venta                                                  AS tipo_cambio_usd,
  round(d.monto_desembolsado / tc.tasa_venta, 2)                 AS monto_usd,
  d.comision_cobrada,
  round(d.comision_cobrada / tc.tasa_venta, 2)                   AS comision_usd,
  d.tasa_aplicada,
  d.plazo_meses
FROM desembolsos_slv d
LEFT JOIN dim_pais_gld p                ON p.moneda = d.moneda
LEFT JOIN solicitudes_prestamo_slv s    ON s.id_solicitud = d.id_solicitud
LEFT JOIN dim_cliente_gld c
  ON  c.id_cliente = d.id_cliente
  AND d.fecha_hora_desembolso >= c.vigente_desde
  AND (c.vigente_hasta IS NULL OR d.fecha_hora_desembolso < c.vigente_hasta)
LEFT JOIN dim_tipo_cambio_gld tc
  ON  tc.moneda = d.moneda
  AND CAST(d.fecha_hora_desembolso AS DATE) >= tc.vigente_desde
  AND (tc.vigente_hasta IS NULL OR CAST(d.fecha_hora_desembolso AS DATE) < tc.vigente_hasta);
