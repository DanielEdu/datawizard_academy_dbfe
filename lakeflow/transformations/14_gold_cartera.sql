-- Sesión 20 · Paso 7 · Gold: tabla ancha de cartera (Cobranzas + Lending).
-- Tercera forma de Gold (Sesión 18): ni estrella ni métrica, sino UNA FILA POR CRÉDITO con todo lo que se sabe de él,
-- lista para un analista de cobranzas o para un dashboard de cartera.
-- numero_credito del legado = id_desembolso de Lending: por ahí la cartera conoce al cliente, su país y su moneda.
-- Es una FOTO a fecha_corte (el último pago registrado): sirve para "cómo está la cartera hoy", no para entrenar un modelo
-- (para eso están las feature tables del paso 8, que guardan desde cuándo se conoce cada dato).

CREATE OR REFRESH MATERIALIZED VIEW cartera_creditos_gld
COMMENT 'Cartera: una fila por crédito con cuotas, pagos, gestiones y mora a fecha_corte. Montos en moneda local y saldo también en USD'
CLUSTER BY (numero_credito)
AS
WITH corte AS (
  SELECT CAST(max(fecha_pago) AS DATE) AS fecha_corte FROM pagos_slv
),
cuotas AS (
  SELECT numero_credito,
         count(*)                                                        AS n_cuotas,
         count_if(estado_cuota = 'Pagada')                               AS cuotas_pagadas,
         count_if(estado_cuota = 'Vencida')                              AS cuotas_vencidas,
         sum(monto_capital) FILTER (WHERE estado_cuota <> 'Pagada')      AS saldo_capital,
         sum(monto_cuota)   FILTER (WHERE estado_cuota = 'Vencida')      AS monto_vencido,
         min(fecha_vencimiento) FILTER (WHERE estado_cuota = 'Vencida')  AS vencida_mas_antigua
  FROM cuotas_slv
  GROUP BY numero_credito
),
pagos AS (
  SELECT numero_credito, count(*) AS n_pagos, sum(monto_pagado) AS monto_pagado, max(fecha_pago) AS fecha_ultimo_pago
  FROM pagos_slv
  GROUP BY numero_credito
),
gestiones AS (
  SELECT numero_credito, count(*) AS n_gestiones, max_by(resultado, fecha_gestion) AS resultado_ultima_gestion
  FROM gestiones_cobranza_slv
  GROUP BY numero_credito
),
cartera AS (
  SELECT
    c.numero_credito,
    d.id_cliente,
    d.id_pais,
    pa.pais,
    d.moneda,
    cl.nivel_riesgo                                                   AS nivel_riesgo_al_desembolso,
    d.fecha_desembolso,
    d.monto_desembolsado,
    corte.fecha_corte,
    c.n_cuotas,
    c.cuotas_pagadas,
    c.cuotas_vencidas,
    coalesce(c.saldo_capital, 0)                                      AS saldo_capital,
    coalesce(c.monto_vencido, 0)                                      AS monto_vencido,
    greatest(coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0), 0) AS dias_mora,   -- una cuota marcada Vencida después del corte no da mora negativa
    coalesce(p.n_pagos, 0)                                            AS n_pagos,
    coalesce(p.monto_pagado, 0)                                       AS monto_pagado,
    p.fecha_ultimo_pago,
    coalesce(g.n_gestiones, 0)                                        AS n_gestiones,
    coalesce(g.resultado_ultima_gestion, 'Sin gestion')               AS resultado_ultima_gestion
  FROM cuotas c
  CROSS JOIN corte
  LEFT JOIN pagos p                   ON p.numero_credito = c.numero_credito
  LEFT JOIN gestiones g               ON g.numero_credito = c.numero_credito
  LEFT JOIN fact_desembolsos_gld d    ON d.id_desembolso = c.numero_credito
  LEFT JOIN dim_cliente_gld cl        ON cl.sk_cliente = d.sk_cliente
  LEFT JOIN dim_pais_gld pa           ON pa.id_pais = d.id_pais
)
SELECT
  ca.*,
  round(ca.saldo_capital / tc.tasa_venta, 2)                          AS saldo_capital_usd,
  CASE WHEN ca.dias_mora = 0   THEN 'Al dia'
       WHEN ca.dias_mora <= 30 THEN '1-30'
       WHEN ca.dias_mora <= 60 THEN '31-60'
       WHEN ca.dias_mora <= 90 THEN '61-90'
       ELSE 'Mas de 90' END                                           AS tramo_mora,
  CASE WHEN ca.dias_mora > 30 THEN 1 ELSE 0 END                       AS es_mora_30
FROM cartera ca
LEFT JOIN dim_tipo_cambio_gld tc
  ON  tc.moneda = ca.moneda
  AND ca.fecha_corte >= tc.vigente_desde
  AND (tc.vigente_hasta IS NULL OR ca.fecha_corte < tc.vigente_hasta);
