-- Sesión 21 · Paso 6 · Tabla ancha de cartera — VERSIÓN GOBERNADA (row filter + column mask).
-- Reemplaza transformations/14_gold_cartera.sql (la versión con comentarios de la Sesión 20.2). Misma lógica; agrega:
--   - MASK mascara_id_cliente en id_cliente: solo el grupo `cobranzas` ve a qué cliente pertenece cada crédito
--   - WITH ROW FILTER filtro_pais_cartera ON (pais): cada grupo ve solo los créditos de su país
-- La tabla le pertenece al pipeline: ALTER TABLE ... SET ROW FILTER falla sobre ella. Las políticas se declaran aquí,
-- en su definición, igual que los comentarios y las llaves. Las dos funciones se crean ANTES con SQL (paso 5 de la guía).
-- ${politicas} lo reemplaza el pipeline con su Configuration (clave politicas = <catálogo>.<esquema> donde viven las funciones):
-- dentro de un pipeline una función de Unity Catalog se nombra completa.

CREATE OR REFRESH MATERIALIZED VIEW cartera_creditos_gld (
  numero_credito              BIGINT        NOT NULL COMMENT 'Llave: número de crédito del sistema de cobranzas (= id_desembolso de Lending)',
  id_cliente                  BIGINT                 MASK ${politicas}.mascara_id_cliente COMMENT 'Cliente dueño del crédito',
  id_pais                     SMALLINT               COMMENT 'País del crédito: 1 Perú, 2 Colombia, 3 Bolivia',
  pais                        STRING                 COMMENT 'País del crédito: Perú, Colombia o Bolivia (con tilde)',
  moneda                      STRING                 COMMENT 'Moneda local del crédito: PEN, COP o BOB',
  nivel_riesgo_al_desembolso  STRING                 COMMENT 'Nivel de riesgo A (mejor) a E que tenía el cliente cuando se le desembolsó. No es el riesgo de hoy',
  fecha_desembolso            DATE                   COMMENT 'Fecha del desembolso del crédito',
  monto_desembolsado          DECIMAL(18,2)          COMMENT 'Monto desembolsado en MONEDA LOCAL. No sumar entre países',
  fecha_corte                 DATE                   COMMENT 'Fecha de la foto: el último pago registrado. Toda la fila describe el crédito a esta fecha',
  n_cuotas                    BIGINT                 COMMENT 'Cuotas del plan de pagos',
  cuotas_pagadas              BIGINT                 COMMENT 'Cuotas con estado Pagada a la fecha de corte',
  cuotas_vencidas             BIGINT                 COMMENT 'Cuotas vencidas sin pagar a la fecha de corte',
  saldo_capital               DECIMAL(18,2)          COMMENT 'Capital pendiente en MONEDA LOCAL. No sumar entre países: para totales usar saldo_capital_usd',
  monto_vencido               DECIMAL(18,2)          COMMENT 'Monto de las cuotas vencidas en MONEDA LOCAL. No sumar entre países',
  dias_mora                   INT                    COMMENT 'Días desde la cuota vencida impaga más antigua hasta la fecha de corte. 0 = al día. Un crédito está EN MORA si dias_mora > 0',
  n_pagos                     BIGINT                 COMMENT 'Pagos recibidos',
  monto_pagado                DECIMAL(18,2)          COMMENT 'Total pagado en MONEDA LOCAL. No sumar entre países',
  fecha_ultimo_pago           TIMESTAMP              COMMENT 'Último pago recibido',
  n_gestiones                 BIGINT                 COMMENT 'Gestiones de cobranza realizadas',
  resultado_ultima_gestion    STRING                 COMMENT 'Resultado de la última gestión: Compromiso de pago, Sin respuesta, Rechazo, Pago inmediato o Sin gestion',
  saldo_capital_usd           DECIMAL(18,2)          COMMENT 'Capital pendiente en USD (tasa de la fecha de corte). Usar este para sumar saldos entre países',
  tramo_mora                  STRING                 COMMENT 'Tramo de mora: Al dia, 1-30, 31-60, 61-90, Mas de 90 (sin tildes)',
  es_mora_30                  INT                    COMMENT '1 si dias_mora > 30 (mora mayor a 30 días, "cartera en riesgo"), 0 si no. "En mora" a secas es dias_mora > 0',
  CONSTRAINT pk_cartera_creditos_gld PRIMARY KEY (numero_credito)
)
COMMENT 'Cartera de créditos: una fila por crédito con cuotas, pagos, gestiones y mora a la fecha de corte. Úsala para cartera, saldos, mora y cobranza. En mora = dias_mora > 0. Mora mayor a 30 días = es_mora_30 = 1. Los montos locales (saldo_capital, monto_vencido, monto_pagado, monto_desembolsado) no se suman entre países: para totales usar saldo_capital_usd. NO sirve para colocación por mes (usar kpi_colocacion). Datos sintéticos.'
CLUSTER BY (numero_credito)
WITH ROW FILTER ${politicas}.filtro_pais_cartera ON (pais)
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
  CAST(ca.numero_credito AS BIGINT)                        AS numero_credito,
  CAST(ca.id_cliente AS BIGINT)                            AS id_cliente,
  CAST(ca.id_pais AS SMALLINT)                             AS id_pais,
  CAST(ca.pais AS STRING)                                  AS pais,
  CAST(ca.moneda AS STRING)                                AS moneda,
  CAST(ca.nivel_riesgo_al_desembolso AS STRING)            AS nivel_riesgo_al_desembolso,
  CAST(ca.fecha_desembolso AS DATE)                        AS fecha_desembolso,
  CAST(ca.monto_desembolsado AS DECIMAL(18,2))             AS monto_desembolsado,
  CAST(ca.fecha_corte AS DATE)                             AS fecha_corte,
  CAST(ca.n_cuotas AS BIGINT)                              AS n_cuotas,
  CAST(ca.cuotas_pagadas AS BIGINT)                        AS cuotas_pagadas,
  CAST(ca.cuotas_vencidas AS BIGINT)                       AS cuotas_vencidas,
  CAST(ca.saldo_capital AS DECIMAL(18,2))                  AS saldo_capital,
  CAST(ca.monto_vencido AS DECIMAL(18,2))                  AS monto_vencido,
  CAST(ca.dias_mora AS INT)                                AS dias_mora,
  CAST(ca.n_pagos AS BIGINT)                               AS n_pagos,
  CAST(ca.monto_pagado AS DECIMAL(18,2))                   AS monto_pagado,
  CAST(ca.fecha_ultimo_pago AS TIMESTAMP)                  AS fecha_ultimo_pago,
  CAST(ca.n_gestiones AS BIGINT)                           AS n_gestiones,
  CAST(ca.resultado_ultima_gestion AS STRING)              AS resultado_ultima_gestion,
  CAST(round(ca.saldo_capital / tc.tasa_venta, 2) AS DECIMAL(18,2)) AS saldo_capital_usd,
  CASE WHEN ca.dias_mora = 0   THEN 'Al dia'
       WHEN ca.dias_mora <= 30 THEN '1-30'
       WHEN ca.dias_mora <= 60 THEN '31-60'
       WHEN ca.dias_mora <= 90 THEN '61-90'
       ELSE 'Mas de 90' END                                AS tramo_mora,
  CASE WHEN ca.dias_mora > 30 THEN 1 ELSE 0 END            AS es_mora_30
FROM cartera ca
LEFT JOIN dim_tipo_cambio_gld tc
  ON  tc.moneda = ca.moneda
  AND ca.fecha_corte >= tc.vigente_desde
  AND (tc.vigente_hasta IS NULL OR ca.fecha_corte < tc.vigente_hasta);
