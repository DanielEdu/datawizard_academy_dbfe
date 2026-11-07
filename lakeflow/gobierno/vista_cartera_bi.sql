-- Sesión 21 · Paso 8 · La política viaja con la tabla: una vista para BI sobre la cartera gobernada.
-- BI no necesita el detalle por crédito: recibe una vista agregada por país y tramo, sin id_cliente, y SELECT solo sobre ella.
-- El row filter de cartera_creditos_gld se sigue aplicando a través de la vista, con la identidad de quien consulta:
-- un analista que no está en cartera_peru ni en cartera_todos ve la vista vacía. Una vista no es una puerta trasera.
CREATE OR REPLACE VIEW datawizard_dev.lending.v_cartera_bi
COMMENT 'Cartera agregada por país y tramo de mora para BI. Sin id_cliente ni números de crédito'
AS SELECT
  pais,
  tramo_mora,
  count(*)                      AS creditos,
  sum(saldo_capital_usd)        AS saldo_capital_usd,
  sum(es_mora_30)               AS creditos_mora_30
FROM datawizard_dev.lending.cartera_creditos_gld
GROUP BY pais, tramo_mora
