-- Sesión 21 · Paso 5 · Row filter: qué créditos de la cartera ve cada grupo.
-- Una función de Unity Catalog que devuelve TRUE para las filas visibles. El pipeline la aplica a cartera_creditos_gld
-- (WITH ROW FILTER en gobierno/14_gold_cartera.sql) y se evalúa en CADA consulta, con la identidad de quien consulta.
-- Los grupos de Unity Catalog son de la CUENTA: se pregunta con is_account_group_member(). is_member() solo mira grupos locales
-- del workspace, que Unity Catalog no acepta en un GRANT.
-- No hay atajo para admins a propósito: así el dueño de la tabla también ve el efecto al entrar y salir de los grupos.
CREATE OR REPLACE FUNCTION datawizard_dev.lending.filtro_pais_cartera(pais STRING)
RETURNS BOOLEAN
COMMENT 'Row filter de la cartera: cartera_todos ve todo; cartera_peru solo los créditos de Perú'
RETURN is_account_group_member('cartera_todos') OR (pais = 'Perú' AND is_account_group_member('cartera_peru'))
