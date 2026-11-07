-- Sesión 21 · Paso 5 · Column mask: quién ve a qué cliente pertenece cada crédito.
-- El primer parámetro es la columna enmascarada; lo que devuelve reemplaza el valor para quien no cumple la condición.
-- Solo cobranzas necesita saber de quién es cada crédito (para gestionarlo); el resto ve NULL.
CREATE OR REPLACE FUNCTION datawizard_dev.lending.mascara_id_cliente(id_cliente BIGINT)
RETURNS BIGINT
COMMENT 'Column mask de id_cliente: solo el grupo cobranzas ve el valor real'
RETURN CASE WHEN is_account_group_member('cobranzas') THEN id_cliente ELSE NULL END
