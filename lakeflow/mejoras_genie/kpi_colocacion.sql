-- Sesión 20.2 · Paso 6a · kpi_colocacion — VERSIÓN MEJORADA PARA GENIE: la misma metric view de la Sesión 20 con SYNONYMS.
-- Los sinónimos le dicen a Genie (y a cualquier consumidor) cómo habla el negocio: "cuánto se prestó" = monto_colocado_usd.
-- Requieren DBR 17.3+ (serverless o SQL warehouse actual). Reemplaza la definición con CREATE OR REPLACE: no hay que borrar nada.
-- No es parte del pipeline: se crea con SQL (SQL editor o notebook en serverless) sobre sus tablas Gold.
-- Las MEDIDAS son agregados (SUM, COUNT); los ratios se componen con MEASURE() y Databricks los recalcula bien
-- para cada corte. Así nadie promedia tickets ni mezcla monedas.
-- Para consultarla:  SELECT pais, MEASURE(monto_colocado_usd) FROM datawizard_dev.lakeflow.kpi_colocacion GROUP BY pais
CREATE OR REPLACE VIEW datawizard_dev.lakeflow.kpi_colocacion
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "KPIs de colocación de créditos de Wizard Bank (datos sintéticos). Un desembolso es colocación solo si está Completado. Montos en USD con la tasa del día del desembolso. El nivel de riesgo es el que tenía el cliente al desembolsar."
source: datawizard_dev.lakeflow.fact_desembolsos_gld
joins:
  - name: dim_pais
    source: datawizard_dev.lakeflow.dim_pais_gld
    'on': source.id_pais = dim_pais.id_pais
  - name: dim_cliente
    source: datawizard_dev.lakeflow.dim_cliente_gld
    'on': source.sk_cliente = dim_cliente.sk_cliente
  - name: dim_producto
    source: datawizard_dev.lakeflow.dim_producto_gld
    'on': source.id_producto = dim_producto.id_producto
dimensions:
  - name: mes
    expr: "trunc(source.fecha_desembolso, 'MM')"
    comment: "Mes del desembolso, como su día 1"
    synonyms:
      - "mes del desembolso"
      - "periodo"
  - name: anio
    expr: year(source.fecha_desembolso)
    comment: "Año del desembolso"
  - name: pais
    expr: dim_pais.pais
    comment: "País del desembolso: Perú, Colombia o Bolivia"
    synonyms:
      - "país"
      - "mercado"
  - name: producto
    expr: dim_producto.nombre_producto
    comment: "Producto de crédito"
    synonyms:
      - "tipo de crédito"
      - "producto de crédito"
  - name: tipo_producto
    expr: dim_producto.tipo_producto
    comment: "Libre disponibilidad, Consumo o Educativo"
  - name: nivel_riesgo
    expr: dim_cliente.nivel_riesgo
    comment: "Nivel de riesgo A (mejor) a E del cliente al momento del desembolso"
    synonyms:
      - "riesgo"
      - "calificación"
      - "segmento de riesgo"
  - name: rango_score
    expr: dim_cliente.rango_score
    comment: "Rango del score interno al desembolsar: Alto (700+), Medio (600-699), Bajo"
  - name: situacion_laboral
    expr: dim_cliente.situacion_laboral
    comment: "Situación laboral del cliente al desembolsar"
  - name: estado_desembolso
    expr: source.estado_desembolso
    comment: "Completado, Fallido o Reversado. Las medidas de colocación ya filtran Completado"
measures:
  - name: desembolsos
    expr: COUNT(1) FILTER (WHERE source.es_completado = 1)
    comment: "Cantidad de desembolsos completados"
    synonyms:
      - "créditos colocados"
      - "préstamos desembolsados"
      - "número de desembolsos"
  - name: monto_colocado_usd
    expr: SUM(source.monto_usd) FILTER (WHERE source.es_completado = 1)
    comment: "Monto colocado en USD: suma de los desembolsos completados"
    synonyms:
      - "colocación"
      - "monto colocado"
      - "monto desembolsado"
      - "cuánto se prestó"
      - "cuánto se colocó"
  - name: comision_usd
    expr: SUM(source.comision_usd) FILTER (WHERE source.es_completado = 1)
    comment: "Comisiones cobradas en USD en los desembolsos completados"
  - name: clientes_con_desembolso
    expr: COUNT(DISTINCT source.id_cliente) FILTER (WHERE source.es_completado = 1)
    comment: "Clientes distintos con al menos un desembolso completado"
  - name: tasa_x_monto
    expr: SUM(source.tasa_aplicada * source.monto_usd) FILTER (WHERE source.es_completado = 1)
    comment: "Auxiliar para la tasa ponderada: tasa anual por monto en USD"
  - name: ticket_promedio_usd
    expr: MEASURE(monto_colocado_usd) / MEASURE(desembolsos)
    comment: "Monto promedio por desembolso completado, en USD"
    synonyms:
      - "ticket"
      - "monto promedio"
      - "préstamo promedio"
  - name: tasa_promedio_ponderada
    expr: MEASURE(tasa_x_monto) / MEASURE(monto_colocado_usd)
    comment: "Tasa anual promedio ponderada por monto colocado (porcentaje, ej. 28.5)"
    synonyms:
      - "tasa promedio"
      - "tasa de interés promedio"
  - name: pct_no_completados
    expr: COUNT(1) FILTER (WHERE source.es_completado = 0) / COUNT(1)
    comment: "Proporción de desembolsos Fallidos o Reversados sobre todos los intentos (0 a 1)"
    synonyms:
      - "porcentaje de fallidos"
      - "desembolsos fallidos"
      - "tasa de fallas"
$$
