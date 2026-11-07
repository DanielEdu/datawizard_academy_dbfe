-- Sesión 20 · Paso 2 · Gold: las dimensiones del modelo estrella, como MATERIALIZED VIEWS en SQL.
-- Gold une y agrega datasets completos: se lee en batch (sin STREAM) y el pipeline recalcula lo que cambió.
-- Un pipeline mezcla .py y .sql: estas vistas leen las Silver de la Sesión 19 por su nombre corto.
-- Nombres del negocio (pais, no nombre_pais) y sin columnas técnicas ni datos personales.

-- País: 3 filas. La moneda une a los desembolsos con su tipo de cambio
CREATE OR REFRESH MATERIALIZED VIEW dim_pais_gld
COMMENT 'Dimensión país: una fila por país (Perú, Colombia, Bolivia) con su moneda local (PEN, COP, BOB)'
AS SELECT
  id_pais,
  codigo_iso,
  nombre_pais   AS pais,
  moneda_codigo AS moneda
FROM paises_slv;

-- Campaña de marketing. El presupuesto queda en moneda local: la conversión a USD se hace en la métrica de ROI
CREATE OR REFRESH MATERIALIZED VIEW dim_campania_gld
COMMENT 'Dimensión campaña: una fila por campaña de marketing. presupuesto_local está en la moneda del país de la campaña'
AS SELECT
  id_campania,
  id_pais,
  nombre_campania,
  tipo_campania,
  fecha_inicio,
  fecha_fin,
  presupuesto AS presupuesto_local
FROM campanias_slv;

-- Producto: de la SCD2 de Silver se publica solo la versión vigente (las condiciones de hoy)
CREATE OR REFRESH MATERIALIZED VIEW dim_producto_gld
COMMENT 'Dimensión producto: una fila por producto de crédito con sus condiciones vigentes hoy (montos en moneda local)'
AS SELECT
  id_producto,
  id_pais,
  nombre_producto,
  tipo_producto,
  monto_minimo,
  monto_maximo,
  plazo_min_meses,
  plazo_max_meses,
  tasa_interes_anual,
  es_activo
FROM productos_slv
WHERE __END_AT IS NULL;

-- Calendario: no viene de ninguna fuente, se genera. id_fecha (AAAAMMDD) es la llave que usan los hechos
CREATE OR REFRESH MATERIALIZED VIEW dim_tiempo_gld
COMMENT 'Dimensión calendario: un día por fila, de 2024 a 2027'
AS SELECT
  CAST(date_format(fecha, 'yyyyMMdd') AS INT) AS id_fecha,
  fecha,
  year(fecha)                   AS anio,
  quarter(fecha)                AS trimestre,
  month(fecha)                  AS mes,
  trunc(fecha, 'MM')            AS primer_dia_mes,
  date_format(fecha, 'yyyy-MM') AS anio_mes,
  dayofweek(fecha) IN (1, 7)    AS es_fin_de_semana
FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS fecha);

-- Cliente con historia: una fila por VERSIÓN (la SCD2 de clientes_slv), con una llave por versión: sk_cliente.
-- Los hechos guardan el sk_cliente vigente cuando ocurrieron: así "colocación por nivel de riesgo" usa el riesgo
-- que tenía el cliente al desembolsar, no el de hoy.
CREATE OR REFRESH MATERIALIZED VIEW dim_cliente_gld
COMMENT 'Dimensión cliente con historia (SCD2): una fila por versión del cliente, sin datos personales. Para el estado de hoy filtra es_vigente'
CLUSTER BY (id_cliente)
AS SELECT
  xxhash64(id_cliente, __START_AT.fecha_version, __START_AT._ingestado_en) AS sk_cliente,
  id_cliente,
  id_pais,
  ciudad,
  situacion_laboral,
  ingreso_mensual_declarado,
  score_interno,
  nivel_riesgo,
  CASE WHEN score_interno >= 700 THEN 'Alto'
       WHEN score_interno >= 600 THEN 'Medio'
       ELSE 'Bajo' END                                   AS rango_score,
  id_campania_captacion,
  fecha_registro,
  -- La primera versión cubre desde siempre: un hecho anterior a la primera fecha_version igual encuentra cliente
  CASE WHEN row_number() OVER (PARTITION BY id_cliente ORDER BY __START_AT) = 1 THEN TIMESTAMP'1900-01-01'
       ELSE __START_AT.fecha_version END                 AS vigente_desde,
  __END_AT.fecha_version                                 AS vigente_hasta,      -- NULL = versión vigente
  __END_AT IS NULL                                       AS es_vigente
FROM clientes_slv;

-- Tipo de cambio a USD con su rango de vigencia: un día sin publicación usa la última tasa conocida.
-- Lo usan los hechos (monto_usd), el ROI de campañas y la cartera.
CREATE OR REFRESH MATERIALIZED VIEW dim_tipo_cambio_gld
COMMENT 'Tipo de cambio de cada moneda a USD con su rango de vigencia [vigente_desde, vigente_hasta). Dividir un monto local por tasa_venta da USD'
AS SELECT
  moneda_origen                                                       AS moneda,
  fecha                                                               AS vigente_desde,
  lead(fecha) OVER (PARTITION BY moneda_origen ORDER BY fecha)        AS vigente_hasta,   -- NULL = tasa vigente hoy
  tasa_venta
FROM tipos_cambio_slv
WHERE moneda_destino = 'USD';
