-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.dim_productos
-- Familia : Maestra historizada
-- Patrón  : scd2
-- Origen  : bronze.lending.productos_prestamo_brz
-- Clave   : id_producto
-- Orden   : fecha_actualizacion, fecha_creacion
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.dim_productos') (
  -- ── Clave técnica: única por versión ──
  sk_producto            BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key: única por VERSIÓN del producto',

  -- ── Columnas de negocio (tipadas) ──
  id_producto            INT NOT NULL             COMMENT 'Clave de negocio: se repite en cada versión',
  tasa_interes_anual     DECIMAL(6,3)             COMMENT 'Tipo 2: tasa de interés anual (porcentaje)',
  monto_minimo           DECIMAL(14,2)            COMMENT 'Tipo 2: monto mínimo financiable',
  monto_maximo           DECIMAL(14,2)            COMMENT 'Tipo 2: monto máximo financiable',
  plazo_min_meses        SMALLINT                 COMMENT 'Tipo 2: plazo mínimo en meses',
  plazo_max_meses        SMALLINT                 COMMENT 'Tipo 2: plazo máximo en meses',
  es_activo              BOOLEAN                  COMMENT 'Tipo 2: si el producto se comercializa',
  nombre_producto        STRING                   COMMENT 'Tipo 1: nombre comercial',
  tipo_producto          STRING                   COMMENT 'Tipo 1: categoría del producto',
  id_pais                SMALLINT                 COMMENT 'Tipo 0: país donde se comercializa',

  -- ── Control de vigencia (SCD Tipo 2) ──
  fecha_inicio_vigencia  TIMESTAMP NOT NULL       COMMENT 'Desde cuándo rige esta versión (fecha_actualizacion del cambio en el origen)',
  fecha_fin_vigencia     TIMESTAMP                COMMENT 'Hasta cuándo rigió; NULL = versión vigente. Igual al inicio de la versión siguiente: sin huecos ni solapes',
  es_vigente             BOOLEAN NOT NULL         COMMENT 'TRUE solo en la versión actual. Invariante: exactamente una vigente por clave de negocio',
  hash_atributos         STRING NOT NULL          COMMENT 'SHA-256 de las columnas Tipo 2 (nulos como <null>). Si cambia, se abre una versión nueva',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo        STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts   TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts          TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_producto, es_vigente)
COMMENT 'Catálogo de productos de préstamo historizado con SCD Tipo 2: la tasa y las condiciones cambian, y un crédito se otorgó con las que regían ese día. Tasa, montos, plazos y es_activo son Tipo 2; nombre y tipo son Tipo 1; el país es Tipo 0. Se construye desde bronze.lending.productos_prestamo_brz con la misma función SCD2 que dim_clientes. CDF activo.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.productos_prestamo_brz',
  'load_pattern'                       = 'scd2',
  'business_key'                       = 'id_producto',
  -- Change Data Feed: Gold la lee de forma incremental con readChangeFeed.
  'delta.enableChangeDataFeed'         = 'true',
  -- Column mapping por nombre: renombrar o eliminar columnas sin reescribir archivos.
  'delta.columnMapping.mode'           = 'name',
  -- Menos archivos y más grandes al escribir; compacta los chicos después de cada MERGE.
  'delta.autoOptimize.optimizeWrite'   = 'true',
  'delta.autoOptimize.autoCompact'     = 'true',
  -- Time travel de 30 días para auditar y recuperar; archivos borrados se purgan a los 7.
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
