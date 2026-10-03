-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.ofertas_preaprobadas
-- Familia : Transaccional con estado
-- Patrón  : upsert
-- Origen  : bronze.lending.ofertas_preaprobadas_brz
-- Clave   : id_oferta
-- Orden   : fecha_actualizacion, fecha_creacion, fecha_generacion
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.ofertas_preaprobadas') (
  -- ── Columnas de negocio (tipadas) ──
  id_oferta             BIGINT NOT NULL          COMMENT 'Clave de negocio: identificador de la oferta',
  id_cliente            BIGINT                   COMMENT 'Cliente destinatario (referencia a dim_clientes)',
  id_producto           INT                      COMMENT 'Producto ofertado (referencia a dim_productos)',
  id_campania           INT                      COMMENT 'Campaña que originó la oferta; NULL si no vino de una campaña',
  monto_ofertado        DECIMAL(14,2)            COMMENT 'Monto preaprobado en moneda local. Regla: > 0',
  plazo_meses_ofertado  SMALLINT                 COMMENT 'Plazo ofertado en meses',
  tasa_ofertada         DECIMAL(6,3)             COMMENT 'Tasa de interés ofertada (porcentaje)',
  motor_asignacion      STRING                   COMMENT 'Modelo que asignó la oferta: modelo_v1 o modelo_v2 (A/B testing)',
  fecha_generacion      TIMESTAMP                COMMENT 'Fecha de negocio: cuándo se generó la oferta',
  fecha_vigencia_fin    DATE                     COMMENT 'Último día de vigencia de la oferta',
  estado_oferta         STRING                   COMMENT 'Vigente, Expirada o Aceptada; muta en el origen',
  fecha_creacion        TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el origen (UTC)',
  fecha_actualizacion   TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el origen. Si la versión llegó sin ella (primera carga), toma la primera fecha disponible de la lista de orden. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_oferta, fecha_generacion)
COMMENT 'Ofertas de préstamo preaprobadas que el motor de asignación genera para clientes existentes: la entrada del funnel de originación. Una fila por oferta con su estado vigente (Vigente, Expirada, Aceptada). Llega por deltas desde Azure SQL; el MERGE upsert con guarda de orden impide que una versión vieja pise una nueva. CDF activo: Gold construye el funnel de forma incremental.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.ofertas_preaprobadas_brz',
  'load_pattern'                       = 'upsert',
  'business_key'                       = 'id_oferta',
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
