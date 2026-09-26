-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.desembolsos_estados
-- Familia : Historial de estados
-- Patrón  : insert_only
-- Origen  : bronze.lending.desembolsos_brz
-- Clave   : id_desembolso, fecha_actualizacion
-- Orden   : fecha_actualizacion
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.desembolsos_estados') (
  -- ── Columnas de negocio (tipadas) ──
  id_desembolso         BIGINT NOT NULL          COMMENT 'Clave (1/2): desembolso',
  fecha_actualizacion   TIMESTAMP NOT NULL       COMMENT 'Clave (2/2): momento de la versión en el origen',
  estado_desembolso     STRING NOT NULL          COMMENT 'Estado en esa versión',
  monto_desembolsado    DECIMAL(14,2)            COMMENT 'Monto en esa versión',
  referencia_bancaria   STRING                   COMMENT 'Referencia bancaria en esa versión',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_desembolso, fecha_actualizacion)
COMMENT 'Recorrido de estados de cada desembolso (por ejemplo Completado y luego Reversado): una fila por versión observada en los deltas. Mismo patrón que solicitudes_estados: se escribe desde el micro-lote de desembolsos, insert-only por (id_desembolso, fecha_actualizacion). Ejercicio de la S16. CDF activo.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.desembolsos_brz',
  'load_pattern'                       = 'insert_only',
  'business_key'                       = 'id_desembolso, fecha_actualizacion',
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
