-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.desembolsos
-- Familia : Transaccional con estado
-- Patrón  : upsert
-- Origen  : bronze.lending.desembolsos_brz
-- Clave   : id_desembolso
-- Orden   : fecha_actualizacion, fecha_creacion, fecha_hora_desembolso
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.desembolsos') (
  -- ── Columnas de negocio (tipadas) ──
  id_desembolso          BIGINT NOT NULL          COMMENT 'Clave de negocio; equivale a numero_credito en silver.cobranzas',
  id_solicitud           BIGINT                   COMMENT 'Solicitud desembolsada (una sola por desembolso)',
  id_cliente             BIGINT                   COMMENT 'Cliente que recibe el desembolso',
  monto_desembolsado     DECIMAL(14,2)            COMMENT 'Monto desembolsado. Regla: > 0',
  moneda                 STRING                   COMMENT 'PEN, COP o BOB',
  tasa_aplicada          DECIMAL(6,3)             COMMENT 'Tasa de interés aplicada (porcentaje)',
  plazo_meses            SMALLINT                 COMMENT 'Plazo del crédito en meses',
  comision_cobrada       DECIMAL(12,2)            COMMENT 'Comisión cobrada en el desembolso',
  cuenta_destino_masked  STRING                   COMMENT 'PII parcial: cuenta destino enmascarada (****1234)',
  fecha_hora_desembolso  TIMESTAMP                COMMENT 'Fecha de negocio: cuándo se desembolsó',
  estado_desembolso      STRING                   COMMENT 'Estado vigente: Completado, Fallido o Reversado',
  referencia_bancaria    STRING                   COMMENT 'Referencia de la transferencia bancaria',
  fecha_creacion         TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el origen (UTC)',
  fecha_actualizacion    TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el origen. Si la versión llegó sin ella (primera carga), toma la primera fecha disponible de la lista de orden. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo        STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts   TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts          TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_desembolso, fecha_hora_desembolso)
COMMENT 'Desembolsos de los préstamos aprobados, uno por solicitud: cierran el funnel y originan el crédito que se cobra en cobranzas (id_desembolso = numero_credito). Una fila por desembolso con su estado vigente (Completado, Fallido, Reversado). Llega por deltas con MERGE upsert y guarda de orden. Contiene PII parcial (cuenta enmascarada). CDF activo: Gold construye hechos de colocación.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.desembolsos_brz',
  'load_pattern'                       = 'upsert',
  'business_key'                       = 'id_desembolso',
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
