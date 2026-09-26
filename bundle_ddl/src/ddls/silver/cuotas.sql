-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.cobranzas.cuotas
-- Familia : Transaccional con estado
-- Patrón  : upsert
-- Origen  : bronze.cobranzas.cuotas_brz
-- Clave   : id_cuota
-- Orden   : fecha_modificacion, fecha_creacion
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.cobranzas.cuotas') (
  -- ── Columnas de negocio (tipadas) ──
  id_cuota              BIGINT NOT NULL          COMMENT 'Clave de negocio: identificador de la cuota',
  numero_credito        BIGINT                   COMMENT 'Crédito; equivale a silver.lending.desembolsos.id_desembolso',
  numero_cuota          SMALLINT                 COMMENT 'Número de la cuota dentro del cronograma',
  fecha_vencimiento     DATE                     COMMENT 'Fecha de vencimiento',
  monto_cuota           DECIMAL(12,2)            COMMENT 'Monto total. Regla: = capital + interés',
  monto_capital         DECIMAL(12,2)            COMMENT 'Porción de capital',
  monto_interes         DECIMAL(12,2)            COMMENT 'Porción de interés',
  estado_cuota          STRING                   COMMENT 'Estado vigente: Pendiente, Pagada o Vencida',
  fecha_creacion        TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el sistema de cobranzas (UTC)',
  fecha_modificacion    TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el sistema de cobranzas. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_cuota, fecha_vencimiento)
COMMENT 'Cronograma de cuotas de cada crédito, del sistema legado de cobranzas (on-premise, ingesta batch vía ADF). Una fila por cuota con su estado vigente (Pendiente, Pagada, Vencida). MERGE upsert con guarda por fecha_modificacion, el watermark del legado. Se enlaza con Lending por numero_credito = silver.lending.desembolsos.id_desembolso (la integración es trabajo de Gold). CDF activo.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'legacy_cobranzas_onprem',
  'source_table'                       = 'bronze.cobranzas.cuotas_brz',
  'load_pattern'                       = 'upsert',
  'business_key'                       = 'id_cuota',
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
