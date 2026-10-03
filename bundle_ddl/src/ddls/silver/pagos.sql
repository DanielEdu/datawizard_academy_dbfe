-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.cobranzas.pagos
-- Familia : Hecho inmutable
-- Patrón  : insert_only
-- Origen  : bronze.cobranzas.pagos_brz
-- Clave   : id_pago
-- Orden   : fecha_modificacion, fecha_creacion, fecha_pago
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.cobranzas.pagos') (
  -- ── Columnas de negocio (tipadas) ──
  id_pago               BIGINT NOT NULL          COMMENT 'Clave de negocio: identificador del pago',
  numero_credito        BIGINT                   COMMENT 'Crédito pagado',
  numero_cuota          SMALLINT                 COMMENT 'Cuota pagada',
  fecha_pago            TIMESTAMP                COMMENT 'Fecha de negocio: cuándo se registró el pago',
  monto_pagado          DECIMAL(12,2)            COMMENT 'Monto pagado. Regla: > 0',
  medio_pago            STRING                   COMMENT 'Medio de pago',
  fecha_creacion        TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el sistema de cobranzas (UTC)',
  fecha_modificacion    TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el sistema de cobranzas. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_pago, fecha_pago)
COMMENT 'Pagos efectivamente cobrados, uno por evento de pago, del sistema legado de cobranzas. Un pago no cambia después de registrado: MERGE insert-only por id_pago (sin WHEN MATCHED), así reprocesar un archivo no duplica ni altera nada. Se cruza con cuotas por (numero_credito, numero_cuota). CDF activo: Gold calcula recaudación y mora de forma incremental.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'legacy_cobranzas_onprem',
  'source_table'                       = 'bronze.cobranzas.pagos_brz',
  'load_pattern'                       = 'insert_only',
  'business_key'                       = 'id_pago',
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
