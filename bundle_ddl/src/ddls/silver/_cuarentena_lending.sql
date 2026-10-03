-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending._cuarentena
-- Filas de Bronze que no pasaron las reglas de calidad de su tabla Silver.
-- Una tabla por schema. Garantiza que el conteo cuadre:
--   claves de Bronze = Silver + cuarentena (+ duplicados resueltos)
-- Append simple: cada lote agrega sus rechazos; se revisan y corrigen en la fuente.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending._cuarentena') (
  tabla           STRING    NOT NULL COMMENT 'Tabla Silver a la que iba la fila (p. ej. solicitudes_prestamo)',
  motivos         STRING    NOT NULL COMMENT 'Reglas que falló, separadas por coma (nombres del YAML de configuración)',
  fila            STRING    NOT NULL COMMENT 'La fila completa, ya tipada, serializada como JSON',
  _origen_archivo STRING             COMMENT 'Archivo de landing que trajo la fila',
  _batch_id       BIGINT             COMMENT 'Micro-lote de foreachBatch que la rechazó',
  _procesado_ts   TIMESTAMP NOT NULL COMMENT 'Momento del rechazo'
)
USING DELTA
CLUSTER BY (tabla, _procesado_ts)
COMMENT 'Cuarentena de silver.lending: filas rechazadas por las reglas de calidad, con el motivo y la fila completa en JSON. Nada se descarta en silencio: si una fila no llega a Silver, está aquí.'
TBLPROPERTIES (
  -- Etiquetas propias: capa y propósito.
  'quality'                            = 'silver',
  'load_pattern'                       = 'append',
  -- Sin CDF: nadie la consume de forma incremental; se consulta para revisar calidad.
  'delta.enableChangeDataFeed'         = 'false',
  -- Solo recibe inserts: se protege contra updates o deletes accidentales.
  'delta.appendOnly'                   = 'true',
  'delta.columnMapping.mode'           = 'name',
  'delta.autoOptimize.optimizeWrite'   = 'true',
  'delta.autoOptimize.autoCompact'     = 'true',
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
