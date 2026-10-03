-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.tipos_cambio
-- Familia : Catálogo (referencia externa)
-- Patrón  : upsert
-- Origen  : bronze.lending.tipos_cambio_brz
-- Clave   : fecha, moneda_origen, moneda_destino
-- Orden   : _bronze_ingestion_ts
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.tipos_cambio') (
  -- ── Columnas de negocio (tipadas) ──
  fecha                 DATE NOT NULL            COMMENT 'Clave: fecha de negocio a la que aplica la tasa',
  moneda_origen         STRING NOT NULL          COMMENT 'Clave: moneda local PEN, COP o BOB',
  moneda_destino        STRING NOT NULL          COMMENT 'Clave: moneda destino, USD',
  id_tipo_cambio        INT                      COMMENT 'Identificador del registro en la fuente; no es clave en Silver (el proveedor puede reenviar con otro id)',
  tasa_compra           DECIMAL(12,6)            COMMENT 'Tasa de compra. Regla: > 0',
  tasa_venta            DECIMAL(12,6)            COMMENT 'Tasa de venta. Regla: >= tasa_compra',
  fecha_creacion        TIMESTAMP                COMMENT 'Auditoría de la fuente',
  fecha_actualizacion   TIMESTAMP                COMMENT 'Auditoría de la fuente; no se usa como orden porque el proveedor no la mantiene',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
-- Sin CLUSTER BY: tabla pequeña, cabe en pocos archivos y Gold la lee completa.
COMMENT 'Tipo de cambio diario de cada moneda local (PEN, COP, BOB) contra USD, del feed de un proveedor externo que llega como CSV diario a landing. Una fila por (fecha, moneda_origen, moneda_destino). Si el proveedor corrige una tasa reenvía el archivo: gana el último ingestado (orden = _bronze_ingestion_ts), sin historia. Gold la usa para consolidar montos en USD. Tabla pequeña: sin CDF ni clustering.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.tipos_cambio_brz',
  'load_pattern'                       = 'upsert',
  'business_key'                       = 'fecha, moneda_origen, moneda_destino',
  -- Change Data Feed: no hace falta: tabla pequeña que Gold relee completa.
  'delta.enableChangeDataFeed'         = 'false',
  -- Column mapping por nombre: renombrar o eliminar columnas sin reescribir archivos.
  'delta.columnMapping.mode'           = 'name',
  -- Menos archivos y más grandes al escribir; compacta los chicos después de cada MERGE.
  'delta.autoOptimize.optimizeWrite'   = 'true',
  'delta.autoOptimize.autoCompact'     = 'true',
  -- Time travel de 30 días para auditar y recuperar; archivos borrados se purgan a los 7.
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
