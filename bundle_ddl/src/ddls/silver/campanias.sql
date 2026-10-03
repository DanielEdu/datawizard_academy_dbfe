-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.campanias
-- Familia : Catálogo
-- Patrón  : upsert
-- Origen  : bronze.lending.campanias_brz
-- Clave   : id_campania
-- Orden   : fecha_actualizacion, fecha_creacion, fecha_inicio
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.campanias') (
  -- ── Columnas de negocio (tipadas) ──
  id_campania           INT NOT NULL             COMMENT 'Clave de negocio: identificador de la campaña',
  id_pais               SMALLINT                 COMMENT 'País de la campaña (referencia a paises)',
  nombre_campania       STRING                   COMMENT 'Nombre comercial de la campaña',
  tipo_campania         STRING                   COMMENT 'Captacion, Reactivacion o Cross-sell',
  fecha_inicio          DATE                     COMMENT 'Fecha de negocio: inicio de la campaña',
  fecha_fin             DATE                     COMMENT 'Fecha de negocio: fin de la campaña; NULL si sigue abierta',
  presupuesto           DECIMAL(14,2)            COMMENT 'Presupuesto asignado en moneda local del país',
  fecha_creacion        TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el origen (UTC)',
  fecha_actualizacion   TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el origen. Si la versión llegó sin ella (primera carga), toma la primera fecha disponible de la lista de orden. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
-- Sin CLUSTER BY: tabla pequeña, cabe en pocos archivos y Gold la lee completa.
COMMENT 'Campañas de marketing de Wizard Bank (captación, reactivación, cross-sell). Una fila por campaña con su último estado (SCD Tipo 1): nadie pregunta por el presupuesto anterior de una campaña. Se construye desde bronze.lending.campanias_brz con MERGE upsert. Tabla pequeña: sin CDF ni clustering.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.campanias_brz',
  'load_pattern'                       = 'upsert',
  'business_key'                       = 'id_campania',
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
