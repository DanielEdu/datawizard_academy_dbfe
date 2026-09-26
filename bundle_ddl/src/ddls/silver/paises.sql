-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.paises
-- Familia : Catálogo
-- Patrón  : upsert
-- Origen  : bronze.lending.paises_brz
-- Clave   : id_pais
-- Orden   : fecha_actualizacion, fecha_creacion
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.paises') (
  -- ── Columnas de negocio (tipadas) ──
  id_pais               SMALLINT NOT NULL        COMMENT 'Clave de negocio: identificador del país',
  codigo_iso            STRING                   COMMENT 'Código ISO 3166-1 alfa-2 (PE, CO, BO). Regla: dos letras mayúsculas',
  nombre_pais           STRING                   COMMENT 'Nombre del país',
  moneda_codigo         STRING                   COMMENT 'Código ISO 4217 de la moneda local (PEN, COP, BOB)',
  fecha_creacion        TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el origen (UTC)',
  fecha_actualizacion   TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el origen. Si la versión llegó sin ella (primera carga), toma la primera fecha disponible de la lista de orden. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
-- Sin CLUSTER BY: tabla pequeña, cabe en pocos archivos y Gold la lee completa.
COMMENT 'Catálogo de los países donde opera Wizard Bank (Perú, Colombia, Bolivia) con su moneda local. Una fila por país, sin historia: un cambio se sobrescribe (SCD Tipo 1). Se construye desde bronze.lending.paises_brz tipando y quedándose con la versión más reciente por id_pais (MERGE upsert con guarda de orden). Tabla pequeña: Gold la relee completa, por eso no lleva CDF ni clustering.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.paises_brz',
  'load_pattern'                       = 'upsert',
  'business_key'                       = 'id_pais',
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
