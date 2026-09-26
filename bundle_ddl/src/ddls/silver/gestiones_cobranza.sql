-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.cobranzas.gestiones_cobranza
-- Familia : Hecho inmutable
-- Patrón  : insert_only
-- Origen  : bronze.cobranzas.gestiones_cobranza_brz
-- Clave   : id_gestion
-- Orden   : fecha_modificacion, fecha_creacion, fecha_gestion
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.cobranzas.gestiones_cobranza') (
  -- ── Columnas de negocio (tipadas) ──
  id_gestion            BIGINT NOT NULL          COMMENT 'Clave de negocio: identificador de la gestión',
  numero_credito        BIGINT                   COMMENT 'Crédito gestionado',
  fecha_gestion         TIMESTAMP                COMMENT 'Fecha de negocio: cuándo se hizo la gestión',
  tipo_gestion          STRING                   COMMENT 'Llamada, SMS, Email, Visita o Carta Notarial',
  resultado             STRING                   COMMENT 'Resultado de la gestión',
  dias_mora_al_momento  SMALLINT                 COMMENT 'Días de mora del crédito al momento de la gestión. Regla: >= 0',
  gestor                STRING                   COMMENT 'Gestor que realizó la acción',
  fecha_creacion        TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el sistema de cobranzas (UTC)',
  fecha_modificacion    TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el sistema de cobranzas. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo       STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_gestion, fecha_gestion)
COMMENT 'Acciones del equipo de recuperación sobre créditos en mora (llamada, SMS, email, visita, carta notarial) con su resultado. Una acción registrada no cambia: MERGE insert-only por id_gestion. CDF activo: Gold mide efectividad de la gestión.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'legacy_cobranzas_onprem',
  'source_table'                       = 'bronze.cobranzas.gestiones_cobranza_brz',
  'load_pattern'                       = 'insert_only',
  'business_key'                       = 'id_gestion',
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
