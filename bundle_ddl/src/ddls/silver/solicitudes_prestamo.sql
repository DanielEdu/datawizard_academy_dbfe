-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.solicitudes_prestamo
-- Familia : Transaccional con estado
-- Patrón  : upsert
-- Origen  : bronze.lending.solicitudes_prestamo_brz
-- Clave   : id_solicitud
-- Orden   : fecha_actualizacion, fecha_creacion, fecha_hora_solicitud
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.solicitudes_prestamo') (
  -- ── Columnas de negocio (tipadas) ──
  id_solicitud            BIGINT NOT NULL          COMMENT 'Clave de negocio: identificador de la solicitud',
  id_oferta               BIGINT                   COMMENT 'Oferta de origen; NULL si no partió de una oferta',
  id_cliente              BIGINT                   COMMENT 'Cliente solicitante (referencia a dim_clientes). Regla: no nulo',
  id_producto             INT                      COMMENT 'Producto solicitado (referencia a dim_productos)',
  canal                   STRING                   COMMENT 'APP_IOS, APP_ANDROID o WEB',
  monto_solicitado        DECIMAL(14,2)            COMMENT 'Monto solicitado en moneda local. Regla: > 0',
  plazo_meses_solicitado  SMALLINT                 COMMENT 'Plazo solicitado en meses',
  fecha_hora_solicitud    TIMESTAMP                COMMENT 'Fecha de negocio: cuándo se envió la solicitud',
  estado_solicitud        STRING                   COMMENT 'Estado vigente. Regla: uno de los cinco estados válidos',
  score_evaluacion        SMALLINT                 COMMENT 'Score del motor de riesgo; NULL mientras no se evalúa',
  decision_motor          STRING                   COMMENT 'Aprobar, Rechazar o Revision manual',
  monto_aprobado          DECIMAL(14,2)            COMMENT 'Monto aprobado; NULL si no fue aprobada',
  tasa_aprobada           DECIMAL(6,3)             COMMENT 'Tasa aprobada (porcentaje); NULL si no fue aprobada',
  motivo_rechazo          STRING                   COMMENT 'Motivo del rechazo; NULL si no fue rechazada',
  fecha_hora_resolucion   TIMESTAMP                COMMENT 'Fecha de negocio: cuándo se resolvió',
  fecha_creacion          TIMESTAMP                COMMENT 'Auditoría de la fuente: cuándo se insertó la fila en el origen (UTC)',
  fecha_actualizacion     TIMESTAMP NOT NULL       COMMENT 'Columna de orden: última modificación en el origen. Si la versión llegó sin ella (primera carga), toma la primera fecha disponible de la lista de orden. Decide qué versión gana en el MERGE',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo         STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts    TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts           TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_solicitud, fecha_hora_solicitud)
COMMENT 'Solicitudes de préstamo iniciadas desde la app o la web, con la evaluación de riesgo embebida. Una fila por solicitud con su estado VIGENTE (Iniciada, En evaluacion, Aprobada, Rechazada, Desistida). Llega por deltas: el MERGE upsert con guarda de orden deja la versión más nueva. El recorrido de estados vive en solicitudes_estados, escrita desde el mismo micro-lote. CDF activo: Gold construye los hechos de originación de forma incremental.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.solicitudes_prestamo_brz',
  'load_pattern'                       = 'upsert',
  'business_key'                       = 'id_solicitud',
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
