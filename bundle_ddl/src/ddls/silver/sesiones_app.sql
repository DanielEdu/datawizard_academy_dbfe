-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.app.sesiones_app
-- Familia : Entidad derivada de eventos
-- Patrón  : append_session_window
-- Origen  : app.eventos_app (Silver)
-- Clave   : id_sesion, inicio_sesion
-- Orden   : fin_sesion
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.app.sesiones_app') (
  -- ── Columnas de negocio (tipadas) ──
  id_sesion           STRING NOT NULL          COMMENT 'Clave (1/2): sesión de la app',
  inicio_sesion       TIMESTAMP NOT NULL       COMMENT 'Clave (2/2): inicio de la ventana de sesión (primer evento)',
  fin_sesion          TIMESTAMP NOT NULL       COMMENT 'Fin de la ventana: último evento + 30 minutos de inactividad',
  id_cliente          BIGINT                   COMMENT 'Cliente de la sesión',
  canal               STRING                   COMMENT 'Canal de la sesión',
  n_eventos           BIGINT                   COMMENT 'Eventos en la sesión',
  n_vistas            BIGINT                   COMMENT 'Ofertas vistas en la sesión',
  hizo_click          BOOLEAN                  COMMENT 'Hubo al menos un oferta_click',
  simulo              BOOLEAN                  COMMENT 'Hubo al menos una simulacion_realizada',
  inicio_solicitud    BOOLEAN                  COMMENT 'Hubo al menos una solicitud_iniciada',
  monto_simulado_max  DECIMAL(14,2)            COMMENT 'Mayor monto simulado en la sesión',

  -- ── Columnas técnicas (trazabilidad) ──
  _procesado_ts       TIMESTAMP NOT NULL       COMMENT 'Momento en que el stream escribió la sesión'
)
USING DELTA
CLUSTER BY (id_cliente, inicio_sesion)
COMMENT 'Sesiones de uso de la app: una fila por sesión, construida en streaming desde silver.app.eventos_app con session_window de 30 minutos de inactividad y watermark de 2 horas. Modo append: cada sesión se escribe una sola vez, cuando el watermark confirma que cerró. Es una entidad con grano propio (no una métrica), por eso vive en Silver. CDF activo: Gold calcula conversión por sesión.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'kafka_eventos_app',
  'source_table'                       = 'app.eventos_app (Silver)',
  'load_pattern'                       = 'append_session_window',
  'business_key'                       = 'id_sesion, inicio_sesion',
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
