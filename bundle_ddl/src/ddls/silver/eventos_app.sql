-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.app.eventos_app
-- Familia : Evento
-- Patrón  : append_dedup_watermark
-- Origen  : bronze.app.eventos_app_brz
-- Clave   : id_evento
-- Orden   : ts_evento
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.app.eventos_app') (
  -- ── Columnas de negocio (tipadas) ──
  id_evento             STRING NOT NULL          COMMENT 'Clave: UUID generado por la app; base de la deduplicación',
  tipo_evento           STRING NOT NULL          COMMENT 'oferta_vista, oferta_click, simulacion_realizada o solicitud_iniciada',
  ts_evento             TIMESTAMP NOT NULL       COMMENT 'Tiempo del EVENTO (reloj del dispositivo, UTC)',
  fecha_evento          DATE NOT NULL            COMMENT 'Fecha de ts_evento, para filtrar y clusterizar',
  id_cliente            BIGINT NOT NULL          COMMENT 'Cliente que generó el evento',
  id_oferta             BIGINT                   COMMENT 'Oferta sobre la que ocurrió el evento',
  id_pais               SMALLINT                 COMMENT 'País del cliente',
  canal                 STRING                   COMMENT 'APP_IOS, APP_ANDROID o WEB',
  id_sesion             STRING                   COMMENT 'Sesión de la app a la que pertenece el evento',
  version_app           STRING                   COMMENT 'Versión de la app',
  sistema_operativo     STRING                   COMMENT 'Sistema operativo del dispositivo',
  ubicacion_pantalla    STRING                   COMMENT 'Dónde se mostró la oferta: home_banner, home_card, push, detalle_oferta',
  posicion              INT                      COMMENT 'Posición de la oferta en la pantalla',
  tiempo_visible_seg    DECIMAL(6,2)             COMMENT 'Segundos que la oferta estuvo visible',
  monto_simulado        DECIMAL(14,2)            COMMENT 'Solo simulacion_realizada: monto simulado',
  plazo_simulado        SMALLINT                 COMMENT 'Solo simulacion_realizada: plazo simulado en meses',
  retraso_seg           BIGINT                   COMMENT 'Segundos entre ts_evento y la llegada a Bronze',

  -- ── Columnas técnicas (trazabilidad) ──
  _kafka_offset         BIGINT                   COMMENT 'Offset de Kafka del mensaje, para trazabilidad',
  _bronze_ingestion_ts  TIMESTAMP                COMMENT 'Momento en que el evento entró a Bronze',
  _procesado_ts         TIMESTAMP NOT NULL       COMMENT 'Momento en que el stream de Silver lo escribió'
)
USING DELTA
CLUSTER BY (fecha_evento, tipo_evento)
COMMENT 'Telemetría de la app de Wizard Bank: una fila por evento (oferta_vista, oferta_click, simulacion_realizada, solicitud_iniciada). Se construye en streaming desde bronze.app.eventos_app_brz con dropDuplicatesWithinWatermark por id_evento y escritura append. ts_evento es el tiempo del dispositivo; retraso_seg mide cuánto tardó en llegar. Un tipo de evento desconocido se acepta (es una señal, no un error) y id_cliente no se valida contra dim_clientes (el cliente puede llegar después). CDF activo.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'kafka_eventos_app',
  'source_table'                       = 'bronze.app.eventos_app_brz',
  'load_pattern'                       = 'append_dedup_watermark',
  'business_key'                       = 'id_evento',
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
