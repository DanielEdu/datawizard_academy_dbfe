-- Bronze · bronze.app.eventos_app_brz
-- Origen: topic Kafka wizard.lending.eventos-app (telemetría de la app). En el curso llega
--         como .parquet con el schema de la fuente Kafka de Spark (key, value, topic, partition,
--         offset, timestamp, timestampType) a landing/wizard_bank_kafka/eventos_app/  →  Auto Loader.
-- Bronze = el registro de Kafka con el value JSON parseado y aplanado (contrato de la Sesión 11),
--         más el payload crudo para no perder lo que no parsea. Append-only, sin dedup.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.app.eventos_app_brz') (
  -- ── Columnas de negocio (value JSON parseado con el esquema del evento) ──
  id_evento          STRING    COMMENT 'UUID generado por la app; NULL si el payload no parseó',
  tipo_evento        STRING    COMMENT 'oferta_vista, oferta_click, simulacion_realizada, solicitud_iniciada (u otro nuevo)',
  timestamp_evento   TIMESTAMP COMMENT 'Tiempo del EVENTO (reloj del dispositivo, UTC)',
  id_cliente         BIGINT    COMMENT 'Cliente que generó el evento',
  id_oferta          BIGINT    COMMENT 'Oferta sobre la que ocurrió el evento',
  id_pais            INT       COMMENT 'País del cliente',
  canal              STRING    COMMENT 'APP_IOS, APP_ANDROID o WEB',
  id_sesion          STRING    COMMENT 'sesion.id_sesion',
  version_app        STRING    COMMENT 'sesion.version_app',
  sistema_operativo  STRING    COMMENT 'sesion.sistema_operativo',
  modelo_dispositivo STRING    COMMENT 'sesion.modelo_dispositivo',
  ubicacion_pantalla STRING    COMMENT 'contexto.ubicacion_pantalla',
  posicion           INT       COMMENT 'contexto.posicion',
  tiempo_visible_seg DOUBLE    COMMENT 'contexto.tiempo_visible_seg',
  monto_simulado     DOUBLE    COMMENT 'contexto.monto_simulado (solo simulacion_realizada)',
  plazo_simulado     INT       COMMENT 'contexto.plazo_simulado (solo simulacion_realizada)',

  -- ── Metadata de Kafka ──
  _kafka_key         STRING    COMMENT 'key del mensaje (id_cliente): define la partición',
  _kafka_topic       STRING    COMMENT 'Topic de origen',
  _kafka_particion   INT       COMMENT 'Partición del topic',
  _kafka_offset      BIGINT    COMMENT 'Offset dentro de la partición: (topic, partición, offset) identifica el mensaje',
  _kafka_timestamp   TIMESTAMP COMMENT 'Momento en que el broker recibió el mensaje (CreateTime)',
  _payload_crudo     STRING    COMMENT 'value tal cual (JSON en texto): conserva el mensaje aunque no parsee',

  -- ── Metadata de ingesta ──
  _metadata STRUCT<
    file_path:              STRING    COMMENT 'Ruta completa del archivo de origen',
    file_name:              STRING    COMMENT 'Nombre del archivo de origen',
    file_size:              BIGINT    COMMENT 'Tamaño del archivo en bytes',
    file_block_start:       BIGINT    COMMENT 'Byte de inicio del bloque leído',
    file_block_length:      BIGINT    COMMENT 'Longitud en bytes del bloque leído',
    file_modification_time: TIMESTAMP COMMENT 'Última modificación del archivo en el storage'
  > COMMENT 'Columna de metadata de archivo de Auto Loader (_metadata)',
  _ingestion_ts      TIMESTAMP COMMENT 'Momento de ingesta en Bronze: current_timestamp()'
)
USING DELTA
CLUSTER BY (_ingestion_ts)
COMMENT 'Telemetría de la app de Wizard Bank: un registro por mensaje de Kafka del topic wizard.lending.eventos-app, con el value JSON aplanado y la metadata de Kafka (partición, offset, timestamp). Puede traer duplicados (at-least-once), eventos tardíos y payloads corruptos (id_evento NULL, revisar _payload_crudo): los resuelve silver.app.eventos_app. Los genera productor_eventos.py.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'kafka_eventos_app',
  'source_table'                      = 'wizard.lending.eventos-app',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
