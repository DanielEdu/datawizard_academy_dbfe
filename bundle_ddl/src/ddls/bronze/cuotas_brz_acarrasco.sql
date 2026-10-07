-- Bronze · bronze._acarrasco.cuotas_brz
-- Origen: sistema legado on-premise (simulado en Azure SQL) · cobranzas.cuotas  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '._acarrasco.cuotas_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_cuota STRING COMMENT 'Identificador de la cuota (IDENTITY en la fuente). Tipo en fuente: BIGINT',
  numero_credito STRING COMMENT 'Crédito al que pertenece; equivale a lending.desembolsos.id_desembolso. Tipo en fuente: BIGINT',
  numero_cuota STRING COMMENT 'Número de cuota dentro del cronograma. Tipo en fuente: SMALLINT',
  fecha_vencimiento STRING COMMENT 'Fecha de vencimiento de la cuota. Tipo en fuente: DATE',
  monto_cuota STRING COMMENT 'Monto total de la cuota. Tipo en fuente: DECIMAL(12,2)',
  monto_capital STRING COMMENT 'Porción de capital de la cuota. Tipo en fuente: DECIMAL(12,2)',
  monto_interes STRING COMMENT 'Porción de interés de la cuota. Tipo en fuente: DECIMAL(12,2)',
  estado_cuota STRING COMMENT 'Estado: Pendiente, Pagada o Vencida; muta. Tipo en fuente: NVARCHAR(20)',
  fecha_creacion STRING COMMENT 'Auditoría: cuándo se insertó la fila en el sistema de cobranzas (UTC). Tipo en fuente: TIMESTAMP',
  fecha_modificacion STRING COMMENT 'Auditoría: última modificación de la fila; es el watermark que consume el pipeline incremental. Tipo en fuente: TIMESTAMP',

  -- ── Metadata de ingesta ──
  _metadata STRUCT<
    file_path:              STRING    COMMENT 'Ruta completa del archivo de origen',
    file_name:              STRING    COMMENT 'Nombre del archivo de origen',
    file_size:              BIGINT    COMMENT 'Tamaño del archivo en bytes',
    file_block_start:       BIGINT    COMMENT 'Byte de inicio del bloque leído',
    file_block_length:      BIGINT    COMMENT 'Longitud en bytes del bloque leído',
    file_modification_time: TIMESTAMP COMMENT 'Última modificación del archivo en el storage'
  > COMMENT 'Columna de metadata de archivo de Auto Loader (_metadata); se escribe directo con select("*", "_metadata")',
  _rescued_data STRING    COMMENT 'JSON con datos que no calzaron con el schema (rescuedDataColumn de Auto Loader)',
  _ingestion_ts TIMESTAMP COMMENT 'Momento de ingesta en Bronze: current_timestamp()'
)
USING DELTA
CLUSTER BY (_ingestion_ts)
COMMENT 'Cronograma de cuotas de cada crédito desembolsado, con capital, interés y estado (Pendiente, Pagada, Vencida). Pertenece al sistema legado de cobranzas, independiente de Lending, y se enlaza con Lending por numero_credito, que equivale a desembolsos.id_desembolso. Los genera generar_datos_cobranzas.py (datos sintéticos) y se cargan en Azure SQL, schema cobranzas, que simula el sistema legado de cobranzas. Se ingesta en batch incremental (ADF) usando fecha_modificacion como watermark.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'legacy_cobranzas_onprem',
  'source_table'                      = 'cobranzas.cuotas',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
