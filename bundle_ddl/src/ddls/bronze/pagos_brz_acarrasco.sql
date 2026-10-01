-- Bronze · bronze._acarrasco.pagos_brz
-- Origen: sistema legado on-premise (simulado en Azure SQL) · cobranzas.pagos  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '._acarrasco.pagos_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_pago STRING COMMENT 'Identificador del pago (IDENTITY en la fuente). Tipo en fuente: BIGINT',
  numero_credito STRING COMMENT 'Crédito pagado; equivale a lending.desembolsos.id_desembolso. Tipo en fuente: BIGINT',
  numero_cuota STRING COMMENT 'Cuota pagada; se cruza con cuotas por (numero_credito, numero_cuota). Tipo en fuente: SMALLINT',
  fecha_pago STRING COMMENT 'Fecha de negocio: cuándo se registró el pago. Tipo en fuente: DATETIME2',
  monto_pagado STRING COMMENT 'Monto efectivamente pagado. Tipo en fuente: DECIMAL(12,2)',
  medio_pago STRING COMMENT 'Medio de pago utilizado. Tipo en fuente: NVARCHAR(30)',
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
COMMENT 'Pagos efectivamente cobrados a los clientes, uno por evento de pago, con monto y medio de pago. Complementa a cuotas: se cruza por (numero_credito, numero_cuota). Pertenece al sistema legado de cobranzas. Los genera generar_datos_cobranzas.py (datos sintéticos) y se cargan en Azure SQL, schema cobranzas, que simula el sistema legado de cobranzas. Se ingesta en batch incremental (ADF) usando fecha_modificacion como watermark.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'legacy_cobranzas_onprem',
  'source_table'                      = 'cobranzas.pagos',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
