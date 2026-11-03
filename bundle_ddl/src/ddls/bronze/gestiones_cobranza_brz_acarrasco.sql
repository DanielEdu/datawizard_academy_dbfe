-- Bronze · bronze._acarrasco.gestiones_cobranza_brz
-- Origen: sistema legado on-premise (simulado en Azure SQL) · cobranzas.gestiones_cobranza  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '._acarrasco.gestiones_cobranza_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_gestion STRING COMMENT 'Identificador de la gestión (IDENTITY en la fuente). Tipo en fuente: BIGINT',
  numero_credito STRING COMMENT 'Crédito gestionado; equivale a lending.desembolsos.id_desembolso. Tipo en fuente: BIGINT',
  fecha_gestion STRING COMMENT 'Fecha de negocio: cuándo se realizó la gestión. Tipo en fuente: DATETIME2',
  tipo_gestion STRING COMMENT 'Tipo: Llamada, SMS, Email, Visita o Carta Notarial. Tipo en fuente: NVARCHAR(20)',
  resultado STRING COMMENT 'Resultado de la gestión. Tipo en fuente: NVARCHAR(30)',
  dias_mora_al_momento STRING COMMENT 'Días de mora del crédito al momento de la gestión. Tipo en fuente: SMALLINT',
  gestor STRING COMMENT 'Gestor de cobranza que realizó la acción. Tipo en fuente: NVARCHAR(60)',
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
COMMENT 'Acciones del equipo de recuperación sobre créditos en mora: llamadas, SMS, emails, visitas y cartas notariales, con su resultado y los días de mora al momento de la gestión. Pertenece al sistema legado de cobranzas. Los genera generar_datos_cobranzas.py (datos sintéticos) y se cargan en Azure SQL, schema cobranzas, que simula el sistema legado de cobranzas. Se ingesta en batch incremental (ADF) usando fecha_modificacion como watermark.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'legacy_cobranzas_onprem',
  'source_table'                      = 'cobranzas.gestiones_cobranza',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
