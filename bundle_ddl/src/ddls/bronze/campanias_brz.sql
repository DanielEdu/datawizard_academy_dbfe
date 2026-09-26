-- Bronze · bronze.lending_gzl.campanias_brz
-- Origen: Azure SQL · lending.campanias  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema_lending || '.campanias_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_campania STRING COMMENT 'Identificador de la campaña (PK en la fuente). Tipo en fuente: INT',
  id_pais STRING COMMENT 'País de la campaña (FK a paises). Tipo en fuente: SMALLINT',
  nombre_campania STRING COMMENT 'Nombre comercial de la campaña. Tipo en fuente: NVARCHAR(120)',
  tipo_campania STRING COMMENT 'Tipo: Captacion, Reactivacion o Cross-sell. Tipo en fuente: NVARCHAR(30)',
  fecha_inicio STRING COMMENT 'Fecha de negocio: inicio de la campaña. Tipo en fuente: DATE',
  fecha_fin STRING COMMENT 'Fecha de negocio: fin de la campaña; puede ser nula si sigue abierta. Tipo en fuente: DATE',
  presupuesto STRING COMMENT 'Presupuesto asignado en moneda local. Tipo en fuente: DECIMAL(14,2)',
  fecha_creacion STRING COMMENT 'Auditoría: cuándo se insertó la fila en la fuente (SYSUTCDATETIME, UTC). Tipo en fuente: TIMESTAMP',
  fecha_actualizacion STRING COMMENT 'Auditoría: última modificación de la fila en la fuente; la mantiene un trigger y es la cursor column de la ingesta incremental. Tipo en fuente: TIMESTAMP',

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
COMMENT 'Campañas de marketing con las que Wizard Bank capta o reactiva clientes y les hace cross-sell. Cada cliente puede venir asociado a la campaña que lo captó. Tabla maestra de baja volatilidad de la base de Lending. Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. Se ingesta como snapshot.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.campanias',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
