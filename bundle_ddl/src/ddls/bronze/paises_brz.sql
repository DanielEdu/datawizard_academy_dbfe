-- Bronze · bronze.lending.paises_brz
-- Origen: Azure SQL · lending.paises  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.paises_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_pais STRING COMMENT 'Identificador del país (PK en la fuente). Tipo en fuente: SMALLINT',
  codigo_iso STRING COMMENT 'Código ISO 3166-1 alfa-2 del país (PE, CO, BO). Tipo en fuente: CHAR(2)',
  nombre_pais STRING COMMENT 'Nombre del país. Tipo en fuente: NVARCHAR(60)',
  moneda_codigo STRING COMMENT 'Código ISO 4217 de la moneda local (PEN, COP, BOB). Tipo en fuente: CHAR(3)',
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
COMMENT 'Catálogo de los países donde opera Wizard Bank (Perú, Colombia y Bolivia) con su moneda local. Es una tabla maestra pequeña y casi estática de la base transaccional de Lending. Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. Se ingesta como snapshot completo.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.paises',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
