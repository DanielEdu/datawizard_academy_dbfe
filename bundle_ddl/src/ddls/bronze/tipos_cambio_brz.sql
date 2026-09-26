-- Bronze · bronze.lending.tipos_cambio_brz
-- Origen: Azure SQL · lending.tipos_cambio  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.tipos_cambio_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_tipo_cambio STRING COMMENT 'Identificador del registro (PK en la fuente). Tipo en fuente: INT',
  fecha STRING COMMENT 'Fecha de negocio a la que aplica la tasa. Tipo en fuente: DATE',
  moneda_origen STRING COMMENT 'Moneda local de origen: PEN, COP o BOB. Tipo en fuente: CHAR(3)',
  moneda_destino STRING COMMENT 'Moneda destino; por defecto USD. Tipo en fuente: CHAR(3)',
  tasa_compra STRING COMMENT 'Tasa de compra de la moneda origen contra la destino. Tipo en fuente: DECIMAL(12,6)',
  tasa_venta STRING COMMENT 'Tasa de venta de la moneda origen contra la destino. Tipo en fuente: DECIMAL(12,6)',
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
COMMENT 'Tipos de cambio diarios de cada moneda local (PEN, COP, BOB) hacia USD, usados en Gold para convertir montos a una moneda común. No proviene de un sistema del banco: es una serie sintética que emula el feed diario de un proveedor externo, y se cruza por (fecha, moneda) sin FK. Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. A diferencia de las demás tablas, se entrega como archivos CSV diarios en landing (preparar_landing_tipos_cambio.py), ideal para demostrar Auto Loader.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.tipos_cambio',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
