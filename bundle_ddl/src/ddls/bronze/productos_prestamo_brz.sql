-- Bronze · bronze.lending.productos_prestamo_brz
-- Origen: Azure SQL · lending.productos_prestamo  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.productos_prestamo_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_producto STRING COMMENT 'Identificador del producto (PK en la fuente). Tipo en fuente: INT',
  id_pais STRING COMMENT 'País donde se comercializa (FK a paises). Tipo en fuente: SMALLINT',
  nombre_producto STRING COMMENT 'Nombre comercial del producto. Tipo en fuente: NVARCHAR(120)',
  tipo_producto STRING COMMENT 'Categoría del producto de préstamo. Tipo en fuente: NVARCHAR(40)',
  monto_minimo STRING COMMENT 'Monto mínimo financiable. Tipo en fuente: DECIMAL(14,2)',
  monto_maximo STRING COMMENT 'Monto máximo financiable. Tipo en fuente: DECIMAL(14,2)',
  plazo_min_meses STRING COMMENT 'Plazo mínimo en meses. Tipo en fuente: SMALLINT',
  plazo_max_meses STRING COMMENT 'Plazo máximo en meses. Tipo en fuente: SMALLINT',
  tasa_interes_anual STRING COMMENT 'Tasa de interés anual vigente (porcentaje). Tipo en fuente: DECIMAL(6,3)',
  es_activo STRING COMMENT 'Indica si el producto está vigente (1) o descontinuado (0). Tipo en fuente: BIT',
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
COMMENT 'Catálogo de productos de préstamo de Wizard Bank por país, con rangos de monto, plazo y tasa de interés anual. La tasa cambia con el tiempo, por eso en Silver se modela como SCD Tipo 2. Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. Se ingesta como snapshot y cada cambio de tasa llega como una versión nueva de la fila.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.productos_prestamo',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
