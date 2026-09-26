-- Bronze · bronze.lending_gzl.ofertas_preaprobadas_brz
-- Origen: Azure SQL · lending.ofertas_preaprobadas  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema_lending || '.ofertas_preaprobadas_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_oferta STRING COMMENT 'Identificador de la oferta (PK en la fuente). Tipo en fuente: BIGINT',
  id_cliente STRING COMMENT 'Cliente destinatario (FK a clientes). Tipo en fuente: BIGINT',
  id_producto STRING COMMENT 'Producto ofertado (FK a productos_prestamo). Tipo en fuente: INT',
  id_campania STRING COMMENT 'Campaña que originó la oferta (FK a campanias); puede ser nula. Tipo en fuente: INT',
  monto_ofertado STRING COMMENT 'Monto preaprobado en moneda local. Tipo en fuente: DECIMAL(14,2)',
  plazo_meses_ofertado STRING COMMENT 'Plazo ofertado en meses. Tipo en fuente: SMALLINT',
  tasa_ofertada STRING COMMENT 'Tasa de interés ofertada (porcentaje). Tipo en fuente: DECIMAL(6,3)',
  motor_asignacion STRING COMMENT 'Modelo que asignó la oferta: modelo_v1 o modelo_v2. Tipo en fuente: NVARCHAR(40)',
  fecha_generacion STRING COMMENT 'Fecha de negocio: cuándo se generó la oferta. Tipo en fuente: DATETIME2',
  fecha_vigencia_fin STRING COMMENT 'Último día de vigencia de la oferta. Tipo en fuente: DATE',
  estado_oferta STRING COMMENT 'Estado: Vigente, Expirada o Aceptada; muta con el tiempo. Tipo en fuente: NVARCHAR(20)',
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
COMMENT 'Ofertas de préstamo preaprobadas que el motor de asignación genera para clientes existentes; son la entrada del funnel de originación. El estado muta (Vigente, Expirada, Aceptada) y el motor (modelo_v1 o modelo_v2) permite hacer A/B testing. Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. Se ingesta de forma incremental por fecha_actualizacion.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.ofertas_preaprobadas',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
