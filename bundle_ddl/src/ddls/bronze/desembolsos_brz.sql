-- Bronze · bronze.lending_gzl.desembolsos_brz
-- Origen: Azure SQL · lending.desembolsos  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema_lending || '.desembolsos_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_desembolso STRING COMMENT 'Identificador del desembolso (PK en la fuente); equivale al numero_credito de cobranzas. Tipo en fuente: BIGINT',
  id_solicitud STRING COMMENT 'Solicitud desembolsada (FK única a solicitudes_prestamo). Tipo en fuente: BIGINT',
  id_cliente STRING COMMENT 'Cliente que recibe el desembolso (FK a clientes). Tipo en fuente: BIGINT',
  monto_desembolsado STRING COMMENT 'Monto desembolsado. Tipo en fuente: DECIMAL(14,2)',
  moneda STRING COMMENT 'Moneda del desembolso: PEN, COP o BOB. Tipo en fuente: CHAR(3)',
  tasa_aplicada STRING COMMENT 'Tasa de interés aplicada (porcentaje). Tipo en fuente: DECIMAL(6,3)',
  plazo_meses STRING COMMENT 'Plazo del crédito en meses. Tipo en fuente: SMALLINT',
  comision_cobrada STRING COMMENT 'Comisión cobrada en el desembolso. Tipo en fuente: DECIMAL(12,2)',
  cuenta_destino_masked STRING COMMENT 'PII parcial: cuenta destino enmascarada (****1234). Tipo en fuente: NVARCHAR(30)',
  fecha_hora_desembolso STRING COMMENT 'Fecha de negocio: cuándo se desembolsó. Tipo en fuente: DATETIME2',
  estado_desembolso STRING COMMENT 'Estado: Completado, Fallido o Reversado; muta. Tipo en fuente: NVARCHAR(20)',
  referencia_bancaria STRING COMMENT 'Referencia de la transferencia bancaria. Tipo en fuente: NVARCHAR(50)',
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
COMMENT 'Desembolsos de los préstamos aprobados; cierran el funnel de originación (uno por solicitud). El estado puede mutar (Completado, Fallido, Reversado) y contiene la cuenta destino enmascarada. También se captura como eventos CDC. Es el origen del crédito que luego se cobra en el sistema de cobranzas (numero_credito). Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. Se ingesta de forma incremental por fecha_actualizacion.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.desembolsos',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
