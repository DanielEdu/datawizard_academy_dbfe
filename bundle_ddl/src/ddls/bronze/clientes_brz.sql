-- Bronze · bronze.lending.clientes_brz
-- Origen: Azure SQL · lending.clientes  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.clientes_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_cliente STRING COMMENT 'Identificador del cliente (PK en la fuente). Tipo en fuente: BIGINT',
  id_pais STRING COMMENT 'País de residencia (FK a paises). Tipo en fuente: SMALLINT',
  tipo_documento STRING COMMENT 'Tipo de documento de identidad: DNI, CC o CI. Tipo en fuente: NVARCHAR(10)',
  numero_documento STRING COMMENT 'PII: número de documento de identidad. Tipo en fuente: NVARCHAR(20)',
  nombres STRING COMMENT 'PII: nombres del cliente. Tipo en fuente: NVARCHAR(80)',
  apellidos STRING COMMENT 'PII: apellidos del cliente. Tipo en fuente: NVARCHAR(80)',
  fecha_nacimiento STRING COMMENT 'PII: fecha de nacimiento. Tipo en fuente: DATE',
  email STRING COMMENT 'PII: correo electrónico; puede ser nulo. Tipo en fuente: NVARCHAR(120)',
  ciudad STRING COMMENT 'Ciudad de residencia. Tipo en fuente: NVARCHAR(80)',
  situacion_laboral STRING COMMENT 'Situación laboral declarada; cambia con el tiempo. Tipo en fuente: NVARCHAR(30)',
  ingreso_mensual_declarado STRING COMMENT 'Ingreso mensual declarado en moneda local; cambia con el tiempo. Tipo en fuente: DECIMAL(14,2)',
  score_interno STRING COMMENT 'Score crediticio interno; cambia con el tiempo. Tipo en fuente: SMALLINT',
  nivel_riesgo STRING COMMENT 'Nivel de riesgo de A (mejor) a E (peor). Tipo en fuente: CHAR(1)',
  id_campania_captacion STRING COMMENT 'Campaña que captó al cliente (FK a campanias); puede ser nula. Tipo en fuente: INT',
  fecha_registro STRING COMMENT 'Fecha de negocio: cuándo se registró el cliente. Tipo en fuente: DATETIME2',
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
COMMENT 'Maestro de clientes de Wizard Bank. Contiene datos personales (PII) y atributos que cambian con el tiempo, como situación laboral, ingreso declarado y score interno, por lo que se modela como SCD Tipo 2 en Silver. Nace cuando una persona se registra en la app o la web. Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. Se ingesta de forma incremental por fecha_actualizacion.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.clientes',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
