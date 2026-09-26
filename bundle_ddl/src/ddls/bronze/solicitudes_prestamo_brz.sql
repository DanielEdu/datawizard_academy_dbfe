-- Bronze · bronze.lending_gzl.solicitudes_prestamo_brz
-- Origen: Azure SQL · lending.solicitudes_prestamo  →  Auto Loader (cloudFiles)
-- Bronze = copia fiel de la fuente: todo en STRING, append-only, sin dedup ni modelado.
-- Los tipos reales se aplican en Silver.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema_lending || '.solicitudes_prestamo_brz') (
  -- ── Columnas de negocio (todas STRING, tal cual llegan del archivo) ──
  id_solicitud STRING COMMENT 'Identificador de la solicitud (PK en la fuente). Tipo en fuente: BIGINT',
  id_oferta STRING COMMENT 'Oferta de origen (FK a ofertas_preaprobadas); nula si no partió de una oferta. Tipo en fuente: BIGINT',
  id_cliente STRING COMMENT 'Cliente solicitante (FK a clientes). Tipo en fuente: BIGINT',
  id_producto STRING COMMENT 'Producto solicitado (FK a productos_prestamo). Tipo en fuente: INT',
  canal STRING COMMENT 'Canal de originación: APP_IOS, APP_ANDROID o WEB. Tipo en fuente: NVARCHAR(20)',
  monto_solicitado STRING COMMENT 'Monto solicitado en moneda local. Tipo en fuente: DECIMAL(14,2)',
  plazo_meses_solicitado STRING COMMENT 'Plazo solicitado en meses. Tipo en fuente: SMALLINT',
  fecha_hora_solicitud STRING COMMENT 'Fecha de negocio: cuándo se envió la solicitud. Tipo en fuente: DATETIME2',
  estado_solicitud STRING COMMENT 'Estado: Iniciada, En evaluacion, Aprobada, Rechazada o Desistida; muta. Tipo en fuente: NVARCHAR(25)',
  score_evaluacion STRING COMMENT 'Score calculado por el motor de riesgo. Tipo en fuente: SMALLINT',
  decision_motor STRING COMMENT 'Decisión del motor: Aprobar, Rechazar o Revision manual. Tipo en fuente: NVARCHAR(20)',
  monto_aprobado STRING COMMENT 'Monto aprobado; nulo si no fue aprobada. Tipo en fuente: DECIMAL(14,2)',
  tasa_aprobada STRING COMMENT 'Tasa aprobada (porcentaje); nula si no fue aprobada. Tipo en fuente: DECIMAL(6,3)',
  motivo_rechazo STRING COMMENT 'Motivo del rechazo; nulo si no fue rechazada. Tipo en fuente: NVARCHAR(120)',
  fecha_hora_resolucion STRING COMMENT 'Fecha de negocio: cuándo se resolvió la solicitud. Tipo en fuente: DATETIME2',
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
COMMENT 'Solicitudes de préstamo que los clientes inician desde la app (iOS/Android) o la web, normalmente a partir de una oferta preaprobada. Incluye la evaluación de riesgo embebida (score, decisión del motor, monto y tasa aprobados). Cambia de estado (Iniciada, En evaluacion, Aprobada, Rechazada, Desistida), por eso además de la carga batch también se captura como eventos CDC. Los genera generar_datos_wizard_bank.py (datos sintéticos) y se cargan en Azure SQL, base wizardbank. Se ingesta de forma incremental por fecha_actualizacion.'
TBLPROPERTIES (
  'quality'                           = 'bronze',
  'source_system'                     = 'azure_sql_wizardbank',
  'source_table'                      = 'lending.solicitudes_prestamo',
  'delta.appendOnly'                  = 'true',
  'delta.enableChangeDataFeed'        = 'false',
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.columnMapping.mode'          = 'name',
  'delta.logRetentionDuration'        = 'interval 30 days',
  'delta.deletedFileRetentionDuration'= 'interval 7 days'
);
