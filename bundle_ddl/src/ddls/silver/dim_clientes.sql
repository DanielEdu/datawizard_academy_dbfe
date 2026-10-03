-- ════════════════════════════════════════════════════════════════════════
-- Silver · silver.lending.dim_clientes
-- Familia : Maestra historizada
-- Patrón  : scd2
-- Origen  : bronze.lending.clientes_brz
-- Clave   : id_cliente
-- Orden   : fecha_actualizacion, fecha_creacion, fecha_registro
-- Silver = tipos reales, clave única probada, estado resuelto. No es append-only:
-- el MERGE actualiza filas. Lo inválido va a _cuarentena, nunca se descarta.
-- NOT NULL solo en clave, orden y columnas técnicas obligatorias: el resto de
-- las reglas se valida en el job (cuarentena) para que una fila sucia no tumbe el lote.
-- El catálogo llega como parámetro (:catalog) desde el job.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.dim_clientes') (
  -- ── Clave técnica: única por versión ──
  sk_cliente                 BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key: única por VERSIÓN del cliente. Los hechos de Gold la guardan para congelar la foto del cliente',

  -- ── Columnas de negocio (tipadas) ──
  id_cliente                 BIGINT NOT NULL          COMMENT 'Clave de negocio: se repite en cada versión',
  ingreso_mensual_declarado  DECIMAL(14,2)            COMMENT 'Tipo 2: ingreso mensual declarado en moneda local',
  situacion_laboral          STRING                   COMMENT 'Tipo 2: situación laboral declarada',
  score_interno              SMALLINT                 COMMENT 'Tipo 2: score crediticio interno',
  nivel_riesgo               STRING                   COMMENT 'Tipo 2: nivel de riesgo A (mejor) a E (peor)',
  email                      STRING                   COMMENT 'Tipo 1 · PII: correo electrónico',
  ciudad                     STRING                   COMMENT 'Tipo 1: ciudad de residencia',
  nombres                    STRING                   COMMENT 'Tipo 1 · PII: nombres (una corrección no es un cambio de negocio)',
  apellidos                  STRING                   COMMENT 'Tipo 1 · PII: apellidos',
  id_pais                    SMALLINT                 COMMENT 'Tipo 0: país de residencia',
  tipo_documento             STRING                   COMMENT 'Tipo 0: DNI, CC o CI',
  numero_documento           STRING                   COMMENT 'Tipo 0 · PII: número de documento',
  fecha_nacimiento           DATE                     COMMENT 'Tipo 0 · PII: fecha de nacimiento',
  id_campania_captacion      INT                      COMMENT 'Tipo 0: campaña que captó al cliente',
  fecha_registro             TIMESTAMP                COMMENT 'Tipo 0: fecha de negocio del registro',

  -- ── Control de vigencia (SCD Tipo 2) ──
  fecha_inicio_vigencia      TIMESTAMP NOT NULL       COMMENT 'Desde cuándo rige esta versión (fecha_actualizacion del cambio en el origen)',
  fecha_fin_vigencia         TIMESTAMP                COMMENT 'Hasta cuándo rigió; NULL = versión vigente. Igual al inicio de la versión siguiente: sin huecos ni solapes',
  es_vigente                 BOOLEAN NOT NULL         COMMENT 'TRUE solo en la versión actual. Invariante: exactamente una vigente por clave de negocio',
  hash_atributos             STRING NOT NULL          COMMENT 'SHA-256 de las columnas Tipo 2 (nulos como <null>). Si cambia, se abre una versión nueva',

  -- ── Columnas técnicas (trazabilidad) ──
  _origen_archivo            STRING                   COMMENT 'Archivo de landing que trajo la versión vigente (_metadata.file_path de Bronze)',
  _bronze_ingestion_ts       TIMESTAMP                COMMENT 'Momento en que esa versión entró a Bronze (_ingestion_ts)',
  _procesado_ts              TIMESTAMP NOT NULL       COMMENT 'Momento en que el job de Silver escribió o actualizó la fila'
)
USING DELTA
CLUSTER BY (id_cliente, fecha_inicio_vigencia)
COMMENT 'Maestro de clientes historizado con SCD Tipo 2: una fila por VERSIÓN del cliente, con vigencia. El tipo SCD se decide por columna: ingreso, situación laboral, score y nivel de riesgo son Tipo 2 (un cambio abre versión nueva); email, ciudad, nombres y apellidos son Tipo 1 (se pisan en la versión vigente); documento, fecha de nacimiento, país, campaña de captación y fecha de registro son Tipo 0. Se construye desde bronze.lending.clientes_brz con MERGE de doble source dentro de foreachBatch. Permite el join temporal: ¿con qué ingreso y score se aprobó cada solicitud? Contiene PII (enmascarar en Unity Catalog, S21). CDF activo.'
TBLPROPERTIES (
  -- Etiquetas propias (Delta no las interpreta): capa, linaje y patrón de carga.
  'quality'                            = 'silver',
  'source_system'                      = 'azure_sql_wizardbank',
  'source_table'                       = 'bronze.lending.clientes_brz',
  'load_pattern'                       = 'scd2',
  'business_key'                       = 'id_cliente',
  -- Change Data Feed: Gold la lee de forma incremental con readChangeFeed.
  'delta.enableChangeDataFeed'         = 'true',
  -- Column mapping por nombre: renombrar o eliminar columnas sin reescribir archivos.
  'delta.columnMapping.mode'           = 'name',
  -- Menos archivos y más grandes al escribir; compacta los chicos después de cada MERGE.
  'delta.autoOptimize.optimizeWrite'   = 'true',
  'delta.autoOptimize.autoCompact'     = 'true',
  -- Time travel de 30 días para auditar y recuperar; archivos borrados se purgan a los 7.
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
