# Spec: bundle app — Ingesta de la telemetría de la app (Kafka) → Bronze

## Objetivo
Un Declarative Automation Bundle con un job, `job_bronze_app_ingest`, que ingesta con Auto Loader
los registros del topic Kafka `wizard.lending.eventos-app` hacia `bronze.app.eventos_app_brz`,
tabla ya creada por `bundle_ddl` (`eventos_app_brz.sql`).

En el curso no hay broker: `productor_eventos.py --destino parquet` escribe los registros como
`.parquet` con el **schema de la fuente Kafka de Spark** y los sube a landing. El job los procesa
igual que si vinieran de `spark.readStream.format("kafka")`: el `value` llega en binario y se parsea.

## Contexto
- Bucket: `s3://lakehouse-datawizard/`
  - Landing: `landing/wizard_bank_kafka/eventos_app/eventos_app_<AAAAMMDD_HHMMSS>.parquet`
  - Checkpoint: `checkpoint/app/eventos_app/`
- Schema de cada archivo (el de la fuente Kafka):

| Columna | Tipo | Contenido |
|---|---|---|
| `key` | BINARY | `id_cliente` en UTF-8 (define la partición) |
| `value` | BINARY | El evento en JSON, UTF-8 |
| `topic` | STRING | `wizard.lending.eventos-app` |
| `partition` | INT | Partición del topic (3 por defecto) |
| `offset` | BIGINT | Secuencia por partición, continúa entre archivos |
| `timestamp` | TIMESTAMP | Llegada al broker (CreateTime) |
| `timestampType` | INT | 0 = CreateTime |

- Contrato del `value`: el `esquema_evento` de la Sesión 11 (id_evento, tipo_evento,
  timestamp_evento ISO-8601, id_cliente, id_oferta, id_pais, canal, `sesion{...}`, `contexto{...}`).

## Estructura del bundle
```
app/
├── databricks.yml                         # variables catalog_bronze y bucket_root
├── resources/job_bronze_app_ingest.yml
├── specs/bronze_ingest_app.md
└── src/main.py                            # leer_landing → aplanar → writeStream
```

## Reglas de ingesta
- Auto Loader `cloudFiles.format = parquet` con el schema Kafka explícito (no se infiere ni evoluciona:
  el registro de Kafka es fijo; lo que cambia vive dentro del `value`).
- `from_json(value::string, esquema_evento)`, aplanado de `sesion` y `contexto`,
  `timestamp_evento` a TIMESTAMP.
- Metadata: `_kafka_key`, `_kafka_topic`, `_kafka_particion`, `_kafka_offset`, `_kafka_timestamp`,
  `_payload_crudo` (el value en texto), `_metadata` (6 campos) y `_ingestion_ts`.
- Un payload que no parsea **no se descarta**: queda con `id_evento` NULL y el mensaje en `_payload_crudo`.
- Sin dedup ni filtros: duplicados, tardíos y tipos nuevos se resuelven en `silver.app.eventos_app` (S17).
- `trigger(availableNow=True)`, append a la tabla gestionada. Job manual, serverless,
  `max_concurrent_runs: 1`, `timeout_seconds: 3600`, `max_retries: 1`.
- Rutas, catálogo y tabla por parámetro; nada escrito a mano en `src/`.

## Criterios de aceptación
1. `databricks bundle validate -t dev` sin errores; `deploy` crea `job_bronze_app_ingest`.
2. `bundle run` termina en SUCCESS y el log informa los registros agregados.
3. `count(*)` = registros de los parquet nuevos; `(_kafka_particion, _kafka_offset)` único.
4. Los `payload_corrupto` del manifiesto tienen `id_evento` NULL y `_payload_crudo` no nulo.
5. Los `duplicado` del manifiesto aparecen dos veces con distinto offset.
6. Idempotencia: una segunda corrida sin archivos nuevos agrega 0 registros.

## Fuera de alcance
Lectura directa desde un broker Kafka/Event Hubs (Sesión 11), Silver y schedules.
