# Spec: bundle_datagen — generadores de datos sintéticos en Databricks

## Objetivo
Un Declarative Automation Bundle que emite datos sintéticos de Wizard Bank desde Databricks, sin
laptop ni broker. Hoy tiene un job, `job_datagen_eventos_app_emit`, que simula un flujo continuo de la
telemetría de la app (topic Kafka `wizard.lending.eventos-app`) hacia landing, para que el job de
Bronze y los streams de Silver (Sesión 17) tengan datos llegando todo el tiempo. Es la versión
"encendida" de `scripts/data_generetor/productor_eventos.py --destino parquet`.

## Contexto
- Bucket: `s3://lakehouse-datawizard/`
  - Landing: `landing/wizard_bank_kafka/eventos_app/eventos_app_<AAAAMMDD_HHMMSS>.parquet`
  - Manifiesto de casos: `landing/wizard_bank_kafka/_manifiestos/eventos_app/casos_silver_<AAAAMMDD_HHMMSS>.csv`
    (fuera de la carpeta de landing que lee Auto Loader).
- Consumidor: `bronze/app` (`job_bronze_app_ingest`), Auto Loader con el schema Kafka explícito.
- Fuente de clientes y ofertas: `bronze.lending.clientes_brz` y `bronze.lending.ofertas_preaprobadas_brz`
  (columnas STRING; se castean con `try_cast`).
- Workspace: `free-edition` (serverless). Los límites de cuota del plan no están verificados en esta spec.
- Referencia funcional: `scripts/data_generetor/productor_eventos.py` y `scripts/data_generetor/README.md`.

## Inputs
- Parámetros del job (todos por `parameters`, ninguno en `src/`):

| Parámetro | Default | Significado |
|---|---|---|
| `--catalog` | `${var.catalog_bronze}` | Catálogo de donde se leen clientes y ofertas |
| `--landing` | `${var.bucket_root}/landing/wizard_bank_kafka/eventos_app` | Carpeta destino |
| `--manifiestos` | `${var.bucket_root}/landing/wizard_bank_kafka/_manifiestos/eventos_app` | Manifiesto de casos; vacío = no se escribe |
| `--staging` | `${var.bucket_root}/landing/wizard_bank_kafka/_staging/eventos_app` | Carpeta temporal fuera de landing donde Spark arma cada archivo |
| `--eps` | 5 | Eventos normales por segundo (±30 % por archivo) |
| `--segundos-por-archivo` | 30 | Cada cuántos segundos sube un archivo (≥ 1) |
| `--minutos` | 50 | Duración de un run; 0 = hasta cancelar |
| `--factor-casos` | 1.0 | Multiplica la probabilidad de los casos de Silver; 0 = ninguno |
| `--max-ofertas` | 100000 | Tope de ofertas cargadas en memoria |
| `--seed` | — | Semilla reproducible |

- Variables del bundle: `catalog_bronze` (default `bronze`), `bucket_root` (default `s3://lakehouse-datawizard`).

## Outputs
- `databricks.yml` con variables y targets `dev` (`mode: development`, default) y `prod` (`mode: production`),
  ambos con `host` y `profile` explícitos.
- `resources/job_datagen_eventos_app_emit.yml`: job con una task `emit_eventos_app` (`spark_python_task`,
  serverless, `environment_version: "4"`).
- `src/emit_eventos_app.py`: el emisor.
- Cada `--segundos-por-archivo` un `.parquet` con el schema de la fuente Kafka de Spark:

| Columna | Tipo | Contenido |
|---|---|---|
| `key` | BINARY | `id_cliente` en UTF-8 |
| `value` | BINARY | Evento en JSON UTF-8 (contrato de la Sesión 11) |
| `topic` | STRING | `wizard.lending.eventos-app` |
| `partition` | INT | 3 particiones; `crc32(key) % 3` |
| `offset` | BIGINT | Secuencia por partición, sin huecos ni repetidos entre archivos y runs |
| `timestamp` | TIMESTAMP (UTC) | Llegada al broker: evento + 0,2–5 s |
| `timestampType` | INT | 0 |

## Reglas
- **Job continuo y pausado.** `continuous.pause_status: PAUSED` en el repo; encenderlo es manual.
  `max_concurrent_runs: 1`, `timeout_seconds: 4200`, sin `max_retries` (el job continuo ya reintenta).
- **Un run emite `--minutos` y termina**; el job continuo lo relanza. Al arrancar cada run se retoma el
  siguiente offset libre por partición leyendo `max(offset)` de lo que hay en landing. Si la carpeta de
  landing no existe, el run falla (no se asume vacía).
- **Eventos "en vivo":** `timestamp_evento` cae en la ventana desde el archivo anterior hasta ahora.
- **Contrato del evento** idéntico al de `productor_eventos.py` y al que parsea `bronze/app/src/main.py`:
  `id_evento, tipo_evento, timestamp_evento, id_cliente, id_oferta, id_pais, canal, sesion{...}, contexto{...}`;
  tipos y proporciones del funnel de la Sesión 8.2.
- **Casos de Silver**, con probabilidad por evento normal × `--factor-casos`:

| Caso | Prob. base | Qué es |
|---|---|---|
| `duplicado` | 2 % | Mismo mensaje con otro offset (reintento del productor) |
| `reenvio_anterior` | 0,5 % | Payload de un archivo anterior (buffer de los últimos 5.000) |
| `llegada_tardia` | 1 % | `timestamp_evento` 30–90 min atrás |
| `fuera_watermark` | 0,5 % | `timestamp_evento` 3–6 h atrás |
| `payload_corrupto` | 0,5 % | JSON cortado a la mitad |
| `tipo_desconocido` | 0,5 % | `tipo_evento = banner_cerrado` |

- Cada archivo con casos genera su manifiesto CSV (`tabla, caso, clave, esperado_en_silver`).
- **Escritura:** serverless no permite copiar desde el disco local con `dbutils.fs.cp`; cada archivo se escribe
  con Spark (`coalesce(1)`) a `--staging`, se mueve el `part-*.parquet` a landing con el nombre final y se borra
  el staging. El manifiesto se escribe con `dbutils.fs.put`.
- Un archivo por intervalo implica archivos pequeños (~2.900 por día a 30 s); el nombre lleva segundos,
  por eso `--segundos-por-archivo ≥ 1`.
- Sin credenciales ni IDs en el repo; el acceso al bucket es por external location de UC.
- Tags del job: `capa: datagen`, `dominio: eventos_app`, `proyecto: wizard-bank`.
- Naming: bundle `bundle_datagen`, job `job_datagen_eventos_app_emit`, task `emit_eventos_app`.
- Agregar otro generador = un `src/emit_<fuente>.py` + un `resources/job_datagen_<fuente>_<acción>.yml`
  + una sección en esta spec. No se toca lo existente.

## Criterios de aceptación
1. `databricks bundle validate -t dev --profile free-edition` termina sin errores.
2. `databricks bundle deploy -t dev` crea el job `job_datagen_eventos_app_emit` en estado pausado.
3. Al encenderlo, aparece un `.parquet` nuevo en landing cada ~30 s y el log informa registros y casos.
4. Por partición, los offsets del conjunto de archivos son contiguos desde 0, también tras un relanzamiento.
5. Tras correr `job_bronze_app_ingest`, `(_kafka_particion, _kafka_offset)` es único y los `payload_corrupto`
   del manifiesto tienen `id_evento` NULL con `_payload_crudo` no nulo.
6. Con `--factor-casos 0` no se generan casos ni manifiesto.
7. Pausar el job detiene la emisión; encenderlo de nuevo continúa la secuencia de offsets.

## Fuera de alcance
La ingesta a Bronze en continuo (`bronze/app`), Silver, Kafka/Event Hubs reales y los generadores de
Lending y Cobranzas (siguen en `scripts/data_generetor/`).
