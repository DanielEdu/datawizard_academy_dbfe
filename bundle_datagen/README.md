# bundle_datagen — generadores de datos que corren en Databricks

Job `job_datagen_eventos_app_emit`: emite en continuo la telemetría de la app (topic Kafka simulado)
como `.parquet` a `landing/wizard_bank_kafka/eventos_app/`, de donde la lee `job_bronze_app_ingest`.
Sirve para el lab de la Sesión 17 (streaming en Silver) sin broker ni laptop encendida.
Spec: `specs/datagen_spec.md`.

## Prerrequisitos

- `bronze.lending.clientes_brz` y `bronze.lending.ofertas_preaprobadas_brz` con datos (el emisor usa los
  clientes y ofertas que ya existen; ver `scripts/data_generetor/README.md`).
- `bronze.app.eventos_app_brz` creada (`bundle_ddl`) y el bundle `bronze/app` desplegado.
- External location de Unity Catalog sobre `s3://lakehouse-datawizard`.

## Uso

```bash
databricks bundle validate -t dev --profile free-edition
databricks bundle deploy   -t dev --profile free-edition
```

El job se despliega **pausado**. Para encenderlo, `Resume` en Jobs & Pipelines (o `databricks bundle run
job_datagen_eventos_app_emit -t dev`). Para apagarlo, `Pause`. Cada run emite 50 min y el job lo relanza.

Ritmo y casos se cambian en los `parameters` de `resources/job_datagen_eventos_app_emit.yml`:
`--eps` (eventos/s), `--segundos-por-archivo`, `--minutos` (duración por run), `--factor-casos`
(0 = sin casos de Silver).

Para consumir en vivo, `job_bronze_app_ingest` (hoy manual, `availableNow`) debe correr de forma
periódica o continua; está fuera de este bundle.
