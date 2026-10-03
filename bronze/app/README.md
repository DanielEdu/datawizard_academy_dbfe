# app — telemetría de la app (Kafka) → Bronze

Bundle con el job `job_bronze_app_ingest`: ingesta con Auto Loader los `.parquet` con registros del
topic `wizard.lending.eventos-app` (`landing/wizard_bank_kafka/eventos_app/`) hacia
`<catalog_bronze>.app.eventos_app_brz`. Spec: `specs/bronze_ingest_app.md`.

Prerrequisitos: tabla creada por `bundle_ddl` (task `eventos_app_brz`) y external location de
Unity Catalog sobre el bucket. Los archivos los genera `scripts/data_generetor/productor_eventos.py
--destino parquet` (ver `scripts/data_generetor/README.md`).

```
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy   -t dev --profile <profile>
databricks bundle run job_bronze_app_ingest -t dev --profile <profile>
```
