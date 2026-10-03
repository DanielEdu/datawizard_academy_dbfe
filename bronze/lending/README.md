# lending — ingesta S3 → Bronze

Bundle con el job `job_bronze_lending_ingest`: ingesta con Auto Loader los CSV de
`landing/wizard_bank_rdb/<tabla>/` hacia `<catalog_bronze>.lending.<tabla>_brz`
(tablas creadas por `bundle_ddl`). Spec: `specs/bronze_ingest.md`.

* `config/tablas_lending.yml`: tablas a ingestar (`enabled`, `opciones`) y `bucket_root`.
* `src/main.py`: entry point (argparse + orquestación).
* `src/ingestion/autoloader.py`: clase `AutoLoaderIngestor`.
* `resources/job_bronze_lending_ingest.yml`: definición del job (serverless, manual).

Prerrequisito: external location de Unity Catalog sobre el bucket.

```
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy   -t dev --profile <profile>
databricks bundle run job_bronze_lending_ingest -t dev --profile <profile>
```

Parámetros sobrescribibles al correr: `--tables paises,clientes`, `--trigger once`, etc.
(ver `parse_args` en `src/main.py`).
