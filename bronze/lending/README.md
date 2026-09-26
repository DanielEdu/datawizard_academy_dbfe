# lending_gzl — ingesta S3 → Bronze

Bundle con el job `job_ingesta_bronze_s3`: ingesta con Auto Loader los CSV de
`landing/wizard_bank_rdb/<tabla>/` hacia `<catalog_bronze>.lending_gzl.<tabla>_brz`
(tablas creadas por `bundle_ddl_gzl`). Spec: `specs/bronze_ingest.md`.

* `config/tablas_lending.yml`: tablas a ingestar (`enabled`, `opciones`) y `bucket_root`.
* `src/main.py`: entry point (argparse + orquestación).
* `src/ingestion/autoloader.py`: clase `AutoLoaderIngestor`.
* `resources/job_ingesta_bronze_s3.yml`: definición del job (serverless, manual).

Prerrequisito: external location de Unity Catalog sobre el bucket.

```
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy   -t dev --profile <profile>
databricks bundle run job_ingesta_bronze_s3 -t dev --profile <profile>
```

Parámetros sobrescribibles al correr: `--tables paises,clientes`, `--trigger once`, etc.
(ver `parse_args` en `src/main.py`).
