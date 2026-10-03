# cobranzas — ingesta S3 → Bronze

Bundle con el job `job_bronze_cobranzas_ingest`: ingesta con Auto Loader los CSV del sistema
legado de cobranzas (`landing/wizard_bank_onp/<tabla>/`) hacia `<catalog_bronze>.cobranzas.<tabla>_brz`
(tablas creadas por `bundle_ddl`). Spec: `specs/bronze_ingest_cobranzas.md`.

* `config/tablas_cobranzas.yml`: tablas a ingestar (`enabled`, `opciones`) y `bucket_root`.
* `src/main.py`: entry point (argparse + orquestación); mismo código que `bronze/lending`.
* `src/ingestion/autoloader.py`: clase `AutoLoaderIngestor`.
* `resources/job_bronze_cobranzas_ingest.yml`: definición del job (serverless, manual).

Prerrequisitos: tablas creadas por `bundle_ddl` (tasks `cuotas_brz`, `pagos_brz`,
`gestiones_cobranza_brz`) y external location de Unity Catalog sobre el bucket.

```
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy   -t dev --profile <profile>
databricks bundle run job_bronze_cobranzas_ingest -t dev --profile <profile>
```

Parámetros sobrescribibles al correr: `--tables cuotas,pagos`, `--trigger once`, etc.
(ver `parse_args` en `src/main.py`).
