# cobranzas — ingesta S3 → Bronze (reto: sufijo `_richard`)

Bundle con el job `job_ingesta_bronze_cobranzas_richard`: ingesta con Auto Loader los CSV de
`landing/cobranzas/<tabla>/` hacia `<catalog_bronze>.cobranzas.<tabla>_richard`
(tablas creadas por `bundle_ddl`). Misma arquitectura que `bronze/lending`
(spec: `bronze/lending/specs/bronze_ingest.md`).

* `config/tablas_cobranzas.yml`: tablas a ingestar (`cuotas`, `pagos`, `gestiones_cobranza`) y `bucket_root`.
* `src/main.py`: entry point (argparse + orquestación).
* `src/ingestion/autoloader.py`: clase `AutoLoaderIngestor`.
* `resources/job_ingesta_bronze_cobranzas_richard.yml`: definición del job (serverless, manual).

Checkpoints y schemaLocation van en `checkpoint/cobranzas_richard/<tabla>/` y
`schemas/cobranzas_richard/<tabla>/`, para no chocar con otras ingestas del bucket.

Prerrequisitos:
1. External location de Unity Catalog sobre el bucket.
2. Tablas destino creadas: desplegar y correr `job_ddl_lakehouse` de `bundle_ddl`
   (incluye `cuotas_richard`, `pagos_richard` y `gestiones_cobranza_richard`).

```
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy   -t dev --profile <profile>
databricks bundle run job_ingesta_bronze_cobranzas_richard -t dev --profile <profile>
```

Parámetros sobrescribibles al correr: `--tables cuotas,pagos`, `--landing_prefix <otro>`, `--trigger once`, etc.
(ver `parse_args` en `src/main.py`).
