# cobranzas_acarrasco — ingesta S3 → Bronze (reto: schema `cobranzas_acarrasco`)

Bundle `cobranzas_acarrasco` con el job `job_ingesta_bronze_cobranzas_acarrasco`: ingesta con Auto Loader los CSV de
`landing/cobranzas/<tabla>/` hacia `<catalog_bronze>.cobranzas_acarrasco.<tabla>_brz`
(tablas creadas por `bundle_ddl`). Misma arquitectura que `bronze/lending`
(spec: `bronze/lending/specs/bronze_ingest.md` y `specs/cobranzas_ingest.md`).

* `config/tablas_cobranzas.yml`: tablas a ingestar y `bucket_root` — **no commiteado, editar antes de desplegar**.
* `src/main.py`: entry point (argparse + orquestación). Difiere de `lending` en 5 defaults.
* `src/ingestion/autoloader.py`: clase `AutoLoaderIngestor` (copia de `lending`).
* `resources/job_ingesta_bronze_cobranzas_acarrasco.yml`: definición del job (serverless, manual).

Checkpoints y schemaLocation van en `checkpoint/cobranzas_acarrasco/<tabla>/` y
`schemas/cobranzas_acarrasco/<tabla>/`, para no chocar con otras ingestas del bucket.

**Decisión de diseño:** se mantiene el sufijo `_brz` en las tablas destino (a diferencia del PR #2 de Richard),
porque es el estándar de la clase 14. Ver `specs/cobranzas_ingest.md` para la justificación completa.

Prerrequisitos:
1. External location de Unity Catalog sobre el bucket.
2. Editar `config/tablas_cobranzas.yml` y poner tu `bucket_root` (ej: `s3://mi-bucket`).
3. CSV de cobranzas subidos a `landing/cobranzas/<tabla>/` en el bucket.
4. Tablas destino creadas: desplegar y correr `job_ddl_lakehouse` de `bundle_ddl`
   (crea el schema `cobranzas_acarrasco` con `cuotas_brz`, `pagos_brz` y `gestiones_cobranza_brz`).

```
databricks bundle validate -t dev --profile free-edition
databricks bundle deploy   -t dev --profile free-edition
databricks bundle run job_ingesta_bronze_cobranzas_acarrasco -t dev --profile free-edition
```

Parámetros sobrescribibles al correr: `--tables cuotas,pagos`, `--landing_prefix <otro>`, `--trigger once`, etc.
(ver `parse_args` en `src/main.py`).
