# cobranzas_jhersson — Ingesta S3 → Bronze con Auto Loader (reto)

Bundle con el job `job_bronze_cobranzas_jhersson_ingest`, que ingesta los CSV del sistema legado de cobranzas
(`landing/wizard_bank_onp/<carpeta>/`) hacia `<catalog_bronze>.cobranzas_jhersson.<tabla>_brz`.
Spec: [specs/bronze-ingest-cobranzas.md](specs/bronze-ingest-cobranzas.md).

* `config/tablas_cobranzas.yml`: una entrada por tabla (nombre, carpeta, formato, enabled, opciones).
* `src/main.py`: entry point (argparse + orquestación); mismo código que `bronze/lending`.
* `resources/job_bronze_cobranzas_jhersson_ingest.yml`: job manual serverless.

Prerrequisitos: tablas creadas por `bundle_ddl` (tasks `*_brz_jhersson`) y la external location de Unity Catalog sobre el bucket.

```
databricks bundle validate -t dev --profile free-edition
databricks bundle deploy   -t dev --profile free-edition
databricks bundle run job_bronze_cobranzas_jhersson_ingest -t dev --profile free-edition
```
