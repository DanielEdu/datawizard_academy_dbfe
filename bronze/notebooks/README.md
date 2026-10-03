# Notebooks de Bronze (paso a paso)

Notebooks para explicar la ingesta a Bronze antes de verla empaquetada en un job.
Hacen lo mismo que los jobs, para una tabla, con widgets para cambiar la fuente.

| Notebook | Qué enseña | Job equivalente |
|---|---|---|
| `01_ingest_archivos_landing.py` | Archivos CSV de landing → Bronze con Auto Loader | `job_bronze_lending_ingest`, `job_bronze_cobranzas_ingest` |
| `02_ingest_eventos_kafka.py` | Mensajes Kafka (en `.parquet`) → parseo del `value` → Bronze | `job_bronze_app_ingest` |

Importarlos en el workspace (o abrirlos desde la carpeta Git) y correrlos en serverless.
Por defecto escriben en tablas de práctica `<tabla>_brz_lab` con su propio checkpoint
(`checkpoint/notebooks/...`), así no duplican las tablas reales que cargan los jobs.
Prerrequisitos: tablas creadas por `bundle_ddl` y archivos en landing (`scripts/data_generetor/README.md`).
