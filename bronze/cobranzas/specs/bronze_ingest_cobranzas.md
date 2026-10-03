# Spec: bundle cobranzas — Ingesta S3 → Bronze con Auto Loader (Wizard Bank on-premise)

## Objetivo
Un Declarative Automation Bundle con un job, `job_bronze_cobranzas_ingest`, que ejecute el
mismo script Python de `bronze/lending` (`main.py`, no un notebook) para ingestar con Auto Loader
los CSV del sistema legado de cobranzas hacia las tablas Bronze `bronze.cobranzas.<tabla>_brz`,
ya creadas por `bundle_ddl`. Todo parametrizado, modular e idempotente.

Las reglas de ingesta, schema evolution, logging, estructura de código y del job son las de
[`bronze/lending/specs/bronze_ingest.md`](../../lending/specs/bronze_ingest.md). Esta spec solo
fija lo propio de cobranzas.

## Contexto
- Fuente: sistema legado on-premise de cobranzas (simulado en Azure SQL), exportado a S3.
- Bucket: `s3://lakehouse-datawizard/`
  - Landing: `landing/wizard_bank_onp/<tabla>/` (un CSV por carpeta, p. ej. `cuotas/cuotas.csv`)
  - Checkpoints: `checkpoint/cobranzas/<tabla>/`
  - Schema de Auto Loader: `schemas/cobranzas/<tabla>/`
- Carpetas hoy en landing: `cuotas`, `gestiones_cobranza`, `pagos`.
- Formato: CSV con header.
- Tablas destino (DDL en `bundle_ddl/src/ddls/bronze/`, schema `cobranzas`):

| Tabla destino | Carpeta en landing | DDL |
|---|---|---|
| `bronze.cobranzas.cuotas_brz` | `cuotas/` | `cuotas_brz.sql` |
| `bronze.cobranzas.pagos_brz` | `pagos/` | `pagos_brz.sql` |
| `bronze.cobranzas.gestiones_cobranza_brz` | `gestiones_cobranza/` | `gestiones_cobranza_brz.sql` |

- Mismo diseño de tabla que lending: columnas de negocio `STRING`, `_metadata` STRUCT de 6
  campos, `_rescued_data`, `_ingestion_ts`, `delta.appendOnly=true`.
- Prerrequisito de Unity Catalog: external location sobre `s3://lakehouse-datawizard/`.
- Este job cubre solo el schema `cobranzas`. Los schemas de los retos de alumnos
  (`cobranzas_jhersson`, `_richard`) tienen sus propios bundles.

## Estructura del bundle
```
cobranzas/
├── databricks.yml
├── resources/job_bronze_cobranzas_ingest.yml
├── config/
│   └── tablas_cobranzas.yml      # una entrada por tabla
├── specs/bronze_ingest_cobranzas.md
└── src/                          # copia de bronze/lending/src, sin cambios
```

## Parámetros del job
Mismo `main.py` que lending; el job sobrescribe estos defaults:

| Parámetro | Valor en el job |
|---|---|
| `--catalog` | `${var.catalog_bronze}` |
| `--schema` | `cobranzas` |
| `--landing_prefix` | `landing/wizard_bank_onp` |
| `--config_path` | `config/tablas_cobranzas.yml` |
| `--checkpoint_prefix` | `checkpoint/cobranzas` |
| `--schema_prefix` | `schemas/cobranzas` |

`bucket_root` sale del YAML (`s3://lakehouse-datawizard`).

## Reglas del job (YAML)
Las de lending, con: nombre `job_bronze_cobranzas_ingest`, task `ingest_cobranzas`, tags
`capa: bronze`, `dominio: ingesta`, `proyecto: wizard-bank`. Sin schedule.

## Criterios de aceptación
1. `databricks bundle validate -t dev --profile <profile>` termina sin errores.
2. `databricks bundle deploy -t dev` crea el job `job_bronze_cobranzas_ingest`.
3. `databricks bundle run job_bronze_cobranzas_ingest -t dev` termina en SUCCESS, con un
   resumen de 3 tablas ok.
4. **Schema calza:** las columnas de negocio de `DESCRIBE bronze.cobranzas.<tabla>_brz`
   coinciden por nombre con el header del CSV y todas son STRING. Si el CSV no trae el id
   IDENTITY de la fuente (`id_cuota`, `id_pago`, `id_gestion`), esa columna queda nula y se
   documenta aquí.
5. **Conteo:** `count(*)` de cada tabla = filas de los CSV en landing.
6. **Metadata:** `_metadata.file_path` apunta a `landing/wizard_bank_onp/`, `_ingestion_ts`
   no es nulo y `_rescued_data` es nulo en una carga normal.
7. **Idempotencia:** una segunda ejecución no agrega filas.
8. **Incremental:** un archivo nuevo en una carpeta de landing solo agrega sus filas.
9. **Aislamiento:** los checkpoints quedan bajo `checkpoint/cobranzas/` y no tocan los de
   lending ni los de los retos.

## Fuera de alcance
Tablas de `lending`, schemas de retos, Silver y Gold, calidad de datos, schedules y
notificaciones.
