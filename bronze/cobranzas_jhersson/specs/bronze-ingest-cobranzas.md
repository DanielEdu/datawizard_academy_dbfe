# Spec: bundle cobranzas_jhersson — Ingesta S3 → Bronze con Auto Loader (Wizard Bank on-premise)

Misma spec que `bronze/lending/specs/bronze-ingest.md` (reglas de ingesta, parámetros, logging,
idempotencia, schema evolution y criterios de aceptación). Solo cambia lo siguiente.

## Diferencias con lending
- Job: `job_bronze_cobranzas_jhersson_ingest`. Schema destino: `cobranzas_jhersson` (`--schema cobranzas_jhersson`),
  creado por `bundle_ddl` junto con sus tablas (`*_brz_jhersson.sql`).
- Landing: `s3://lakehouse-datawizard-de/landing/wizard_bank_onp/<carpeta>/` (`--landing_prefix landing/wizard_bank_onp`).
- YAML de tablas: `config/tablas_cobranzas.yml` (`--config_path config/tablas_cobranzas.yml`).

| Tabla destino | Carpeta en landing | Filas (carga inicial) |
|---|---|---|
| `bronze.cobranzas_jhersson.gestiones_cobranza_brz` | `cobranzas_jhersson/` | 38.222 |
| `bronze.cobranzas_jhersson.cuotas_brz` | `cuotas_jhersson/` | 562.416 |
| `bronze.cobranzas_jhersson.pagos_brz` | `pagos_jhersson/` | 259.346 |

## Notas
- Los CSV no traen `id_gestion`, `id_cuota` ni `id_pago` (IDENTITY en la fuente): esas columnas
  quedan nulas en Bronze. El resto del header calza en nombre y orden con el DDL.
- Checkpoints y schemaLocation: `checkpoint/cobranzas_jhersson/<tabla>/` y `schemas/cobranzas_jhersson/<tabla>/`
  (`--checkpoint_prefix` / `--schema_prefix`), separados de los de otras ingestas.