# AGENTS.md — bundle_datagen

Declarative Automation Bundle con los generadores de datos sintéticos de Wizard Bank que **corren en
Databricks** (los de `scripts/data_generetor/` corren en local). Las reglas de nombres y despliegue del
`AGENTS.md` de la raíz del repo aplican aquí.

## Antes de tocar nada

1. Leer la skill `databricks-core` (CLI, perfiles, bundles) y `databricks-jobs`.
2. Leer `specs/datagen_spec.md`: es la fuente de verdad del bundle. Un cambio de comportamiento se
   hace **primero en la spec** y luego en el código; así el bundle se puede reproducir desde la spec.

## Reglas propias

- `src/emit_eventos_app.py` es un port de `scripts/data_generetor/productor_eventos.py` (destino parquet).
  El contrato del evento (`construir_evento`), los casos de Silver y el schema Kafka deben coincidir con
  el original y con `bronze/app/src/main.py` (que parsea el `value`). Si uno cambia, cambian los tres.
- El job `job_datagen_eventos_app_emit` es `continuous` y está en `PAUSED` en el repo. No ponerlo en
  `UNPAUSED` en el YAML: encenderlo es una decisión del usuario (consume cómputo mientras corra).
- Todo parámetro (catálogo, rutas, ritmo) entra por `parameters` del job; nada escrito a mano en `src/`.
- Agregar otro generador: un `src/emit_<fuente>.py`, un `resources/job_datagen_<fuente>_<acción>.yml` y
  una sección en la spec. No se toca lo existente.
- Perfil del CLI: usar el declarado en `databricks.yml`; `prod` solo bajo petición expresa.
