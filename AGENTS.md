# AGENTS.md

Guía para agentes de IA (y personas) que trabajen en este repo. Aplica a todos los bundles
(`bronze/`, `silver/`, `gold/`, `bundle_ddl/`). Cada bundle puede añadir su propio `AGENTS.md`
con reglas específicas; las de este archivo valen siempre.

## Estructura

Un bundle de Databricks (Declarative Automation Bundle) por carpeta: `<capa>/<fuente>/` con
`databricks.yml`, `resources/*.yml`, `src/`, `config/`, `specs/` y `README.md`.
Los DDL viven en `bundle_ddl/`.

## Convenciones de nombres

Idioma: términos técnicos y acciones en inglés (`ingest`, `deploy`, `refresh`); descripciones y
comentarios en español. Todo en `snake_case`, minúsculas, sin tildes ni espacios.

| Elemento | Patrón | Ejemplo |
|---|---|---|
| Job (`name:`) | `job_<capa>_<fuente>_<acción>` | `job_bronze_lending_ingest` |
| Archivo del recurso | `resources/<nombre del job>.yml` | `resources/job_bronze_lending_ingest.yml` |
| Task (`task_key`) | `<verbo>_<objeto>` | `ingest_lending`, `setup_schemas` |
| Clave del recurso en el YAML | igual al `name:` del job | `job_bronze_lending_ingest:` |
| Pipeline (`name:`) | `pipeline_<capa>_<fuente>_<función>` | `pipeline_bronze_lending_ingest` |
| Bundle (`bundle.name`) | `<fuente>` (o `bundle_ddl`) | `lending` |
| Notebook | `<NN>_<verbo>_<objeto>` | `01_ingest_pagos` |
| Módulo Python | `snake_case` por contenido | `autoloader.py` |
| Archivo SQL de DDL | `<entidad>_<sufijo de capa>.sql` | `pagos_brz.sql` |
| Tabla | `<catálogo>.<schema>.<entidad>_<sufijo de capa>` | `bronze.lending.pagos_brz` |
| Config de tablas | `config/tablas_<fuente>.yml` | `config/tablas_lending.yml` |
| Spec | `specs/<tema>.md` | `specs/bronze_ingest.md` |

- `<capa>`: `bronze`, `silver`, `gold`; para DDL transversal, `ddl`.
- `<fuente>`: sistema o dominio de origen (`lending`, `cobranzas`). En retos con varias versiones
  del mismo bundle se añade la persona a la fuente (`cobranzas_jhersson`).
- `<acción>`: verbo corto (`ingest`, `deploy`, `refresh`, `dq`).
- Sufijos de capa en tablas: `_brz`, `_slv`, `_gld`.
- **Sin sufijo de entorno** (`_dev`, `_prod`): `mode: development` ya antepone `[dev <usuario>]`.
- **No cambiar la clave de un recurso ya desplegado**: el bundle lo trata como uno nuevo, destruye el
  anterior y pierde su historial de runs. Renombrar solo `name:` renombra sin recrear.
- Tasks: el verbo dice qué hace (`ingest`, `setup`, `validate`, `merge`) y el objeto sobre qué (`ingest_pagos`, `setup_schemas_silver`). Sin prefijo `task_`, sin nombre del job ni de la capa si ya van en el job. En un job de DDL con una task por tabla, el `task_key` es el nombre de la tabla (`pagos_brz`), igual que su `.sql`. Cambiar un `task_key` recrea la task y reinicia su historial.
- Un job y su archivo `resources/` se llaman igual, para encontrarlos con una búsqueda.

## Excepción: DDL de alumnos

Los `.sql` de `bundle_ddl/src/ddls/` creados por alumnos en el bootcamp (`*_richard.sql`,
`*_brz_jhersson.sql`, etc.) conservan su nombre original. El patrón aplica a lo nuevo.

## Despliegue

- Perfil del CLI: nunca elegir uno por defecto; usar el declarado en `databricks.yml` y verificar
  que el host del perfil coincida con el del bundle (`databricks auth profiles`).
- Validar antes de desplegar: `databricks bundle validate -t dev`.
- `dev` es el target por defecto; `prod` solo bajo petición expresa.
