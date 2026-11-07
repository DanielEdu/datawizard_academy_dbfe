# AGENTS.md

Guía para agentes de IA (y personas) que trabajen en este repo. Aplica a todos los bundles
(`bronze/`, `silver/`, `gold/`, `bundle_ddl/`). Cada bundle puede añadir su propio `AGENTS.md`
con reglas específicas; las de este archivo valen siempre.

## Estructura

Un bundle de Databricks (Declarative Automation Bundle) por carpeta: `<capa>/<fuente>/` con
`databricks.yml`, `resources/*.yml`, `src/`, `config/`, `specs/` y `README.md`.
Los DDL viven en `bundle_ddl/`. El pipeline declarativo de todo el Lakehouse (Sesiones 19 y 20) vive en `lakeflow/`
(también es un bundle: `transformations/`, `metric_views/`, `mejoras_genie/`, `resources/`).

**Notebooks de clase:** desde la Sesión 19, las guías paso a paso van **fuera de los bundles**, en `notebooks/sesion-<NN>-<tema>/`.
La guía `00_paso_a_paso_*.py` es generada desde `_plantilla.py` con `python notebooks/generar_guias.py`, que inserta el código de
`lakeflow/`: si cambias un archivo de `lakeflow/`, regenera las guías. Los notebooks de las Sesiones 11 a 18 se quedan en
`bronze/notebooks/`, `silver/notebooks/` y `gold/notebooks/` para no romper rutas de los alumnos.

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
| Feature table | `features_<sujeto>_<grano>_gld`, llave con `TIMESERIES` | `features_cliente_mes_gld` |
| Metric view | `kpi_<tema>` (sin sufijo de capa: es una vista sobre Gold) | `kpi_colocacion` |
| Transformación del pipeline | `<NN>_<capa>_<tema>.py\|.sql` en `lakeflow/transformations/` | `13_gold_metricas.sql` |
| Guía de clase | `notebooks/sesion-<NN>-<tema>/00_paso_a_paso_<tema>.py` | `notebooks/sesion-20.2-genie/00_paso_a_paso_genie.py` |
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
