# Spec: bundle_ddl — DDLs del Lakehouse de Wizard Bank

## Objetivo
Crear un Declarative Automation Bundle con un único job, `job_ddl_lakehouse_deploy`, que ejecute
los DDL de todas las capas del Lakehouse (hoy solo Bronze; Silver y Gold se agregarán
después). El job debe ser idempotente: siempre que se ejecute la priemra vez crea los objetos y las siguiente no da error ni elimina ni reemplaza.

## Contexto
- El bundle ya fue inicializado con `databricks bundle init` y se llama `bundle_ddl`.
- Los DDL ya existen como archivos `.sql`, uno por tabla, en `src/ddl/`.
- Ingesta, Auto Loader, Silver y Gold quedan fuera de este bundle.

## Inputs
- Carpeta `src/ddl/` organizada por capa:
  - `src/ddl/00_setup/00_create_schemas.sql`: crea el catálogo `bronze` y los schemas
    `bronze.lending` y `bronze.cobranzas`. Tiene 3 sentencias.
  - `src/ddl/bronze/`: 11 archivos `<tabla>_brz.sql`
    - lending (8): paises, campanias, productos_prestamo, clientes,
      ofertas_preaprobadas, solicitudes_prestamo, desembolsos, tipos_cambio
    - cobranzas (3): cuotas, pagos, gestiones_cobranza
  - `src/ddl/silver/` y `src/ddl/gold/`: vacías, con `.gitkeep`.
- Todos los DDL usan `CREATE ... IF NOT EXISTS`.
- Variable del bundle: `warehouse_id`, resuelta por nombre con `lookup` al SQL warehouse serverless `Serverless Starter Warehouse` (sobrescribible con `--var`).

## Outputs
- `databricks.yml` con las variables y los targets `dev` y `prod`.
- `resources/job_ddl_lakehouse_deploy.yml` con el job.
- El job tiene tasks de tipo `sql_task` con `file.path`:
  1. `setup_schemas` ejecuta `00_setup/00_create_schemas.sql`.
  2. Una task por cada tabla de `bronze/`, con `task_key = <tabla>_brz` y
     `depends_on: setup_schemas`. Las 12 corren en paralelo.
- Resultado esperado: 8 tablas en `bronze.lending` y 4 en `bronze.cobranzas`.
- Eliminar del template todo lo que no se use (notebooks, pipeline y tests de ejemplo).

## Reglas
- Usar `sql_task` sobre el SQL warehouse `${var.warehouse_id}`. usar serverless.
- Si el `sql_task` no admite varias sentencias por archivo, partir `00_create_schemas.sql`
  en un archivo por sentencia. No usar un notebook.
- Sin schedule ni trigger: el job es manual.
- `max_concurrent_runs: 1`, `timeout_seconds: 900`, sin reintentos.
- Targets:
  - `dev` con `mode: development` y `default: true`.
  - `prod` con `mode: production`.
  - Ambos con `workspace.host` y `profile` explícitos. No seleccionar el profile automáticamente.
- Agregar una tabla nueva = un `.sql` en la carpeta de su capa + una task en el YAML.
  No se toca lo existente.
- Sin credenciales ni IDs reales en el repo. El `warehouse_id` se resuelve con `lookup` por nombre.
- Tags del job: `dominio: ddl`, `proyecto: wizard-bank`.
- Naming: bundle `bundle_ddl`, job `job_ddl_lakehouse_deploy`.

## Criterios de aceptación
1. `databricks bundle validate -t dev --profile <profile>` termina sin errores.
2. `databricks bundle deploy -t dev` crea el job con 13 tasks (1 de setup y 12 de Bronze).
3. `databricks bundle run job_ddl_lakehouse_deploy -t dev` termina en SUCCESS.
4. `SHOW TABLES IN bronze.lending` devuelve 8 tablas `_brz` y
   `SHOW TABLES IN bronze.cobranzas` devuelve 4.
5. `DESCRIBE DETAIL bronze.lending.solicitudes_prestamo_brz` muestra
   `clusteringColumns = [_ingestion_ts]`, y `SHOW TBLPROPERTIES` incluye `delta.appendOnly = true`.
6. Una segunda ejecución del job también termina en SUCCESS.

## Fuera de alcance
Ingesta de datos, Auto Loader, pipelines de Silver y Gold, y permisos (GRANT).


---

# Cambio 1 — Catálogo parametrizado

## Objetivo
Ningún DDL debe tener el catálogo escrito a mano. El job pasa el catálogo en tiempo de
ejecución según el target, y hoy vale lo mismo en todos los targets.

## Reglas
- Variables en `databricks.yml`: `catalog_bronze` (default `bronze`). Silver y Gold agregarán
  `catalog_silver` y `catalog_gold` cuando existan.
- Cada target puede sobrescribir la variable en su bloque `variables:`. Hoy `dev` y `prod`
  usan el mismo valor, pero la sobrescritura debe quedar declarada.
- Cada `sql_task` recibe el valor en `parameters: { catalog: ${var.catalog_bronze} }`.
- En los `.sql` se usan parameter markers con `IDENTIFIER`, por ejemplo:
  `CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.lending.paises_brz') (...)`.
  Lo mismo aplica a `CREATE CATALOG` y `CREATE SCHEMA` en `00_setup`.
- No dejar ninguna ocurrencia literal de `bronze.` como catálogo en los `.sql`.
  Los comentarios pueden mencionarlo.
- Las propiedades y el resto del DDL no cambian.
- Si el `sql_task` con archivo no soporta parameter markers, usar la alternativa que valide
  el CLI y dejar la decisión documentada en un comentario del YAML.

## Criterios de aceptación
1. `grep -rE "bronze\.(lending|cobranzas)" src/ddl` no devuelve DDL con el catálogo fijo.
2. `bundle validate` sigue sin errores.
3. Con el valor por defecto, el job sigue terminando en SUCCESS y es idempotente.