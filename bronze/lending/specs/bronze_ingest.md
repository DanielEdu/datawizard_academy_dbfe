# Spec: bundle_ingesta — Ingesta S3 → Bronze con Auto Loader (Wizard Bank RDB)

## Objetivo
Un Declarative Automation Bundle con un job, `job_bronze_lending_ingest`, que ejecute un
script Python (`main.py`, no un notebook) para ingestar con Auto Loader los archivos de
landing en S3 hacia las tablas Bronze `bronze.lending.<tabla>_brz`, ya creadas por
`bundle_ddl`. Todo parametrizado, modular y idempotente.

## Contexto
- Bucket: `s3://lakehouse-datawizard/`
  - Landing: `landing/wizard_bank_rdb/<tabla>/`
  - Checkpoints: `checkpoint/<tabla>/`
  - Schema de Auto Loader: `schemas/<tabla>/`
- Carpetas hoy en landing: campanias, clientes, desembolsos, ofertas_preaprobadas, paises,
  solicitudes_prestamo, tipos_cambio, productos_prestamo.
- Formato de los archivos: CSV con header.
  `tipos_cambio` puede traer subcarpetas `YYYY/MM/DD/`, y Auto Loader las lee de forma recursiva.
- Las tablas Bronze ya existen con este diseño: columnas de negocio todas `STRING`, más
  `_metadata` STRUCT de 6 campos (file_path, file_name, file_size, file_block_start,
  file_block_length, file_modification_time), `_rescued_data` STRING y `_ingestion_ts` TIMESTAMP.
  Tienen `delta.appendOnly=true`.
- Este job cubre solo el schema `lending`. Las tablas de `cobranzas` quedan fuera (job aparte).
- Prerrequisito de Unity Catalog: debe existir una external location con su storage
  credential que cubra `s3://lakehouse-datawizard/`.

## Estructura del bundle
```
lending/
├── databricks.yml
├── resources/job_bronze_lending_ingest.yml
├── config/
│   └── tablas_lending.yml        # una entrada por tabla
└── src/
    ├── main.py                   # entry point: argparse + orquestación
    ├── config/loader.py          # lee y valida el YAML
    ├── ingestion/autoloader.py   # lógica de lectura/escritura (clase AutoLoaderIngestor)
    └── utils/
        ├── logger.py             # logging estándar
        └── paths.py              # construcción de rutas s3 (landing, checkpoint, schema)
```
- Una sola clase, `AutoLoaderIngestor`, con estado mínimo (spark, catalog, schema, rutas
  base, opciones). Sin herencia, sin patrones adicionales.
- El resto son funciones simples. No agregar frameworks ni abstracciones.

## Inputs
### `config/tablas_lending.yml`
Por cada tabla: `nombre`, `formato` (csv), `enabled` (true/false) y `opciones` (dict opcional
para sobrescribir opciones de lectura).
- 8 tablas de lending. `productos_prestamo` con `enabled: false` y un comentario que indica
  que se activa cuando exista su carpeta en landing.

### Parámetros del job (argparse en `main.py`, todos sobrescribibles)
| Parámetro | Default | Descripción |
|---|---|---|
| `--catalog` | (variable del bundle `catalog_bronze`) | catálogo destino |
| `--schema` | `lending` | schema destino |
| `--bucket_root` | `s3://lakehouse-datawizard` | raíz del bucket |
| `--landing_prefix` | `landing/wizard_bank_rdb` | prefijo de landing |
| `--checkpoint_prefix` | `checkpoint` | prefijo de checkpoints |
| `--schema_prefix` | `schemas` | prefijo de schemaLocation |
| `--config_path` | `config/tablas_lending.yml` | YAML de tablas |
| `--tables` | `all` | lista separada por comas o `all` |
| `--trigger` | `availableNow` | `availableNow` u `once` |
| `--suffix` | `_brz` | sufijo de las tablas destino |

Nada de rutas, catálogos ni nombres de tabla escritos a mano dentro del código Python.

## Reglas de ingesta
- Lectura: `spark.readStream.format("cloudFiles")` con:
  - `cloudFiles.format = csv`, `header = true`
  - `cloudFiles.inferColumnTypes = false`, para que todo llegue como STRING
  - `cloudFiles.schemaLocation = <bucket>/<schema_prefix>/<tabla>/`
  - `cloudFiles.schemaEvolutionMode = addNewColumns`
  - `rescuedDataColumn = _rescued_data`
  - No pasar `.schema()`: `addNewColumns` no es compatible con un schema explícito.
- **Schema evolution:**
  - Una columna nueva en los archivos debe agregarse como columna de la tabla, no quedar en `_rescued_data`.
  - Escribir con `mergeSchema = true`.
  - Auto Loader detiene el stream la primera vez que detecta la columna nueva y la incorpora
    al reiniciar. El job debe tolerarlo con `max_retries: 1` a nivel de task, o reintentando
    una vez dentro de `main.py`.
  - Los tipos de columnas existentes no evolucionan. Todo sigue en STRING.
- Metadata en el select antes de escribir:
  - `_metadata` se construye como `struct(_metadata.file_path, _metadata.file_name,
    _metadata.file_size, _metadata.file_block_start, _metadata.file_block_length,
    _metadata.file_modification_time)`. Solo esos 6 campos, para calzar con el DDL.
  - `_ingestion_ts = current_timestamp()`.
- Escritura: `writeStream` en modo append a la tabla `<catalog>.<schema>.<tabla><suffix>`
  (tabla gestionada de UC, no a una ruta), con
  `checkpointLocation = <bucket>/<checkpoint_prefix>/<tabla>/` y
  `trigger(availableNow=True)`, esperando `awaitTermination()`.
- No hacer dedup, casts ni transformaciones de negocio. Bronze es copia fiel.
- Las columnas de negocio del archivo deben coincidir por nombre con las de la tabla.
  Antes de iniciar, comparar el header contra el schema de la tabla y registrar un warning
  por cada columna extra.
- Ejecución:
  - Procesar las tablas `enabled: true` en secuencia dentro de un solo proceso.
  - Si una tabla falla, registrar el error, continuar con las demás y terminar el job en
    FAILED al final si hubo algún fallo. Imprimir un resumen por tabla (ok / error / omitida)
    con la cantidad de filas ingeridas.
  - Si la carpeta de landing de una tabla `enabled: true` no existe, fallar esa tabla con un
    mensaje claro.
- Logging con el módulo `logging`, sin `print`.

## Reglas del job (YAML)
- `spark_python_task` con `python_file: ../src/main.py` y `parameters` que pasan
  `--catalog ${var.catalog_bronze}` y el resto por default.
- Compute serverless con un `environment` que incluya `pyyaml`.
- Verificar que los imports entre `src/config`, `src/ingestion` y `src/utils` resuelven con
  `spark_python_task`. Si no, empaquetar como wheel (`python_wheel_task`) en lugar de mezclar
  soluciones, y documentarlo en un comentario.
- Sin schedule (job manual). `max_concurrent_runs: 1`, `timeout_seconds: 3600`, `max_retries: 1`.
- Variable `catalog_bronze` (default `bronze`) sobrescribible por target.
- Targets `dev` (`mode: development`, default) y `prod` (`mode: production`), con
  `workspace.host` y `profile` explícitos. No seleccionar el profile automáticamente.
- Sin credenciales en el repo. Los accesos a S3 van por la external location de Unity Catalog.
- Tags del job: `capa: bronze`, `dominio: ingesta`, `proyecto: wizard-bank`.

## Criterios de aceptación
1. `databricks bundle validate -t dev --profile <profile>` termina sin errores.
2. `databricks bundle deploy -t dev` crea el job `job_bronze_lending_ingest`.
3. `databricks bundle run job_bronze_lending_ingest -t dev` termina en SUCCESS, con un resumen
   de 7 tablas ok y `productos_prestamo` omitida.
4. **Schema calza:** para cada tabla ingerida, las columnas de negocio de
   `DESCRIBE bronze.lending.<tabla>_brz` coinciden en nombre y orden con el header del CSV,
   y todas son STRING.
5. **Conteo:** el `count(*)` de cada tabla es igual a las filas de los CSV en landing
   (lectura directa con `spark.read`).
6. **Metadata:** `_metadata.file_path` no es nulo y apunta a landing, `_ingestion_ts` no es
   nulo y `_rescued_data` es nulo en una carga normal.
7. **Idempotencia:** una segunda ejecución termina en SUCCESS y no agrega filas
   (`count(*)` idéntico).
8. **Incremental:** al dejar un archivo nuevo en una carpeta de landing, la siguiente
   ejecución solo agrega sus filas.
9. **Schema evolution:** un archivo con una columna nueva agrega esa columna a la tabla
   (visible en `DESCRIBE`), con valores en la columna y no en `_rescued_data`. Probar solo
   contra una tabla de prueba o con autorización explícita del usuario, porque modifica
   la tabla real.
10. `grep` en `src/` no encuentra literales como `s3://`, `bronze` ni nombres de tablas
    (todo viene de parámetros o del YAML).

## Fuera de alcance
Tablas de `cobranzas`, Silver y Gold, validación de calidad de datos, ingesta por
Kafka/CDC, notificaciones y schedules.