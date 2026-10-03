# Manual: generar los datos de Wizard Bank y cargarlos a landing

Tres scripts generan los datos sintéticos de las fuentes de Wizard Bank y los dejan en el bucket
`s3://lakehouse-datawizard`, en la carpeta de landing que lee cada bundle de Bronze.

| Script | Fuente que simula | Landing en S3 | Bundle Bronze | Tablas destino |
|---|---|---|---|---|
| `generar_datos_wizard_bank.py` | Base transaccional Lending (Azure SQL) | `landing/wizard_bank_rdb/<tabla>/` | `bronze/lending` | `bronze.lending.*_brz` (8 tablas) |
| `generar_datos_cobranzas.py` | Sistema legado de cobranzas (on-premise) | `landing/wizard_bank_onp/<tabla>/` | `bronze/cobranzas` | `bronze.cobranzas.*_brz` (3 tablas) |
| `productor_eventos.py` | Topic Kafka de la app (`wizard.lending.eventos-app`) | `landing/wizard_bank_kafka/eventos_app/` | `bronze/app` | `bronze.app.eventos_app_brz` |

`casos_silver.py` no se ejecuta: lo usan los tres scripts para agregar casos de prueba de Silver
y para subir a S3.

---

## 1. Preparar el entorno (una sola vez)

```bash
cd datawizard_academy_dbfe
python3 -m venv .venv && source .venv/bin/activate
pip install pyarrow "boto3[crt]"          # parquet + subida a S3
# Solo si cargas a una base o publicas a Kafka real:
pip install pyodbc confluent-kafka
```

`boto3[crt]` (y no solo `boto3`) hace falta cuando el perfil de AWS se creó con `aws login`.

**Credenciales de AWS.** Un perfil con acceso al bucket:

```bash
aws login --profile datawizard                       # abre el navegador
aws s3 ls s3://lakehouse-datawizard/landing/ --profile datawizard   # verificar
```

Para no repetir `--s3` y `--aws-profile` en cada comando:

```bash
export AWS_PROFILE=datawizard
export WIZARD_BANK_S3=s3://lakehouse-datawizard/landing/wizard_bank_rdb
export COBRANZAS_S3=s3://lakehouse-datawizard/landing/wizard_bank_onp
export EVENTOS_S3=s3://lakehouse-datawizard/landing/wizard_bank_kafka
```

Todos los comandos de este manual se corren desde `scripts/data_generetor/`.

---

## 2. Orden recomendado

1. **Tablas Bronze creadas**: `cd bundle_ddl && databricks bundle deploy -t dev && databricks bundle run job_ddl_lakehouse_deploy -t dev`.
2. **Carga inicial (full)** de cada fuente → sube a landing.
3. **Ingesta Bronze**: `databricks bundle run <job> -t dev` en el bundle de la fuente.
4. **Deltas** cuando quieras simular actividad nueva → otra vez el job de Bronze.

| Fuente | Job de Bronze | Carpeta del bundle |
|---|---|---|
| Lending | `job_bronze_lending_ingest` | `bronze/lending` |
| Cobranzas | `job_bronze_cobranzas_ingest` | `bronze/cobranzas` |
| Eventos app | `job_bronze_app_ingest` | `bronze/app` |

`productor_eventos.py` usa los clientes y ofertas de Lending: genera Lending primero.

---

## 3. Lending — `generar_datos_wizard_bank.py`

**Carga inicial** (50.000 clientes por defecto; `--clientes 1000` para una prueba rápida):

```bash
python generar_datos_wizard_bank.py --destino csv --salida ./csv_wizard_bank \
    --s3 $WIZARD_BANK_S3
```

**Delta** (clientes nuevos con su cascada de ofertas/solicitudes/desembolsos, días nuevos de
tipos de cambio, filas existentes que cambian, y casos de Silver):

```bash
python generar_datos_wizard_bank.py --modo delta --destino csv \
    --base ./csv_wizard_bank --salida ./csv_wizard_bank_delta --seed 7 \
    --s3 $WIZARD_BANK_S3
```

| Parámetro | Para qué |
|---|---|
| `--base` | Carpeta de la carga inicial; de ahí salen los ids máximos y las filas a modificar |
| `--salida` | Dónde escribir el delta. Los deltas previos de esta carpeta también se leen, así que se pueden encadenar |
| `--seed` | Usa una distinta a la del full (42), o la actividad nueva sale igual a la ya cargada |
| `--sufijo` | Solo full: agrega fecha-hora al nombre (`clientes_<AAAAMMDD_HHMMSS>.csv`) |
| `--clientes-delta`, `--dias-delta`, `--updates-delta` | Volumen del delta |
| `--casos-silver N` | Filas por caso de Silver y tabla (default 5, `0` = delta limpio) |

---

## 4. Cobranzas — `generar_datos_cobranzas.py`

**Carga inicial** (~27.000 créditos; los CSV traen `id_cuota`, `id_pago`, `id_gestion`, la clave de Silver):

```bash
python generar_datos_cobranzas.py --destino csv --salida ./csv_cobranzas \
    --s3 $COBRANZAS_S3
```

**Delta**: créditos nuevos, cuotas `Pendiente` que pasan a `Pagada` (con su pago) o `Vencida`
(con su gestión), y casos de Silver:

```bash
python generar_datos_cobranzas.py --modo incremental --destino csv \
    --base ./csv_cobranzas --salida ./csv_cobranzas_delta --seed 7 \
    --s3 $COBRANZAS_S3
```

| Parámetro | Para qué |
|---|---|
| `--base` / `--salida` | Igual que en Lending; los deltas se encadenan |
| `--nuevos-creditos` | Créditos nuevos (default 25) |
| `--cuotas-pagar` / `--cuotas-vencer` | Cuotas existentes que cambian de estado (default 30 / 15) |
| `--casos-silver N` | Filas por caso y tabla (default 5) |
| `--sufijo` | Solo full: fecha-hora en el nombre del archivo |

---

## 5. Eventos de la app — `productor_eventos.py`

Sin broker Kafka: escribe un `.parquet` con **la misma estructura que entrega la fuente Kafka de
Spark** y lo sube a landing. El job de Bronze lo lee como si viniera del topic.

```bash
python productor_eventos.py --destino parquet --base-csv ./csv_wizard_bank \
    --eventos 20000 --salida ./parquet_eventos_app --s3 $EVENTOS_S3
```

Estructura de cada archivo (`eventos_app_<AAAAMMDD_HHMMSS>.parquet`):

| Columna | Tipo | Contenido |
|---|---|---|
| `key` | binary | `id_cliente` (misma key → misma partición) |
| `value` | binary | El evento serializado en JSON UTF-8, como lo publica el productor |
| `topic` | string | `wizard.lending.eventos-app` |
| `partition` | int | Partición (3 por defecto, `--particiones`) |
| `offset` | bigint | Secuencia por partición; continúa desde los `.parquet` previos de `--salida` |
| `timestamp` | timestamp | Llegada al broker: segundos después de `timestamp_evento` |
| `timestampType` | int | `0` = CreateTime |

| Parámetro | Para qué |
|---|---|
| `--base-csv` | Carpeta con `clientes*.csv` y `ofertas_preaprobadas*.csv` (en lugar de `--dsn`) |
| `--eventos` | Eventos normales por archivo (default 20.000) |
| `--horas` | Ventana hacia atrás en la que caen los eventos (default 6) |
| `--casos-silver N` | Registros por caso (default 20) |
| `--seed` | Fija el azar (por defecto cada corrida es distinta) |

Cada corrida es un archivo nuevo: para simular más tráfico, vuelve a correrlo y luego el job.

Con un Event Hubs/Kafka real (Sesión 11) se usa `--destino kafka` con `--bootstrap-servers`
y `--connection-string`; ver `python productor_eventos.py --help`.

---

## 6. Casos para Silver y el manifiesto

Todo delta (y cada archivo de eventos) trae, además de la actividad normal, filas que Silver tiene
que resolver. Cada corrida deja en `--salida` un `casos_silver_<sufijo>.csv` (local, no se sube)
con la tabla, el caso, la clave y lo que Silver debería hacer.

| Caso | Fuentes | Qué debe hacer Silver |
|---|---|---|
| `invalida` | Lending, Cobranzas | Ir a `_cuarentena` con el nombre de la regla |
| `duplicado_en_lote` | Lending, Cobranzas | Una sola fila (dedup antes del MERGE) |
| `reenvio` | Lending, Cobranzas | No cambiar nada (MERGE idempotente) |
| `llegada_tardia` | Lending, Cobranzas | No cambiar nada (guarda de orden) |
| `hecho_modificado` | Cobranzas (pagos, gestiones) | No cambiar nada (insert-only) |
| `correccion` | Lending (tipos_cambio) | Tomar el valor corregido |
| `duplicado` | Eventos | Una sola fila (`dropDuplicatesWithinWatermark`) |
| `reenvio_anterior` | Eventos | Se cuela si su lote ya salió del watermark |
| `llegada_tardia` | Eventos | Entra, con `retraso_seg` alto |
| `fuera_watermark` | Eventos | Descartado si ya se procesó un lote más nuevo |
| `payload_corrupto` | Eventos | `id_evento` NULL en Bronze; Silver lo filtra |
| `tipo_desconocido` | Eventos | Entra: un tipo nuevo es una señal |

Para verificar: carga el manifiesto y compáralo con Silver por clave.

---

## 7. Volver a cargar desde cero

Auto Loader recuerda en el checkpoint qué archivos ya leyó: **un archivo con el mismo nombre no se
vuelve a ingestar**. Si regeneras una carga full con el mismo nombre, reinicia la fuente:

```bash
# 1. Checkpoint y schemaLocation de la fuente (ej. cobranzas)
aws s3 rm s3://lakehouse-datawizard/checkpoint/cobranzas/ --recursive
aws s3 rm s3://lakehouse-datawizard/schemas/cobranzas/ --recursive
```

```sql
-- 2. Vaciar las tablas Bronze: son append-only, hay que quitar la propiedad un momento
ALTER TABLE bronze.cobranzas.cuotas_brz SET TBLPROPERTIES ('delta.appendOnly' = 'false');
TRUNCATE TABLE bronze.cobranzas.cuotas_brz;
ALTER TABLE bronze.cobranzas.cuotas_brz SET TBLPROPERTIES ('delta.appendOnly' = 'true');
-- … igual para cada tabla de la fuente
```

```bash
# 3. Volver a correr el job de Bronze
cd bronze/cobranzas && databricks bundle run job_bronze_cobranzas_ingest -t dev
```

Carpetas por fuente: lending → `checkpoint/<tabla>/` y `schemas/<tabla>/`;
cobranzas → `checkpoint/cobranzas/`, `schemas/cobranzas/`; eventos → `checkpoint/app/eventos_app/`.

Para no tener que reiniciar, usa `--sufijo` en las cargas full: cada archivo tiene nombre nuevo.

---

## 8. Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `unrecognized arguments: --base … --s3 …` | Estás corriendo una copia vieja del script. Usa la de esta carpeta |
| `MissingDependencyException … botocore[crt]` | El perfil viene de `aws login`: `pip install "boto3[crt]"` |
| `AccessDenied` al subir | El perfil apunta a otra cuenta: `aws sts get-caller-identity --profile datawizard` |
| `no trae id_cuota` al generar un delta de cobranzas | La base es de una versión anterior del generador: regenera la carga full |
| El job de Bronze termina bien pero no agrega filas | El archivo tiene un nombre ya ingestado: ver sección 7 |
| `falta pyarrow` | `pip install pyarrow` |
