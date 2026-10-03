# Notebooks de Silver (paso a paso)

Un notebook por tipo de tabla Silver, con los patrones de las Sesiones 15, 16 y 17. Sirven para explicar
cada patrón antes de empaquetarlo en un job.

| Notebook | Familia | Tablas | Patrón | Sesión |
|---|---|---|---|---|
| `01_upsert_transaccional.py` | Catálogo y Transaccional | paises, campanias, ofertas, solicitudes, desembolsos, cuotas (widget `tabla`) | Tipar → validar/cuarentena → última versión del lote → MERGE con guarda de orden | S15 |
| `02_upsert_ultimo_archivo.py` | Referencia externa | tipos_cambio | Clave compuesta; el orden es `_bronze_ingestion_ts`: gana el último archivo | S15 |
| `03_insert_hechos.py` | Hecho inmutable | pagos, gestiones_cobranza (widget `tabla`) | MERGE insert-only (sin `WHEN MATCHED`) | S15 |
| `04_historizar_scd2.py` | Maestra historizada | clientes → dim_clientes | SCD Tipo 2 con doble source, Tipo 1 sobre la vigente, llegadas tardías a cuarentena | S16 |
| `05_registrar_estados.py` | Historial de estados | solicitudes → solicitudes_estados | Insert-only por `(id, fecha_actualizacion)` | S16 |
| `06_deduplicar_eventos.py` | Evento | eventos_app | Streaming append con watermark + `dropDuplicatesWithinWatermark` | S17 |
| `07_construir_sesiones.py` | Evento agregado | eventos_app → sesiones_app | `session_window` + watermark, modo append | S17 |

Todos leen Bronze como stream (`readStream.table` + `foreachBatch` o append, `trigger(availableNow=True)`)
y por defecto escriben en tablas de práctica `<tabla>_lab` y `_cuarentena_lab`, creadas con
`CREATE TABLE ... LIKE` desde el DDL real de `bundle_ddl`, con checkpoints en `checkpoint/notebooks/silver/`.
Con `sufijo_destino` vacío escriben en las tablas reales.

Para ver los casos (inválidas, duplicados, llegadas tardías, reenvíos, correcciones, payloads corruptos):
genera un delta con `scripts/data_generetor` (ver su README), ingéstalo a Bronze y vuelve a correr el
notebook. El manifiesto `casos_silver_<sufijo>.csv` dice qué debe pasar con cada clave.

Orden: `07` lee la salida de `06`.
