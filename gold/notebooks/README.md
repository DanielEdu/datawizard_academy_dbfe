# Notebooks de Gold (paso a paso)

Un notebook por escenario y patrón de Gold, con las Sesiones 18, 19 y 20. Cada uno parte de Silver y construye
las tablas Gold **celda por celda**, sin funciones ni widgets: se lee de arriba abajo. El foco está en la
**feature de Databricks** que cada tabla ejercita (cada notebook termina con la tabla de features usadas).

| Notebook | Escenario | Tablas Gold | Patrón y features | Sesión |
|---|---|---|---|---|
| `01_lending_modelo_estrella.py` | Lending | `dim_pais`, `dim_campania`, `dim_tiempo`, `dim_cliente`, `fact_ofertas`, `fact_solicitudes`, `fact_desembolsos` | Esquema estrella: CTAS, `CLUSTER BY`, join punto-en-el-tiempo contra SCD2, `LEFT JOIN LATERAL` (tipo de cambio), `PRIMARY/FOREIGN KEY` | S18 |
| `02_lending_cdf.py` | Lending | `dim_cliente_vigente`, `cola_sync_crm` | Change Data Feed: `delta.enableChangeDataFeed`, `MERGE`, `DESCRIBE HISTORY`, `table_changes()`, `readChangeFeed` incremental | S18 |
| `03_lending_metricas.py` | Lending | `metricas_originacion_mensual`, `metricas_colocacion_mensual`, `metricas_roi_campania`, `mv_colocacion` | Capa de métricas: vistas como definición única, `INSERT … REPLACE WHERE`, `MERGE … NOT MATCHED BY SOURCE`, **Metric View** + `MEASURE()` | S20 |
| `04_cobranzas_tabla_ancha.py` | Cobranzas | `cartera_creditos`, `snapshot_cartera_tramos` | Tabla ancha para ML: `count_if`, `FILTER`, `max_by`, `mode`; snapshot idempotente con `REPLACE WHERE` + variable de sesión | S18 / S20 |
| `05_cobranzas_optimizacion.py` | Cobranzas | `fact_cuotas_lenta`, `fact_cuotas_lc` | Optimización: `OPTIMIZE`, **Liquid Clustering** (`CLUSTER BY`, `ALTER … CLUSTER BY`, `AUTO`), `ANALYZE TABLE`, `VACUUM`; ZORDER solo como legado | S19 |
| `06_app_metricas.py` | App | `metricas_funnel_diario`, `metricas_sesiones_segmento` | Métricas de eventos: `count_if`, `GROUP BY ALL`, dimensión conforme (`dim_cliente`) | S20 |
| `06b_app_mv_ctr_ubicacion.sql` | App | `mv_ctr_ubicacion` | **Materialized View** + `REFRESH` (se ejecuta en el SQL Editor, no en un notebook) | S20 |

## Orden y dependencias

```
Silver (_lab)
  ├─ 01 estrella ──┬─ 02 cdf              (independiente de 01 salvo por Silver)
  │                ├─ 03 métricas Lending (usa dims y hechos de 01)
  │                └─ 06 métricas App ── 06b MV   (06 usa dim_cliente de 01)
  ├─ 04 tabla ancha Cobranzas
  └─ 05 optimización Cobranzas             (independiente)
```

Corre `01` antes de `03` y `06`. El resto se puede correr en cualquier orden.

## Origen y destino

- **Origen:** las tablas `_lab` de Silver (`silver.lending.*_lab`, `silver.cobranzas.*_lab`, `silver.app.*_lab`), las que llenan los
  notebooks de `silver/notebooks`. Cuando Silver real esté poblado basta quitar el sufijo `_lab` en las consultas.
- **Destino:** schemas de práctica `gold.lending_lab`, `gold.cobranzas_lab`, `gold.app_lab`. No chocan con los Gold «reales»
  que más adelante se creen desde `bundle_ddl`.
- Todos se pueden **repetir**: usan `CREATE OR REPLACE`, `REPLACE WHERE` o empiezan borrando lo suyo (`02` y `05`).

## Qué Silver necesitan

| Notebook | Silver `_lab` requerido | Cómo se llena |
|---|---|---|
| 01, 03 | `paises`, `campanias`, `dim_clientes`, `ofertas_preaprobadas`, `solicitudes_prestamo`, `desembolsos`, `tipos_cambio` | `silver/notebooks/01` (widget `tabla`), `02` y `04` |
| 02 | `dim_clientes` | `silver/notebooks/04` |
| 04, 05 | `cuotas`, `pagos`, `gestiones_cobranza` | `silver/notebooks/01` (`cuotas`) y `03` (`pagos`, `gestiones_cobranza`) |
| 06 | `eventos_app`, `sesiones_app` | `silver/notebooks/06` y `07` |

## Limitaciones conocidas

- **`dim_productos`** no tiene notebook de Silver (el `04` la menciona pero solo construye `dim_clientes`). Por eso Gold no
  tiene `dim_producto`: `id_producto` queda en los hechos como atributo.
- **Tiempo del flujo por segmento** (vista → solicitud): en los datos actuales cada sesión tiene un solo evento, no se puede calcular.
- `05` usa `delta.targetFileSize = 1mb` solo para que una tabla de ~6 MB muestre el *data skipping*. No se usa en producción.
- Una Materialized View no se crea desde el cómputo de un notebook en este workspace (requiere SQL warehouse).
