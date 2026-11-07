# Lakeflow: el Lakehouse de Wizard Bank en un pipeline declarativo

Código del pipeline `pipeline_wizard_bank_lakehouse` (Lakeflow pipelines, antes *Delta Live Tables*): Bronze y Silver
(**Sesión 19**), Gold, capa de métricas y feature tables (**Sesión 20**), y las mejoras de metadatos para Genie (**Sesión 20.2**).
Parte de los mismos archivos de landing que usan los bundles de `bronze/` y los notebooks de `silver/` y `gold/`.

Las **guías paso a paso** que sigue el alumno no están aquí: viven en [`notebooks/`](../notebooks/README.md), una carpeta por sesión.
Este directorio es el código que el alumno pega en su pipeline y, a la vez, un bundle desplegable.

| Ruta | Qué es | Sesión |
|---|---|---|
| `transformations/01_*` … `10_*` | Bronze (4 fuentes) y Silver (14 tablas) | 19 |
| `transformations/11_gold_dimensiones.sql` | 6 dimensiones: país, campaña, producto, tiempo, cliente (SCD2, `sk_cliente`), tipo de cambio con vigencia | 20 |
| `transformations/12_gold_hechos.sql` | 3 hechos: ofertas, solicitudes, desembolsos (USD), con join punto en el tiempo al cliente | 20 |
| `transformations/13_gold_metricas.sql` | 4 métricas materializadas: originación, colocación, ROI de campañas, funnel de la app | 20 |
| `transformations/14_gold_cartera.sql` | Tabla ancha de cartera: una fila por crédito | 20 |
| `transformations/15_gold_features.py` | 2 feature tables con llave `TIMESERIES`: `features_cliente_mes_gld`, `features_app_cliente_dia_gld` | 20 |
| `metric_views/kpi_colocacion.sql`, `kpi_funnel_app.sql` | Metric views (YAML) sobre Gold. **No** son parte del pipeline: se crean con SQL | 20 |
| `mejoras_genie/` | Versiones de `13`, `14` y `kpi_colocacion` con comentario por columna, llaves y sinónimos | 20.2 |
| `gobierno/` | Funciones de row filter y column mask, la cartera con esas políticas (`14_gold_cartera.sql`) y la vista para BI | 21 |
| `ci/lakeflow-ci.yml` | Plantilla de GitHub Actions: validar en cada PR y desplegar a prod en cada merge a `main` | 21.2 |
| `databricks.yml` + `resources/` | Bundle: el esquema, el pipeline y el job con disparador *file arrival* | 19 (reto) |

## Fuentes y patrones

| Fuente | Landing | Bronze | Silver |
|---|---|---|---|
| Base transaccional (Azure SQL) | `wizard_bank_rdb/<tabla>/` CSV | Auto Loader, todo STRING | Catálogos: materialized view · clientes y productos: AUTO CDC tipo 2 · ofertas, solicitudes, desembolsos: AUTO CDC tipo 1 (+ historial de estados tipo 2) |
| Proveedor externo (tipos de cambio) | `wizard_bank_rdb/tipos_cambio/` CSV | Auto Loader recursivo | AUTO CDC tipo 1, clave compuesta, gana el último archivo |
| Legado on-premise (cobranzas, vía ADF) | `wizard_bank_onp/<tabla>/` CSV | Auto Loader | Cuotas: AUTO CDC · pagos: AUTO CDC como deduplicador · gestiones: streaming append |
| Kafka (telemetría de la app) | `wizard_bank_kafka/eventos_app/` parquet con el registro de Kafka | Auto Loader con esquema fijo, `payload` sin interpretar | `from_json` + watermark + `dropDuplicatesWithinWatermark`; sesiones con `session_window` |

## Gold

| Forma | Tablas | Consumidor |
|---|---|---|
| Estrella | `dim_pais_gld`, `dim_campania_gld`, `dim_producto_gld`, `dim_tiempo_gld`, `dim_cliente_gld`, `dim_tipo_cambio_gld`, `fact_ofertas_gld`, `fact_solicitudes_gld`, `fact_desembolsos_gld` | BI |
| Métricas | `metricas_originacion_mensual_gld`, `metricas_colocacion_mensual_gld`, `metricas_roi_campania_gld`, `metricas_funnel_app_diario_gld` + metric views `kpi_colocacion`, `kpi_funnel_app` | Dashboards, Genie |
| Tabla ancha | `cartera_creditos_gld` | Cobranzas, Genie |
| Feature tables | `features_cliente_mes_gld` (mora), `features_app_cliente_dia_gld` (propensión) | Data Science |

Cruce clave entre sistemas: `numero_credito` del legado de cobranzas = `id_desembolso` de Lending.

## Configuración del pipeline

- Nombre: `pipeline_wizard_bank_lakehouse` · catálogo `datawizard_dev`, esquema `lakeflow` (uno por alumno si comparten workspace) · **serverless**
- Configuration: `landing = s3://lakehouse-datawizard/landing`
- Configuration (desde la Sesión 21): `politicas = <catálogo>.<esquema>` donde viven las funciones de row filter y mask. El bundle la arma sola
- Source: la carpeta `transformations/`

Con el bundle (`cd lakeflow && databricks bundle validate -t dev && databricks bundle deploy -t dev`) el pipeline publica en su
propio esquema, que el bundle crea (`resources/schema_lakeflow.yml`): en `dev` queda `lending_dev` (el target dev no usa
`mode: development`, que antepondría el usuario) y en `prod`, `lending_prod`. Así no choca con el pipeline creado en la UI. `databricks bundle destroy -t dev`
borra el pipeline, el job y el esquema con todas sus tablas.

## Estado de validación

Validado el 06-nov-2026 en el workspace `free-edition`, desplegando este bundle en `dev`
(pipeline `[dev …] pipeline_wizard_bank_lakehouse`, esquema `datawizard_dev.dev_daniel_epr77_lakeflow_bundle`):

- **Pipeline completo** (`01`–`15`): el primer update crea 12 Bronze, 14 Silver, 14 Gold y 2 feature tables. Todas las expectations
  de Gold y features en verde (tras corregir el perfil de cliente de `features_cliente_mes_gld`: la primera versión cubre desde siempre).
- **Segundo update sin archivos nuevos:** ninguna streaming table escribe filas. Gold mezcla `NO_OP`, incremental (`ROW_BASED`,
  `GROUP_AGGREGATE`) y recálculo completo, visible en `planning_information` del event log.
- **Llaves:** `PRIMARY KEY (id_cliente, disponible_desde TIMESERIES)` declarada en las dos feature tables (no hizo falta el plan B).
- **Metric views:** `kpi_colocacion` y `kpi_funnel_app` se crean y responden `MEASURE()`. Cuadran exacto con
  `metricas_colocacion_mensual_gld` en los 3 países (90.755.931,32 USD en total).
- **Guías:** todas las consultas SQL de la Sesión 20 y las 12 preguntas del banco de la 20.2 corren contra estas tablas.
- **`mejoras_genie/13|14`:** su SQL corre contra las Silver reales y los tipos coinciden con la lista de columnas (validado como
  `SELECT`, sin reemplazar los archivos del pipeline). `mejoras_genie/kpi_colocacion.sql` (sinónimos) se crea y responde igual que la original.
- **No validado:** la interfaz de Genie (crear el agente, benchmark, Genie One); es manual.
