# Lakeflow paso a paso: el Lakehouse de Wizard Bank en un pipeline declarativo

Reconstruye **Bronze, Silver y Gold** del curso con **Lakeflow Spark Declarative Pipelines**, partiendo de los mismos archivos de
landing que usan los bundles de `bronze/` y los notebooks de `silver/` y `gold/`. El eje de la clase es el **tipo de fuente**:
de él salen el objeto de Bronze y el patrón de Silver.

| Archivo | Qué es |
|---|---|
| `00_paso_a_paso_lakeflow.py` | Guía de la clase: explica cada paso, trae el código (📋) y celdas para validar (▶️). **Generado**: no editar a mano |
| `transformations/` | El código del pipeline, un archivo por paso. Es lo que el alumno pega en su pipeline |
| `_plantilla_guia.py` + `generar_guia.py` | Plantilla de la guía. `python generar_guia.py` inserta el código de `transformations/` en cada `{{archivo}}` |

## Fuentes y patrones

| Fuente | Landing | Bronze | Silver |
|---|---|---|---|
| Base transaccional (Azure SQL) | `wizard_bank_rdb/<tabla>/` CSV | Auto Loader, todo STRING | Catálogos: materialized view · clientes y productos: AUTO CDC tipo 2 · ofertas, solicitudes, desembolsos: AUTO CDC tipo 1 (+ historial de estados tipo 2) |
| Proveedor externo (tipos de cambio) | `wizard_bank_rdb/tipos_cambio/` CSV | Auto Loader recursivo | AUTO CDC tipo 1, clave compuesta, gana el último archivo |
| Legado on-premise (cobranzas, vía ADF) | `wizard_bank_onp/<tabla>/` CSV | Auto Loader | Cuotas: AUTO CDC · pagos: AUTO CDC como deduplicador · gestiones: streaming append |
| Kafka (telemetría de la app) | `wizard_bank_kafka/eventos_app/` parquet con el registro de Kafka | Auto Loader con esquema fijo, `payload` sin interpretar | `from_json` + watermark + `dropDuplicatesWithinWatermark`; sesiones con `session_window` |

Gold (`11_gold.sql`): materialized views en SQL: `dim_cliente_gld`, `fact_desembolsos_gld` (USD con el tipo de cambio vigente),
`metricas_colocacion_mensual_gld`, `metricas_funnel_app_diario_gld`, `cartera_creditos_gld`.

## Configuración del pipeline

- Nombre: `pipeline_wizard_bank_lakehouse`
- Catálogo `datawizard_dev`, esquema `lakeflow`, **serverless**
- Configuration: `landing = s3://lakehouse-datawizard/landing`
- Source: la carpeta `transformations/`

Validado en el workspace `free-edition` (pipeline de prueba `51653e44-486f-4531-9aac-9b616358efca`, modo development).
