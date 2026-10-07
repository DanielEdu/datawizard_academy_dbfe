# Spec: Bronze Ingesta — Cobranzas (_acarrasco)

**Bundle:** `bronze/cobranzas_acarrasco`
**Schema destino:** `bronze._acarrasco`
**Tablas:** `cuotas_brz`, `pagos_brz`, `gestiones_cobranza_brz`
**Capa:** Bronze (append-only, fidelidad total a la fuente)

---

## Objetivo

Ingerir en capa Bronze los tres archivos CSV que genera el sistema legado de cobranzas
(`landing/cobranzas/<tabla>/`), usando Auto Loader con modo `availableNow` (batch idempotente).

## Fuente de datos

| Campo | Valor |
|---|---|
| Sistema | Legacy cobranzas on-premise (simulado) |
| Formato | CSV con header |
| Ruta landing | `s3://<bucket>/landing/cobranzas/<tabla>/` |
| Frecuencia | Batch incremental — watermark: `fecha_modificacion` |
| Generador | `scripts/data_generetor/generar_datos_cobranzas.py` |

## Tablas destino

| Tabla | Schema | Llave natural | Filas aprox. |
|---|---|---|---|
| `cuotas_brz` | `bronze._acarrasco` | `id_cuota` | Variable |
| `pagos_brz` | `bronze._acarrasco` | `id_pago` | Variable |
| `gestiones_cobranza_brz` | `bronze._acarrasco` | `id_gestion` | Variable |

## Decisiones de diseño

### 1. Sufijo `_brz` en tablas

Richard (PR #2) soltó el sufijo. Se mantiene aquí porque es el estándar de la clase 14:
*tabla con sufijo de capa, schema = origen, catálogo = ambiente*. El sufijo identifica
en qué capa está la tabla sin ambigüedad al consultarla.

### 2. Schema `_acarrasco`, no `cobranzas_acarrasco`

El esquema de namespacing sigue el patrón de Richard (`_richard`): un schema propio por
participante, creado aditivamente en `bundle_ddl/src/ddls/00_setup/00_create_schemas.sql`.
El prefijo `_` marca que es un schema personal de reto, no de producción.

### 3. Checkpoints namespaceados

`checkpoint/cobranzas_acarrasco/<tabla>/` y `schemas/cobranzas_acarrasco/<tabla>/`.
Evita colisión silenciosa si otra instancia del bundle usa tablas homónimas.

### 4. Bundle autocontenido

Los 4 módulos (`config/loader.py`, `ingestion/autoloader.py`, `utils/logger.py`,
`utils/paths.py`) son copia de `lending`. La lógica de ingesta es idéntica; la
diferenciación está en los defaults de `main.py`.

## Fuera de alcance

- Deduplicación: va en Silver.
- Tipos de datos: van en Silver.
- Tablas de `lending`: las carga `bronze/lending`.
- Schema `cobranzas` (el de producción): lo carga ADF, no este bundle.
