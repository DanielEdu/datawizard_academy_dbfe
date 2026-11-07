# Notebooks por sesión

Guías **paso a paso** que el alumno sigue en clase, una carpeta por sesión, **fuera de los bundles**: los bundles (`bronze/*`,
`bundle_ddl/`, `bundle_datagen/`, `lakeflow/`) son código que se despliega; estos notebooks son material de clase que se importa o
se abre desde la carpeta Git del workspace.

Cada guía explica el paso, trae el código en un bloque 📋 (se copia a un archivo nuevo del pipeline), y tiene celdas ▶️ para validar.
Los pasos 🖱️ son en la interfaz.

| Sesión | Carpeta | Guía | Construye |
|---|---|---|---|
| 19 · Lakeflow: Bronze y Silver | `sesion-19-lakeflow-bronze-silver/` | `00_paso_a_paso_lakeflow.py` | `pipeline_wizard_bank_lakehouse` con 12 Bronze y 14 Silver |
| 20 · Gold, métricas y features | `sesion-20-gold-metricas-features/` | `00_paso_a_paso_gold.py` | En el mismo pipeline: estrella, 4 métricas, cartera, 2 feature tables; y 2 metric views |
| 20.2 · Genie | `sesion-20.2-genie/` | `00_paso_a_paso_genie.py` | Genie Agent con benchmark antes/después, metadatos mejorados, Genie One |

Las tres sesiones son **una sola construcción**: la 20 le agrega Gold al pipeline de la 19 y la 20.2 usa el Gold de la 20.
No se borra nada entre sesiones; la limpieza está al final de la 20.2.

## Cómo se generan las guías

`00_paso_a_paso_*.py` es **generado**: no se edita a mano. Se edita `_plantilla.py` de cada sesión y se corre:

```bash
python notebooks/generar_guias.py
```

El generador inserta el código real de `lakeflow/` donde la plantilla dice `{{transformations/…}}` (bloque para copiar) o
`{{ejecutar:metric_views/…}}` (celda que ejecuta el SQL con el catálogo y esquema del alumno). Así la guía y el pipeline nunca se
desincronizan: si cambias un archivo de `lakeflow/`, vuelve a generar.

## Sesiones anteriores

Las sesiones 11 a 18 se dictaron con los notebooks que viven junto a cada capa. Se mantienen en su lugar para no romper rutas que los
alumnos ya usan:

| Sesiones | Dónde | Qué |
|---|---|---|
| 11 · 12 · 14 | `bronze/notebooks/` | Ingesta a Bronze con Auto Loader (archivos y Kafka) |
| 15 · 16 · 17 | `silver/notebooks/` | MERGE idempotente, SCD2, historial de estados, streaming de eventos |
| 18 | `gold/notebooks/` | Gold imperativo: estrella, CDF, métricas, tabla ancha, optimización. La Sesión 20 lo reconstruye en el pipeline |
