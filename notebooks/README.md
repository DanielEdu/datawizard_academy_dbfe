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
| 21 · Unity Catalog | `sesion-21-unity-catalog-gobierno/` | `00_paso_a_paso_gobierno.py` | Grupos de cuenta y `GRANT`, row filter y column mask en la cartera (declarados en el pipeline), vista para BI, linaje, auditoría y secret scope |
| 21.2 · CI/CD | `sesion-21.2-cicd-ambientes/` | `00_paso_a_paso_cicd.py` | El bundle `lakeflow/` de dev a prod con GitHub Actions: validar en el PR, desplegar al fusionar a `main` |
| 22 · IA sobre el Lakehouse y Fabric | `sesion-22-ia-lakehouse-fabric/` | `00_paso_a_paso_ia.py` | Foundation Model APIs, AI Functions sobre mensajes de clientes (`mensajes_clientes_ia`), `ai_forecast`, trazas de MLflow, un índice de AI Search con un RAG, los mensajes en el Genie Agent y el mapa de interoperabilidad con Fabric (90 min) |

Son **una sola construcción**: la 20 le agrega Gold al pipeline de la 19, la 20.2 usa ese Gold, la 21 lo gobierna, la 21.2 lo
lleva a prod y la 22 le suma IA. El curso cierra con el reto de `proyecto-final/proyecto_final.html`. No se borra nada entre sesiones; cada guía trae su limpieza al final del módulo.

## Cómo se generan las guías

`00_paso_a_paso_*.py` es **generado**: no se edita a mano. Se edita `_plantilla.py` de cada sesión y se corre:

```bash
python notebooks/generar_guias.py
```

El generador inserta el código real de `lakeflow/` donde la plantilla dice `{{transformations/…}}` (bloque para copiar) o
`{{ejecutar:metric_views/…}}` (celda que ejecuta el SQL con `{CATALOGO}.{ESQUEMA}` del alumno en lugar de `datawizard_dev.lending`). Así la guía y el pipeline nunca se
desincronizan: si cambias un archivo de `lakeflow/`, vuelve a generar.

## Sesiones anteriores

Las sesiones 11 a 18 se dictaron con los notebooks que viven junto a cada capa. Se mantienen en su lugar para no romper rutas que los
alumnos ya usan:

| Sesiones | Dónde | Qué |
|---|---|---|
| 11 · 12 · 14 | `bronze/notebooks/` | Ingesta a Bronze con Auto Loader (archivos y Kafka) |
| 15 · 16 · 17 | `silver/notebooks/` | MERGE idempotente, SCD2, historial de estados, streaming de eventos |
| 18 | `gold/notebooks/` | Gold imperativo: estrella, CDF, métricas, tabla ancha, optimización. La Sesión 20 lo reconstruye en el pipeline |
