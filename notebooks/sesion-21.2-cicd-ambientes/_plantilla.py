# Databricks notebook source

# MAGIC %md
# MAGIC # Sesión 21.2 · CI/CD paso a paso: del pipeline en dev a prod con el bundle y GitHub Actions
# MAGIC **Data Wizard Academy · Databricks Track · Módulo 5**
# MAGIC `v1.0 · creada 2026-11-07 · actualizada 2026-11-07`
# MAGIC
# MAGIC Hasta hoy el pipeline lo creaste y lo corriste tú, desde la interfaz. En un banco nadie toca producción con el mouse: un cambio
# MAGIC entra por un **pull request**, una máquina lo **valida**, alguien lo **aprueba** y otra máquina lo **despliega**. Hoy armas ese
# MAGIC camino para el pipeline de Wizard Bank, con lo que ya tienes: el bundle `lakeflow/` (Sesión 19) y GitHub.
# MAGIC
# MAGIC ```
# MAGIC  rama feature ─► pull request ─► CI: bundle validate ✅ ─► revisión + merge a main ─► CD: bundle deploy -t prod ─► prod
# MAGIC ```
# MAGIC
# MAGIC **Cómo se trabaja:** casi todo pasa en **GitHub** 🖱️ y en **tu terminal** 🖥️; las celdas ▶️ de este notebook comprueban en el
# MAGIC workspace lo que desplegaste.
# MAGIC
# MAGIC | Bloque | Pasos | Min |
# MAGIC |---|---|---|
# MAGIC | Introducción | Ambientes y promoción | 15 |
# MAGIC | **Bloque 1 · Ambientes con el bundle** | 0 tu copia del repo · 1 leer los targets · 2 desplegar dev y prod a mano | 45 |
# MAGIC | **Bloque 2 · CI/CD con GitHub Actions** | 3 token · 4 secretos · 5 el workflow · 6 proteger main | 50 |
# MAGIC | **Bloque 3 · Un cambio de punta a punta** | 7 rama → PR → merge → prod · 8 volver atrás | 50 |
# MAGIC | Cierre | Lo que cambia en una empresa · checklist | 20 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Introducción
# MAGIC ### Un ambiente es una copia aislada del mismo código
# MAGIC | | DEV | PROD |
# MAGIC |---|---|---|
# MAGIC | Para qué | Probar cambios sin miedo | Lo que usa el negocio (Genie, dashboards) |
# MAGIC | Quién despliega | Cada desarrollador, desde su máquina | Solo el pipeline de CI/CD |
# MAGIC | Nombre de los recursos | `pipeline_wizard_bank_lakehouse_dev` | `pipeline_wizard_bank_lakehouse_prod` |
# MAGIC | Dónde publica | `lending_dev` | `lending_prod` |
# MAGIC
# MAGIC Tu esquema `lending` (Sesiones 19 a 21) no es ninguno de los dos: lo armó a mano un pipeline de la UI. Desde hoy, dev y prod los crea el código.
# MAGIC
# MAGIC El **mismo** código va a los dos: lo que cambia son variables (esquema, nombres, quién corre). Eso es lo que declara `databricks.yml`.
# MAGIC
# MAGIC ### En Free Edition
# MAGIC | Aquí | En una empresa |
# MAGIC |---|---|
# MAGIC | Un workspace y un catálogo: los ambientes se separan por **esquema** | Un catálogo (`bronze_dev`, `bronze_prod`…) o un workspace por ambiente |
# MAGIC | DEV y PROD | DEV → **STG** (pruebas con datos de prueba) → PROD |
# MAGIC | El CI usa tu token personal | El CI usa un **service principal** con OAuth: si te vas de la empresa, el deploy sigue funcionando |

# COMMAND ----------

CATALOGO = "datawizard_dev"
ESQUEMA_DEV = "lending_dev"     # variable schema del target dev en databricks.yml
ESQUEMA_PROD = "lending_prod"   # variable schema del target prod
print("dev :", f"{CATALOGO}.{ESQUEMA_DEV}")
print("prod:", f"{CATALOGO}.{ESQUEMA_PROD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 1 · Ambientes con el bundle
# MAGIC ## Paso 0 — Tu copia del repo 🖱️ 🖥️
# MAGIC El CI/CD corre en **tu** repositorio, con **tus** credenciales.
# MAGIC 1. En GitHub, abre el repo del curso y haz **Fork** (arriba a la derecha) a tu cuenta.
# MAGIC 2. Clónalo y entra a la carpeta del bundle:
# MAGIC ```bash
# MAGIC git clone https://github.com/<tu-usuario>/datawizard_academy_dbfe.git
# MAGIC cd datawizard_academy_dbfe/lakeflow
# MAGIC ```
# MAGIC 3. En `databricks.yml`, cambia el `host` de los dos targets por la URL de **tu** workspace (la que ves en el navegador, hasta
# MAGIC    `.com`) y `profile` por el nombre de tu perfil del CLI (`databricks auth profiles`). Haz commit de ese cambio en `main`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Leer los targets 🗣️
# MAGIC Abre `lakeflow/databricks.yml`. Todo el CI/CD descansa en esta parte:
# MAGIC ```yaml
# MAGIC targets:
# MAGIC   dev:
# MAGIC     default: true              # el target si no pasas -t
# MAGIC     presets:
# MAGIC       pipelines_development: true   # pipeline en modo development
# MAGIC       trigger_pause_status: PAUSED  # el job no corre solo en dev
# MAGIC   prod:
# MAGIC     mode: production           # el bundle exige que nadie lo despliegue "a medias"
# MAGIC     variables:
# MAGIC       schema: lending_prod     # dev usa el default de la variable: lending_dev
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         job_lakehouse_wizard_bank_refresh:
# MAGIC           trigger:
# MAGIC             pause_status: PAUSED   # en el curso: que prod no corra solo con cada archivo
# MAGIC ```
# MAGIC
# MAGIC > ¿Por qué dev no usa `mode: development`? Ese modo antepone `dev_<usuario>_` a los esquemas (quedaría `dev_tu_usuario_lending_dev`).
# MAGIC > Aquí cada alumno tiene su propio workspace, así que tomamos de ese modo solo lo útil, explícito en `presets`. En una empresa,
# MAGIC > con varios desarrolladores en el mismo workspace, se deja `mode: development` para que cada uno tenga su dev aislado.
# MAGIC
# MAGIC | Archivo | Qué es | Igual en dev y prod |
# MAGIC |---|---|---|
# MAGIC | `transformations/*` | El código del pipeline | ✅ el mismo |
# MAGIC | `resources/schema_lakeflow.yml` | El esquema donde publica | ❌ cambia el nombre |
# MAGIC | `resources/pipeline_wizard_bank_lakehouse.yml` | El pipeline (serverless, `landing`, `politicas`) | ✅, con el esquema de cada target |
# MAGIC | `resources/job_lakehouse_wizard_bank_refresh.yml` | El job con *file arrival* | ✅, pausado en los dos |
# MAGIC
# MAGIC **Pregunta:** ¿por qué la configuración `politicas` del pipeline (Sesión 21) no está escrita a mano en el YAML?
# MAGIC <details><summary>Ver respuesta</summary>Porque cambia por ambiente: en dev las funciones de row filter y mask viven en el esquema de dev, en prod en el de prod. El YAML la arma con variables (<code>${var.catalog}.${resources.schemas.schema_lakeflow.name}</code>): el mismo código sirve para los dos. Lo que se escribe a mano en un ambiente es lo que se rompe al promover.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Desplegar dev y prod a mano 🖥️
# MAGIC Antes de automatizar, hazlo a mano una vez: así sabes qué va a hacer la máquina.
# MAGIC ```bash
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy   -t dev
# MAGIC databricks bundle validate -t prod
# MAGIC databricks bundle summary  -t prod      # qué crearía, sin crear nada
# MAGIC ```
# MAGIC Todavía **no** despliegues a prod: eso lo hará el CI en el paso 7. Corre la celda para ver qué hay en el workspace.

# COMMAND ----------

for esquema in [ESQUEMA_DEV, ESQUEMA_PROD]:
    existe = spark.sql(f"SHOW SCHEMAS IN {CATALOGO} LIKE '{esquema}'").count() > 0
    tablas = spark.sql(f"SHOW TABLES IN {CATALOGO}.{esquema}").count() if existe else 0
    print(f"{'✅' if existe else '⏳'} {CATALOGO}.{esquema}: {'existe' if existe else 'todavía no existe'} · {tablas} tablas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 2 · CI/CD con GitHub Actions
# MAGIC ## Paso 3 — Un token para la máquina 🖱️
# MAGIC GitHub necesita credenciales para hablar con tu workspace.
# MAGIC 1. Arriba a la derecha → tu usuario → **Settings** → **Developer** → **Access tokens** → **Generate new token**.
# MAGIC 2. Comentario: `github-actions-lakeflow`; vigencia: 90 días. Si el formulario pide **Scopes**, elige **all-apis**
# MAGIC    (el bundle usa workspace, pipelines, jobs y Unity Catalog). Con menos scopes el deploy falla con
# MAGIC    `403 Provided access token does not have required scopes: workspace`. **Copia el token ahora**: no se vuelve a mostrar.
# MAGIC 3. **Nunca** lo pegues en el código, en un notebook ni en un commit. Solo va al paso 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Secretos en GitHub 🖱️
# MAGIC En tu fork: **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.
# MAGIC El formulario pide un secreto a la vez, con dos campos: **Name** y **Secret**. Llénalo y pulsa **Add secret**;
# MAGIC después vuelve a **New repository secret** para el segundo:
# MAGIC
# MAGIC | Vez | Name (escríbelo tal cual) | Secret (el valor) |
# MAGIC |---|---|---|
# MAGIC | 1 | `DATABRICKS_HOST` | La URL de tu workspace, ej. `https://dbc-xxxx.cloud.databricks.com` (sin `/` al final ni lo que va después de `.com`) |
# MAGIC | 2 | `DATABRICKS_TOKEN` | El token del paso 3 (empieza con `dapi`) |
# MAGIC
# MAGIC Al terminar, la lista **Repository secrets** muestra los dos nombres. El valor ya no se puede ver, solo reemplazar:
# MAGIC si te equivocaste, entra al secreto y pulsa **Update**.
# MAGIC
# MAGIC Es la misma idea del secret scope de la Sesión 21: el valor lo guarda la plataforma y el código solo lo nombra
# MAGIC (`${{ secrets.DATABRICKS_TOKEN }}`). GitHub lo oculta en los logs con `***`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — El workflow 🖥️
# MAGIC El workflow ya está escrito en `lakeflow/ci/lakeflow-ci.yml`. GitHub solo ejecuta los que están en `.github/workflows/`, así que
# MAGIC lo copias ahí:
# MAGIC ```bash
# MAGIC cd ..                                         # raíz del repo
# MAGIC mkdir -p .github/workflows
# MAGIC cp lakeflow/ci/lakeflow-ci.yml .github/workflows/
# MAGIC git add .github/workflows/lakeflow-ci.yml
# MAGIC git commit -m "CI/CD del bundle lakeflow"
# MAGIC git push origin main
# MAGIC ```
# MAGIC 📋 Lo que hace:
{{ci/lakeflow-ci.yml}}
# MAGIC | Evento | Job | Qué corre |
# MAGIC |---|---|---|
# MAGIC | Pull request a `main` que toca `lakeflow/` | `validar` | `databricks bundle validate -t prod` |
# MAGIC | Push a `main` (el merge del PR) | `desplegar` | `validate` y `deploy -t prod` |
# MAGIC
# MAGIC Como acabas de hacer push a `main`, el job `desplegar` corre ahora. Míralo en la pestaña **Actions** de tu fork: es tu primer deploy
# MAGIC a prod hecho por una máquina.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Proteger main 🖱️
# MAGIC Si cualquiera puede hacer push directo a `main`, el PR es opcional y el CI no protege nada.
# MAGIC 1. En tu fork: **Settings** → **Branches** → **Add branch ruleset** (o *Add rule*), para la rama `main`.
# MAGIC 2. Marca **Require a pull request before merging**.
# MAGIC 3. Marca **Require status checks to pass** y agrega el check **`validar`** (aparece después de que corra una vez, en el paso 7).
# MAGIC 4. Guarda. Desde ahora `main` solo cambia con un PR cuyo `validar` esté en verde.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 3 · Un cambio de punta a punta
# MAGIC ## Paso 7 — Rama → PR → merge → prod 🖥️ 🖱️
# MAGIC El cambio: mejorar el comentario de `metricas_roi_campania_gld`, para que Genie entienda mejor qué es el retorno.
# MAGIC 1. Crea una rama y haz el cambio:
# MAGIC ```bash
# MAGIC git switch -c mejora-comentario-roi
# MAGIC # en lakeflow/transformations/13_gold_metricas.sql, cambia el COMMENT de metricas_roi_campania_gld, por ejemplo:
# MAGIC #   'KPI de retorno por campaña: USD colocado por los clientes captados por cada USD invertido. Grano: campaña. Para "mejor campaña" usar usd_colocado_por_usd_invertido'
# MAGIC git commit -am "ROI: comentario que dice cómo elegir la mejor campaña"
# MAGIC git push -u origin mejora-comentario-roi
# MAGIC ```
# MAGIC 2. En GitHub, abre el **Pull request** hacia `main`. En la pestaña **Checks** corre `validar`: espera el ✅.
# MAGIC 3. **Prueba que el CI te protege:** en la rama, rompe el YAML a propósito (por ejemplo borra los dos puntos de `serverless:` en
# MAGIC    `resources/pipeline_wizard_bank_lakehouse.yml`), haz push y mira cómo `validar` queda en ❌ y el botón de merge se bloquea.
# MAGIC    Arréglalo y vuelve a hacer push.
# MAGIC 4. Con `validar` en verde, **Merge pull request**. En **Actions** corre `desplegar`.
# MAGIC 5. Corre la celda: el pipeline de prod ya existe en el workspace.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for p in w.pipelines.list_pipelines(filter="name LIKE '%pipeline_wizard_bank_lakehouse%'"):
    ambiente = "dev" if p.name.startswith("[dev") else "prod"
    print(f"{ambiente:5} {p.name:55} {p.state} · id {p.pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC El deploy **crea o actualiza** el pipeline, pero no lo corre. Para llenar prod la primera vez: abre el pipeline de prod
# MAGIC (`pipeline_wizard_bank_lakehouse`, sin prefijo) → **Run pipeline**. Es una carga completa: tarda unos minutos.
# MAGIC Después, revisa que el comentario nuevo llegó a prod:

# COMMAND ----------

if spark.sql(f"SHOW TABLES IN {CATALOGO}.{ESQUEMA_PROD} LIKE 'metricas_roi_campania_gld'").count():
    print(spark.sql(f"""SELECT comment FROM {CATALOGO}.information_schema.tables
                        WHERE table_schema = '{ESQUEMA_PROD}' AND table_name = 'metricas_roi_campania_gld'""").first()[0])
else:
    print("⏳ Todavía no: corre el pipeline de prod una vez (Run pipeline)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — Volver atrás 🖥️
# MAGIC Si un cambio que llegó a prod resulta malo, no se arregla a mano en el workspace: se **revierte en Git** y el CI vuelve a desplegar.
# MAGIC ```bash
# MAGIC git switch main && git pull
# MAGIC git switch -c revertir-comentario-roi
# MAGIC git revert --no-edit HEAD        # crea un commit que deshace el merge (si fue merge commit: git revert -m 1 HEAD)
# MAGIC git push -u origin revertir-comentario-roi
# MAGIC ```
# MAGIC Y el mismo camino: PR → `validar` ✅ → merge → `desplegar`. El historial queda en Git: qué cambió, quién, cuándo y por qué se revirtió.
# MAGIC
# MAGIC **Pregunta:** alguien entra al workspace de prod y edita a mano un archivo del pipeline. ¿Qué pasa en el siguiente merge?
# MAGIC <details><summary>Ver respuesta</summary>El deploy lo pisa: el bundle deja el workspace igual a lo que dice Git. Por eso el cambio a mano se pierde, y está bien que se pierda: lo que no está en Git no está revisado. En producción, además, el workspace de prod se deja de solo lectura para las personas y solo el service principal del CI puede desplegar.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cierre — Lo que cambia en una empresa
# MAGIC | Aquí | En una empresa |
# MAGIC |---|---|
# MAGIC | `dev` y `prod` | `dev` → `stg` → `prod`, un target por ambiente en `databricks.yml` |
# MAGIC | Merge a `main` despliega a prod | Merge despliega a **stg**; prod se despliega con un **tag** (`v1.4.0`) o con aprobación manual (GitHub **Environments** con *required reviewers*) |
# MAGIC | Token personal en un secreto de GitHub | **Service principal** con OAuth (o federación OIDC de GitHub, sin secretos guardados) |
# MAGIC | Separación por esquema | Separación por catálogo o por workspace, con `GRANT` distintos en cada uno |
# MAGIC
# MAGIC ## Checklist
# MAGIC | Entregable | Listo si… |
# MAGIC |---|---|
# MAGIC | Fork con `databricks.yml` apuntando a tu workspace | `databricks bundle validate -t prod` pasa en tu máquina |
# MAGIC | Secretos `DATABRICKS_HOST` y `DATABRICKS_TOKEN` | El job `desplegar` corrió en verde |
# MAGIC | `main` protegida | No se puede hacer merge con `validar` en rojo |
# MAGIC | Un cambio de punta a punta | El comentario nuevo de `metricas_roi_campania_gld` está en prod |
# MAGIC
# MAGIC - [ ] Sé explicar qué cambia entre dev y prod y qué es igual
# MAGIC - [ ] Sé por qué prod solo lo despliega el CI y cómo se vuelve atrás un cambio
# MAGIC - [ ] Sé dónde va una credencial en el CI y por qué en una empresa es un service principal
# MAGIC
# MAGIC ## 🧹 Limpieza
# MAGIC `databricks bundle destroy -t prod` y `-t dev` borran el pipeline, el job y el esquema de cada ambiente con todas sus tablas.
# MAGIC Revoca el token del paso 3 (Settings → Developer → Access tokens) cuando ya no lo uses.
