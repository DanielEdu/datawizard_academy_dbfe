# Databricks notebook source

# MAGIC %md
# MAGIC # Sesión 20.2 · Genie paso a paso: un Genie Agent sobre el Gold de Wizard Bank, medido con un benchmark
# MAGIC **Data Wizard Academy · Databricks Track · Módulo 4**
# MAGIC `v1.0 · creada 2026-11-06 · actualizada 2026-11-06`
# MAGIC
# MAGIC **Continúa la Sesión 20.** Tu pipeline ya publica Gold (estrella, métricas, cartera y features) y creaste dos metric views.
# MAGIC Hoy le das ese Gold a **un consumidor más: el negocio, preguntando en español**. Construyes un **Genie Agent**, mides cuántas
# MAGIC preguntas responde bien, mejoras los **metadatos** que lee, lo mides otra vez y lo encuentras en **Genie One**, donde lo usa el negocio.
# MAGIC
# MAGIC **Cómo se trabaja:** los pasos 🖱️ son en la interfaz de Genie; los ▶️ se corren aquí; el 📋 reemplaza dos archivos del pipeline.
# MAGIC
# MAGIC | Bloque | Pasos | Min |
# MAGIC |---|---|---|
# MAGIC | Introducción | La familia Genie y qué le toca al Data Engineer | 15 |
# MAGIC | **Bloque 1 · El agente tal cual** | 0 punto de partida · 1 elegir las fuentes · 2 crear el agente · 3 banco de preguntas · 4 benchmark ANTES | 50 |
# MAGIC | **Bloque 2 · Mejorar y medir** | 5 auditar · 6 mejorar (6a metadatos en el pipeline, 6b conocimiento en el agente) · 7 benchmark DESPUÉS | 75 |
# MAGIC | **Bloque 3 · El negocio** | 8 Genie One | 20 |
# MAGIC | Cierre | Checklist | 10 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Introducción
# MAGIC ### Genie: la familia de experiencias de AI de Databricks
# MAGIC Desde 2026 **Genie** es la marca de todas las experiencias de AI de Databricks (absorbió *AI/BI Genie*, *Databricks One* y *Databricks Assistant*):
# MAGIC
# MAGIC | Pieza | Qué es | Hoy |
# MAGIC |---|---|---|
# MAGIC | **Genie Agent** (antes *Genie Space*) | Un agente de texto → SQL sobre un conjunto de tablas y metric views que **tú eliges**, con instrucciones, ejemplos y un benchmark. Se comparte y se puede publicar en Microsoft Teams o Slack | Lo construyes |
# MAGIC | **Genie One** (antes *Databricks One*) | La interfaz del usuario de negocio: chat sobre los datos gobernados, sin ver notebooks, pipelines ni clústeres | Lo usas como negocio |
# MAGIC | **Genie Code** (antes *Databricks Assistant*) | El agente que te ayuda a programar en notebooks, SQL y pipelines | Ya lo usaste en la Sesión 13 |
# MAGIC
# MAGIC **Por qué es trabajo del Data Engineer:** Genie no adivina el negocio. Responde bien si las tablas tienen buen **grano**, **comentarios**
# MAGIC que dicen qué significa cada columna y cuándo **no** usarla, **llaves**, y si las definiciones del KPI ya existen (las **metric views**).
# MAGIC Todo eso lo escribe el Data Engineer, y se mide como cualquier producto: con un **benchmark**.
# MAGIC
# MAGIC ```
# MAGIC  Gold (Sesión 20) ──► kpi_colocacion · kpi_funnel_app (metric views)          ──► Genie Agent ──► Genie One / Teams
# MAGIC                       metricas_originacion_mensual_gld · metricas_roi_campania_gld · cartera_creditos_gld
# MAGIC ```
# MAGIC
# MAGIC > **Costo:** Genie One, los Genie Agents y Genie Code comparten una franquicia mensual de DBUs gratis por usuario identificado
# MAGIC > (150 DBUs desde julio de 2026); lo que pase de ahí se cobra. Los *service principals* pagan desde el primer token.
# MAGIC > Verifica las condiciones vigentes antes de cada cohorte: cambian.

# COMMAND ----------

CATALOGO = "datawizard_dev"
ESQUEMA = "lending"


def t(nombre):
    return f"{CATALOGO}.{ESQUEMA}.{nombre}"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 1 · El agente tal cual
# MAGIC ## Paso 0 — Punto de partida ▶️
# MAGIC Deben existir las tablas Gold y las dos metric views de la Sesión 20.

# COMMAND ----------

FUENTES = ["kpi_colocacion", "kpi_funnel_app", "metricas_originacion_mensual_gld", "metricas_roi_campania_gld", "cartera_creditos_gld"]
for nombre in FUENTES:
    assert spark.catalog.tableExists(t(nombre)), f"Falta {t(nombre)}: termina la Sesión 20 (pipeline + paso 5 de metric views)"
    print(f"✅ {t(nombre)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Qué le das a Genie 🗣️
# MAGIC | Fuente | Tipo | Para qué preguntas |
# MAGIC |---|---|---|
# MAGIC | `kpi_colocacion` | Metric view | Colocación, ticket, tasa, por mes, año, país, producto o nivel de riesgo |
# MAGIC | `kpi_funnel_app` | Metric view | Vistas, clicks, CTR y conversión de la app |
# MAGIC | `metricas_originacion_mensual_gld` | Tabla de métricas | Funnel de ofertas: conversión, aprobación, comparar motores |
# MAGIC | `metricas_roi_campania_gld` | Tabla de métricas | Retorno de campañas |
# MAGIC | `cartera_creditos_gld` | Tabla ancha | Cartera, saldos, mora y cobranza |
# MAGIC
# MAGIC **Lo que NO le das:** Silver (`*_slv`), los hechos y dimensiones sueltos, ni las feature tables. Con menos fuentes, y ya modeladas
# MAGIC para el consumidor, Genie se equivoca menos. Si le das `fact_desembolsos_gld` **y** `kpi_colocacion`, tiene dos caminos para la
# MAGIC misma pregunta y puede elegir el que olvida filtrar los desembolsos fallidos.
# MAGIC
# MAGIC **Pregunta:** ¿por qué una metric view es mejor fuente para Genie que una tabla con el KPI ya calculado?
# MAGIC <details><summary>Ver respuesta</summary>Porque la definición del KPI viaja con ella. En una tabla, Genie ve una columna <code>ticket_promedio_usd</code> por mes y, para el ticket del año, puede promediarla (mal). En la metric view, <code>ticket_promedio_usd</code> es una medida: Genie escribe <code>MEASURE(ticket_promedio_usd)</code> y Databricks la recalcula como suma de montos sobre suma de desembolsos para el corte que se pida. El filtro "solo Completados" también va dentro de la medida: Genie no lo puede olvidar.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Crear el Genie Agent, tal cual 🖱️
# MAGIC Lo creas **sin descripción, instrucciones ni ejemplos**: así ves qué tan lejos llega con lo que hoy dicen tus tablas.
# MAGIC 1. Menú lateral → **Agents** → **Create Agent** → tarjeta **Genie Agent**. (Si tu workspace muestra **Genie** → **New**, es lo mismo).
# MAGIC 2. Nombre: `Wizard Bank · colocación y cartera · <tu nombre>`.
# MAGIC 3. **Data**: agrega las 5 fuentes del paso 1, de **tu** esquema.
# MAGIC 4. **SQL warehouse**: el serverless del curso.
# MAGIC 5. Pregúntale **"¿Cuál es el saldo de capital total de la cartera?"** y abre **Show code**. Anota qué columna sumó: la vas a
# MAGIC    necesitar en el paso 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — El banco de preguntas ▶️
# MAGIC Un **benchmark** es una lista de preguntas del negocio, cada una con el **SQL que da la respuesta correcta**. Genie responde cada
# MAGIC pregunta y compara su resultado con el de ese SQL.
# MAGIC
# MAGIC El banco tiene 12 preguntas de tres niveles. Las difíciles tienen una **trampa**: un camino que parece razonable y da otro número.
# MAGIC
# MAGIC | Nivel | Qué mide |
# MAGIC |---|---|
# MAGIC | Fácil | Una fuente, un filtro o una medida directa |
# MAGIC | Media | Un valor de negocio ("en mora"), la moneda, un ratio que no se promedia |
# MAGIC | Difícil | Ranking, elegir entre dos fuentes que parecen servir |
# MAGIC
# MAGIC Corre la celda: ejecuta las 12 respuestas correctas contra **tus** tablas y deja cada pregunta lista para copiar al benchmark.
# MAGIC
# MAGIC > **Regla:** el banco **mide**, no **enseña**. Nunca pegues estas preguntas ni su SQL como consultas de ejemplo del agente (paso 6b).
# MAGIC > Si lo haces, el agente se las aprende de memoria y el benchmark deja de decirte si entiende el negocio.

# COMMAND ----------

T = {n: t(n) for n in FUENTES}

BANCO = [
    (1, "Fácil", "¿Cuántos créditos hay por tramo de mora?",
     f"SELECT tramo_mora, COUNT(*) AS creditos FROM {T['cartera_creditos_gld']} GROUP BY tramo_mora",
     "—"),
    (2, "Media", "¿Cuánto se colocó en USD cada mes de 2025?",
     f"SELECT mes, MEASURE(monto_colocado_usd) AS monto_colocado_usd FROM {T['kpi_colocacion']} WHERE anio = 2025 GROUP BY mes",
     "metricas_originacion_mensual_gld también tiene mes y monto_colocado_usd, pero su mes es el de la OFERTA: el total casi cuadra y mes a mes no"),
    (3, "Media", "¿Cuál es el ticket promedio en USD por país?",
     f"SELECT pais, MEASURE(ticket_promedio_usd) AS ticket_promedio_usd FROM {T['kpi_colocacion']} GROUP BY pais",
     "AVG(monto_desembolsado) de la cartera está en moneda local: mezcla soles, pesos y bolivianos"),
    (4, "Fácil", "¿Qué ubicación en pantalla tiene el mejor CTR?",
     f"SELECT ubicacion_pantalla, MEASURE(ctr) AS ctr FROM {T['kpi_funnel_app']} GROUP BY ubicacion_pantalla ORDER BY ctr DESC LIMIT 1",
     "—"),
    (5, "Media", "¿Cuántos clientes tienen al menos un crédito en mora?",
     f"SELECT COUNT(DISTINCT id_cliente) AS clientes_en_mora FROM {T['cartera_creditos_gld']} WHERE dias_mora > 0",
     "COUNT(*) cuenta créditos, no clientes: uno de cada cuatro clientes tiene más de un crédito"),
    (6, "Media", "¿Cuál es el saldo de capital total de la cartera?",
     f"SELECT SUM(saldo_capital_usd) AS saldo_capital_usd FROM {T['cartera_creditos_gld']}",
     "SUM(saldo_capital) suma soles, pesos colombianos y bolivianos como si fueran lo mismo"),
    (7, "Difícil", "¿Qué campaña tuvo el mejor retorno por dólar invertido?",
     f"SELECT nombre_campania, usd_colocado_por_usd_invertido FROM {T['metricas_roi_campania_gld']} WHERE ranking_roi = 1",
     "La de mayor monto_colocado_usd no es la de mejor retorno"),
    (8, "Difícil", "¿Cuánto se colocó en USD en 2025 por nivel de riesgo del cliente?",
     f"SELECT nivel_riesgo, MEASURE(monto_colocado_usd) AS monto_colocado_usd FROM {T['kpi_colocacion']} WHERE anio = 2025 GROUP BY nivel_riesgo",
     "La cartera tiene nivel_riesgo_al_desembolso, pero sus montos están en moneda local"),
    (9, "Difícil", "¿Qué motor de asignación convierte mejor las ofertas en desembolsos?",
     f"""SELECT motor_asignacion, SUM(desembolsos) / SUM(ofertas) AS conversion
         FROM {T['metricas_originacion_mensual_gld']} GROUP BY motor_asignacion ORDER BY conversion DESC LIMIT 1""",
     "AVG(conversion_end_to_end) promedia ratios mensuales: pesa igual un mes chico que uno grande"),
    (10, "Media", "¿Cuál es la tasa de aprobación de solicitudes por país?",
     f"SELECT pais, SUM(solicitudes_aprobadas) / SUM(solicitudes) AS tasa_aprobacion FROM {T['metricas_originacion_mensual_gld']} GROUP BY pais",
     "Misma trampa: AVG(tasa_aprobacion)"),
    (11, "Difícil", "¿Qué porcentaje del saldo de capital está en mora de más de 30 días?",
     f"""SELECT 100 * SUM(CASE WHEN es_mora_30 = 1 THEN saldo_capital_usd ELSE 0 END) / SUM(saldo_capital_usd) AS pct_saldo_mora_30
         FROM {T['cartera_creditos_gld']}""",
     "Es porcentaje del SALDO en USD, no de la cantidad de créditos ni del saldo en moneda local"),
    (12, "Media", "¿Cuántos desembolsos completados hubo en el último mes con datos?",
     f"SELECT mes, MEASURE(desembolsos) AS desembolsos FROM {T['kpi_colocacion']} GROUP BY mes ORDER BY mes DESC LIMIT 1",
     "El 'mes' de metricas_originacion es el mes de la OFERTA, no del desembolso"),
]

filas = []
for n, nivel, pregunta, sql, trampa in BANCO:
    try:
        r = spark.sql(sql).collect()
        resp = " · ".join(", ".join(str(v) for v in row) for row in r[:5]) + (" …" if len(r) > 5 else "")
    except Exception as e:
        resp = f"❌ ERROR: {str(e)[:150]}"
    filas.append((n, nivel, pregunta, resp, trampa))
display(spark.createDataFrame(filas, "n INT, nivel STRING, pregunta STRING, respuesta_correcta STRING, trampa STRING"))

# COMMAND ----------

# Para el paso 4: cada pregunta con su SQL, lista para copiar al benchmark
for n, nivel, pregunta, sql, _ in BANCO:
    print(f"── {n} · {nivel}\nPregunta: {pregunta}\nSQL:\n{' '.join(sql.split())}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Benchmark ANTES 🖱️
# MAGIC 1. En tu agente → **Benchmark** (arriba, junto a *Chat* y *Monitor*) → **Add benchmark**. Por cada pregunta de la celda de arriba:
# MAGIC    pega la **pregunta** y, en la respuesta esperada, el **SQL**. (Con 12 toma unos minutos: repártanse las preguntas en parejas).
# MAGIC 2. **Run all benchmarks**.
# MAGIC 3. Abre el resultado de cada pregunta. Genie la marca como correcta, incorrecta o *needs review* (cuando el resultado se parece pero
# MAGIC    no es igual: otra columna, otro redondeo). Revisa las *needs review* tú mismo.
# MAGIC 4. Anota el puntaje en `PUNTAJE_ANTES`, en la celda de abajo.
# MAGIC
# MAGIC **Pregunta:** en el paso 2 preguntaste por el saldo total. ¿Qué columna sumó Genie?
# MAGIC <details><summary>Ver respuesta</summary>Lo más probable es <code>saldo_capital</code>: el nombre calza perfecto con la pregunta. Pero está en moneda local, y sumarla mezcla soles, pesos colombianos y bolivianos (un crédito colombiano pesa unas mil veces más que uno peruano). Nada en los metadatos le dice que exista <code>saldo_capital_usd</code> ni que la otra no se suma entre países: la columna no tiene comentario. Las metric views no tienen ese problema porque su medida ya es en USD.</details>

# COMMAND ----------

PUNTAJE_ANTES = None   # ← cuántas de las 12 respondió bien el benchmark

# Alternativa sin la pestaña Benchmark: pega el SQL que generó Genie (Show code) y la celda lo compara con la respuesta correcta
SQL_GENIE = {
    6: """ -- pega aquí el SQL de Genie para la pregunta 6 """,
    9: """ -- pega aquí el SQL de Genie para la pregunta 9 """,
}


def firma(df):
    """Resultado como conjunto de filas, sin importar nombres ni orden de columnas; números redondeados a 2 decimales."""
    def norm(x):
        return str(round(float(x), 2)) if isinstance(x, (int, float)) or hasattr(x, "as_tuple") else str(x)
    return {tuple(sorted(norm(x) for x in r)) for r in df.collect()}


CORRECTO = {n: sql for n, _, _, sql, _ in BANCO}
for n, sql in SQL_GENIE.items():
    if "pega aquí" in sql:
        print(f"Pregunta {n}: ⏳ sin SQL todavía")
        continue
    try:
        ok = firma(spark.sql(sql)) == firma(spark.sql(CORRECTO[n]))
        print(f"Pregunta {n}: {'✅ coincide' if ok else '❌ distinto: mira qué tabla, columna, filtro o agregación eligió Genie'}")
    except Exception as e:
        print(f"Pregunta {n}: ❌ el SQL falla: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 2 · Mejorar y medir
# MAGIC ## Paso 5 — Auditar lo que ve Genie ▶️
# MAGIC Genie no lee tu pipeline ni este notebook. Lee lo que Unity Catalog dice de cada fuente: **nombre, comentario de la tabla, nombre y
# MAGIC comentario de cada columna, llaves y valores**. La celda revisa eso en las 3 tablas que le diste (las metric views traen sus
# MAGIC comentarios en el YAML desde la Sesión 20):
# MAGIC
# MAGIC | Revisión | Por qué le importa a Genie |
# MAGIC |---|---|
# MAGIC | Comentario de tabla que diga para qué sirve y para qué no (≥ 150 caracteres) | Con dos fuentes que parecen servir, elige por el comentario |
# MAGIC | Todas las columnas comentadas | Sin comentario, `saldo_capital` se adivina por el nombre |
# MAGIC | Llave primaria declarada | Le dice el grano: una fila por qué |
# MAGIC | Columnas de texto con pocos valores | Candidatas a *entity matching*: que "peru" encuentre `Perú` y "al día" encuentre `Al dia` |
# MAGIC | Columnas con el mismo nombre en varias fuentes | `monto_colocado_usd` en dos tablas con significado distinto: solo el comentario las distingue |
# MAGIC
# MAGIC Las tres primeras dan el **puntaje de preparación** (máximo 9: 3 por tabla).

# COMMAND ----------

TABLAS = ["metricas_originacion_mensual_gld", "metricas_roi_campania_gld", "cartera_creditos_gld"]


def tiene_pk(nombre):
    return spark.sql(f"""SELECT 1 FROM {CATALOGO}.information_schema.table_constraints
                         WHERE table_schema = '{ESQUEMA}' AND table_name = '{nombre}' AND constraint_type = 'PRIMARY KEY'""").count() > 0


def auditar():
    resumen, columnas = [], {}
    for nombre in TABLAS:
        com_tabla = spark.sql(f"SELECT comment FROM {CATALOGO}.information_schema.tables "
                              f"WHERE table_schema = '{ESQUEMA}' AND table_name = '{nombre}'").first()[0] or ""
        cols = spark.sql(f"SELECT column_name, data_type, comment FROM {CATALOGO}.information_schema.columns "
                         f"WHERE table_schema = '{ESQUEMA}' AND table_name = '{nombre}' ORDER BY ordinal_position").collect()
        columnas[nombre] = {c.column_name for c in cols}
        sin_comentario = [c.column_name for c in cols if not c.comment]
        categoricas = []
        for c in cols:
            if c.data_type == "STRING":
                valores = [r[0] for r in spark.table(t(nombre)).select(c.column_name).distinct().limit(21).collect()]
                if len(valores) <= 20:
                    categoricas.append(f"{c.column_name}: {', '.join(sorted(str(v) for v in valores))}")
        checks = [len(com_tabla) >= 150, not sin_comentario, tiene_pk(nombre)]
        resumen.append((nombre, f"{'✅' if checks[0] else '❌'} {len(com_tabla)} caracteres (mín. 150)",
                        f"{'✅' if checks[1] else '❌'} {len(cols) - len(sin_comentario)}/{len(cols)}",
                        "✅" if checks[2] else "❌", " | ".join(categoricas) or "—", sum(checks)))
    repetidas = {}
    for nombre, cols in columnas.items():
        for c in cols:
            repetidas.setdefault(c, []).append(nombre)
    repetidas = {c: ts for c, ts in repetidas.items() if len(ts) > 1}
    df = spark.createDataFrame(resumen, "tabla STRING, comentario_tabla STRING, columnas_comentadas STRING, pk STRING, "
                                        "valores_para_entity_matching STRING, puntaje INT")
    return df, repetidas


auditoria_antes, repetidas = auditar()
display(auditoria_antes)
print(f"Puntaje de preparación ANTES: {auditoria_antes.groupBy().sum('puntaje').first()[0]} / 9")
print("Columnas con el mismo nombre en varias tablas:", dict(sorted(repetidas.items())))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** las feature tables de la Sesión 20 sí tienen comentario en cada columna y llave primaria. ¿Por qué estas tablas no?
# MAGIC <details><summary>Ver respuesta</summary>Porque a las feature tables les declaraste <code>schema=</code> con comentario por columna: para Data Science la llave era obligatoria. Para Genie es igual de necesario, solo que nadie te lo exigió. Lo que el Data Engineer no escribe en Unity Catalog, el agente lo adivina.</details>
# MAGIC
# MAGIC **Fíjate en lo que la auditoría no encuentra:** datos personales. Gold no trae nombres, documentos ni emails desde la Sesión 20. El
# MAGIC lugar para quitar la PII es **Gold**, no el agente: si la tabla no la tiene, ningún consumidor la puede filtrar por error.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Mejorar
# MAGIC Dos capas, en este orden: primero lo que vale para **cualquier** consumidor (Unity Catalog); después lo que es solo de **este** agente.
# MAGIC
# MAGIC ### 6a · Metadatos en Unity Catalog, desde el pipeline 📋
# MAGIC Las tablas Gold le pertenecen al pipeline: los comentarios y las llaves se escriben **en su código**, no con un `ALTER` a mano. Si lo
# MAGIC haces a mano, el código y la tabla dejan de coincidir: el próximo *Full refresh* lo borra.
# MAGIC
# MAGIC En `lakeflow/mejoras_genie/` están las versiones mejoradas. La lógica es la misma de la Sesión 20; cambia esto:
# MAGIC
# MAGIC | Archivo | Comentario de tabla nuevo | Llave | Comentarios de columna que más pesan |
# MAGIC |---|---|---|---|
# MAGIC | `13_gold_metricas.sql` | Originación: "NO la uses para el monto colocado de un mes"; ROI: "mejor campaña = retorno, no monto" | `(mes, pais, motor_asignacion)`, `(id_campania)`… | `mes` (de la oferta), `monto_colocado_usd`, ratios "no promediar" |
# MAGIC | `14_gold_cartera.sql` | Define "en mora" y prohíbe sumar montos locales entre países | `(numero_credito)` | `saldo_capital` ("no sumar entre países"), `dias_mora`, `es_mora_30` |
# MAGIC | `kpi_colocacion.sql` | — | — | **Sinónimos**: "cuánto se prestó" = `monto_colocado_usd`, "calificación" = `nivel_riesgo` |
# MAGIC
# MAGIC **Fíjate en qué dicen los comentarios:** no repiten el nombre de la columna. Dicen **qué significa**, **en qué unidad** y **cuándo no
# MAGIC usarla**. Eso último es lo que desarma las trampas.
# MAGIC
# MAGIC 1. En tu pipeline, abre `transformations/13_gold_metricas.sql` y reemplaza todo su contenido por el del bloque de abajo. Igual con `14_gold_cartera.sql`.
# MAGIC 2. **Run pipeline** (no hace falta *Full refresh*: las materialized views se recalculan solas cuando cambia su definición).
# MAGIC 3. Corre la celda de `kpi_colocacion` mejorada y la auditoría otra vez.
# MAGIC
# MAGIC 📋 Reemplaza **`transformations/13_gold_metricas.sql`**
{{mejoras_genie/13_gold_metricas.sql}}
# MAGIC 📋 Reemplaza **`transformations/14_gold_cartera.sql`**
{{mejoras_genie/14_gold_cartera.sql}}
# MAGIC > **Plan B si el update falla por las llaves** (un mensaje sobre `CONSTRAINT` o `PRIMARY KEY`): borra las líneas `CONSTRAINT …`
# MAGIC > **y la coma de la línea anterior**. Los comentarios, que son lo que más pesa para Genie, se quedan.

# COMMAND ----------

{{ejecutar:mejoras_genie/kpi_colocacion.sql}}

# COMMAND ----------

auditoria_despues, _ = auditar()
display(auditoria_despues)
print(f"Puntaje de preparación DESPUÉS: {auditoria_despues.groupBy().sum('puntaje').first()[0]} / 9")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b · Conocimiento en el Genie Agent 🖱️
# MAGIC Lo que no es un dato de la tabla sino del **uso** que le da este agente va en su configuración. Aplícalo en este orden (de lo más
# MAGIC estructurado a lo menos), en el panel **Configure** (arriba a la derecha): pestañas **About · Sources · Instructions · Examples**.
# MAGIC
# MAGIC **1. Descripción del agente** (*About* → lápiz de *Description*)
# MAGIC ```
# MAGIC Responde preguntas sobre el negocio de crédito digital de Wizard Bank (datos sintéticos) en Perú, Colombia y Bolivia:
# MAGIC colocación de créditos en USD, funnel de ofertas, retorno de campañas, funnel de la app y estado de la cartera y la mora.
# MAGIC No tiene datos personales de los clientes ni información de pagos individuales.
# MAGIC ```
# MAGIC
# MAGIC **2. Columnas** (*Sources* → clic en la tabla → lápiz de la columna: ocultar, sinónimos y entity matching)
# MAGIC
# MAGIC | Tabla | Columna | Acción |
# MAGIC |---|---|---|
# MAGIC | `cartera_creditos_gld` | `id_pais` | **Ocultar**: repite `pais` en código y abre la puerta a filtrar por número |
# MAGIC | `cartera_creditos_gld` | `pais`, `tramo_mora`, `nivel_riesgo_al_desembolso`, `resultado_ultima_gestion` | **Entity matching** |
# MAGIC | `metricas_originacion_mensual_gld` | `pais`, `motor_asignacion` | **Entity matching** |
# MAGIC | `metricas_roi_campania_gld` | `nombre_campania`, `tipo_campania` | **Entity matching** |
# MAGIC | `cartera_creditos_gld` | `saldo_capital_usd` | Sinónimos: *saldo, deuda, capital pendiente* |
# MAGIC | `cartera_creditos_gld` | `dias_mora` | Sinónimos: *atraso, días de atraso* |
# MAGIC
# MAGIC Si la columna ya muestra la etiqueta **Entity matching**, ya está activo (Genie lo prende solo en columnas de texto con pocos valores).
# MAGIC
# MAGIC **3. Joins: ninguno.** Cada fuente responde sola, y las metric views ya traen sus joins adentro. Un join entre `cartera_creditos_gld`
# MAGIC y `kpi_colocacion` invitaría a mezclar el grano "crédito hoy" con "desembolso en su mes".
# MAGIC
# MAGIC **4. Medida y filtro** (*Examples* → **Add** → **Measure** / **Filter**; son las *SQL expressions* reutilizables). Se crean sobre una
# MAGIC tabla y cada columna va calificada con el nombre de la tabla:
# MAGIC
# MAGIC | Tipo | Tabla | Nombre | Código |
# MAGIC |---|---|---|---|
# MAGIC | Measure | `cartera_creditos_gld` | Saldo en mora > 30 días (USD) | `SUM(CASE WHEN cartera_creditos_gld.es_mora_30 = 1 THEN cartera_creditos_gld.saldo_capital_usd ELSE 0 END)` |
# MAGIC | Filter | `cartera_creditos_gld` | Crédito en mora | `cartera_creditos_gld.dias_mora > 0` |
# MAGIC
# MAGIC **5. Una consulta de ejemplo** (*Examples* → **Add** → **Example query**), que enseña un patrón sin ser una pregunta del banco:
# MAGIC - Pregunta: *¿Cuánto saldo en USD hay por tramo de mora en cada país?*
# MAGIC ```sql
# MAGIC SELECT pais, tramo_mora, SUM(saldo_capital_usd) AS saldo_capital_usd
# MAGIC FROM datawizard_dev.lending.cartera_creditos_gld
# MAGIC GROUP BY pais, tramo_mora
# MAGIC ORDER BY pais, tramo_mora
# MAGIC ```
# MAGIC
# MAGIC **6. Instrucciones generales** (*Instructions* → *General Instructions*; lo último, y corto: solo lo que no cabe en lo anterior)
# MAGIC ```
# MAGIC - Los datos son sintéticos. En el texto de la respuesta muestra los montos en USD con separador de miles, pero en el SQL no los redondees.
# MAGIC - Si la pregunta no dice el año, usa el último año con datos y dilo en la respuesta.
# MAGIC - Si te preguntan algo que no está en las fuentes (por ejemplo, datos personales de un cliente o ingresos del banco), dilo en vez de estimarlo.
# MAGIC ```
# MAGIC
# MAGIC **Por qué en este orden:** una instrucción como "para saldos usa saldo_capital_usd" funciona, pero solo en este agente y solo mientras
# MAGIC nadie la borre. El comentario de la columna funciona en este agente, en Genie One, en Genie Code, en un dashboard y para la siguiente
# MAGIC persona que abra la tabla.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Benchmark DESPUÉS ▶️
# MAGIC 1. **Benchmark** → **Run all benchmarks**. No cambies ninguna pregunta ni ningún SQL: si cambias el examen, ya no comparas.
# MAGIC 2. Anota el puntaje en `PUNTAJE_DESPUES` y corre la celda.
# MAGIC 3. Revisa pregunta por pregunta: ¿qué mejora arregló cada una? ¿Alguna que antes estaba bien ahora está mal? Eso es una **regresión**.

# COMMAND ----------

PUNTAJE_DESPUES = None   # ← cuántas de las 12 respondió bien el benchmark

if PUNTAJE_ANTES is None or PUNTAJE_DESPUES is None:
    print("⏳ Anota los dos puntajes: PUNTAJE_ANTES (paso 4) y PUNTAJE_DESPUES (aquí)")
else:
    print(f"Benchmark: {PUNTAJE_ANTES}/12 → {PUNTAJE_DESPUES}/12  ({PUNTAJE_DESPUES - PUNTAJE_ANTES:+d})")
for etiqueta, nombre in [("ANTES", "auditoria_antes"), ("DESPUÉS", "auditoria_despues")]:
    if nombre in globals():
        print(f"Preparación de tablas {etiqueta}: {globals()[nombre].groupBy().sum('puntaje').first()[0]}/9")

# COMMAND ----------

# MAGIC %md
# MAGIC **Antes de celebrar o lamentar, abre cada ❌ y cada ⚠️.** El benchmark compara **resultados**, no ideas:
# MAGIC
# MAGIC | Lo que ves | Qué puede estar pasando | Qué hacer |
# MAGIC |---|---|---|
# MAGIC | ❌ con el SQL correcto | Genie redondeó (`ROUND`) o cambió el tipo (`CAST`) porque una instrucción de presentación lo empujó | Corregir la **instrucción**, no la tabla: "en el SQL no redondees" |
# MAGIC | ⚠️ *needs review* | Trajo una columna de más (el porcentaje y el total) o los nombres son otros | Si el número es correcto, márcala a mano como correcta |
# MAGIC | ❌ que antes era ✅ | Regresión: un comentario o una instrucción nueva lo llevó a otra fuente | Leer **Show code** y ver qué fuente eligió y por qué |
# MAGIC | ✅ por casualidad | La respuesta coincide aunque el camino sea malo (ej. un solo país con datos) | Probar la misma pregunta con otro filtro |
# MAGIC
# MAGIC Dos lecciones: (1) el SQL esperado no redondea ni formatea, y (2) una instrucción de presentación puede romper el SQL. Por eso las
# MAGIC instrucciones van al final y cortas.
# MAGIC
# MAGIC **Prueba una regresión a propósito:** borra la medida *Saldo en mora > 30 días (USD)* del paso 6b y corre solo la pregunta 11. Si cae,
# MAGIC ya sabes qué la sostenía. Vuelve a ponerla.
# MAGIC
# MAGIC **Pregunta:** ¿por qué no basta con agregar las 12 preguntas del banco como consultas de ejemplo? Subiría a 12 de 12.
# MAGIC <details><summary>Ver respuesta</summary>Porque el puntaje dejaría de medir algo. El agente copiaría el SQL de memoria y la próxima pregunta parecida, pero distinta ("¿cuál es el saldo de la cartera colombiana en mora?"), volvería a caer en la trampa. Es como entrenar un modelo con el set de prueba. Las mejoras que valen son las que arreglan una <b>familia</b> de preguntas: un comentario, una medida, un sinónimo. En producción el banco tiene 30 o más preguntas y se le agregan las que el negocio hace y Genie falla.</details>
# MAGIC
# MAGIC **Un agente que el negocio usa es un producto.** Igual que el pipeline tiene expectations y el código tiene pruebas, el agente tiene un
# MAGIC benchmark, y cada cambio en las tablas o en el agente se mide con él antes de publicarse.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 3 · El negocio
# MAGIC ## Paso 8 — Genie One: la vista del usuario de negocio 🖱️
# MAGIC El analista de cartera no entra a notebooks ni a pipelines. Entra a **Genie One**.
# MAGIC 1. Comparte tu agente con un compañero (**Share** → permiso *Can run*). Para responder, esa persona también necesita `SELECT` sobre tus
# MAGIC    tablas y metric views (los permisos se ven a fondo en la Sesión 21).
# MAGIC 2. Abre **Genie One** desde el selector de apps del workspace (arriba a la derecha).
# MAGIC 3. Busca tu agente `Wizard Bank · colocación y cartera · <tu nombre>` y hazle una pregunta que **no** esté en el banco
# MAGIC    (ej.: *"¿Cómo evolucionó la colocación en Colombia mes a mes en 2025?"*).
# MAGIC 4. Compara con la vista de autor: ¿qué ve el usuario de negocio y qué no? (no ve las instrucciones, ni el benchmark, ni el pipeline).
# MAGIC
# MAGIC > Si tu workspace no muestra Genie One (algunas ediciones lo habilitan después), haz el paso 3 en el chat del agente con la cuenta
# MAGIC > del compañero: la experiencia de consulta es la misma.
# MAGIC
# MAGIC **Pregunta:** alguien de Finanzas pregunta en Genie One "¿cuál es el ingreso promedio de los clientes en mora?". ¿Qué debería pasar?
# MAGIC <details><summary>Ver respuesta</summary>Que Genie diga que no tiene ese dato, en vez de inventarlo (por eso está en las instrucciones del paso 6b). El ingreso está en <code>dim_cliente_gld</code> y en las feature tables, que no le diste al agente. Si el negocio lo pide seguido, es una señal para el Data Engineer: llevar el ingreso (o un rango de ingreso, sin exponer el dato exacto) a una tabla Gold, con su comentario, agregarla al agente y sumar la pregunta al banco. Los Genie Agents se pueden publicar en Teams o Slack: el feedback del negocio se vuelve backlog del Data Engineer.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cierre — Checklist
# MAGIC | Entregable | Listo si… |
# MAGIC |---|---|
# MAGIC | Genie Agent `Wizard Bank · colocación y cartera · <tu nombre>` | 5 fuentes; descripción, columnas, medida y filtro, 1 consulta de ejemplo, instrucciones cortas |
# MAGIC | Gold con metadatos (`mejoras_genie/` en el pipeline) | Auditoría en 9 de 9: comentarios de tabla y columna, llaves |
# MAGIC | `kpi_colocacion` con sinónimos | La celda del paso 6a corrió sin error |
# MAGIC | Benchmark | Las 12 preguntas, corridas antes y después, con los dos puntajes anotados |
# MAGIC
# MAGIC - [ ] Sé diferenciar Genie Agent, Genie One y Genie Code
# MAGIC - [ ] Sé por qué una metric view es mejor fuente para Genie que una tabla con el KPI calculado
# MAGIC - [ ] Sé qué hace un Data Engineer para que Genie responda bien: grano, comentarios que dicen cuándo NO usar una columna, llaves, y medirlo con un benchmark
# MAGIC - [ ] Sé por qué el banco de preguntas no se copia como consultas de ejemplo
# MAGIC - [ ] Encontré mi agente en Genie One
# MAGIC
# MAGIC ## 🧹 Limpieza
# MAGIC ⚠️ **No borres el pipeline**: la **Sesión 21** (Unity Catalog: permisos, linaje y seguridad) trabaja sobre estas tablas. Al cerrar el módulo:
# MAGIC 1. Tu Genie Agent → ⋮ → **Delete**.
# MAGIC 2. Las metric views no son del pipeline: `DROP VIEW` de `kpi_colocacion` y `kpi_funnel_app`.
# MAGIC 3. **Jobs & Pipelines** → `pipeline_wizard_bank_lakehouse` → ⋮ → **Delete**: se van todas sus tablas (Bronze, Silver, Gold y features).
# MAGIC 4. Los archivos de landing **no** se tocan: son la fuente de todo el curso.
