# Databricks notebook source

# MAGIC %md
# MAGIC # Sesión 22 · IA sobre el Lakehouse paso a paso: modelos, AI Functions, AI Search, Genie y el puente a Fabric
# MAGIC **Data Wizard Academy · Databricks Track · Cierre del curso**
# MAGIC `v1.0 · creada 2026-11-13 · actualizada 2026-11-13`
# MAGIC
# MAGIC **Última sesión.** Tu Lakehouse de Wizard Bank ya tiene Bronze, Silver, Gold, métricas, features, un Genie Agent, gobierno y CI/CD.
# MAGIC Hoy pruebas los servicios de IA que trae **tu Free Edition** sobre esos datos, y ves cómo se conecta todo con Microsoft Fabric.
# MAGIC Todo se prueba en 90 minutos: cada paso es una celda que corres y ves responder.
# MAGIC
# MAGIC | Servicio de Databricks | Qué hace | Lo pruebas en |
# MAGIC |---|---|---|
# MAGIC | **Foundation Model APIs** (dentro de Model Serving) | Modelos de lenguaje ya desplegados, pago por token | Bloque 1 |
# MAGIC | **AI Functions** (`ai_classify`, `ai_extract`, `ai_mask`, `ai_query`, `ai_forecast`…) | IA dentro de SQL, fila por fila | Bloque 2 |
# MAGIC | **MLflow Tracing** | Ver qué le mandaste al modelo y qué te devolvió | Bloque 3 |
# MAGIC | **Databricks AI Search** (antes Vector Search) | Buscar por significado y armar un RAG | Bloque 4 |
# MAGIC | **Genie** (Agent, One, Code) | Preguntar a los datos en español | Bloque 5 |
# MAGIC
# MAGIC **Cómo se trabaja:** los pasos ▶️ se corren aquí y los 🖱️ son en la interfaz.
# MAGIC
# MAGIC | Bloque | Pasos | Min |
# MAGIC |---|---|---|
# MAGIC | Introducción | El mapa de servicios de IA y qué hay en Free Edition | 8 |
# MAGIC | **Bloque 1 · Modelos listos** | 0 punto de partida · 1 qué modelos tienes · 2 Playground · 3 tu primer `ai_query` | 12 |
# MAGIC | **Bloque 2 · AI Functions** | 4 los mensajes · 5 lanzar AI Search · 6 enriquecer · 7 el tablero · 8 anonimizar · 9 triage en JSON · 10 pronóstico · 11 laboratorio libre | 30 |
# MAGIC | **Bloque 3 · Python y trazas** | 12 instalar · 13 llamar al modelo y ver la traza | 10 |
# MAGIC | **Bloque 4 · AI Search y RAG** | 14 el índice · 15 buscar por significado · 16 un RAG en diez líneas | 15 |
# MAGIC | **Bloque 5 · Genie y Fabric** | 17 Genie Agent · 18 Genie Code · 19 dónde vive el dato | 10 |
# MAGIC | Cierre | Checklist, limpieza y el proyecto final | 5 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Introducción
# MAGIC ### Cuatro marcas, una plataforma
# MAGIC En 2026 Databricks ordenó toda su IA en cuatro marcas. "Mosaic AI" ya no existe como nombre.
# MAGIC
# MAGIC | Marca | Qué agrupa | Ejemplos |
# MAGIC |---|---|---|
# MAGIC | **Genie** | Las experiencias de IA para personas | Genie Agent (antes Genie Space), Genie One (antes Databricks One), Genie Code (antes Databricks Assistant) |
# MAGIC | **Agent Bricks** | La plataforma para construir agentes | Knowledge Assistant, Supervisor Agent, agentes propios con MLflow |
# MAGIC | **Unity Catalog** | El gobierno de datos **y** de IA | Unity Gateway (antes AI Gateway): presupuesto, ruteo y guardrails del tráfico a modelos |
# MAGIC | **Lakeflow** | La ingeniería de datos que alimenta todo lo anterior | Lo que construiste en las sesiones 19 a 21 |
# MAGIC
# MAGIC Y debajo, los servicios técnicos: **Model Serving** (con las Foundation Model APIs), **AI Functions**, **Databricks AI Search**,
# MAGIC **MLflow** y **Lakebase Search**.
# MAGIC
# MAGIC ### Qué trae tu Free Edition
# MAGIC | ✅ Sí | ❌ No (o muy limitado) |
# MAGIC |---|---|
# MAGIC | Foundation Model APIs (Llama, Qwen, GPT-OSS, Gemma y modelos de embeddings) | Endpoints con GPU o provisioned throughput |
# MAGIC | Todas las AI Functions | Knowledge Assistant y la mayoría de Agent Bricks |
# MAGIC | **Un** endpoint de AI Search con una unidad | Más de un endpoint de AI Search, índices Direct Access |
# MAGIC | Genie Agent, Genie Code y MLflow | Uso comercial |
# MAGIC
# MAGIC > Los límites cambian seguido. La fuente es la página *Free Edition limitations* de la documentación, no esta tabla.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 1 · Modelos listos para usar
# MAGIC ## Paso 0 — Punto de partida ▶️
# MAGIC Usas el mismo esquema de las sesiones 19 a 21. Hoy creas dos tablas nuevas ahí: `mensajes_clientes` y `mensajes_clientes_ia`.

# COMMAND ----------

CATALOGO = "datawizard_dev"
ESQUEMA = "lending"
E = f"{CATALOGO}.{ESQUEMA}"
ENDPOINT_BUSQUEDA = "dw_ai_search"   # Free Edition permite un solo endpoint de AI Search: si ya tienes uno, la guía lo reutiliza

for tabla in ["metricas_colocacion_mensual_gld", "cartera_creditos_gld", "kpi_colocacion"]:
    existe = spark.catalog.tableExists(f"{E}.{tabla}")
    print(f"{'✅' if existe else '❌'} {E}.{tabla}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — ¿Qué modelos tienes? ▶️
# MAGIC Las **Foundation Model APIs** son modelos que Databricks ya desplegó por ti: no instalas nada y pagas por token. Cada uno es un
# MAGIC *serving endpoint* con nombre `databricks-…`. Los `chat` responden texto; los `embeddings` convierten texto en un vector de números
# MAGIC (los usas en el Bloque 4).

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()   # dentro de un notebook se autentica solo, con tu usuario
modelos = [(e.name, e.task) for e in w.serving_endpoints.list() if e.name.startswith("databricks-")]
display(spark.createDataFrame(modelos, "endpoint STRING, tipo STRING").orderBy("tipo", "endpoint"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — El Playground 🖱️
# MAGIC Antes de escribir código, compara modelos a mano.
# MAGIC 1. Menú izquierdo → **Playground**. Si no lo ves, sigue al paso 3: la experiencia es la misma desde SQL.
# MAGIC 2. Elige `databricks-meta-llama-3-3-70b-instruct`. Con **+** agrega un segundo modelo, por ejemplo `databricks-gpt-oss-20b`.
# MAGIC 3. Pregunta a los dos: *"Explica en dos frases qué es la mora de un crédito, para un cliente que nunca pidió un préstamo."*
# MAGIC 4. Compara la respuesta, el tiempo y el largo. **No hay "el mejor modelo"**: hay el que alcanza para tu tarea al menor costo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Tu primer `ai_query` ▶️
# MAGIC `ai_query()` llama a cualquier endpoint desde SQL. Arriba del notebook aparecieron dos controles (*widgets*): **elige un modelo,
# MAGIC cambia la pregunta y vuelve a correr la celda de abajo.** El modelo y la pregunta viajan como parámetros (`:modelo`, `:prompt`),
# MAGIC no pegados en el texto del SQL.

# COMMAND ----------

dbutils.widgets.dropdown("modelo", "databricks-meta-llama-3-3-70b-instruct",
                         ["databricks-meta-llama-3-3-70b-instruct", "databricks-meta-llama-3-1-8b-instruct",
                          "databricks-gpt-oss-20b", "databricks-gpt-oss-120b", "databricks-qwen3-next-80b-a3b-instruct",
                          "databricks-gemma-3-12b"], "1. Modelo")
dbutils.widgets.text("prompt", "Explica en dos frases qué es un lakehouse a alguien de finanzas", "2. Pregunta")

# COMMAND ----------

modelo, prompt = dbutils.widgets.get("modelo"), dbutils.widgets.get("prompt")
display(spark.sql("SELECT :modelo AS modelo, ai_query(:modelo, :prompt) AS respuesta", args={"modelo": modelo, "prompt": prompt}))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** cambia a `databricks-meta-llama-3-1-8b-instruct` con la misma pregunta. ¿Qué ganas y qué pierdes con un modelo más chico?
# MAGIC <details><summary>Ver respuesta</summary>Ganas velocidad y costo: tiene unos 8 mil millones de parámetros contra 70 mil millones. Pierdes calidad en tareas que piden razonar o redactar con cuidado. Para clasificar 10 millones de mensajes en tres categorías, el chico suele alcanzar y la factura es otra.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 2 · AI Functions: IA dentro de SQL
# MAGIC ## Paso 4 — Los mensajes de los clientes ▶️
# MAGIC Todo lo que construiste en el curso es dato **estructurado**: montos, fechas, estados. Pero el banco también recibe texto libre por
# MAGIC la app, WhatsApp, correo y call center. Esta tabla trae 30 mensajes inventados, con algunos datos personales a propósito.
# MAGIC Lleva *Change Data Feed* activado porque AI Search lo necesita para indexarla (Bloque 4).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {E}.mensajes_clientes (
  id_mensaje  INT       COMMENT 'Llave del mensaje',
  fecha       DATE      COMMENT 'Fecha en que llegó el mensaje',
  canal       STRING    COMMENT 'Por dónde llegó: app, whatsapp, correo o call center',
  pais        STRING    COMMENT 'País del cliente: Perú, Colombia o Bolivia',
  texto       STRING    COMMENT 'Mensaje tal como lo escribió el cliente. Puede traer datos personales: no compartir sin ai_mask'
)
COMMENT 'Sesión 22: mensajes sintéticos de clientes de Wizard Bank para probar AI Functions y AI Search. Datos inventados.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
INSERT INTO {E}.mensajes_clientes VALUES
 (1,  DATE'2026-10-01','app','Perú','Me cobraron dos veces la cuota de septiembre, ya pagué el día 5 y otra vez me descontaron. Quiero mi devolución.'),
 (2,  DATE'2026-10-01','whatsapp','Colombia','Buenas, ¿puedo adelantar las cuotas de mi crédito sin pagar penalidad?'),
 (3,  DATE'2026-10-02','correo','Bolivia','Excelente atención, el desembolso llegó en menos de una hora. ¡Gracias Wizard Bank!'),
 (4,  DATE'2026-10-02','call center','Perú','Soy Carla Mendoza, DNI 45879632, no reconozco un préstamo a mi nombre. Creo que es fraude.'),
 (5,  DATE'2026-10-03','app','Colombia','La app se cierra cada vez que intento ver mi cronograma de pagos, uso Android.'),
 (6,  DATE'2026-10-03','whatsapp','Perú','Perdí mi trabajo este mes, ¿me pueden reprogramar las cuotas? No quiero caer en mora.'),
 (7,  DATE'2026-10-04','correo','Bolivia','La tasa que me ofrecieron en la campaña era 18% y en el contrato dice 24%. Exijo una explicación.'),
 (8,  DATE'2026-10-04','app','Colombia','¿Cuál es el horario para pagar en agentes? Quiero pagar en efectivo.'),
 (9,  DATE'2026-10-05','call center','Perú','Me llaman de cobranza a las 10 de la noche, eso es acoso. Mi número es 987654321, ya no me llamen.'),
 (10, DATE'2026-10-05','app','Bolivia','Muy fácil pedir el préstamo desde el celular, lo recomiendo.'),
 (11, DATE'2026-10-06','whatsapp','Colombia','Pagué por PSE y el pago no aparece, ya pasaron tres días. Adjunto comprobante.'),
 (12, DATE'2026-10-06','correo','Perú','Solicito mi constancia de no adeudo, terminé de pagar mi crédito en agosto.'),
 (13, DATE'2026-10-07','app','Colombia','No me llega el código de verificación por SMS para entrar a la app.'),
 (14, DATE'2026-10-07','call center','Bolivia','Quiero cancelar mi crédito por completo, ¿cuánto debo hoy?'),
 (15, DATE'2026-10-08','whatsapp','Perú','Me descontaron un seguro que nunca pedí, quiero que lo quiten y me devuelvan lo cobrado.'),
 (16, DATE'2026-10-08','app','Colombia','Gracias por aprobar mi préstamo tan rápido, me salvaron el mes.'),
 (17, DATE'2026-10-09','correo','Perú','Hola, soy Jorge Quispe, mi correo es jorge.quispe@mail.com. ¿Me pueden enviar mi cronograma actualizado?'),
 (18, DATE'2026-10-09','call center','Colombia','Me rechazaron la solicitud y no me dicen por qué. ¿Qué puedo hacer para que me aprueben?'),
 (19, DATE'2026-10-10','app','Bolivia','La app me cobra en dólares y mi crédito es en bolivianos, ¿por qué?'),
 (20, DATE'2026-10-10','whatsapp','Perú','Me llegó un mensaje pidiendo mi clave para "actualizar datos", ¿es de ustedes?'),
 (21, DATE'2026-10-11','correo','Colombia','El asesor fue muy amable y me explicó todo el plan de pagos. Felicitaciones.'),
 (22, DATE'2026-10-11','app','Perú','Pagué la cuota con un día de atraso y me cobraron una mora altísima.'),
 (23, DATE'2026-10-12','call center','Bolivia','¿Puedo cambiar la fecha de pago de mis cuotas al día 30? Me pagan a fin de mes.'),
 (24, DATE'2026-10-12','whatsapp','Colombia','Ya van dos débitos automáticos fallidos aunque tengo saldo en la cuenta.'),
 (25, DATE'2026-10-13','app','Perú','¿Qué necesito para pedir un segundo préstamo? Ya pagué la mitad del primero.'),
 (26, DATE'2026-10-13','correo','Bolivia','Nadie responde mis correos desde hace una semana, pésimo servicio.'),
 (27, DATE'2026-10-14','app','Colombia','La nueva versión de la app quedó muy bonita y más rápida.'),
 (28, DATE'2026-10-14','call center','Perú','Me cobraron comisión por desembolso y en la oferta decía sin comisiones.'),
 (29, DATE'2026-10-15','whatsapp','Bolivia','¿Tienen oficinas en Santa Cruz? Prefiero hacer el trámite en persona.'),
 (30, DATE'2026-10-15','app','Colombia','Hicieron un retiro de mi cuenta que no autoricé, por favor bloqueen todo ya.')
""")
display(spark.table(f"{E}.mensajes_clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Lanzar AI Search ahora, usarlo después ▶️
# MAGIC El primer índice de AI Search tarda unos 15 minutos en estar listo: Databricks prepara el cómputo del endpoint la primera vez.
# MAGIC Por eso lo lanzas ya y sigues con los pasos 6 a 13; en el Bloque 4 lo usas. La celda crea el endpoint (o reutiliza el que tengas:
# MAGIC Free Edition permite **uno solo**) y le pide el índice sobre `mensajes_clientes`. Qué es cada cosa, en el paso 14.

# COMMAND ----------

from datetime import timedelta
from databricks.sdk.service.vectorsearch import (EndpointType, VectorIndexType, DeltaSyncVectorIndexSpecRequest,
                                                 EmbeddingSourceColumn, PipelineType)

existentes = [e.name for e in w.vector_search_endpoints.list_endpoints()]
if existentes:
    ENDPOINT_BUSQUEDA = existentes[0]
    print(f"✅ Reutilizo el endpoint que ya tienes: {ENDPOINT_BUSQUEDA}")
else:
    w.vector_search_endpoints.create_endpoint_and_wait(name=ENDPOINT_BUSQUEDA, endpoint_type=EndpointType.STANDARD,
                                                       timeout=timedelta(minutes=20))
    print(f"✅ Endpoint {ENDPOINT_BUSQUEDA} creado")

INDICE = f"{E}.mensajes_clientes_idx"
if INDICE not in [i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=ENDPOINT_BUSQUEDA)]:
    w.vector_search_indexes.create_index(
        name=INDICE,
        endpoint_name=ENDPOINT_BUSQUEDA,
        primary_key="id_mensaje",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=f"{E}.mensajes_clientes",
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(name="texto",
                                                            embedding_model_endpoint_name="databricks-qwen3-embedding-0-6b")],
        ),
    )
    print(f"⏳ Índice {INDICE} pedido. Sigue con el paso 6: lo usas en el Bloque 4.")
else:
    print(f"✅ El índice {INDICE} ya existía (lo pondrás al día en el paso 14)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Enriquecer los mensajes con IA ▶️
# MAGIC Una sola consulta, cuatro funciones, cada una hace una tarea concreta sobre cada fila:
# MAGIC
# MAGIC | Función | Qué devuelve |
# MAGIC |---|---|
# MAGIC | `ai_classify(texto, ARRAY(...))` | Una de las categorías que **tú** defines |
# MAGIC | `ai_analyze_sentiment(texto)` | `positive`, `negative`, `neutral` o `mixed` |
# MAGIC | `ai_extract(texto, ARRAY(...))` | Un JSON con los campos que pediste, si están en el texto |
# MAGIC | `ai_mask(texto, ARRAY(...))` | El texto con esos datos reemplazados por `[MASKED]` |
# MAGIC
# MAGIC El resultado es una tabla Delta normal: lo que era texto libre ahora se puede filtrar, contar y darle a Genie. Tarda cerca de un minuto.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {E}.mensajes_clientes_ia
COMMENT 'Sesión 22: mensajes de clientes enriquecidos con AI Functions (categoría, sentimiento, datos extraídos y texto anonimizado). Datos sintéticos.'
AS
SELECT id_mensaje, fecha, canal, pais, texto,
       ai_classify(texto, ARRAY('reclamo de cobro', 'fraude o seguridad', 'problema con la app',
                                'consulta', 'felicitación', 'pedido de reprogramación')) AS categoria,
       ai_analyze_sentiment(texto)                                                     AS sentimiento,
       ai_extract(texto, ARRAY('producto', 'monto', 'fecha'))                          AS datos_extraidos,
       ai_mask(texto, ARRAY('persona', 'dni', 'telefono', 'email'))                    AS texto_anonimo
FROM {E}.mensajes_clientes
""")
display(spark.table(f"{E}.mensajes_clientes_ia").select("id_mensaje", "pais", "categoria", "sentimiento", "texto").orderBy("id_mensaje"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — El tablero de reclamos ▶️
# MAGIC Ahora es una consulta de siempre. Debajo del resultado pulsa **+** → **Visualization** → **Bar**: categoría en X, `mensajes` en Y,
# MAGIC `pais` como grupo. Así de rápido el texto libre llega a un dashboard.

# COMMAND ----------

display(spark.sql(f"""
SELECT categoria, pais, count(*) AS mensajes, count_if(sentimiento = 'negative') AS negativos
FROM {E}.mensajes_clientes_ia
GROUP BY categoria, pais
ORDER BY mensajes DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — Anonimizar antes de compartir ▶️
# MAGIC Revisa los mensajes 4, 9, 17 y 29. Los tres primeros traen nombre, DNI, teléfono o correo, y `ai_mask` los tapó.

# COMMAND ----------

display(spark.sql(f"SELECT id_mensaje, texto, texto_anonimo FROM {E}.mensajes_clientes_ia WHERE id_mensaje IN (4, 9, 17, 29) ORDER BY id_mensaje"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿qué pasó con el mensaje 29?
# MAGIC <details><summary>Ver respuesta</summary>Lo más probable es que diga "hacer el trámite en [MASKED]": el modelo leyó "persona" como si fuera un dato personal. <code>ai_mask</code> es un modelo, no una regla: se equivoca hacia los dos lados. Sirve para limpiar texto antes de mandarlo a un tercero o a un tablero, pero un dato estructurado sensible (un DNI en su propia columna) se protege como en la Sesión 21: con una column mask y grupos de Unity Catalog.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 9 — Triage con respuesta en JSON ▶️
# MAGIC Cuando necesitas varios campos a la vez, `ai_query` con `responseFormat` obliga al modelo a devolver JSON con la forma que pides.
# MAGIC `from_json` lo convierte en columnas. Es la base de cualquier automatización: el resultado entra directo a una tabla o a un flujo.

# COMMAND ----------

display(spark.sql(f"""
WITH t AS (
  SELECT id_mensaje, texto,
         from_json(
           ai_query('databricks-meta-llama-3-3-70b-instruct',
                    concat('Eres analista de servicio al cliente de un banco. Devuelve prioridad (alta, media o baja), ',
                           'el área responsable y una respuesta de máximo 30 palabras. Mensaje: ', texto),
                    responseFormat => 'STRUCT<triage: STRUCT<prioridad: STRING, area_responsable: STRING, respuesta_sugerida: STRING>>'),
           'prioridad STRING, area_responsable STRING, respuesta_sugerida STRING') AS triage
  FROM {E}.mensajes_clientes
  WHERE id_mensaje IN (4, 6, 22, 30)
)
SELECT id_mensaje, texto, triage.prioridad, triage.area_responsable, triage.respuesta_sugerida FROM t
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 10 — Pronóstico de colocación con `ai_forecast` ▶️
# MAGIC No todo es texto. `ai_forecast` proyecta una serie de tiempo sin entrenar nada: le das la tabla, la columna de fecha, la de valor y
# MAGIC hasta dónde quieres proyectar. Aquí, la colocación mensual en USD de la Sesión 20, por país, hasta marzo de 2027.
# MAGIC Debajo del resultado: **+** → **Visualization** → **Line**, `mes` en X y las tres columnas de pronóstico en Y.

# COMMAND ----------

display(spark.sql(f"""
SELECT * FROM ai_forecast(
  TABLE(SELECT mes, pais, monto_colocado_usd FROM {E}.metricas_colocacion_mensual_gld),
  horizon   => '2027-03-01',
  time_col  => 'mes',
  value_col => 'monto_colocado_usd',
  group_col => 'pais'
)
ORDER BY pais, mes
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿por qué el pronóstico trae `_upper` y `_lower`, y qué le dirías a finanzas si la banda es muy ancha?
# MAGIC <details><summary>Ver respuesta</summary>Son los límites de un intervalo de predicción: el valor real debería caer entre ellos casi siempre. Una banda ancha significa que la historia es corta o muy variable (aquí son pocos meses de datos sintéticos). A finanzas le dices el rango, no solo el número del medio: "entre X e Y, lo más probable Z".</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 11 — Laboratorio libre ▶️
# MAGIC Elige una función en el widget **3. Función**, escribe tu propio texto en **4. Tu texto** y corre la celda. Prueba un mensaje real que
# MAGIC te haya llegado de un banco, un texto con faltas de ortografía o una frase en inglés.

# COMMAND ----------

dbutils.widgets.dropdown("funcion", "ai_classify",
                         ["ai_classify", "ai_analyze_sentiment", "ai_extract", "ai_mask", "ai_summarize",
                          "ai_translate", "ai_fix_grammar", "ai_gen"], "3. Función")
dbutils.widgets.text("texto", "ayer fuistes al banco y no habian nadie para atenderme, quiero saber mi saldo", "4. Tu texto")

# COMMAND ----------

LLAMADAS = {
    "ai_classify":          "ai_classify(:texto, ARRAY('reclamo', 'consulta', 'felicitación', 'fraude'))",
    "ai_analyze_sentiment": "ai_analyze_sentiment(:texto)",
    "ai_extract":           "ai_extract(:texto, ARRAY('persona', 'producto', 'monto', 'fecha'))",
    "ai_mask":              "ai_mask(:texto, ARRAY('persona', 'dni', 'telefono', 'email', 'direccion'))",
    "ai_summarize":         "ai_summarize(:texto, 15)",
    "ai_translate":         "ai_translate(:texto, 'en')",
    "ai_fix_grammar":       "ai_fix_grammar(:texto)",
    "ai_gen":               "ai_gen(:texto)",
}
funcion = dbutils.widgets.get("funcion")
display(spark.sql(f"SELECT :texto AS entrada, {LLAMADAS[funcion]} AS `{funcion}`", args={"texto": dbutils.widgets.get("texto")}))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Costo.** Cada fila es una llamada al modelo. 30 mensajes no cuestan nada; 30 millones, sí. En producción: pruebas con una
# MAGIC > muestra (`LIMIT`), procesas solo lo nuevo (en un pipeline, una streaming table sobre los mensajes) y eliges el modelo más chico
# MAGIC > que alcance.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 3 · Python y trazas con MLflow
# MAGIC ## Paso 12 — Instalar el cliente ▶️
# MAGIC Desde Python hablas con los mismos modelos usando el cliente de OpenAI, apuntado a tu workspace (`DatabricksOpenAI`). La instalación
# MAGIC reinicia Python: por eso la celda siguiente vuelve a declarar las variables.

# COMMAND ----------

# MAGIC %pip install -q -U databricks-openai mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOGO = "datawizard_dev"
ESQUEMA = "lending"
E = f"{CATALOGO}.{ESQUEMA}"
ENDPOINT_BUSQUEDA = "dw_ai_search"
MODELO = "databricks-meta-llama-3-3-70b-instruct"

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
existentes = [e.name for e in w.vector_search_endpoints.list_endpoints()]
if existentes:
    ENDPOINT_BUSQUEDA = existentes[0]
print("listo · endpoint de búsqueda:", ENDPOINT_BUSQUEDA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 13 — Llamar al modelo y ver la traza ▶️
# MAGIC `mlflow.openai.autolog()` registra cada llamada como una **traza**: qué mensajes enviaste, qué respondió el modelo, cuántos tokens
# MAGIC usó y cuánto tardó. Al correr la celda aparece la traza debajo del resultado; ábrela. Así se depura una aplicación de IA: no
# MAGIC adivinando, mirando qué viajó.

# COMMAND ----------

import mlflow
from databricks_openai import DatabricksOpenAI

mlflow.openai.autolog()
cliente = DatabricksOpenAI()

respuesta = cliente.chat.completions.create(
    model=MODELO,
    messages=[
        {"role": "system", "content": "Eres el asistente de Wizard Bank. Respondes en español, en máximo 3 frases, sin inventar tasas ni montos."},
        {"role": "user", "content": "Pagué mi cuota con un día de atraso, ¿me van a cobrar mora?"},
    ],
    max_tokens=200,
)
print(respuesta.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 4 · AI Search y un RAG
# MAGIC ## Paso 14 — El índice de búsqueda ▶️
# MAGIC **Databricks AI Search** (antes Vector Search) convierte cada mensaje en un vector con un modelo de *embeddings* y los guarda en un
# MAGIC índice. Buscar es encontrar los vectores más parecidos al de tu pregunta: busca por **significado**, no por palabras exactas.
# MAGIC
# MAGIC | Decisión | Aquí | Por qué |
# MAGIC |---|---|---|
# MAGIC | Tipo de índice | **Delta Sync** | Se sincroniza solo con la tabla (por eso el Change Data Feed del paso 4) |
# MAGIC | Embeddings | **Administrados**, con `databricks-qwen3-embedding-0-6b` | Databricks calcula los vectores; el modelo entiende español |
# MAGIC | Sincronización | `TRIGGERED` | Se actualiza cuando lo pides, no en continuo (más barato) |
# MAGIC
# MAGIC La celda espera a que el índice del paso 5 termine. Si sigue en *pending endpoint provisioning*, avanza al Bloque 5 y vuelve.

# COMMAND ----------

import time

INDICE = f"{E}.mensajes_clientes_idx"
estado = w.vector_search_indexes.get_index(INDICE).status
if estado.ready:
    w.vector_search_indexes.sync_index(INDICE)   # ya estaba listo: lo pone al día con la tabla (por si la recreaste)
    time.sleep(10)
while not (estado := w.vector_search_indexes.get_index(INDICE).status).ready:
    print("⏳", (estado.message or "indexando…")[:90])
    time.sleep(30)
print(f"✅ índice {INDICE} listo · filas indexadas:", estado.indexed_row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 15 — Buscar por significado ▶️
# MAGIC La pregunta de abajo no comparte ninguna palabra importante con el mensaje que debería encontrar. Cámbiala y vuelve a correr:
# MAGIC *"problemas para entrar a la aplicación"*, *"me trataron bien"*, *"no puedo pagar este mes"*.

# COMMAND ----------

def buscar(pregunta, n=3):
    r = w.vector_search_indexes.query_index(index_name=INDICE, columns=["id_mensaje", "pais", "texto"],
                                            query_text=pregunta, num_results=n)
    return r.result.data_array   # cada fila: id_mensaje, pais, texto, score

for fila in buscar("me sacaron plata sin permiso"):
    print(fila)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 16 — Un RAG en diez líneas ▶️
# MAGIC **RAG** (*Retrieval-Augmented Generation*) es buscar primero y responder después, usando solo lo que encontraste. Así el modelo
# MAGIC responde sobre **tus** datos y no inventa. Es el patrón detrás de los asistentes sobre documentos de una empresa.

# COMMAND ----------

@mlflow.trace
def responder(pregunta):
    encontrados = buscar(pregunta, n=4)
    contexto = "\n".join(f"- Mensaje {int(f[0])} ({f[1]}): {f[2]}" for f in encontrados)
    r = cliente.chat.completions.create(
        model=MODELO,
        messages=[
            {"role": "system", "content": "Eres analista de experiencia del cliente de Wizard Bank. Responde SOLO con los mensajes "
                                          "que te paso, citando su número. Si no alcanzan para responder, dilo."},
            {"role": "user", "content": f"Mensajes encontrados:\n{contexto}\n\nPregunta: {pregunta}"},
        ],
        max_tokens=300,
    )
    return r.choices[0].message.content

print(responder("¿De qué se quejan los clientes sobre los cobros, y qué deberíamos revisar primero?"))

# COMMAND ----------

# MAGIC %md
# MAGIC Abre la traza: vas a ver el paso de búsqueda y la llamada al modelo, con el contexto exacto que recibió. Prueba también una pregunta
# MAGIC que los mensajes no responden, por ejemplo *"¿cuál es la tasa de interés del préstamo vehicular?"*: el modelo debería decir que no
# MAGIC lo sabe.
# MAGIC
# MAGIC > En producción no armas esto a mano: un **Genie Agent** o un agente de **Agent Bricks** usan el índice como herramienta, y
# MAGIC > **Unity Gateway** controla el gasto y los permisos del tráfico al modelo.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 5 · Genie y el puente a Fabric
# MAGIC ## Paso 17 — Los mensajes, en tu Genie Agent 🖱️
# MAGIC El texto libre que enriqueciste ya es una tabla con comentarios. Dásela al agente de la Sesión 20.2.
# MAGIC 1. Abre tu Genie Agent → **Configure** → **Data** → **Add** → `mensajes_clientes_ia`.
# MAGIC 2. Pregúntale: *"¿Cuántos reclamos de cobro con sentimiento negativo hubo por país?"* y *"Muéstrame los mensajes de fraude, sin
# MAGIC    datos personales"*. La segunda debería usar `texto_anonimo`; si no lo hace, es un comentario que falta en la columna.
# MAGIC 3. Encuentra el agente en **Genie One** (la vista del usuario de negocio) y repite una pregunta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 18 — Genie Code sobre esta guía 🖱️
# MAGIC Genie Code (antes Databricks Assistant) vive en el notebook.
# MAGIC 1. Ve a la celda del paso 16 y abre Genie Code (el ícono de chispa en la celda o el panel derecho).
# MAGIC 2. Pídele: *"Explica esta función y agrega un filtro para buscar solo mensajes de un país"*.
# MAGIC 3. Revisa lo que propone antes de aceptarlo: Genie Code acelera, pero el código sigue siendo tuyo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 19 — Dónde vive el dato: el puente a Fabric ▶️
# MAGIC Microsoft Fabric guarda todo en **OneLake** en formato **Delta**, el mismo que tus tablas. Por eso pueden convivir sin copiar datos.
# MAGIC Corre la celda: tu tabla Gold es Delta, en un almacenamiento de objetos, gobernada por Unity Catalog.

# COMMAND ----------

detalle = spark.sql(f"DESCRIBE DETAIL {E}.metricas_colocacion_mensual_gld").select("format", "location", "numFiles", "sizeInBytes")
display(detalle)

# COMMAND ----------

# MAGIC %md
# MAGIC | Camino | Dirección | Qué hace | Estado (nov-2026) |
# MAGIC |---|---|---|---|
# MAGIC | **Mirroring de Unity Catalog** | Databricks → Fabric | Fabric ve un catálogo de UC como un ítem de OneLake, sin copiar archivos. Power BI lo lee en Direct Lake | GA, solo Azure Databricks |
# MAGIC | **Shortcuts de OneLake** | Almacenamiento → Fabric | Un acceso directo a una carpeta Delta en ADLS, S3 o GCS | GA |
# MAGIC | **Federación del catálogo de OneLake** | Fabric → Databricks | Databricks consulta tablas de OneLake desde Unity Catalog, sin moverlas | Verificar: preview a GA en 2026 |
# MAGIC | **OpenSharing** (antes Delta Sharing) | Databricks → cualquiera | Compartir tablas con otra organización o herramienta por un protocolo abierto | GA |
# MAGIC | **Conector de Power BI** | Fabric → Databricks | Power BI consulta un SQL warehouse de Databricks | GA |
# MAGIC
# MAGIC **Pregunta:** tu Free Edition corre en AWS. ¿Cuál de los caminos podrías usar para que un equipo con Fabric lea tu Gold?
# MAGIC <details><summary>Ver respuesta</summary>El mirroring no: es solo para Azure Databricks. Te quedan OpenSharing (compartes las tablas y Power BI o Fabric las leen) o un shortcut de OneLake a la carpeta S3, que exige tener tu propio almacenamiento. En una empresa con Azure Databricks, el camino natural es el mirroring: Databricks produce y gobierna, Fabric y Power BI consumen.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Cierre
# MAGIC - [ ] Comparé dos modelos y sé elegir el más chico que alcanza
# MAGIC - [ ] Convertí texto libre en columnas con `ai_classify`, `ai_analyze_sentiment`, `ai_extract` y `ai_mask`
# MAGIC - [ ] Sé por qué `ai_mask` no reemplaza a una column mask de Unity Catalog
# MAGIC - [ ] Obtuve JSON con `ai_query` + `responseFormat` y proyecté una serie con `ai_forecast`
# MAGIC - [ ] Vi la traza de una llamada en MLflow
# MAGIC - [ ] Creé un índice de AI Search, busqué por significado y armé un RAG
# MAGIC - [ ] Le di los mensajes enriquecidos a mi Genie Agent
# MAGIC - [ ] Sé qué camino usar para que Fabric lea mi Gold
# MAGIC
# MAGIC ### 🧹 Limpieza del curso
# MAGIC Es la última sesión. Cuando termines el proyecto final (no antes), en este orden:
# MAGIC 1. Borra el índice y el endpoint de AI Search: el endpoint es el único que tienes y consume aunque no lo uses.
# MAGIC 2. Borra tus Genie Agents y las vistas que no son del pipeline (`kpi_colocacion`, `kpi_funnel_app`, `v_cartera_bi`).
# MAGIC 3. `databricks bundle destroy -t dev` (y `-t prod`) en `lakeflow/`, y recién entonces el pipeline de la UI, que se lleva sus tablas.
# MAGIC 4. Los archivos de landing no se tocan nunca.
# MAGIC
# MAGIC ### 🏁 El proyecto final
# MAGIC El reto está en `proyecto-final/proyecto_final.html` de este repo: un Lakehouse propio, de punta a punta, con todo lo que viste en
# MAGIC el curso. Ábrelo en el navegador.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Celda de limpieza (córrela solo al final del proyecto final)
# MAGIC Descomenta las líneas y corre la celda.

# COMMAND ----------

# w.vector_search_indexes.delete_index(f"{E}.mensajes_clientes_idx")
# w.vector_search_endpoints.delete_endpoint(ENDPOINT_BUSQUEDA)
# spark.sql(f"DROP TABLE IF EXISTS {E}.mensajes_clientes_ia")
# spark.sql(f"DROP TABLE IF EXISTS {E}.mensajes_clientes")
