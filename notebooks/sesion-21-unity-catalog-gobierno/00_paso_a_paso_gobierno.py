# Databricks notebook source

# MAGIC %md
# MAGIC # Sesión 21 · Unity Catalog paso a paso: permisos, row filters, máscaras, linaje y secretos
# MAGIC **Data Wizard Academy · Databricks Track · Módulo 5**
# MAGIC `v1.0 · creada 2026-11-07 · actualizada 2026-11-07`
# MAGIC
# MAGIC **Continúa la Sesión 20.2.** Tu pipeline ya publica Gold en `datawizard_dev.lending` y el negocio lo consulta con Genie. Hoy
# MAGIC respondes dos preguntas que cualquier banco te va a hacer antes de dejarte en producción:
# MAGIC
# MAGIC | Pregunta | Herramienta | Bloques |
# MAGIC |---|---|---|
# MAGIC | **¿Quién puede ver el dato?** ¿Todo, solo su país, sin saber de qué cliente es? | Unity Catalog: `GRANT`, row filters, column masks | 1 y 2 |
# MAGIC | **¿Por dónde pasó el dato y quién lo tocó?** | Linaje y auditoría (`system.access`) | 3 |
# MAGIC | **¿Quién puede usar la credencial de la base de datos?** | Secret scopes | 4 |
# MAGIC
# MAGIC **Cómo se trabaja:** los pasos ▶️ se corren aquí, los 🖱️ son en la interfaz y el 📋 reemplaza un archivo del pipeline.
# MAGIC
# MAGIC | Bloque | Pasos | Min |
# MAGIC |---|---|---|
# MAGIC | Introducción | El modelo de permisos de Unity Catalog | 15 |
# MAGIC | **Bloque 1 · Permisos** | 0 punto de partida · 1 mapa de acceso · 2 grupos · 3 GRANT | 40 |
# MAGIC | **Bloque 2 · Seguridad fina** | 4 identidad · 5 funciones de política · 6 declararlas en el pipeline · 7 probarlas · 8 una vista para BI | 60 |
# MAGIC | **Bloque 3 · Linaje y auditoría** | 9 linaje · 10 auditoría | 25 |
# MAGIC | **Bloque 4 · Secretos** | 11 crear el secret scope · 12 usarlo | 30 |
# MAGIC | Cierre | Checklist | 10 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Introducción
# MAGIC ### El modelo de permisos en una imagen
# MAGIC ```
# MAGIC  catálogo  datawizard_dev            USE CATALOG   → puede "entrar" al catálogo
# MAGIC    └─ esquema  lending               USE SCHEMA    → puede "entrar" al esquema
# MAGIC        └─ tabla / vista              SELECT        → puede leer
# MAGIC              └─ filas y columnas     row filter / column mask → QUÉ filas y QUÉ valores ve al leer
# MAGIC ```
# MAGIC - Para leer una tabla se necesitan **los tres** permisos: `USE CATALOG` + `USE SCHEMA` + `SELECT`. Falta uno y no ve nada.
# MAGIC - Los permisos se **heredan hacia abajo**: `SELECT` sobre el esquema vale para todas sus tablas, también las que se creen mañana.
# MAGIC - Se dan a **grupos**, nunca a personas: cuando alguien cambia de área se le cambia el grupo, no veinte `GRANT`.
# MAGIC - Row filters y column masks **no dan acceso**: lo recortan. Primero `SELECT`, después el filtro.
# MAGIC
# MAGIC ### En Free Edition
# MAGIC | Aquí | En una empresa |
# MAGIC |---|---|
# MAGIC | Eres el único usuario y eres admin: pruebas los permisos **entrando y saliendo de los grupos** | Cada persona entra con su cuenta |
# MAGIC | Los grupos son **de la cuenta** (los que acepta Unity Catalog): se preguntan con `is_account_group_member('grupo')` | Igual; los grupos suelen venir sincronizados desde Entra ID |
# MAGIC | Secret scope respaldado por Databricks | Secret scope respaldado por **Azure Key Vault** (o el gestor de secretos de la nube) |

# COMMAND ----------

# Dónde vive todo: el catálogo y el esquema de tu pipeline (Sesiones 19 y 20)
CATALOGO = "datawizard_dev"
ESQUEMA = "lending"


def t(nombre):
    return f"{CATALOGO}.{ESQUEMA}.{nombre}"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 1 · Permisos
# MAGIC ## Paso 0 — Punto de partida ▶️
# MAGIC Deben existir el Gold de la Sesión 20 y las metric views.

# COMMAND ----------

for nombre in ["cartera_creditos_gld", "metricas_colocacion_mensual_gld", "features_cliente_mes_gld", "kpi_colocacion"]:
    assert spark.catalog.tableExists(t(nombre)), f"Falta {t(nombre)}: termina la Sesión 20"
    print(f"✅ {t(nombre)}")
print("Tú eres:", spark.sql("SELECT current_user()").first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — El mapa de acceso 🗣️
# MAGIC Antes de escribir un `GRANT` se decide **quién ve qué** y se deja por escrito. Esta es la decisión para Wizard Bank:
# MAGIC
# MAGIC | Grupo | Quiénes son | Qué ven |
# MAGIC |---|---|---|
# MAGIC | `analistas_bi` | Dashboards y reportes | Las métricas, las metric views y una vista agregada de la cartera. **No** el detalle por crédito |
# MAGIC | `data_science` | Modelos de mora y propensión | Las feature tables |
# MAGIC | `cobranzas` | Gestores de cobranza | La cartera **con** `id_cliente` (necesitan saber a quién llamar) |
# MAGIC | `cartera_peru` | Equipo de cartera de Perú | Solo los créditos de Perú |
# MAGIC | `cartera_todos` | Riesgo regional | Los créditos de los tres países |
# MAGIC
# MAGIC Fíjate que los dos últimos no dicen *qué tabla* sino *qué filas* de la misma tabla: eso no se resuelve con `GRANT`, sino con un
# MAGIC **row filter** (bloque 2). Y "ver la cartera sin saber de qué cliente es cada crédito" es un **column mask**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Crear los grupos 🖱️
# MAGIC 1. Arriba a la derecha → tu usuario → **Settings** → **Identity and access** → **Groups** → **Manage** → **Add group**.
# MAGIC 2. Crea los cinco grupos del paso 1: `analistas_bi`, `data_science`, `cobranzas`, `cartera_peru`, `cartera_todos`.
# MAGIC 3. **No te agregues todavía a ninguno.** En el paso 7 vas a entrar y salir de ellos para ver el efecto.
# MAGIC
# MAGIC > ⚠️ **Grupos de cuenta, no del workspace.** Unity Catalog solo acepta grupos **de la cuenta** en un `GRANT`. La pantalla de arriba
# MAGIC > los crea así. Por terminal, **no** uses `databricks groups create`: crea un grupo local del workspace y el `GRANT` falla con
# MAGIC > `PRINCIPAL_DOES_NOT_EXIST`. El equivalente correcto es:
# MAGIC > ```bash
# MAGIC > databricks api post /api/2.0/account/scim/v2/Groups --json '{"displayName": "cartera_peru"}'
# MAGIC > ```

# COMMAND ----------

GRUPOS = ["analistas_bi", "data_science", "cobranzas", "cartera_peru", "cartera_todos"]
existentes = {r.name for r in spark.sql("SHOW GROUPS").collect()}
for g in GRUPOS:
    print(f"{'✅' if g in existentes else '❌ falta crear'} {g}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — GRANT: el acceso por grupo ▶️
# MAGIC Cada grupo necesita entrar al catálogo y al esquema, y después `SELECT` solo sobre lo que le toca. Ninguno recibe `SELECT` sobre
# MAGIC el esquema completo: eso le daría también las tablas que se creen mañana (Bronze, Silver) sin que nadie lo decida.

# COMMAND ----------

PERMISOS = {
    "analistas_bi":  ["metricas_colocacion_mensual_gld", "metricas_originacion_mensual_gld", "metricas_roi_campania_gld",
                      "metricas_funnel_app_diario_gld", "kpi_colocacion", "kpi_funnel_app"],
    "data_science":  ["features_cliente_mes_gld", "features_app_cliente_dia_gld"],
    "cobranzas":     ["cartera_creditos_gld"],
    "cartera_peru":  ["cartera_creditos_gld"],
    "cartera_todos": ["cartera_creditos_gld"],
}
for grupo, tablas in PERMISOS.items():
    spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `{grupo}`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.{ESQUEMA} TO `{grupo}`")
    for tabla in tablas:
        spark.sql(f"GRANT SELECT ON TABLE {t(tabla)} TO `{grupo}`")
    print(f"✅ {grupo}: USE CATALOG, USE SCHEMA y SELECT sobre {len(tablas)} objetos")

# COMMAND ----------

# ¿Quién puede leer la cartera? SHOW GRANTS lo responde por objeto
display(spark.sql(f"SHOW GRANTS ON TABLE {t('cartera_creditos_gld')}"))

# COMMAND ----------

# Y al revés: ¿qué puede hacer un grupo en el esquema?
display(spark.sql(f"SHOW GRANTS `data_science` ON SCHEMA {CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Quitar un permiso** es `REVOKE`. Pruébalo: data_science no necesita la feature table de la app para el modelo de mora.

# COMMAND ----------

spark.sql(f"REVOKE SELECT ON TABLE {t('features_app_cliente_dia_gld')} FROM `data_science`")
display(spark.sql(f"SHOW GRANTS `data_science` ON TABLE {t('features_app_cliente_dia_gld')}"))   # vacío: ya no tiene SELECT
spark.sql(f"GRANT SELECT ON TABLE {t('features_app_cliente_dia_gld')} TO `data_science`")         # lo devolvemos

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ **Míralo en la UI:** Catalog Explorer → `cartera_creditos_gld` → pestaña **Permissions**. Son los mismos `GRANT`, y desde ahí
# MAGIC también se pueden dar y quitar.
# MAGIC
# MAGIC **Pregunta:** las tablas Gold le pertenecen al pipeline. ¿Por qué el `GRANT` sí funciona sobre ellas y en el paso 6 el row filter no?
# MAGIC <details><summary>Ver respuesta</summary>Porque un permiso es <b>de Unity Catalog</b>, no de la definición de la tabla: se puede dar o quitar sobre cualquier objeto, sea de quien sea. Un row filter o una máscara, en cambio, cambian <b>cómo se lee</b> la tabla, y la definición de una tabla del pipeline la controla el pipeline: si se la cambias por fuera, el próximo update la pisa. Por eso las políticas se declaran en su código, como los comentarios y las llaves de la Sesión 20.2.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 2 · Seguridad fina
# MAGIC ## Paso 4 — ¿Quién está consultando? ▶️
# MAGIC Toda la seguridad fina descansa en dos funciones que Databricks evalúa **en cada consulta**, con la identidad de quien consulta:
# MAGIC `current_user()` (quién es) y `is_account_group_member('grupo')` (si está en un grupo de la cuenta).

# COMMAND ----------

display(spark.sql(f"""
  SELECT current_user()                AS yo,
         is_account_group_member('cartera_peru')   AS en_cartera_peru,
         is_account_group_member('cartera_todos')  AS en_cartera_todos,
         is_account_group_member('cobranzas')      AS en_cobranzas"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Las funciones de política ▶️
# MAGIC Un row filter y un column mask son **funciones de Unity Catalog** (`CREATE FUNCTION`). Viven en el esquema, como las tablas, y
# MAGIC se reutilizan en todas las tablas que las necesiten.
# MAGIC
# MAGIC | Función | Recibe | Devuelve | Regla |
# MAGIC |---|---|---|---|
# MAGIC | `filtro_pais_cartera` | el país de la fila | `TRUE` si la fila se ve | `cartera_todos` ve todo; `cartera_peru` solo Perú; el resto, nada |
# MAGIC | `mascara_id_cliente` | el `id_cliente` | el valor que se muestra | `cobranzas` ve el valor real; el resto, `NULL` |
# MAGIC
# MAGIC No hay atajo para admins a propósito: así tú, que eres el dueño, también ves el efecto.

# COMMAND ----------

# Fuente: lakeflow/gobierno/filtro_pais_cartera.sql (generado: no editar aquí)
sql = f"""
-- Sesión 21 · Paso 5 · Row filter: qué créditos de la cartera ve cada grupo.
-- Una función de Unity Catalog que devuelve TRUE para las filas visibles. El pipeline la aplica a cartera_creditos_gld
-- (WITH ROW FILTER en gobierno/14_gold_cartera.sql) y se evalúa en CADA consulta, con la identidad de quien consulta.
-- Los grupos de Unity Catalog son de la CUENTA: se pregunta con is_account_group_member(). is_member() solo mira grupos locales
-- del workspace, que Unity Catalog no acepta en un GRANT.
-- No hay atajo para admins a propósito: así el dueño de la tabla también ve el efecto al entrar y salir de los grupos.
CREATE OR REPLACE FUNCTION {CATALOGO}.{ESQUEMA}.filtro_pais_cartera(pais STRING)
RETURNS BOOLEAN
COMMENT 'Row filter de la cartera: cartera_todos ve todo; cartera_peru solo los créditos de Perú'
RETURN is_account_group_member('cartera_todos') OR (pais = 'Perú' AND is_account_group_member('cartera_peru'))
"""
spark.sql(sql)

# COMMAND ----------

# Fuente: lakeflow/gobierno/mascara_id_cliente.sql (generado: no editar aquí)
sql = f"""
-- Sesión 21 · Paso 5 · Column mask: quién ve a qué cliente pertenece cada crédito.
-- El primer parámetro es la columna enmascarada; lo que devuelve reemplaza el valor para quien no cumple la condición.
-- Solo cobranzas necesita saber de quién es cada crédito (para gestionarlo); el resto ve NULL.
CREATE OR REPLACE FUNCTION {CATALOGO}.{ESQUEMA}.mascara_id_cliente(id_cliente BIGINT)
RETURNS BIGINT
COMMENT 'Column mask de id_cliente: solo el grupo cobranzas ve el valor real'
RETURN CASE WHEN is_account_group_member('cobranzas') THEN id_cliente ELSE NULL END
"""
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Declararlas en el pipeline 📋
# MAGIC Primero comprueba que **no** se pueden pegar por fuera: `cartera_creditos_gld` es una materialized view del pipeline.

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {t('cartera_creditos_gld')} SET ROW FILTER {t('filtro_pais_cartera')} ON (pais)")
except Exception as e:
    print("❌ Esperado:", str(e).split("SQLSTATE")[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC Las políticas van en la definición de la tabla, en el código del pipeline:
# MAGIC - `MASK ${politicas}.mascara_id_cliente` en la columna `id_cliente`.
# MAGIC - `WITH ROW FILTER ${politicas}.filtro_pais_cartera ON (pais)` en la tabla.
# MAGIC
# MAGIC Dentro de un pipeline una función de Unity Catalog se nombra **completa** (`catálogo.esquema.función`). Para no escribir tu esquema
# MAGIC en el código, se usa una clave de configuración del pipeline que el SQL lee como `${politicas}`.
# MAGIC
# MAGIC 1. Pipeline → **Settings** → **Configuration** → **Add configuration**: clave **`politicas`**, valor **`datawizard_dev.lending`**
# MAGIC    (tu catálogo y esquema). Guarda.
# MAGIC 2. Abre `transformations/14_gold_cartera.sql` y reemplaza todo su contenido por el bloque de abajo. Es la versión de la Sesión
# MAGIC    20.2 (con comentarios y llave) más las dos políticas.
# MAGIC 3. **Run pipeline.** Si quieres que tarde menos: flecha junto a *Run pipeline* → **Refresh selection** → solo `cartera_creditos_gld`.
# MAGIC
# MAGIC 📋 Reemplaza **`transformations/14_gold_cartera.sql`**
# MAGIC ```sql
# MAGIC -- Sesión 21 · Paso 6 · Tabla ancha de cartera — VERSIÓN GOBERNADA (row filter + column mask).
# MAGIC -- Reemplaza transformations/14_gold_cartera.sql (la versión con comentarios de la Sesión 20.2). Misma lógica; agrega:
# MAGIC --   - MASK mascara_id_cliente en id_cliente: solo el grupo `cobranzas` ve a qué cliente pertenece cada crédito
# MAGIC --   - WITH ROW FILTER filtro_pais_cartera ON (pais): cada grupo ve solo los créditos de su país
# MAGIC -- La tabla le pertenece al pipeline: ALTER TABLE ... SET ROW FILTER falla sobre ella. Las políticas se declaran aquí,
# MAGIC -- en su definición, igual que los comentarios y las llaves. Las dos funciones se crean ANTES con SQL (paso 5 de la guía).
# MAGIC -- ${politicas} lo reemplaza el pipeline con su Configuration (clave politicas = <catálogo>.<esquema> donde viven las funciones):
# MAGIC -- dentro de un pipeline una función de Unity Catalog se nombra completa.
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW cartera_creditos_gld (
# MAGIC   numero_credito              BIGINT        NOT NULL COMMENT 'Llave: número de crédito del sistema de cobranzas (= id_desembolso de Lending)',
# MAGIC   id_cliente                  BIGINT                 MASK ${politicas}.mascara_id_cliente COMMENT 'Cliente dueño del crédito',
# MAGIC   id_pais                     SMALLINT               COMMENT 'País del crédito: 1 Perú, 2 Colombia, 3 Bolivia',
# MAGIC   pais                        STRING                 COMMENT 'País del crédito: Perú, Colombia o Bolivia (con tilde)',
# MAGIC   moneda                      STRING                 COMMENT 'Moneda local del crédito: PEN, COP o BOB',
# MAGIC   nivel_riesgo_al_desembolso  STRING                 COMMENT 'Nivel de riesgo A (mejor) a E que tenía el cliente cuando se le desembolsó. No es el riesgo de hoy',
# MAGIC   fecha_desembolso            DATE                   COMMENT 'Fecha del desembolso del crédito',
# MAGIC   monto_desembolsado          DECIMAL(18,2)          COMMENT 'Monto desembolsado en MONEDA LOCAL. No sumar entre países',
# MAGIC   fecha_corte                 DATE                   COMMENT 'Fecha de la foto: el último pago registrado. Toda la fila describe el crédito a esta fecha',
# MAGIC   n_cuotas                    BIGINT                 COMMENT 'Cuotas del plan de pagos',
# MAGIC   cuotas_pagadas              BIGINT                 COMMENT 'Cuotas con estado Pagada a la fecha de corte',
# MAGIC   cuotas_vencidas             BIGINT                 COMMENT 'Cuotas vencidas sin pagar a la fecha de corte',
# MAGIC   saldo_capital               DECIMAL(18,2)          COMMENT 'Capital pendiente en MONEDA LOCAL. No sumar entre países: para totales usar saldo_capital_usd',
# MAGIC   monto_vencido               DECIMAL(18,2)          COMMENT 'Monto de las cuotas vencidas en MONEDA LOCAL. No sumar entre países',
# MAGIC   dias_mora                   INT                    COMMENT 'Días desde la cuota vencida impaga más antigua hasta la fecha de corte. 0 = al día. Un crédito está EN MORA si dias_mora > 0',
# MAGIC   n_pagos                     BIGINT                 COMMENT 'Pagos recibidos',
# MAGIC   monto_pagado                DECIMAL(18,2)          COMMENT 'Total pagado en MONEDA LOCAL. No sumar entre países',
# MAGIC   fecha_ultimo_pago           TIMESTAMP              COMMENT 'Último pago recibido',
# MAGIC   n_gestiones                 BIGINT                 COMMENT 'Gestiones de cobranza realizadas',
# MAGIC   resultado_ultima_gestion    STRING                 COMMENT 'Resultado de la última gestión: Compromiso de pago, Sin respuesta, Rechazo, Pago inmediato o Sin gestion',
# MAGIC   saldo_capital_usd           DECIMAL(18,2)          COMMENT 'Capital pendiente en USD (tasa de la fecha de corte). Usar este para sumar saldos entre países',
# MAGIC   tramo_mora                  STRING                 COMMENT 'Tramo de mora: Al dia, 1-30, 31-60, 61-90, Mas de 90 (sin tildes)',
# MAGIC   es_mora_30                  INT                    COMMENT '1 si dias_mora > 30 (mora mayor a 30 días, "cartera en riesgo"), 0 si no. "En mora" a secas es dias_mora > 0',
# MAGIC   CONSTRAINT pk_cartera_creditos_gld PRIMARY KEY (numero_credito)
# MAGIC )
# MAGIC COMMENT 'Cartera de créditos: una fila por crédito con cuotas, pagos, gestiones y mora a la fecha de corte. Úsala para cartera, saldos, mora y cobranza. En mora = dias_mora > 0. Mora mayor a 30 días = es_mora_30 = 1. Los montos locales (saldo_capital, monto_vencido, monto_pagado, monto_desembolsado) no se suman entre países: para totales usar saldo_capital_usd. NO sirve para colocación por mes (usar kpi_colocacion). Datos sintéticos.'
# MAGIC CLUSTER BY (numero_credito)
# MAGIC WITH ROW FILTER ${politicas}.filtro_pais_cartera ON (pais)
# MAGIC AS
# MAGIC WITH corte AS (
# MAGIC   SELECT CAST(max(fecha_pago) AS DATE) AS fecha_corte FROM pagos_slv
# MAGIC ),
# MAGIC cuotas AS (
# MAGIC   SELECT numero_credito,
# MAGIC          count(*)                                                        AS n_cuotas,
# MAGIC          count_if(estado_cuota = 'Pagada')                               AS cuotas_pagadas,
# MAGIC          count_if(estado_cuota = 'Vencida')                              AS cuotas_vencidas,
# MAGIC          sum(monto_capital) FILTER (WHERE estado_cuota <> 'Pagada')      AS saldo_capital,
# MAGIC          sum(monto_cuota)   FILTER (WHERE estado_cuota = 'Vencida')      AS monto_vencido,
# MAGIC          min(fecha_vencimiento) FILTER (WHERE estado_cuota = 'Vencida')  AS vencida_mas_antigua
# MAGIC   FROM cuotas_slv
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC pagos AS (
# MAGIC   SELECT numero_credito, count(*) AS n_pagos, sum(monto_pagado) AS monto_pagado, max(fecha_pago) AS fecha_ultimo_pago
# MAGIC   FROM pagos_slv
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC gestiones AS (
# MAGIC   SELECT numero_credito, count(*) AS n_gestiones, max_by(resultado, fecha_gestion) AS resultado_ultima_gestion
# MAGIC   FROM gestiones_cobranza_slv
# MAGIC   GROUP BY numero_credito
# MAGIC ),
# MAGIC cartera AS (
# MAGIC   SELECT
# MAGIC     c.numero_credito,
# MAGIC     d.id_cliente,
# MAGIC     d.id_pais,
# MAGIC     pa.pais,
# MAGIC     d.moneda,
# MAGIC     cl.nivel_riesgo                                                   AS nivel_riesgo_al_desembolso,
# MAGIC     d.fecha_desembolso,
# MAGIC     d.monto_desembolsado,
# MAGIC     corte.fecha_corte,
# MAGIC     c.n_cuotas,
# MAGIC     c.cuotas_pagadas,
# MAGIC     c.cuotas_vencidas,
# MAGIC     coalesce(c.saldo_capital, 0)                                      AS saldo_capital,
# MAGIC     coalesce(c.monto_vencido, 0)                                      AS monto_vencido,
# MAGIC     greatest(coalesce(datediff(corte.fecha_corte, c.vencida_mas_antigua), 0), 0) AS dias_mora,   -- una cuota marcada Vencida después del corte no da mora negativa
# MAGIC     coalesce(p.n_pagos, 0)                                            AS n_pagos,
# MAGIC     coalesce(p.monto_pagado, 0)                                       AS monto_pagado,
# MAGIC     p.fecha_ultimo_pago,
# MAGIC     coalesce(g.n_gestiones, 0)                                        AS n_gestiones,
# MAGIC     coalesce(g.resultado_ultima_gestion, 'Sin gestion')               AS resultado_ultima_gestion
# MAGIC   FROM cuotas c
# MAGIC   CROSS JOIN corte
# MAGIC   LEFT JOIN pagos p                   ON p.numero_credito = c.numero_credito
# MAGIC   LEFT JOIN gestiones g               ON g.numero_credito = c.numero_credito
# MAGIC   LEFT JOIN fact_desembolsos_gld d    ON d.id_desembolso = c.numero_credito
# MAGIC   LEFT JOIN dim_cliente_gld cl        ON cl.sk_cliente = d.sk_cliente
# MAGIC   LEFT JOIN dim_pais_gld pa           ON pa.id_pais = d.id_pais
# MAGIC )
# MAGIC SELECT
# MAGIC   CAST(ca.numero_credito AS BIGINT)                        AS numero_credito,
# MAGIC   CAST(ca.id_cliente AS BIGINT)                            AS id_cliente,
# MAGIC   CAST(ca.id_pais AS SMALLINT)                             AS id_pais,
# MAGIC   CAST(ca.pais AS STRING)                                  AS pais,
# MAGIC   CAST(ca.moneda AS STRING)                                AS moneda,
# MAGIC   CAST(ca.nivel_riesgo_al_desembolso AS STRING)            AS nivel_riesgo_al_desembolso,
# MAGIC   CAST(ca.fecha_desembolso AS DATE)                        AS fecha_desembolso,
# MAGIC   CAST(ca.monto_desembolsado AS DECIMAL(18,2))             AS monto_desembolsado,
# MAGIC   CAST(ca.fecha_corte AS DATE)                             AS fecha_corte,
# MAGIC   CAST(ca.n_cuotas AS BIGINT)                              AS n_cuotas,
# MAGIC   CAST(ca.cuotas_pagadas AS BIGINT)                        AS cuotas_pagadas,
# MAGIC   CAST(ca.cuotas_vencidas AS BIGINT)                       AS cuotas_vencidas,
# MAGIC   CAST(ca.saldo_capital AS DECIMAL(18,2))                  AS saldo_capital,
# MAGIC   CAST(ca.monto_vencido AS DECIMAL(18,2))                  AS monto_vencido,
# MAGIC   CAST(ca.dias_mora AS INT)                                AS dias_mora,
# MAGIC   CAST(ca.n_pagos AS BIGINT)                               AS n_pagos,
# MAGIC   CAST(ca.monto_pagado AS DECIMAL(18,2))                   AS monto_pagado,
# MAGIC   CAST(ca.fecha_ultimo_pago AS TIMESTAMP)                  AS fecha_ultimo_pago,
# MAGIC   CAST(ca.n_gestiones AS BIGINT)                           AS n_gestiones,
# MAGIC   CAST(ca.resultado_ultima_gestion AS STRING)              AS resultado_ultima_gestion,
# MAGIC   CAST(round(ca.saldo_capital / tc.tasa_venta, 2) AS DECIMAL(18,2)) AS saldo_capital_usd,
# MAGIC   CASE WHEN ca.dias_mora = 0   THEN 'Al dia'
# MAGIC        WHEN ca.dias_mora <= 30 THEN '1-30'
# MAGIC        WHEN ca.dias_mora <= 60 THEN '31-60'
# MAGIC        WHEN ca.dias_mora <= 90 THEN '61-90'
# MAGIC        ELSE 'Mas de 90' END                                AS tramo_mora,
# MAGIC   CASE WHEN ca.dias_mora > 30 THEN 1 ELSE 0 END            AS es_mora_30
# MAGIC FROM cartera ca
# MAGIC LEFT JOIN dim_tipo_cambio_gld tc
# MAGIC   ON  tc.moneda = ca.moneda
# MAGIC   AND ca.fecha_corte >= tc.vigente_desde
# MAGIC   AND (tc.vigente_hasta IS NULL OR ca.fecha_corte < tc.vigente_hasta);
# MAGIC ```
# MAGIC > Si el update falla con `UNRESOLVED_ROUTINE`, falta la clave `politicas` en Configuration o las funciones no se crearon en ese esquema (paso 5).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Probarlas: entra y sal de los grupos ▶️ 🖱️
# MAGIC Corre la celda de abajo **después de cada cambio de grupo** (Settings → Identity and access → Groups → el grupo → **Add members**).
# MAGIC Un cambio de grupo tarda **unos minutos** en verse en las consultas (en la prueba de esta guía, cerca de 4). Las tres primeras
# MAGIC columnas te dicen en qué grupos te ve Databricks ahora: si todavía no dicen lo que esperas, espera y vuelve a correrla.
# MAGIC Mientras tanto puedes avanzar con el paso 8 y volver.
# MAGIC
# MAGIC | Ronda | Haz esto (Settings → Groups) | Deberías ver |
# MAGIC |---|---|---|
# MAGIC | 1 | Nada: no estás en ningún grupo | **0 filas**: el row filter no te deja ver nada |
# MAGIC | 2 | Agrégate a `cartera_peru` | Solo **Perú**, con `id_cliente` vacío (la máscara) |
# MAGIC | 3 | Agrégate a `cobranzas` | Perú, ahora **con** `id_cliente` |
# MAGIC | 4 | Agrégate a `cartera_todos` | Los **tres países** |

# COMMAND ----------

display(spark.sql(f"""
  SELECT is_account_group_member('cartera_peru') AS peru, is_account_group_member('cobranzas') AS cobranzas,
         is_account_group_member('cartera_todos') AS todos,
         pais, count(*) AS creditos, count(id_cliente) AS creditos_con_id_cliente
  FROM {t('cartera_creditos_gld')}
  GROUP BY ALL ORDER BY pais"""))

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ **Míralo en la UI:** Catalog Explorer → `cartera_creditos_gld` → **Overview**: la tabla muestra el row filter y la columna
# MAGIC `id_cliente`, su máscara.
# MAGIC
# MAGIC **Pregunta:** tu Genie Agent de la Sesión 20.2 usa `cartera_creditos_gld`. ¿Qué responde ahora si un analista de Perú le pregunta
# MAGIC por el saldo total de la cartera?
# MAGIC <details><summary>Ver respuesta</summary>El saldo <b>de Perú</b>. Genie no tiene permisos propios: consulta con la identidad de quien pregunta, y el row filter se aplica igual que en este notebook. Por eso el gobierno se pone en la tabla y no en cada consumidor: Genie, un dashboard, un notebook o Power BI ven exactamente lo mismo.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 — Una vista para BI: la política viaja con la tabla ▶️
# MAGIC `analistas_bi` no necesita el detalle por crédito: recibe una vista agregada por país y tramo, sin `id_cliente`, y `SELECT` solo
# MAGIC sobre ella. Lo interesante: el row filter de la cartera **se sigue aplicando a través de la vista**.

# COMMAND ----------

# Fuente: lakeflow/gobierno/vista_cartera_bi.sql (generado: no editar aquí)
sql = f"""
-- Sesión 21 · Paso 8 · La política viaja con la tabla: una vista para BI sobre la cartera gobernada.
-- BI no necesita el detalle por crédito: recibe una vista agregada por país y tramo, sin id_cliente, y SELECT solo sobre ella.
-- El row filter de cartera_creditos_gld se sigue aplicando a través de la vista, con la identidad de quien consulta:
-- un analista que no está en cartera_peru ni en cartera_todos ve la vista vacía. Una vista no es una puerta trasera.
CREATE OR REPLACE VIEW {CATALOGO}.{ESQUEMA}.v_cartera_bi
COMMENT 'Cartera agregada por país y tramo de mora para BI. Sin id_cliente ni números de crédito'
AS SELECT
  pais,
  tramo_mora,
  count(*)                      AS creditos,
  sum(saldo_capital_usd)        AS saldo_capital_usd,
  sum(es_mora_30)               AS creditos_mora_30
FROM {CATALOGO}.{ESQUEMA}.cartera_creditos_gld
GROUP BY pais, tramo_mora
"""
spark.sql(sql)

# COMMAND ----------

spark.sql(f"GRANT SELECT ON VIEW {t('v_cartera_bi')} TO `analistas_bi`")
display(spark.sql(f"SELECT * FROM {t('v_cartera_bi')} ORDER BY pais, tramo_mora"))
# Sales de cartera_peru y cartera_todos y vuelve a correr la celda: la vista queda vacía. Una vista no es una puerta trasera

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 3 · Linaje y auditoría
# MAGIC ## Paso 9 — Linaje: de dónde sale cada tabla ▶️ 🖱️
# MAGIC Unity Catalog registra solo, sin que escribas nada, qué tabla se construyó a partir de cuál. Lo ves de dos formas:
# MAGIC 1. 🖱️ Catalog Explorer → `cartera_creditos_gld` → pestaña **Lineage** → **See lineage graph**: hacia atrás llegas hasta Bronze.
# MAGIC 2. ▶️ Con SQL, en la tabla de sistema `system.access.table_lineage`:

# COMMAND ----------

display(spark.sql(f"""
  SELECT source_table_full_name AS lee_de, target_table_full_name AS escribe_en, entity_type AS quien, max(event_date) AS ultima_vez
  FROM system.access.table_lineage
  WHERE target_table_full_name = '{t('cartera_creditos_gld')}' AND event_date >= current_date() - 30
  GROUP BY ALL ORDER BY lee_de"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** mañana cambia el formato de `pagos_slv`. ¿Cómo sabes qué tablas Gold y qué dashboards se pueden romper?
# MAGIC <details><summary>Ver respuesta</summary>Mirando el linaje <b>hacia adelante</b>: <code>WHERE source_table_full_name = '…pagos_slv'</code>. Es el análisis de impacto: antes de cambiar algo, sabes a quién avisar. En el grafo de Catalog Explorer es el lado derecho.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 10 — Auditoría: quién dio qué permiso ▶️
# MAGIC Cada `GRANT`, `REVOKE` y lectura queda en `system.access.audit`. **Llega con retraso** (de minutos a un par de horas): los permisos
# MAGIC que diste hoy pueden no aparecer todavía. Vuelve a correr la celda al final de la clase.

# COMMAND ----------

display(spark.sql(f"""
  SELECT event_time, user_identity.email AS quien, action_name AS accion,
         request_params.securable_full_name AS objeto, request_params.changes AS cambios
  FROM system.access.audit
  WHERE event_date >= current_date() - 1
    AND service_name = 'unityCatalog' AND action_name = 'updatePermissions'
    AND request_params.securable_full_name LIKE '{CATALOGO}.{ESQUEMA}.%'
  ORDER BY event_time DESC LIMIT 20"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ▶️ BLOQUE 4 · Secretos
# MAGIC Hasta aquí controlaste **quién ve el dato**. Falta **quién usa la credencial**: la contraseña de la base Azure SQL de Lending que
# MAGIC usa el extractor de la Sesión 9 no puede estar escrita en un notebook ni en el repo.
# MAGIC
# MAGIC | Dónde vive el secreto | Cuándo |
# MAGIC |---|---|
# MAGIC | Secret scope **respaldado por Databricks** | Free Edition y equipos pequeños: Databricks guarda el valor cifrado |
# MAGIC | Secret scope **respaldado por Azure Key Vault** | Empresa en Azure: el valor vive en Key Vault, lo rota seguridad, Databricks solo lo lee |
# MAGIC
# MAGIC En los dos casos el código es el mismo: `dbutils.secrets.get(scope, key)`. Cambia dónde se guarda, no cómo se usa.
# MAGIC
# MAGIC ## Paso 11 — Crear el secret scope 🖥️
# MAGIC Desde **tu terminal** (no desde el notebook: escribir el valor en una celda lo dejaría en el historial):
# MAGIC ```bash
# MAGIC databricks secrets create-scope wizard_bank
# MAGIC databricks secrets put-secret wizard_bank azure_sql_password      # pide el valor; usa uno de práctica, nunca uno real
# MAGIC databricks secrets list-secrets wizard_bank                       # lista las claves, nunca los valores
# MAGIC ```
# MAGIC Y quién puede leerlo, igual que con las tablas: por grupo.
# MAGIC ```bash
# MAGIC databricks secrets put-acl wizard_bank data_science READ
# MAGIC databricks secrets list-acls wizard_bank
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 12 — Usarlo sin verlo ▶️

# COMMAND ----------

print([s.key for s in dbutils.secrets.list("wizard_bank")])

clave = dbutils.secrets.get(scope="wizard_bank", key="azure_sql_password")
print("La clave es:", clave)              # Databricks la reemplaza por [REDACTED]: nunca aparece en la salida
print("Largo:", len(clave))               # el valor está ahí, se puede usar

# Así la usaría el extractor de la Sesión 9: la credencial entra a la conexión sin pasar por el código ni por el repo
url_jdbc = "jdbc:sqlserver://<servidor>.database.windows.net:1433;database=wizard_bank"
opciones = {"url": url_jdbc, "user": "etl_lending", "password": clave}
print({k: v for k, v in opciones.items() if k != "password"})

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** si `print(clave)` muestra `[REDACTED]`, ¿la clave está segura en el notebook?
# MAGIC <details><summary>Ver respuesta</summary>Está mejor que escrita, pero <b>no</b> es una barrera: un <code>print(clave[::-1])</code> o escribirla en una tabla la sacan. El redactado evita accidentes en la salida; la protección real es el ACL del scope (quién puede hacer <code>get</code>) y que el valor nunca esté en el código ni en Git. Por eso en el paso 11 el valor se escribe en la terminal y no en una celda.</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cierre — Checklist
# MAGIC | Entregable | Listo si… |
# MAGIC |---|---|
# MAGIC | 5 grupos | Existen y `SHOW GRANTS` muestra el acceso de cada uno |
# MAGIC | Row filter y column mask en `cartera_creditos_gld` | Las cuatro rondas del paso 7 dieron lo esperado |
# MAGIC | `v_cartera_bi` | `analistas_bi` tiene `SELECT` sobre la vista y no sobre la tabla |
# MAGIC | Linaje | Sabes qué tablas alimentan la cartera y a quién afecta un cambio en `pagos_slv` |
# MAGIC | Secret scope `wizard_bank` | `dbutils.secrets.get` devuelve la clave y la salida muestra `[REDACTED]` |
# MAGIC
# MAGIC - [ ] Sé explicar por qué se necesitan `USE CATALOG`, `USE SCHEMA` y `SELECT`, y por qué se dan a grupos
# MAGIC - [ ] Sé cuándo usar `GRANT`, row filter, column mask o una vista
# MAGIC - [ ] Sé por qué las políticas de una tabla del pipeline se declaran en su código
# MAGIC - [ ] Sé dónde guardar una credencial y cómo usarla sin escribirla
# MAGIC
# MAGIC **Siguiente: Sesión 21.2.** Llevas el pipeline de `dev` a `prod` con el bundle y lo automatizas con GitHub Actions.
# MAGIC
# MAGIC ## 🧹 Limpieza
# MAGIC Al terminar el módulo: sácate de los grupos o bórralos (Settings → Groups) y `databricks secrets delete-scope wizard_bank`.
# MAGIC Si quieres volver a la cartera sin políticas, restaura `transformations/14_gold_cartera.sql` a la versión de la Sesión 20.2.
