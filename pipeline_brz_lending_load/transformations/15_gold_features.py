# Sesión 20 · Paso 8 · Feature tables para Data Science, dentro del pipeline.
# Una tabla de Unity Catalog con PRIMARY KEY es una feature table; si la llave incluye una columna TIMESERIES,
# Data Science puede unirla "punto en el tiempo": cada etiqueta ve solo lo que se sabía en su fecha.
#
#   features_cliente_mes_gld     → modelo de MORA: comportamiento de pago, cobranza y uso, una fila por cliente y mes
#   features_app_cliente_dia_gld → modelo de PROPENSIÓN a solicitar: uso de la app, una fila por cliente y día
#
# Regla de oro: las features salen de datos que guardan HISTORIA (eventos con fecha y dimensiones SCD2), nunca de
# una foto de hoy. estado_cuota de cuotas_slv es SCD tipo 1 (solo el último estado): una cuota que hoy figura
# "Pagada" pudo estar impaga al cierre de marzo. Por eso el atraso se reconstruye con la FECHA de cada pago.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------- features_cliente_mes_gld ----------

ESQUEMA_FEATURES_MES = """
  id_cliente                    BIGINT        NOT NULL COMMENT 'Llave: cliente',
  disponible_desde              TIMESTAMP     NOT NULL COMMENT 'Desde cuándo se conoce la fila: el cierre del mes (día 1 del mes siguiente, 00:00)',
  mes                           DATE                   COMMENT 'Mes que resume la fila, como su día 1',
  id_pais                       SMALLINT               COMMENT 'País del cliente al cierre del mes: 1 Perú, 2 Colombia, 3 Bolivia',
  nivel_riesgo                  STRING                 COMMENT 'Nivel de riesgo A (mejor) a E vigente al cierre del mes (clientes_slv, SCD2)',
  score_interno                 SMALLINT               COMMENT 'Score interno vigente al cierre del mes',
  situacion_laboral             STRING                 COMMENT 'Situación laboral vigente al cierre del mes',
  ingreso_mensual_declarado     DECIMAL(14,2)          COMMENT 'Ingreso declarado vigente al cierre del mes, en moneda local',
  cuotas_vencen_mes             BIGINT                 COMMENT 'Cuotas de sus créditos que vencían en el mes',
  cuotas_a_tiempo_mes           BIGINT                 COMMENT 'De esas, las pagadas hasta su fecha de vencimiento',
  cuotas_impagas_al_cierre_mes  BIGINT                 COMMENT 'De esas, las que seguían sin pagar al cierre del mes',
  max_dias_atraso_mes           INT                    COMMENT 'Mayor atraso en días de las cuotas del mes: hasta el pago, o hasta el cierre si seguía impaga',
  n_pagos_mes                   BIGINT                 COMMENT 'Pagos registrados en el mes',
  n_gestiones_mes               BIGINT                 COMMENT 'Gestiones de cobranza recibidas en el mes',
  n_solicitudes_mes             BIGINT                 COMMENT 'Solicitudes de préstamo hechas en el mes',
  n_sesiones_app_mes            BIGINT                 COMMENT 'Sesiones en la app en el mes',
  cuotas_impagas_3m             BIGINT                 COMMENT 'Cuotas impagas al cierre, sumadas en los 3 meses calendario que terminan en mes',
  pct_cuotas_a_tiempo_3m        DOUBLE                 COMMENT 'Cuotas a tiempo / cuotas que vencían, en los 3 meses calendario que terminan en mes. NULL si no vencía ninguna',
  n_gestiones_3m                BIGINT                 COMMENT 'Gestiones de cobranza en los 3 meses calendario que terminan en mes',
  n_sesiones_app_3m             BIGINT                 COMMENT 'Sesiones en la app en los 3 meses calendario que terminan en mes',
  CONSTRAINT pk_features_cliente_mes_gld PRIMARY KEY (id_cliente, disponible_desde TIMESERIES)
"""


@dp.materialized_view(
    name="features_cliente_mes_gld",
    comment=("Feature table para el modelo de mora: comportamiento de pago, cobranza y uso de cada cliente, una fila por "
             "cliente y mes cerrado. Llave (id_cliente, disponible_desde TIMESERIES) para joins punto en el tiempo"),
    schema=ESQUEMA_FEATURES_MES,
    cluster_by=["id_cliente"],
)
@dp.expect_or_fail("impagas_coherentes", "cuotas_impagas_al_cierre_mes <= cuotas_vencen_mes")
@dp.expect("cliente_con_perfil", "nivel_riesgo IS NOT NULL")
def features_cliente_mes_gld():
    # numero_credito del legado = id_desembolso de Lending: así cada cuota, pago y gestión encuentra a su cliente
    creditos = spark.read.table("desembolsos_slv").select(F.col("id_desembolso").alias("numero_credito"), "id_cliente")
    pagos = spark.read.table("pagos_slv")
    mes_de = lambda c: F.trunc(c, "month")

    # 1) Cada cuota con su PRIMER pago: ¿estaba pagada al cierre del mes en que vencía? ¿a tiempo? ¿cuánto atraso?
    primer_pago = pagos.groupBy("numero_credito", "numero_cuota").agg(F.min("fecha_pago").alias("fecha_pago"))
    cuotas = (spark.read.table("cuotas_slv").select("numero_credito", "numero_cuota", "fecha_vencimiento")
              .join(creditos, "numero_credito")
              .join(primer_pago, ["numero_credito", "numero_cuota"], "left")
              .withColumn("mes", mes_de("fecha_vencimiento"))
              .withColumn("cierre", F.add_months("mes", 1).cast("timestamp"))
              .withColumn("pagada_al_cierre", F.col("fecha_pago").isNotNull() & (F.col("fecha_pago") < F.col("cierre")))
              .withColumn("a_tiempo", F.col("fecha_pago").isNotNull() & (F.to_date("fecha_pago") <= F.col("fecha_vencimiento")))
              .withColumn("dias_atraso", F.greatest(F.lit(0), F.datediff(
                  F.when(F.col("pagada_al_cierre"), F.col("fecha_pago")).otherwise(F.col("cierre")), F.col("fecha_vencimiento")))))
    cuotas_mes = cuotas.groupBy("id_cliente", "mes").agg(
        F.count("*").alias("cuotas_vencen_mes"),
        F.count_if("a_tiempo").alias("cuotas_a_tiempo_mes"),
        F.count_if(~F.col("pagada_al_cierre")).alias("cuotas_impagas_al_cierre_mes"),
        F.max("dias_atraso").alias("max_dias_atraso_mes"))

    # 2) Eventos con fecha: pagos, gestiones, solicitudes y sesiones de la app, contados por mes
    def por_mes(df, col_fecha, nombre):
        return df.groupBy("id_cliente", mes_de(col_fecha).alias("mes")).agg(F.count("*").alias(nombre))

    pagos_mes = por_mes(pagos.join(creditos, "numero_credito"), "fecha_pago", "n_pagos_mes")
    gestiones_mes = por_mes(spark.read.table("gestiones_cobranza_slv").join(creditos, "numero_credito"), "fecha_gestion", "n_gestiones_mes")
    solicitudes_mes = por_mes(spark.read.table("solicitudes_prestamo_slv"), "fecha_hora_solicitud", "n_solicitudes_mes")
    sesiones_mes = por_mes(spark.read.table("sesiones_app_slv").filter("id_cliente IS NOT NULL"), "inicio_sesion", "n_sesiones_app_mes")

    # 3) Una fila por cliente y mes con actividad, SOLO de meses cerrados: el mes en curso todavía no se conoce completo
    corte = pagos.agg(F.max("fecha_pago").alias("fecha_corte"))
    partes = [cuotas_mes, pagos_mes, gestiones_mes, solicitudes_mes, sesiones_mes]
    base = partes[0].select("id_cliente", "mes")
    for p in partes[1:]:
        base = base.unionByName(p.select("id_cliente", "mes"))
    mensual = base.distinct()
    for p in partes:
        mensual = mensual.join(p, ["id_cliente", "mes"], "left")
    mensual = (mensual.fillna(0)
               .withColumn("disponible_desde", F.add_months("mes", 1).cast("timestamp"))
               .crossJoin(corte).filter(F.col("disponible_desde") <= F.col("fecha_corte"))
               .withColumn("_mes_num", F.year("mes") * 12 + F.month("mes")))

    # 4) Ventanas de 3 meses CALENDARIO (rangeBetween sobre el número de mes, no sobre filas)
    w_3m = Window.partitionBy("id_cliente").orderBy("_mes_num").rangeBetween(-2, 0)
    mensual = (mensual
               .withColumn("cuotas_impagas_3m", F.sum("cuotas_impagas_al_cierre_mes").over(w_3m))
               .withColumn("pct_cuotas_a_tiempo_3m", F.sum("cuotas_a_tiempo_mes").over(w_3m)
                           / F.nullif(F.sum("cuotas_vencen_mes").over(w_3m), F.lit(0)))
               .withColumn("n_gestiones_3m", F.sum("n_gestiones_mes").over(w_3m))
               .withColumn("n_sesiones_app_3m", F.sum("n_sesiones_app_mes").over(w_3m)))

    # 5) Perfil del cliente VIGENTE AL CIERRE del mes: join punto en el tiempo contra la SCD2 de Silver
    clientes = spark.read.table("clientes_slv").select(
        F.col("id_cliente").alias("c_id"), "id_pais", "nivel_riesgo", "score_interno", "situacion_laboral",
        "ingreso_mensual_declarado",
        F.col("__START_AT.fecha_version").alias("c_desde"), F.col("__END_AT.fecha_version").alias("c_hasta"),
        F.row_number().over(Window.partitionBy("id_cliente").orderBy("__START_AT")).alias("_n_version"))
    # Igual que dim_cliente_gld: la PRIMERA versión cubre desde siempre. Sin esto, un mes con actividad anterior a la
    # primera fecha_version del cliente queda sin perfil (pasa con los datos sintéticos de cobranzas)
    clientes = clientes.withColumn("c_desde", F.when(F.col("_n_version") == 1, F.lit("1900-01-01").cast("timestamp"))
                                              .otherwise(F.col("c_desde"))).drop("_n_version")
    vigente_al_cierre = ((F.col("id_cliente") == F.col("c_id"))
                         & (F.col("c_desde") < F.col("disponible_desde"))
                         & (F.col("c_hasta").isNull() | (F.col("c_hasta") >= F.col("disponible_desde"))))
    return (mensual.join(clientes, vigente_al_cierre, "left")
            .select(
                F.col("id_cliente").cast("bigint"), "disponible_desde", "mes",
                F.col("id_pais").cast("smallint"), "nivel_riesgo", F.col("score_interno").cast("smallint"),
                "situacion_laboral", F.col("ingreso_mensual_declarado").cast("decimal(14,2)"),
                F.col("cuotas_vencen_mes").cast("bigint"), F.col("cuotas_a_tiempo_mes").cast("bigint"),
                F.col("cuotas_impagas_al_cierre_mes").cast("bigint"), F.col("max_dias_atraso_mes").cast("int"),
                F.col("n_pagos_mes").cast("bigint"), F.col("n_gestiones_mes").cast("bigint"),
                F.col("n_solicitudes_mes").cast("bigint"), F.col("n_sesiones_app_mes").cast("bigint"),
                F.col("cuotas_impagas_3m").cast("bigint"), F.col("pct_cuotas_a_tiempo_3m").cast("double"),
                F.col("n_gestiones_3m").cast("bigint"), F.col("n_sesiones_app_3m").cast("bigint")))


# ---------- features_app_cliente_dia_gld ----------

ESQUEMA_FEATURES_APP = """
  id_cliente                  BIGINT    NOT NULL COMMENT 'Llave: cliente',
  disponible_desde            TIMESTAMP NOT NULL COMMENT 'Desde cuándo se conoce la fila: el fin del día (00:00 del día siguiente)',
  fecha                       DATE               COMMENT 'Día que resume la fila',
  vistas_dia                  BIGINT             COMMENT 'Ofertas vistas en el día',
  clicks_dia                  BIGINT             COMMENT 'Clicks en ofertas en el día',
  simulaciones_dia            BIGINT             COMMENT 'Simulaciones de crédito en el día',
  solicitudes_iniciadas_dia   BIGINT             COMMENT 'Solicitudes iniciadas desde la app en el día',
  vistas_7d                   BIGINT             COMMENT 'Ofertas vistas en los 7 días que terminan en fecha',
  clicks_7d                   BIGINT             COMMENT 'Clicks en los 7 días que terminan en fecha',
  simulaciones_7d             BIGINT             COMMENT 'Simulaciones en los 7 días que terminan en fecha',
  dias_activo_30d             BIGINT             COMMENT 'Días con al menos un evento en los 30 días que terminan en fecha',
  ctr_30d                     DOUBLE             COMMENT 'Clicks / vistas en los 30 días que terminan en fecha. NULL si no vio ofertas',
  monto_simulado_max_30d      DOUBLE             COMMENT 'Mayor monto simulado en los 30 días que terminan en fecha, en moneda local',
  CONSTRAINT pk_features_app_cliente_dia_gld PRIMARY KEY (id_cliente, disponible_desde TIMESERIES)
"""


@dp.materialized_view(
    name="features_app_cliente_dia_gld",
    comment=("Feature table para el modelo de propensión a solicitar: uso de la app de cada cliente, una fila por cliente "
             "y día con actividad, con ventanas de 7 y 30 días. Llave (id_cliente, disponible_desde TIMESERIES)"),
    schema=ESQUEMA_FEATURES_APP,
    cluster_by=["id_cliente"],
)
@dp.expect_or_fail("ventana_coherente", "dias_activo_30d BETWEEN 1 AND 30")   # si falla, la ventana está mal armada
def features_app_cliente_dia_gld():
    diario = (spark.read.table("eventos_app_slv").filter("id_cliente IS NOT NULL")
              .groupBy("id_cliente", F.col("fecha_evento").alias("fecha"))
              .agg(F.count_if(F.col("tipo_evento") == "oferta_vista").alias("vistas_dia"),
                   F.count_if(F.col("tipo_evento") == "oferta_click").alias("clicks_dia"),
                   F.count_if(F.col("tipo_evento") == "simulacion_realizada").alias("simulaciones_dia"),
                   F.count_if(F.col("tipo_evento") == "solicitud_iniciada").alias("solicitudes_iniciadas_dia"),
                   F.max("monto_simulado").alias("_monto_simulado_max"))
              .withColumn("_dia_num", F.datediff("fecha", F.lit("1970-01-01"))))

    # Ventanas de días CALENDARIO: rangeBetween sobre el número de día (un día sin actividad no es una fila)
    w_7d = Window.partitionBy("id_cliente").orderBy("_dia_num").rangeBetween(-6, 0)
    w_30d = Window.partitionBy("id_cliente").orderBy("_dia_num").rangeBetween(-29, 0)
    return (diario
            .withColumn("disponible_desde", F.date_add("fecha", 1).cast("timestamp"))
            .withColumn("vistas_7d", F.sum("vistas_dia").over(w_7d))
            .withColumn("clicks_7d", F.sum("clicks_dia").over(w_7d))
            .withColumn("simulaciones_7d", F.sum("simulaciones_dia").over(w_7d))
            .withColumn("dias_activo_30d", F.count("*").over(w_30d))
            .withColumn("ctr_30d", F.sum("clicks_dia").over(w_30d) / F.nullif(F.sum("vistas_dia").over(w_30d), F.lit(0)))
            .withColumn("monto_simulado_max_30d", F.max("_monto_simulado_max").over(w_30d))
            .select(F.col("id_cliente").cast("bigint"), "disponible_desde", "fecha",
                    *[F.col(c).cast("bigint") for c in ["vistas_dia", "clicks_dia", "simulaciones_dia", "solicitudes_iniciadas_dia",
                                                         "vistas_7d", "clicks_7d", "simulaciones_7d", "dias_activo_30d"]],
                    F.col("ctr_30d").cast("double"), F.col("monto_simulado_max_30d").cast("double")))
