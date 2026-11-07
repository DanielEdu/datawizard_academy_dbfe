# Paso 10 · Silver de Cobranzas (fuente legado). Tres tipos de tabla en el mismo sistema:
#   cuotas    → su estado cambia (Pendiente → Pagada / Vencida): AUTO CDC tipo 1
#   pagos     → hecho que no cambia, pero el legado a veces REENVÍA filas: AUTO CDC tipo 1 como deduplicador por clave
#   gestiones → hecho que no cambia: streaming table simple (append) + expectations
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------- cuotas ----------

@dp.temporary_view(name="cuotas_tipadas")
@dp.expect_or_drop("clave_presente", "id_cuota IS NOT NULL")
@dp.expect_or_drop("estado_valido", "estado_cuota IN ('Pendiente','Pagada','Vencida')")
@dp.expect("cuota_cuadra", "abs(monto_cuota - (monto_capital + monto_interes)) <= 0.01")
def cuotas_tipadas():
    return (spark.readStream.table("cuotas_brz")
            .selectExpr(
                "try_cast(id_cuota AS BIGINT) AS id_cuota",
                "try_cast(numero_credito AS BIGINT) AS numero_credito",
                "try_cast(numero_cuota AS SMALLINT) AS numero_cuota",
                "try_cast(fecha_vencimiento AS DATE) AS fecha_vencimiento",
                "try_cast(monto_cuota AS DECIMAL(12,2)) AS monto_cuota",
                "try_cast(monto_capital AS DECIMAL(12,2)) AS monto_capital",
                "try_cast(monto_interes AS DECIMAL(12,2)) AS monto_interes",
                "estado_cuota",
                "coalesce(try_cast(fecha_modificacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP)) AS fecha_version",
                "_ingestado_en"))


dp.create_streaming_table(name="cuotas_slv", comment="Estado vigente de cada cuota", cluster_by=["numero_credito"])

dp.create_auto_cdc_flow(
    target="cuotas_slv", source="cuotas_tipadas",
    keys=["id_cuota"],
    sequence_by=F.struct("fecha_version", "_ingestado_en"),
    stored_as_scd_type=1,
    except_column_list=["_ingestado_en"],
)

# ---------- pagos ----------

@dp.temporary_view(name="pagos_tipados")
@dp.expect_or_drop("clave_presente", "id_pago IS NOT NULL AND numero_credito IS NOT NULL")
@dp.expect_or_drop("monto_positivo", "monto_pagado > 0")
def pagos_tipados():
    return (spark.readStream.table("pagos_brz")
            .selectExpr(
                "try_cast(id_pago AS BIGINT) AS id_pago",
                "try_cast(numero_credito AS BIGINT) AS numero_credito",
                "try_cast(numero_cuota AS SMALLINT) AS numero_cuota",
                "try_cast(fecha_pago AS TIMESTAMP) AS fecha_pago",
                "try_cast(monto_pagado AS DECIMAL(12,2)) AS monto_pagado",
                "medio_pago",
                "_ingestado_en"))


dp.create_streaming_table(name="pagos_slv", comment="Pagos únicos por id_pago (los reenvíos del legado no se duplican)")

dp.create_auto_cdc_flow(
    target="pagos_slv", source="pagos_tipados",
    keys=["id_pago"],
    sequence_by="_ingestado_en",
    stored_as_scd_type=1,
    except_column_list=["_ingestado_en"],
)

# ---------- gestiones ----------

@dp.table(name="gestiones_cobranza_slv", comment="Gestiones de cobranza válidas (append)")
@dp.expect_or_drop("clave_presente", "id_gestion IS NOT NULL AND numero_credito IS NOT NULL")
@dp.expect("resultado_informado", "resultado IS NOT NULL")
def gestiones_cobranza_slv():
    return (spark.readStream.table("gestiones_cobranza_brz")    # streaming simple: cada fila nueva se agrega
            .selectExpr(
                "try_cast(id_gestion AS BIGINT) AS id_gestion",
                "try_cast(numero_credito AS BIGINT) AS numero_credito",
                "try_cast(fecha_gestion AS TIMESTAMP) AS fecha_gestion",
                "tipo_gestion", "resultado",
                "try_cast(dias_mora_al_momento AS SMALLINT) AS dias_mora_al_momento",
                "gestor"))
