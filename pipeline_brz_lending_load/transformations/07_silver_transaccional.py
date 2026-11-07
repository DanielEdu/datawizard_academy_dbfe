# Paso 8 · Silver transaccional: AUTO CDC tipo 1 (el MERGE, sin escribir el MERGE).
# Ofertas, solicitudes y desembolsos CAMBIAN de estado (una solicitud pasa de Iniciada a Aprobada).
# En Silver queda una fila por clave con su último estado. Además, de la MISMA vista de solicitudes
# sale un segundo flow: el historial de estados (SCD tipo 2 que solo versiona el estado).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CDF = {"delta.enableChangeDataFeed": "true"}   # Gold puede leer solo lo que cambió

# ---------- ofertas ----------

@dp.temporary_view(name="ofertas_tipadas")
@dp.expect_or_drop("clave_presente", "id_oferta IS NOT NULL AND id_cliente IS NOT NULL")
@dp.expect_or_drop("monto_positivo", "monto_ofertado > 0")
def ofertas_tipadas():
    return (spark.readStream.table("ofertas_preaprobadas_brz")
            .selectExpr(
                "try_cast(id_oferta AS BIGINT) AS id_oferta",
                "try_cast(id_cliente AS BIGINT) AS id_cliente",
                "try_cast(id_producto AS INT) AS id_producto",
                "try_cast(id_campania AS INT) AS id_campania",
                "try_cast(monto_ofertado AS DECIMAL(14,2)) AS monto_ofertado",
                "try_cast(plazo_meses_ofertado AS SMALLINT) AS plazo_meses_ofertado",
                "try_cast(tasa_ofertada AS DECIMAL(6,3)) AS tasa_ofertada",
                "motor_asignacion",
                "try_cast(fecha_generacion AS TIMESTAMP) AS fecha_generacion",
                "try_cast(fecha_vigencia_fin AS DATE) AS fecha_vigencia_fin",
                "estado_oferta",
                "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
                "try_cast(fecha_generacion AS TIMESTAMP)) AS fecha_version",
                "_ingestado_en"))


dp.create_streaming_table(name="ofertas_preaprobadas_slv", comment="Último estado de cada oferta",
                          table_properties=CDF)

dp.create_auto_cdc_flow(
    target="ofertas_preaprobadas_slv", source="ofertas_tipadas",
    keys=["id_oferta"],                                       # la condición ON del MERGE
    sequence_by=F.struct("fecha_version", "_ingestado_en"),   # si llegan dos versiones, gana la más reciente
    stored_as_scd_type=1,                                     # tipo 1: se sobrescribe, sin historia
    except_column_list=["_ingestado_en"],
)

# ---------- solicitudes ----------

@dp.temporary_view(name="solicitudes_tipadas")
@dp.expect_or_drop("clave_presente", "id_solicitud IS NOT NULL AND id_cliente IS NOT NULL")
@dp.expect_or_drop("monto_positivo", "monto_solicitado > 0")
@dp.expect_or_drop("estado_valido", "estado_solicitud IN ('Iniciada','En evaluacion','Aprobada','Rechazada','Desistida')")
def solicitudes_tipadas():
    return (spark.readStream.table("solicitudes_prestamo_brz")
            .selectExpr(
                "try_cast(id_solicitud AS BIGINT) AS id_solicitud",
                "try_cast(id_oferta AS BIGINT) AS id_oferta",
                "try_cast(id_cliente AS BIGINT) AS id_cliente",
                "try_cast(id_producto AS INT) AS id_producto",
                "canal",
                "try_cast(monto_solicitado AS DECIMAL(14,2)) AS monto_solicitado",
                "try_cast(plazo_meses_solicitado AS SMALLINT) AS plazo_meses_solicitado",
                "try_cast(fecha_hora_solicitud AS TIMESTAMP) AS fecha_hora_solicitud",
                "estado_solicitud",
                "try_cast(score_evaluacion AS SMALLINT) AS score_evaluacion",
                "decision_motor",
                "try_cast(monto_aprobado AS DECIMAL(14,2)) AS monto_aprobado",
                "try_cast(tasa_aprobada AS DECIMAL(6,3)) AS tasa_aprobada",
                "motivo_rechazo",
                "try_cast(fecha_hora_resolucion AS TIMESTAMP) AS fecha_hora_resolucion",
                "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
                "try_cast(fecha_hora_solicitud AS TIMESTAMP)) AS fecha_version",
                "_ingestado_en"))


dp.create_streaming_table(name="solicitudes_prestamo_slv", comment="Último estado de cada solicitud",
                          table_properties=CDF, cluster_by=["fecha_hora_solicitud"])

dp.create_auto_cdc_flow(
    target="solicitudes_prestamo_slv", source="solicitudes_tipadas",
    keys=["id_solicitud"],
    sequence_by=F.struct("fecha_version", "_ingestado_en"),
    stored_as_scd_type=1,
    except_column_list=["_ingestado_en"],
)

# Mismo origen, otro destino: el historial de estados. SCD tipo 2 que abre versión SOLO si cambia el estado.
dp.create_streaming_table(name="solicitudes_estados_slv", comment="Historial de estados de cada solicitud (SCD tipo 2 sobre estado_solicitud)")

dp.create_auto_cdc_flow(
    target="solicitudes_estados_slv", source="solicitudes_tipadas",
    keys=["id_solicitud"],
    sequence_by=F.struct("fecha_version", "_ingestado_en"),
    stored_as_scd_type=2,
    track_history_column_list=["estado_solicitud"],
    except_column_list=["_ingestado_en"],
)

# ---------- desembolsos ----------

@dp.temporary_view(name="desembolsos_tipados")
@dp.expect_or_drop("clave_presente", "id_desembolso IS NOT NULL AND id_solicitud IS NOT NULL")
@dp.expect_or_drop("monto_positivo", "monto_desembolsado > 0")
@dp.expect("moneda_conocida", "moneda IN ('PEN','COP','BOB')")
def desembolsos_tipados():
    return (spark.readStream.table("desembolsos_brz")
            .selectExpr(
                "try_cast(id_desembolso AS BIGINT) AS id_desembolso",
                "try_cast(id_solicitud AS BIGINT) AS id_solicitud",
                "try_cast(id_cliente AS BIGINT) AS id_cliente",
                "try_cast(monto_desembolsado AS DECIMAL(14,2)) AS monto_desembolsado",
                "moneda",
                "try_cast(tasa_aplicada AS DECIMAL(6,3)) AS tasa_aplicada",
                "try_cast(plazo_meses AS SMALLINT) AS plazo_meses",
                "try_cast(comision_cobrada AS DECIMAL(12,2)) AS comision_cobrada",
                "cuenta_destino_masked",
                "try_cast(fecha_hora_desembolso AS TIMESTAMP) AS fecha_hora_desembolso",
                "estado_desembolso",
                "referencia_bancaria",
                "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
                "try_cast(fecha_hora_desembolso AS TIMESTAMP)) AS fecha_version",
                "_ingestado_en"))


dp.create_streaming_table(name="desembolsos_slv", comment="Último estado de cada desembolso", table_properties=CDF)

dp.create_auto_cdc_flow(
    target="desembolsos_slv", source="desembolsos_tipados",
    keys=["id_desembolso"],
    sequence_by=F.struct("fecha_version", "_ingestado_en"),
    stored_as_scd_type=1,
    except_column_list=["_ingestado_en"],
)
