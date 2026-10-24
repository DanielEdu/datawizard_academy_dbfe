# Paso 7 · Silver de maestras con historia: AUTO CDC tipo 2 (clientes y productos).
# Si un cliente cambia su score o un producto su tasa, para riesgo importa la versión vigente CUANDO
# se aprobó el crédito. SCD tipo 2 no sobrescribe: cierra la versión vieja (__END_AT) y abre otra (__START_AT).
#
# Patrón: temporary view (tipar + expectations) → AUTO CDC.
# AUTO CDC no acepta expectations directamente; la vista intermedia filtra y no ocupa almacenamiento.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------- clientes ----------

@dp.temporary_view(name="clientes_tipados")
@dp.expect_or_drop("clave_presente", "id_cliente IS NOT NULL")
@dp.expect("score_en_rango", "score_interno BETWEEN 300 AND 900")       # solo mide: un score raro no borra al cliente
@dp.expect_or_drop("riesgo_valido", "nivel_riesgo IN ('A','B','C','D','E')")
def clientes_tipados():
    return (spark.readStream.table("clientes_brz")
            .selectExpr(
                "try_cast(id_cliente AS BIGINT) AS id_cliente",
                "try_cast(id_pais AS SMALLINT) AS id_pais",
                "tipo_documento", "numero_documento", "nombres", "apellidos",
                "try_cast(fecha_nacimiento AS DATE) AS fecha_nacimiento",
                "email", "ciudad", "situacion_laboral",
                "try_cast(ingreso_mensual_declarado AS DECIMAL(14,2)) AS ingreso_mensual_declarado",
                "try_cast(score_interno AS SMALLINT) AS score_interno",
                "nivel_riesgo",
                "try_cast(id_campania_captacion AS INT) AS id_campania_captacion",
                "try_cast(fecha_registro AS TIMESTAMP) AS fecha_registro",
                # Orden de versiones: la carga inicial no trae fecha_actualizacion (columna agregada en v2)
                "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP), "
                "try_cast(fecha_registro AS TIMESTAMP)) AS fecha_version",
                "_ingestado_en"))


dp.create_streaming_table(name="clientes_slv", comment="Clientes con historia (SCD tipo 2): una fila por versión",
                          cluster_by=["id_cliente"])

dp.create_auto_cdc_flow(
    target="clientes_slv",
    source="clientes_tipados",
    keys=["id_cliente"],
    sequence_by=F.struct("fecha_version", "_ingestado_en"),   # a igual fecha, gana el archivo más nuevo
    stored_as_scd_type=2,                                     # agrega __START_AT y __END_AT
    track_history_column_list=["ingreso_mensual_declarado", "situacion_laboral", "score_interno", "nivel_riesgo"],
    except_column_list=["_ingestado_en"],
)

# ---------- productos ----------

@dp.temporary_view(name="productos_tipados")
@dp.expect_or_drop("clave_presente", "id_producto IS NOT NULL")
@dp.expect_or_drop("tasa_positiva", "tasa_interes_anual > 0")
def productos_tipados():
    return (spark.readStream.table("productos_prestamo_brz")
            .selectExpr(
                "try_cast(id_producto AS INT) AS id_producto",
                "try_cast(id_pais AS SMALLINT) AS id_pais",
                "nombre_producto", "tipo_producto",
                "try_cast(monto_minimo AS DECIMAL(14,2)) AS monto_minimo",
                "try_cast(monto_maximo AS DECIMAL(14,2)) AS monto_maximo",
                "try_cast(plazo_min_meses AS SMALLINT) AS plazo_min_meses",
                "try_cast(plazo_max_meses AS SMALLINT) AS plazo_max_meses",
                "try_cast(tasa_interes_anual AS DECIMAL(6,3)) AS tasa_interes_anual",
                "try_cast(es_activo AS BOOLEAN) AS es_activo",
                "coalesce(try_cast(fecha_actualizacion AS TIMESTAMP), try_cast(fecha_creacion AS TIMESTAMP)) AS fecha_version",
                "_ingestado_en"))


dp.create_streaming_table(name="productos_slv", comment="Productos con historia de tasa (SCD tipo 2)")

dp.create_auto_cdc_flow(
    target="productos_slv",
    source="productos_tipados",
    keys=["id_producto"],
    sequence_by=F.struct("fecha_version", "_ingestado_en"),
    stored_as_scd_type=2,
    track_history_column_list=["tasa_interes_anual", "monto_minimo", "monto_maximo", "es_activo"],
    except_column_list=["_ingestado_en"],
)
