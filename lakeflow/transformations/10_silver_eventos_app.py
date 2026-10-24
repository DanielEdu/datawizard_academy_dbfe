# Paso 11 · Silver de la App (fuente Kafka): interpretar, deduplicar con watermark y armar sesiones.
# Los eventos NO tienen "último estado": cada evento es único. Los problemas son otros:
#   - payloads corruptos (JSON que no parsea)   → expectation: se descartan y se cuentan
#   - el productor reintenta y duplica mensajes  → dropDuplicatesWithinWatermark sobre id_evento
#   - eventos que llegan tarde                   → watermark sobre el tiempo DEL EVENTO, no de llegada
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Contrato del mensaje (Sesión 11): structs anidados `sesion` y `contexto`
ESQUEMA_EVENTO = (
    "id_evento STRING, tipo_evento STRING, timestamp_evento STRING, id_cliente BIGINT, id_oferta BIGINT, "
    "id_pais INT, canal STRING, "
    "sesion STRUCT<id_sesion: STRING, version_app: STRING, sistema_operativo: STRING, modelo_dispositivo: STRING>, "
    "contexto STRUCT<ubicacion_pantalla: STRING, posicion: INT, tiempo_visible_seg: DOUBLE, "
    "monto_simulado: DOUBLE, plazo_simulado: INT>"
)


@dp.table(name="eventos_app_slv", comment="Eventos de la app, interpretados y sin duplicados",
          cluster_by=["fecha_evento"])
@dp.expect_or_drop("payload_valido", "id_evento IS NOT NULL AND ts_evento IS NOT NULL")
@dp.expect_or_drop("tipo_conocido",
                   "tipo_evento IN ('oferta_vista','oferta_click','simulacion_realizada','solicitud_iniciada','banner_cerrado')")
def eventos_app_slv():
    e = F.from_json(F.col("payload"), ESQUEMA_EVENTO)
    return (spark.readStream.table("eventos_app_brz")
            .select(e.alias("e"), "kafka_offset", "kafka_ts")
            .select(
                "e.id_evento", "e.tipo_evento",
                F.to_timestamp("e.timestamp_evento").alias("ts_evento"),
                F.to_date(F.to_timestamp("e.timestamp_evento")).alias("fecha_evento"),
                "e.id_cliente", "e.id_oferta", "e.id_pais", "e.canal",
                F.col("e.sesion.id_sesion").alias("id_sesion"),
                F.col("e.sesion.sistema_operativo").alias("sistema_operativo"),
                F.col("e.contexto.ubicacion_pantalla").alias("ubicacion_pantalla"),
                F.col("e.contexto.monto_simulado").alias("monto_simulado"),
                "kafka_offset", "kafka_ts")
            .withWatermark("ts_evento", "1 hour")                  # cuánto esperamos a un duplicado o un atrasado
            .dropDuplicatesWithinWatermark(["id_evento"]))


@dp.table(name="sesiones_app_slv", comment="Sesiones de navegación: eventos del mismo cliente con menos de 30 min de silencio")
def sesiones_app_slv():
    return (spark.readStream.table("eventos_app_slv")
            .withWatermark("ts_evento", "1 hour")
            .groupBy(F.session_window("ts_evento", "30 minutes").alias("sesion"), "id_cliente", "canal")
            .agg(F.count("*").alias("n_eventos"),
                 F.max(F.col("tipo_evento") == "oferta_click").alias("hizo_click"),
                 F.max(F.col("tipo_evento") == "simulacion_realizada").alias("simulo"),
                 F.max(F.col("tipo_evento") == "solicitud_iniciada").alias("inicio_solicitud"))
            .select(F.col("sesion.start").alias("inicio_sesion"), F.col("sesion.end").alias("fin_sesion"),
                    "id_cliente", "canal", "n_eventos", "hizo_click", "simulo", "inicio_solicitud"))
