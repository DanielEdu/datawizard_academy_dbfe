"""Ingesta de la telemetría de la app (topic Kafka) a Bronze con Auto Loader.

Los archivos de landing son .parquet con el schema de la fuente Kafka de Spark
(key, value, topic, partition, offset, timestamp, timestampType). Este job lee
esos registros, parsea el value JSON con el contrato del evento (Sesión 11),
lo aplana y lo agrega a <catalog>.<schema>.<tabla>_brz. Lo que no parsea queda
con id_evento NULL y el mensaje original en _payload_crudo: Bronze no descarta.
Rutas, catálogo y tabla llegan por parámetro.
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("main")

# Schema del registro de Kafka tal como lo entrega spark.readStream.format("kafka")
SCHEMA_KAFKA = ("key BINARY, value BINARY, topic STRING, partition INT, offset BIGINT, "
                "timestamp TIMESTAMP, timestampType INT")

# Contrato del value (Sesión 11)
ESQUEMA_EVENTO = StructType([
    StructField("id_evento", StringType()),
    StructField("tipo_evento", StringType()),
    StructField("timestamp_evento", StringType()),   # ISO-8601, se castea abajo
    StructField("id_cliente", LongType()),
    StructField("id_oferta", LongType()),
    StructField("id_pais", IntegerType()),
    StructField("canal", StringType()),
    StructField("sesion", StructType([
        StructField("id_sesion", StringType()),
        StructField("version_app", StringType()),
        StructField("sistema_operativo", StringType()),
        StructField("modelo_dispositivo", StringType()),
    ])),
    StructField("contexto", StructType([
        StructField("ubicacion_pantalla", StringType()),
        StructField("posicion", IntegerType()),
        StructField("tiempo_visible_seg", DoubleType()),
        StructField("monto_simulado", DoubleType()),
        StructField("plazo_simulado", IntegerType()),
    ])),
])

CAMPOS_METADATA = ["file_path", "file_name", "file_size", "file_block_start",
                   "file_block_length", "file_modification_time"]


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingesta de eventos Kafka (parquet) → Bronze")
    p.add_argument("--catalog", required=True, help="Catálogo destino")
    p.add_argument("--schema", required=True, help="Schema destino")
    p.add_argument("--tabla", required=True, help="Tabla destino sin sufijo")
    p.add_argument("--suffix", default="_brz", help="Sufijo de la tabla destino")
    p.add_argument("--landing", required=True, help="Carpeta de landing con los .parquet")
    p.add_argument("--checkpoint", required=True, help="checkpointLocation del stream")
    return p.parse_args(argv)


def leer_landing(spark: SparkSession, landing: str) -> DataFrame:
    # Schema explícito: el registro de Kafka es fijo, no hay nada que inferir ni evolucionar
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .schema(SCHEMA_KAFKA)
            .load(landing))


def aplanar(df: DataFrame) -> DataFrame:
    """Registro de Kafka → columnas del evento + metadata de Kafka y de ingesta."""
    payload = F.col("value").cast("string")
    d = F.from_json(payload, ESQUEMA_EVENTO)
    return df.select(
        d["id_evento"].alias("id_evento"),
        d["tipo_evento"].alias("tipo_evento"),
        F.to_timestamp(d["timestamp_evento"]).alias("timestamp_evento"),
        d["id_cliente"].alias("id_cliente"),
        d["id_oferta"].alias("id_oferta"),
        d["id_pais"].alias("id_pais"),
        d["canal"].alias("canal"),
        d["sesion"]["id_sesion"].alias("id_sesion"),
        d["sesion"]["version_app"].alias("version_app"),
        d["sesion"]["sistema_operativo"].alias("sistema_operativo"),
        d["sesion"]["modelo_dispositivo"].alias("modelo_dispositivo"),
        d["contexto"]["ubicacion_pantalla"].alias("ubicacion_pantalla"),
        d["contexto"]["posicion"].alias("posicion"),
        d["contexto"]["tiempo_visible_seg"].alias("tiempo_visible_seg"),
        d["contexto"]["monto_simulado"].alias("monto_simulado"),
        d["contexto"]["plazo_simulado"].alias("plazo_simulado"),
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_particion"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        payload.alias("_payload_crudo"),
        F.struct(*[F.col(f"_metadata.{c}").alias(c) for c in CAMPOS_METADATA]).alias("_metadata"),
        F.current_timestamp().alias("_ingestion_ts"),
    )


def ultima_version(spark: SparkSession, tabla: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first()["version"]


def filas_escritas(spark: SparkSession, tabla: str, desde_version: int) -> int:
    """Filas agregadas por esta corrida, según el historial de Delta. (recentProgress no es
    confiable en serverless: con Spark Connect puede volver vacío al terminar el stream.)"""
    historia = (spark.sql(f"DESCRIBE HISTORY {tabla}")
                .filter(F.col("version") > desde_version)
                .select(F.col("operationMetrics")["numOutputRows"].cast("long").alias("n")))
    return historia.agg(F.coalesce(F.sum("n"), F.lit(0))).first()[0]


def main(argv=None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    destino = f"{args.catalog}.{args.schema}.{args.tabla}{args.suffix}"
    log.info("Ingestando %s → %s", args.landing, destino)

    version_inicial = ultima_version(spark, destino)
    query = (aplanar(leer_landing(spark, args.landing))
             .writeStream
             .option("checkpointLocation", args.checkpoint)
             .trigger(availableNow=True)
             .toTable(destino))
    query.awaitTermination()

    filas = filas_escritas(spark, destino, version_inicial)
    log.info("%s: ok, %d registros", destino, filas)


if __name__ == "__main__":
    main()
