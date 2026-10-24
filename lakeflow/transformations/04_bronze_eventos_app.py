# Paso 5 · Bronze de la App. FUENTE 4: eventos en un bus de mensajes (Kafka, topic wizard.lending.eventos-app).
# En el curso el topic se simula: cada archivo .parquet de landing/wizard_bank_kafka/eventos_app/ trae los
# registros con el MISMO esquema que entrega Spark al leer Kafka (key, value, topic, partition, offset, timestamp).
# Bronze guarda el mensaje tal cual: el JSON del evento va como texto en `payload`; se interpreta en Silver.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")

ESQUEMA_KAFKA = ("key BINARY, value BINARY, topic STRING, partition INT, offset BIGINT, "
                 "timestamp TIMESTAMP, timestampType INT")


@dp.table(name="eventos_app_brz", comment="Fuente Kafka · un mensaje del topic por fila, payload sin interpretar")
def eventos_app_brz():
    # Con Kafka real solo cambia la lectura (las columnas son las mismas):
    #   spark.readStream.format("kafka")
    #        .option("kafka.bootstrap.servers", "...").option("subscribe", "wizard.lending.eventos-app").load()
    mensajes = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .schema(ESQUEMA_KAFKA)                        # el registro de Kafka es fijo: no se infiere
                .load(f"{LANDING}/wizard_bank_kafka/eventos_app"))
    return mensajes.select(
        F.col("value").cast("string").alias("payload"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_particion"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("_metadata.file_path").alias("_archivo"),
        F.current_timestamp().alias("_ingestado_en"),
    )
