# Paso 4 · Bronze de Cobranzas. FUENTE 3: sistema legado on-premise.
# Azure Data Factory copia cada noche un CSV por tabla a landing/wizard_bank_onp/<tabla>/.
# Para el pipeline es igual que la fuente 1 (archivos), pero el sistema es otro: sus columnas de auditoría
# se llaman fecha_modificacion (no fecha_actualizacion) y el lote llega una vez al día.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")


def leer_csv(carpeta):
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{LANDING}/wizard_bank_onp/{carpeta}")
            .withColumn("_archivo", F.col("_metadata.file_path"))
            .withColumn("_ingestado_en", F.current_timestamp()))


@dp.table(name="cuotas_brz", comment="Fuente legado on-premise · plan de cuotas, crudo (el estado de una cuota cambia)")
def cuotas_brz():
    return leer_csv("cuotas")


@dp.table(name="pagos_brz", comment="Fuente legado on-premise · pagos, crudo (hecho: no cambia, pero puede reenviarse)")
def pagos_brz():
    return leer_csv("pagos")


@dp.table(name="gestiones_cobranza_brz", comment="Fuente legado on-premise · gestiones de cobranza, crudo")
def gestiones_cobranza_brz():
    return leer_csv("gestiones_cobranza")
