# Paso 3 · Bronze de tipos de cambio. FUENTE 2: archivo de un proveedor externo.
# No viene de nuestra base: un tercero publica un CSV con las tasas (a veces en subcarpetas AAAA/MM/DD
# y a veces reenviando días ya publicados). No hay columna confiable de "última modificación":
# el orden lo da el archivo que llegó después.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")


@dp.table(name="tipos_cambio_brz", comment="Fuente externa (proveedor) · tipos de cambio a USD, crudo")
def tipos_cambio_brz():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("recursiveFileLookup", "true")           # el proveedor puede usar subcarpetas por fecha
            .load(f"{LANDING}/wizard_bank_rdb/tipos_cambio")
            .withColumn("_archivo", F.col("_metadata.file_path"))
            .withColumn("_ingestado_en", F.current_timestamp()))
