# Paso 2 · Bronze de Lending. FUENTE 1: base de datos transaccional (Azure SQL).
# El extractor deja un CSV por tabla en landing/wizard_bank_rdb/<tabla>/ (carga inicial + deltas).
# Auto Loader los toma tal como llegan: todo STRING, más el archivo de origen y la hora de ingesta.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing", "s3://lakehouse-datawizard/landing")   # Settings → Configuration


def leer_csv(carpeta):
    return (spark.readStream.format("cloudFiles")          # Auto Loader: solo archivos nuevos
            .option("cloudFiles.format", "csv")
            .option("header", "true")                        # sin inferColumnTypes: Bronze guarda STRING
            .load(f"{LANDING}/wizard_bank_rdb/{carpeta}")
            .withColumn("_archivo", F.col("_metadata.file_path"))
            .withColumn("_ingestado_en", F.current_timestamp()))


@dp.table(name="paises_brz", comment="Fuente transaccional · catálogo de países, crudo")
def paises_brz():
    return leer_csv("paises")


@dp.table(name="campanias_brz", comment="Fuente transaccional · campañas de marketing, crudo")
def campanias_brz():
    return leer_csv("campanias")


@dp.table(name="productos_prestamo_brz", comment="Fuente transaccional · productos de crédito, crudo")
def productos_prestamo_brz():
    return leer_csv("productos_prestamo")


@dp.table(name="clientes_brz", comment="Fuente transaccional · clientes, crudo (cada delta trae nuevos y cambios)")
def clientes_brz():
    return leer_csv("clientes")


@dp.table(name="ofertas_preaprobadas_brz", comment="Fuente transaccional · ofertas preaprobadas, crudo")
def ofertas_preaprobadas_brz():
    return leer_csv("ofertas_preaprobadas")


@dp.table(name="solicitudes_prestamo_brz", comment="Fuente transaccional · solicitudes, crudo (una solicitud llega varias veces con otro estado)")
def solicitudes_prestamo_brz():
    return leer_csv("solicitudes_prestamo")


@dp.table(name="desembolsos_brz", comment="Fuente transaccional · desembolsos, crudo")
def desembolsos_brz():
    return leer_csv("desembolsos")
