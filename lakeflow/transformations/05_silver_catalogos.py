# Paso 6 · Silver de catálogos: países y campañas como MATERIALIZED VIEW.
# Son tablas chicas (3 y 30 filas) que casi no cambian: no tiene sentido leerlas en streaming.
# Se leen COMPLETAS con spark.read (batch), se tipan y se deja la última versión de cada clave.
# El pipeline mantiene la vista al día en cada update.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(name="paises_slv", comment="Catálogo de países: una fila por país")
def paises_slv():
    ultima = Window.partitionBy("id_pais").orderBy(F.col("_ingestado_en").desc())
    return (spark.read.table("paises_brz")                     # spark.read: batch, no readStream
            .selectExpr("try_cast(id_pais AS SMALLINT) AS id_pais", "codigo_iso", "nombre_pais",
                        "moneda_codigo", "_ingestado_en")
            .withColumn("_rn", F.row_number().over(ultima))
            .filter("_rn = 1 AND id_pais IS NOT NULL")
            .drop("_rn", "_ingestado_en"))


@dp.materialized_view(name="campanias_slv", comment="Catálogo de campañas: una fila por campaña")
@dp.expect("presupuesto_positivo", "presupuesto > 0")          # solo se mide; la fila pasa
def campanias_slv():
    ultima = Window.partitionBy("id_campania").orderBy(F.col("_ingestado_en").desc())
    return (spark.read.table("campanias_brz")
            .selectExpr("try_cast(id_campania AS INT) AS id_campania",
                        "try_cast(id_pais AS SMALLINT) AS id_pais",
                        "nombre_campania", "tipo_campania",
                        "try_cast(fecha_inicio AS DATE) AS fecha_inicio",
                        "try_cast(fecha_fin AS DATE) AS fecha_fin",
                        "try_cast(presupuesto AS DECIMAL(14,2)) AS presupuesto",
                        "_ingestado_en")
            .withColumn("_rn", F.row_number().over(ultima))
            .filter("_rn = 1 AND id_campania IS NOT NULL")
            .drop("_rn", "_ingestado_en"))
