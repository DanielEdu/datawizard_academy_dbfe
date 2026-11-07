# Paso 9 · Silver de tipos de cambio (fuente externa): AUTO CDC tipo 1 con clave compuesta.
# Un día y un par de monedas = una tasa. Si el proveedor reenvía un día corregido, gana el ARCHIVO más
# nuevo: el orden lo da _ingestado_en, no una columna del proveedor.
from pyspark import pipelines as dp


@dp.temporary_view(name="tipos_cambio_tipados")
@dp.expect_or_drop("clave_presente", "fecha IS NOT NULL AND moneda_origen IS NOT NULL AND moneda_destino IS NOT NULL")
@dp.expect_or_drop("tasa_positiva", "tasa_venta > 0 AND tasa_compra > 0")
@dp.expect("compra_menor_que_venta", "tasa_compra <= tasa_venta")
def tipos_cambio_tipados():
    return (spark.readStream.table("tipos_cambio_brz")
            .selectExpr(
                "try_cast(fecha AS DATE) AS fecha",
                "moneda_origen", "moneda_destino",
                "try_cast(tasa_compra AS DECIMAL(18,6)) AS tasa_compra",
                "try_cast(tasa_venta AS DECIMAL(18,6)) AS tasa_venta",
                "_archivo", "_ingestado_en"))


dp.create_streaming_table(name="tipos_cambio_slv", comment="Una tasa por día y par de monedas; gana el último archivo del proveedor")

dp.create_auto_cdc_flow(
    target="tipos_cambio_slv", source="tipos_cambio_tipados",
    keys=["fecha", "moneda_origen", "moneda_destino"],   # clave compuesta
    sequence_by="_ingestado_en",                         # el archivo más nuevo gana
    stored_as_scd_type=1,
)
