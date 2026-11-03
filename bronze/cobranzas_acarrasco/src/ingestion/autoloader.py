"""Ingesta de archivos de landing a tablas gestionadas de UC con Auto Loader (copia fiel, append-only)."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils import paths
from utils.logger import get_logger

log = get_logger(__name__)

# Columnas técnicas que agrega la ingesta; no vienen en el archivo.
METADATA_COLUMNS = ("_metadata", "_rescued_data", "_ingestion_ts")

# Campos de _metadata que se persisten (deben calzar con el DDL de la tabla).
METADATA_FIELDS = (
    "file_path",
    "file_name",
    "file_size",
    "file_block_start",
    "file_block_length",
    "file_modification_time",
)

# Error con el que Auto Loader detiene el stream al detectar columnas nuevas (addNewColumns).
SCHEMA_CHANGE_ERROR = "UNKNOWN_FIELD_EXCEPTION"


class AutoLoaderIngestor:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        bucket_root: str,
        landing_prefix: str,
        checkpoint_prefix: str,
        schema_prefix: str,
        suffix: str,
        trigger: str,
    ):
        if trigger not in ("availableNow", "once"):
            raise ValueError(f"trigger no soportado: {trigger!r} (usar availableNow u once)")
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.bucket_root = bucket_root
        self.landing_prefix = landing_prefix
        self.checkpoint_prefix = checkpoint_prefix
        self.schema_prefix = schema_prefix
        self.suffix = suffix
        self.trigger = trigger

    def target_table(self, tabla: str) -> str:
        return f"{self.catalog}.{self.schema}.{tabla}{self.suffix}"

    def ingest(self, tabla: dict) -> int:
        """Ingesta una tabla y devuelve la cantidad de filas agregadas.

        Si Auto Loader detiene el stream por una columna nueva, reintenta una vez:
        al reiniciar incorpora la columna al schemaLocation y mergeSchema la agrega a la tabla.
        """
        nombre = tabla["nombre"]
        landing = paths.landing_path(self.bucket_root, self.landing_prefix, nombre)
        target = self.target_table(nombre)

        self._check_landing_exists(landing)
        self._check_header(tabla, landing, target)

        antes = self.spark.table(target).count()
        try:
            self._run_stream(tabla, landing, target)
        except Exception as e:
            if SCHEMA_CHANGE_ERROR not in str(e):
                raise
            log.warning("%s: columnas nuevas detectadas en landing; reiniciando el stream una vez", nombre)
            self._run_stream(tabla, landing, target)
        return self.spark.table(target).count() - antes

    def _check_landing_exists(self, landing: str) -> None:
        from databricks.sdk.runtime import dbutils

        try:
            dbutils.fs.ls(landing)
        except Exception as e:
            raise FileNotFoundError(f"No existe la carpeta de landing {landing} (o no es accesible): {e}") from e

    def _check_header(self, tabla: dict, landing: str, target: str) -> None:
        """Compara el header del archivo con las columnas de negocio de la tabla; warning por cada extra."""
        header = (
            self.spark.read.format(tabla["formato"])
            .option("header", "true")
            .option("recursiveFileLookup", "true")
            .load(landing)
            .columns
        )
        columnas_tabla = [c for c in self.spark.table(target).columns if c not in METADATA_COLUMNS]
        for col in header:
            if col not in columnas_tabla:
                log.warning("%s: columna '%s' del archivo no existe en %s (se agregará por schema evolution)",
                            tabla["nombre"], col, target)
        for col in columnas_tabla:
            if col not in header:
                log.warning("%s: columna '%s' de %s no viene en el archivo (quedará nula)",
                            tabla["nombre"], col, target)

    def _read(self, tabla: dict, landing: str) -> DataFrame:
        opciones = {
            "cloudFiles.format": tabla["formato"],
            "header": "true",
            "cloudFiles.inferColumnTypes": "false",
            "cloudFiles.schemaLocation": paths.schema_path(self.bucket_root, self.schema_prefix, tabla["nombre"]),
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "rescuedDataColumn": "_rescued_data",
        }
        opciones.update(tabla["opciones"])
        # Sin .schema(): addNewColumns no es compatible con un schema explícito.
        return self.spark.readStream.format("cloudFiles").options(**opciones).load(landing)

    def _run_stream(self, tabla: dict, landing: str, target: str) -> None:
        df = self._read(tabla, landing).select(
            "*",
            F.struct(*[F.col(f"_metadata.{c}").alias(c) for c in METADATA_FIELDS]).alias("_metadata"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
        writer = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation",
                    paths.checkpoint_path(self.bucket_root, self.checkpoint_prefix, tabla["nombre"]))
            .option("mergeSchema", "true")
        )
        writer = writer.trigger(availableNow=True) if self.trigger == "availableNow" else writer.trigger(once=True)
        writer.toTable(target).awaitTermination()
