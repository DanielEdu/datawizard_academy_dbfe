"""Ingesta con Auto Loader: landing (S3) → tabla Bronze gestionada de Unity Catalog."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.logger import get_logger
from utils.paths import checkpoint_path, landing_path, schema_path

log = get_logger(__name__)

# Campos de _metadata que se guardan; deben calzar con el STRUCT del DDL de Bronze.
METADATA_FIELDS = (
    "file_path",
    "file_name",
    "file_size",
    "file_block_start",
    "file_block_length",
    "file_modification_time",
)
COLUMNAS_TECNICAS = {"_metadata", "_rescued_data", "_ingestion_ts"}


class AutoLoaderIngestor:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        suffix: str,
        bucket_root: str,
        landing_prefix: str,
        checkpoint_prefix: str,
        schema_prefix: str,
        trigger: str,
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.suffix = suffix
        self.bucket_root = bucket_root
        self.landing_prefix = landing_prefix
        self.checkpoint_prefix = checkpoint_prefix
        self.schema_prefix = schema_prefix
        self.trigger = trigger

    def ingest(self, tabla: dict) -> int:
        """Ingesta una tabla y devuelve la cantidad de filas escritas."""
        nombre = tabla["nombre"]
        destino = f"{self.catalog}.{self.schema}.{nombre}{self.suffix}"
        landing = landing_path(self.bucket_root, self.landing_prefix, tabla["carpeta"])
        log.info("%s: %s → %s", nombre, landing, destino)

        header = self._leer_header(landing, tabla)
        self._comparar_columnas(header, destino)

        # Una columna nueva detiene el stream (UnknownFieldException) y Databricks marca la task
        # como fallida aunque se capture la excepción; la incorpora el reintento de la task
        # (max_retries: 1), porque el schema nuevo ya quedó en schemaLocation.
        return self._ejecutar_stream(tabla, landing, destino)

    def _leer_header(self, landing: str, tabla: dict) -> list[str]:
        """Columnas del header de landing. Falla con un mensaje claro si la carpeta no existe."""
        opciones = {k: v for k, v in tabla["opciones"].items() if not k.startswith("cloudFiles.")}
        try:
            df = (
                self.spark.read.format(tabla["formato"])
                .option("header", "true")
                .option("inferSchema", "false")
                .option("recursiveFileLookup", "true")
                .options(**opciones)
                .load(landing)
            )
            return df.columns
        except Exception as e:
            if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e):
                raise FileNotFoundError(f"No existe la carpeta de landing {landing}") from None
            raise

    def _comparar_columnas(self, header: list[str], destino: str) -> None:
        """Registra un warning por cada columna del archivo que no existe en la tabla destino."""
        try:
            columnas_tabla = set(self.spark.table(destino).columns) - COLUMNAS_TECNICAS
        except Exception as e:
            raise RuntimeError(f"No se pudo leer la tabla {destino}; ¿se ejecutó bundle_ddl? ({e})") from None
        for col in header:
            if col not in columnas_tabla:
                log.warning("%s: columna extra '%s' en landing (se agregará por schema evolution)", destino, col)

    def _ejecutar_stream(self, tabla: dict, landing: str, destino: str) -> int:
        nombre = tabla["nombre"]
        df = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", tabla["formato"])
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaLocation", schema_path(self.bucket_root, self.schema_prefix, nombre))
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .options(**tabla["opciones"])
            .load(landing)
        )

        negocio = [c for c in df.columns if c != "_rescued_data"]
        df = df.select(
            *[F.col(f"`{c}`") for c in negocio],
            F.struct(*[F.col(f"_metadata.{f}") for f in METADATA_FIELDS]).alias("_metadata"),
            F.col("_rescued_data"),
            F.current_timestamp().alias("_ingestion_ts"),
        )

        writer = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path(self.bucket_root, self.checkpoint_prefix, nombre))
            .option("mergeSchema", "true")
        )
        writer = writer.trigger(availableNow=True) if self.trigger == "availableNow" else writer.trigger(once=True)

        # Filas ingeridas = diferencia de count(*). En serverless el progreso del stream no trae
        # numInputRows; la tabla es append-only y el job no corre en paralelo, así que es exacto.
        filas_antes = self.spark.table(destino).count()
        query = writer.toTable(destino)
        query.awaitTermination()
        return self.spark.table(destino).count() - filas_antes
