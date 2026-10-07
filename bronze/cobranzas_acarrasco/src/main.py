"""Entry point del job de ingesta S3: ingesta landing → tablas destino con Auto Loader.

Todas las rutas, catálogo, schema y tablas vienen de parámetros o del YAML de configuración.
"""

import argparse
import os
import sys
from pathlib import Path

# spark_python_task ejecuta este archivo como script: se agrega src/ al sys.path para que
# resuelvan los paquetes config/, ingestion/ y utils/ sin empaquetar un wheel.
SRC_DIR = Path(os.path.abspath(__file__ if "__file__" in globals() else sys.argv[0])).parent
BUNDLE_ROOT = SRC_DIR.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config.loader import load_config, select_tables  # noqa: E402
from ingestion.autoloader import AutoLoaderIngestor  # noqa: E402
from utils.logger import get_logger  # noqa: E402

log = get_logger("main")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingesta de landing a tablas gestionadas con Auto Loader")
    p.add_argument("--catalog", required=True, help="Catálogo destino (lo pasa el job desde la variable del bundle)")
    p.add_argument("--schema", default="_acarrasco", help="Schema destino")
    p.add_argument("--bucket_root", default=None, help="Raíz del bucket; por defecto bucket_root del YAML")
    p.add_argument("--landing_prefix", default="landing/cobranzas", help="Prefijo de landing")
    p.add_argument("--checkpoint_prefix", default="checkpoint/cobranzas_acarrasco", help="Prefijo de checkpoints")
    p.add_argument("--schema_prefix", default="schemas/cobranzas_acarrasco", help="Prefijo de schemaLocation")
    p.add_argument("--config_path", default="config/tablas_cobranzas.yml",
                   help="YAML de tablas (relativo a la raíz del bundle o absoluto)")
    p.add_argument("--tables", default="all", help="Tablas separadas por comas o 'all'")
    p.add_argument("--trigger", default="availableNow", choices=["availableNow", "once"])
    p.add_argument("--suffix", default="_brz", help="Sufijo de las tablas destino")
    return p.parse_args(argv)


def resolve_config_path(config_path: str) -> Path:
    path = Path(config_path)
    return path if path.is_absolute() else BUNDLE_ROOT / path


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    config = load_config(resolve_config_path(args.config_path))

    bucket_root = args.bucket_root or config["bucket_root"]
    if not bucket_root:
        raise ValueError("Falta bucket_root: pasar --bucket_root o definirlo en el YAML")

    tablas = select_tables(config["tablas"], args.tables)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    ingestor = AutoLoaderIngestor(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        bucket_root=bucket_root,
        landing_prefix=args.landing_prefix,
        checkpoint_prefix=args.checkpoint_prefix,
        schema_prefix=args.schema_prefix,
        suffix=args.suffix,
        trigger=args.trigger,
    )

    resumen = []  # (tabla, estado, filas, detalle)
    for tabla in tablas:
        nombre = tabla["nombre"]
        if not tabla["enabled"]:
            log.info("%s: omitida (enabled: false)", nombre)
            resumen.append((nombre, "omitida", 0, ""))
            continue
        log.info("%s: ingestando en %s", nombre, ingestor.target_table(nombre))
        try:
            filas = ingestor.ingest(tabla)
            log.info("%s: ok, %d filas", nombre, filas)
            resumen.append((nombre, "ok", filas, ""))
        except Exception as e:
            log.exception("%s: error", nombre)
            resumen.append((nombre, "error", 0, str(e).splitlines()[0] if str(e) else type(e).__name__))

    log.info("Resumen de la ingesta:")
    for nombre, estado, filas, detalle in resumen:
        log.info("  %-25s %-8s %8d filas %s", nombre, estado, filas, detalle)

    errores = [r[0] for r in resumen if r[1] == "error"]
    if errores:
        raise RuntimeError(f"Fallaron {len(errores)} tabla(s): {', '.join(errores)}")


if __name__ == "__main__":
    main()
