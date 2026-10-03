"""Entry point del job de ingesta: Auto Loader de landing → capa Bronze.

Procesa en secuencia las tablas `enabled: true` del YAML. Si una falla, sigue con las demás
y al final termina con error si hubo algún fallo.
"""

import argparse
import sys
from pathlib import Path

# spark_python_task ejecuta este archivo como script: se agrega src/ al sys.path para que
# resuelvan los paquetes config/, ingestion/ y utils/ sin empaquetar un wheel.
SRC_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path(sys.argv[0]).resolve().parent
BUNDLE_ROOT = SRC_DIR.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pyspark.sql import SparkSession  # noqa: E402

from config.loader import load_config  # noqa: E402
from ingestion.autoloader import AutoLoaderIngestor  # noqa: E402
from utils.logger import get_logger  # noqa: E402

log = get_logger("ingesta")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingesta S3 → Bronze con Auto Loader")
    p.add_argument("--catalog", required=True, help="Catálogo destino (variable del bundle)")
    p.add_argument("--schema", default="lending", help="Schema destino")
    p.add_argument("--bucket_root", default=None, help="Raíz del bucket; si se omite, se usa bucket_root del YAML")
    p.add_argument("--landing_prefix", default="landing/wizard_bank_rdb", help="Prefijo de landing")
    p.add_argument("--checkpoint_prefix", default="checkpoint", help="Prefijo de checkpoints")
    p.add_argument("--schema_prefix", default="schemas", help="Prefijo de schemaLocation")
    p.add_argument("--config_path", default="config/tablas_lending.yml", help="YAML de tablas (relativo a la raíz del bundle)")
    p.add_argument("--tables", default="all", help="Tablas separadas por comas, o 'all'")
    p.add_argument("--trigger", default="availableNow", choices=["availableNow", "once"])
    p.add_argument("--suffix", default="_brz", help="Sufijo de las tablas destino")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    config_path = Path(args.config_path)
    if not config_path.is_absolute():
        config_path = BUNDLE_ROOT / config_path
    config = load_config(config_path)

    bucket_root = args.bucket_root or config["bucket_root"]
    if not bucket_root:
        raise ValueError("Falta bucket_root: pásalo con --bucket_root o defínelo en el YAML")

    tablas = config["tablas"]
    nombres_yaml = {t["nombre"] for t in tablas}
    seleccion = None if args.tables == "all" else {t.strip() for t in args.tables.split(",") if t.strip()}

    resumen = []  # (tabla, estado, filas, detalle)
    for desconocida in sorted((seleccion or set()) - nombres_yaml):
        log.error("%s: no está en %s", desconocida, config_path.name)
        resumen.append((desconocida, "error", 0, "no está en el YAML"))

    ingestor = AutoLoaderIngestor(
        spark=SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        schema=args.schema,
        suffix=args.suffix,
        bucket_root=bucket_root,
        landing_prefix=args.landing_prefix,
        checkpoint_prefix=args.checkpoint_prefix,
        schema_prefix=args.schema_prefix,
        trigger=args.trigger,
    )

    for tabla in tablas:
        nombre = tabla["nombre"]
        if seleccion is not None and nombre not in seleccion:
            continue
        if not tabla["enabled"]:
            log.info("%s: omitida (enabled: false)", nombre)
            resumen.append((nombre, "omitida", 0, "enabled: false"))
            continue
        try:
            filas = ingestor.ingest(tabla)
            log.info("%s: ok, %d filas", nombre, filas)
            resumen.append((nombre, "ok", filas, ""))
        except Exception as e:
            log.exception("%s: error", nombre)
            resumen.append((nombre, "error", 0, str(e).splitlines()[0][:200]))

    log.info("Resumen de la ingesta:")
    for nombre, estado, filas, detalle in resumen:
        log.info("  %-25s %-8s %10d  %s", nombre, estado, filas, detalle)

    fallidas = [r[0] for r in resumen if r[1] == "error"]
    if fallidas:
        raise RuntimeError(f"Fallaron {len(fallidas)} tabla(s): {', '.join(fallidas)}")


if __name__ == "__main__":
    main()
