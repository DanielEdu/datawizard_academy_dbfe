#!/usr/bin/env python3
"""Sube los CSV generados a sus prefijos de landing en S3.

Sin --apply solo muestra el plan. La carga real consulta el perfil de AWS,
comprueba los objetos existentes y verifica el tamaño de cada objeto subido.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = REPO_ROOT / "scripts" / "datos_generados"
DEFAULT_BUCKET = "lakehouse-datawizard-gzl"
DEFAULT_PROFILE = "datawizard-local"
DEFAULT_REGION = "eu-north-1"
DEFAULT_LENDING_PREFIX = "landing/wizard_bank_rdb"
DEFAULT_COBRANZAS_PREFIX = "landing/wizard_bank_cobranzas"

LENDING_TABLES = (
    "paises",
    "campanias",
    "productos_prestamo",
    "clientes",
    "ofertas_preaprobadas",
    "solicitudes_prestamo",
    "desembolsos",
    "tipos_cambio",
)
COBRANZAS_TABLES = ("cuotas", "pagos", "gestiones_cobranza")


def find_aws_cli() -> str:
    aws = shutil.which("aws")
    if aws:
        return aws

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        windows_aws = Path(local_app_data) / "Programs" / "Amazon" / "AWSCLIV2" / "aws.exe"
        if windows_aws.is_file():
            return str(windows_aws)

    raise FileNotFoundError("No encuentro AWS CLI. Comprueba la instalación y el PATH.")


def run_aws(aws_cli: str, args: list[str], profile: str, region: str) -> str:
    command = [aws_cli, *args, "--profile", profile, "--region", region]
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Falló AWS CLI ({result.returncode}): {' '.join(args)}\n{detail}")
    return result.stdout.strip()


def table_for_file(path: Path) -> tuple[str, str]:
    """Devuelve (dominio, tabla), admitiendo CSV delta con sufijo en el nombre."""
    stem = path.stem
    for domain, tables in (("lending", LENDING_TABLES), ("cobranzas", COBRANZAS_TABLES)):
        for table in tables:
            if stem == table or (domain == "lending" and stem.startswith(f"{table}_")):
                return domain, table
    raise ValueError(f"CSV sin tabla de destino reconocida: {path.name}")


def build_plan(input_dir: Path, bucket: str, lending_prefix: str, cobranzas_prefix: str) -> list[dict]:
    if not input_dir.is_dir():
        raise FileNotFoundError(f"No existe la carpeta de datos generados: {input_dir}")

    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No encontré archivos CSV directamente dentro de {input_dir}")

    prefixes = {"lending": lending_prefix.strip("/"), "cobranzas": cobranzas_prefix.strip("/")}
    plan = []
    for path in csv_files:
        domain, table = table_for_file(path)
        key = f"{prefixes[domain]}/{table}/{path.name}"
        plan.append({"source": path, "key": key, "uri": f"s3://{bucket}/{key}"})
    return plan


def list_target_keys(aws_cli: str, bucket: str, prefixes: list[str], profile: str, region: str) -> set[str]:
    existing: set[str] = set()
    for prefix in prefixes:
        output = run_aws(
            aws_cli,
            ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", f"{prefix}/", "--output", "json"],
            profile,
            region,
        )
        response = json.loads(output)
        existing.update(item["Key"] for item in (response.get("Contents") or []))
    return existing


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sube los CSV de datos_generados a landing en S3; por defecto solo muestra una vista previa."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR, help="Carpeta que contiene los CSV.")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="Nombre del bucket S3, sin s3://.")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="Perfil de AWS CLI.")
    parser.add_argument("--region", default=DEFAULT_REGION, help="Región del bucket S3.")
    parser.add_argument("--lending-prefix", default=DEFAULT_LENDING_PREFIX, help="Prefijo de landing para Lending.")
    parser.add_argument(
        "--cobranzas-prefix",
        default=DEFAULT_COBRANZAS_PREFIX,
        help="Prefijo de landing para Cobranzas.",
    )
    parser.add_argument("--apply", action="store_true", help="Realiza la carga. Sin esta opción no modifica S3.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Permite reemplazar objetos con la misma clave; requiere --apply.",
    )
    args = parser.parse_args()

    bucket = args.bucket.strip()
    if not bucket or "/" in bucket or bucket.startswith("s3:"):
        raise ValueError("--bucket debe ser solo el nombre del bucket, sin s3:// ni prefijos.")
    if args.overwrite and not args.apply:
        raise ValueError("--overwrite requiere --apply.")

    plan = build_plan(args.input_dir, bucket, args.lending_prefix, args.cobranzas_prefix)
    total_bytes = sum(item["source"].stat().st_size for item in plan)
    print(f"Bucket: s3://{bucket}/")
    print(f"Perfil: {args.profile} | Región: {args.region}")
    print(f"Archivos: {len(plan)} | Tamaño total: {total_bytes:,} bytes")
    for item in plan:
        print(f"  {item['source'].name} → {item['uri']}")

    if not args.apply:
        print("Vista previa: no se hicieron cambios en S3. Añade --apply para subir los archivos.")
        return 0

    aws_cli = find_aws_cli()
    identity = json.loads(
        run_aws(aws_cli, ["sts", "get-caller-identity", "--output", "json"], args.profile, args.region)
    )
    print(f"Identidad AWS: cuenta {identity['Account']} | ARN {identity['Arn']}")

    prefixes = sorted({item["key"].rsplit("/", 1)[0].rsplit("/", 1)[0] for item in plan})
    existing_keys = list_target_keys(aws_cli, bucket, prefixes, args.profile, args.region)
    collisions = [item["key"] for item in plan if item["key"] in existing_keys]
    if collisions and not args.overwrite:
        locations = "\n".join(f"  s3://{bucket}/{key}" for key in collisions)
        raise RuntimeError(
            "Ya existen objetos con las mismas claves. No se subió ningún archivo. "
            "Usa --overwrite junto con --apply para reemplazarlos:\n" + locations
        )

    for item in plan:
        run_aws(
            aws_cli,
            ["s3", "cp", str(item["source"]), item["uri"], "--only-show-errors"],
            args.profile,
            args.region,
        )
        metadata = json.loads(
            run_aws(aws_cli, ["s3api", "head-object", "--bucket", bucket, "--key", item["key"], "--output", "json"], args.profile, args.region)
        )
        expected_size = item["source"].stat().st_size
        if metadata.get("ContentLength") != expected_size:
            raise RuntimeError(
                f"Tamaño distinto tras subir {item['uri']}: "
                f"local={expected_size}, S3={metadata.get('ContentLength')}"
            )
        print(f"Subido y verificado: {item['uri']} ({expected_size:,} bytes)")

    print(f"Carga verificada: {len(plan)} objetos en s3://{bucket}/.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
