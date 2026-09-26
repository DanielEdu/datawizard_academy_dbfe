#!/usr/bin/env python3
"""Crea marcadores vacíos para las carpetas de landing de Lending en S3.

Por defecto solo imprime una vista previa. Usa --apply para crear objetos de
cero bytes con clave terminada en '/', sin subir ni modificar archivos CSV.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import yaml
except ImportError:
    print(
        "Falta PyYAML. Ejecuta con el entorno del bundle: "
        "uv run --project bronze/lending python scripts/preparar_landing_s3.py",
        file=sys.stderr,
    )
    raise SystemExit(2)


REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "bronze" / "lending" / "config" / "tablas_lending.yml"
DEFAULT_PROFILE = "datawizard-local"
DEFAULT_REGION = "eu-north-1"
DEFAULT_LANDING_PREFIX = "landing/wizard_bank_rdb"


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


def read_configuration() -> tuple[str, list[str]]:
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(f"No existe la configuración: {CONFIG_PATH}")

    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    bucket_root = config.get("bucket_root")
    if not isinstance(bucket_root, str) or not bucket_root.startswith("s3://"):
        raise ValueError("bucket_root debe tener formato s3://nombre-del-bucket")
    bucket = bucket_root.removeprefix("s3://").strip("/")

    tables = config.get("tablas")
    if not isinstance(tables, list) or not tables:
        raise ValueError("La configuración debe contener una lista no vacía en 'tablas'.")

    enabled_tables = [
        table["nombre"]
        for table in tables
        if isinstance(table, dict)
        and table.get("enabled") is True
        and isinstance(table.get("nombre"), str)
    ]
    if not enabled_tables:
        raise ValueError("No hay tablas habilitadas para crear carpetas de landing.")
    return bucket, enabled_tables


def list_objects(aws_cli: str, bucket: str, profile: str, region: str) -> list[dict]:
    output = run_aws(
        aws_cli,
        ["s3api", "list-objects-v2", "--bucket", bucket, "--output", "json"],
        profile,
        region,
    )
    response = json.loads(output)
    return response.get("Contents", []) or []


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepara únicamente los prefijos de landing S3 definidos para Lending."
    )
    parser.add_argument("--bucket", help="Bucket S3; por defecto usa bucket_root del YAML.")
    parser.add_argument("--prefix", default=DEFAULT_LANDING_PREFIX, help="Prefijo base de landing.")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="Perfil de AWS CLI.")
    parser.add_argument("--region", default=DEFAULT_REGION, help="Región del bucket S3.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Crea los marcadores vacíos. Sin esta opción solo muestra una vista previa.",
    )
    args = parser.parse_args()

    configured_bucket, tables = read_configuration()
    bucket = (args.bucket or configured_bucket).strip()
    if not bucket or "/" in bucket:
        raise ValueError("--bucket debe ser solo el nombre del bucket, sin s3:// ni prefijos.")

    prefix = args.prefix.strip("/")
    if not prefix:
        raise ValueError("--prefix no puede estar vacío.")
    folder_keys = [f"{prefix}/{table}/" for table in tables]

    print(f"Bucket: s3://{bucket}/")
    print(f"Perfil: {args.profile} | Región: {args.region}")
    print(f"Marcadores de carpeta: {len(folder_keys)}")
    for key in folder_keys:
        print(f"  s3://{bucket}/{key}")

    if not args.apply:
        print("Vista previa: no se hicieron cambios en S3. Añade --apply para crear solo estos marcadores.")
        return 0

    aws_cli = find_aws_cli()
    identity = json.loads(
        run_aws(aws_cli, ["sts", "get-caller-identity", "--output", "json"], args.profile, args.region)
    )
    print(f"Identidad: cuenta {identity['Account']} | ARN {identity['Arn']}")

    before = list_objects(aws_cli, bucket, args.profile, args.region)
    before_keys = {item["Key"] for item in before}
    created = []
    skipped = []

    with tempfile.NamedTemporaryFile(prefix="s3-folder-", suffix=".empty", delete=False) as empty_file:
        empty_path = empty_file.name

    try:
        for key in folder_keys:
            if any(existing_key.startswith(key) for existing_key in before_keys):
                print(f"Ya existe contenido; se conserva: s3://{bucket}/{key}")
                skipped.append(key)
                continue

            run_aws(
                aws_cli,
                [
                    "s3api",
                    "put-object",
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    "--body",
                    empty_path,
                ],
                args.profile,
                args.region,
            )
            created.append(key)
            print(f"Creado: s3://{bucket}/{key}")
    finally:
        Path(empty_path).unlink(missing_ok=True)

    after = list_objects(aws_cli, bucket, args.profile, args.region)
    after_by_key = {item["Key"]: item for item in after}
    added_keys = set(after_by_key) - before_keys
    unexpected = [
        key
        for key in added_keys
        if key not in folder_keys or not key.endswith("/") or after_by_key[key].get("Size") != 0
    ]
    missing_folders = [
        key for key in folder_keys if not any(existing_key.startswith(key) for existing_key in after_by_key)
    ]

    if unexpected or missing_folders:
        raise RuntimeError(
            "La verificación de S3 no coincide con lo esperado. "
            f"Objetos añadidos inesperados: {unexpected}; carpetas no visibles: {missing_folders}"
        )

    print(
        f"Verificado: {len(created)} marcadores vacíos creados, "
        f"{len(skipped)} prefijos existentes conservados; "
        "ningún archivo de datos subido o modificado."
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
