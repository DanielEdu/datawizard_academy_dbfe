"""Construcción de rutas en el bucket: landing, checkpoint y schemaLocation."""


def _join(*partes: str) -> str:
    return "/".join(p.strip("/") for p in partes if p) + "/"


def landing_path(bucket_root: str, landing_prefix: str, carpeta: str) -> str:
    return _join(bucket_root.rstrip("/"), landing_prefix, carpeta)


def checkpoint_path(bucket_root: str, checkpoint_prefix: str, tabla: str) -> str:
    return _join(bucket_root.rstrip("/"), checkpoint_prefix, tabla)


def schema_path(bucket_root: str, schema_prefix: str, tabla: str) -> str:
    return _join(bucket_root.rstrip("/"), schema_prefix, tabla)
