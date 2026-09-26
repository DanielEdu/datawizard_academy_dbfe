"""Construcción de rutas de landing, checkpoint y schemaLocation."""


def _join(*parts: str) -> str:
    """Une segmentos de ruta con '/' y termina en '/' (carpeta)."""
    head, *rest = parts
    segments = [head.rstrip("/")] + [p.strip("/") for p in rest if p.strip("/")]
    return "/".join(segments) + "/"


def landing_path(bucket_root: str, landing_prefix: str, tabla: str) -> str:
    return _join(bucket_root, landing_prefix, tabla)


def checkpoint_path(bucket_root: str, checkpoint_prefix: str, tabla: str) -> str:
    return _join(bucket_root, checkpoint_prefix, tabla)


def schema_path(bucket_root: str, schema_prefix: str, tabla: str) -> str:
    return _join(bucket_root, schema_prefix, tabla)
