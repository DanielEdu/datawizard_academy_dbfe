"""Lectura y validación del YAML de tablas."""

from pathlib import Path

import yaml

_FORMATOS_SOPORTADOS = {"csv"}


def load_config(path: Path) -> dict:
    """Lee el YAML y valida su estructura. Devuelve {'bucket_root': str | None, 'tablas': [dict]}."""
    if not path.is_file():
        raise FileNotFoundError(f"No existe el archivo de configuración: {path}")

    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    tablas = data.get("tablas")
    if not isinstance(tablas, list) or not tablas:
        raise ValueError(f"{path}: 'tablas' debe ser una lista no vacía")

    vistos = set()
    for i, t in enumerate(tablas):
        if not isinstance(t, dict):
            raise ValueError(f"{path}: la entrada {i} de 'tablas' no es un objeto")
        nombre = t.get("nombre")
        if not isinstance(nombre, str) or not nombre:
            raise ValueError(f"{path}: la entrada {i} no tiene 'nombre'")
        if nombre in vistos:
            raise ValueError(f"{path}: la tabla '{nombre}' está duplicada")
        vistos.add(nombre)
        if t.get("formato") not in _FORMATOS_SOPORTADOS:
            raise ValueError(f"{path}: '{nombre}' tiene formato no soportado: {t.get('formato')!r}")
        if not isinstance(t.get("enabled"), bool):
            raise ValueError(f"{path}: '{nombre}' debe tener 'enabled' true/false")
        opciones = t.setdefault("opciones", {}) or {}
        if not isinstance(opciones, dict):
            raise ValueError(f"{path}: 'opciones' de '{nombre}' debe ser un diccionario")
        t["opciones"] = {k: str(v) for k, v in opciones.items()}

    return {"bucket_root": data.get("bucket_root"), "tablas": tablas}


def select_tables(tablas: list[dict], tables_arg: str) -> list[dict]:
    """Filtra por --tables ('all' o lista separada por comas). Falla si se pide una tabla inexistente."""
    if tables_arg.strip().lower() == "all":
        return tablas
    pedidas = [n.strip() for n in tables_arg.split(",") if n.strip()]
    por_nombre = {t["nombre"]: t for t in tablas}
    faltantes = [n for n in pedidas if n not in por_nombre]
    if faltantes:
        raise ValueError(f"--tables incluye tablas que no están en la configuración: {faltantes}")
    return [por_nombre[n] for n in pedidas]
