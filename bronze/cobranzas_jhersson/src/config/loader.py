"""Lectura y validación del YAML de tablas."""

from pathlib import Path

import yaml

FORMATOS_SOPORTADOS = {"csv"}


def load_config(config_path: Path) -> dict:
    """Lee el YAML y valida cada entrada. Devuelve {'bucket_root': str | None, 'tablas': [dict]}."""
    if not config_path.is_file():
        raise FileNotFoundError(f"No existe el archivo de configuración: {config_path}")

    with config_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    tablas = raw.get("tablas")
    if not isinstance(tablas, list) or not tablas:
        raise ValueError(f"{config_path}: 'tablas' debe ser una lista no vacía")

    vistas = set()
    validadas = []
    for i, t in enumerate(tablas):
        if not isinstance(t, dict) or not t.get("nombre"):
            raise ValueError(f"{config_path}: la entrada {i} no tiene 'nombre'")
        nombre = t["nombre"]
        if nombre in vistas:
            raise ValueError(f"{config_path}: tabla duplicada '{nombre}'")
        vistas.add(nombre)

        formato = t.get("formato")
        if formato not in FORMATOS_SOPORTADOS:
            raise ValueError(f"{config_path}: '{nombre}' tiene formato '{formato}'; soportados: {FORMATOS_SOPORTADOS}")
        if not isinstance(t.get("enabled"), bool):
            raise ValueError(f"{config_path}: '{nombre}' debe tener 'enabled: true|false'")
        opciones = t.get("opciones") or {}
        if not isinstance(opciones, dict):
            raise ValueError(f"{config_path}: 'opciones' de '{nombre}' debe ser un diccionario")

        validadas.append(
            {
                "nombre": nombre,
                "carpeta": t.get("carpeta") or nombre,
                "formato": formato,
                "enabled": t["enabled"],
                "opciones": {k: str(v) for k, v in opciones.items()},
            }
        )

    return {"bucket_root": raw.get("bucket_root"), "tablas": validadas}
