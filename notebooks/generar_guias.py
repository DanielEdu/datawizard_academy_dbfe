"""Genera las guías paso a paso de cada sesión desde su `_plantilla.py`, insertando el código real de `lakeflow/`.
Así la guía y el pipeline nunca se desincronizan.

Marcas en la plantilla (rutas relativas a lakeflow/):
  {{transformations/01_bronze_lending.py}}  → bloque de código 📋 dentro de una celda markdown (el alumno lo copia al pipeline)
  {{ejecutar:metric_views/kpi_colocacion.sql}} → celda ejecutable: el SQL dentro de spark.sql(), con el catálogo y el esquema
                                              de la guía (CATALOGO, ESQUEMA) en lugar de datawizard_dev.lending

Uso (desde la raíz del repo o desde notebooks/):  python notebooks/generar_guias.py
"""
import pathlib
import re

AQUI = pathlib.Path(__file__).parent
LAKEFLOW = AQUI.parent / "lakeflow"

# carpeta de la sesión → nombre de la guía generada
GUIAS = {
    "sesion-19-lakeflow-bronze-silver": "00_paso_a_paso_lakeflow.py",
    "sesion-20-gold-metricas-features": "00_paso_a_paso_gold.py",
    "sesion-20.2-genie": "00_paso_a_paso_genie.py",
}


def bloque(ruta):
    archivo = LAKEFLOW / ruta
    lenguaje = "sql" if archivo.suffix == ".sql" else "python"
    lineas = [f"# MAGIC ```{lenguaje}"]
    lineas += [f"# MAGIC {l}".rstrip() for l in archivo.read_text().rstrip().splitlines()]
    lineas += ["# MAGIC ```"]
    return "\n".join(lineas)


def ejecutable(ruta):
    """El nombre datawizard_dev.lending del archivo se muestra como {CATALOGO}.{ESQUEMA} (f-string): el alumno ve de dónde sale."""
    sql = (LAKEFLOW / ruta).read_text().rstrip().replace("datawizard_dev.lending", "{CATALOGO}.{ESQUEMA}")
    assert '"""' not in sql, f"{ruta}: no puede contener triple comilla"
    assert not re.search(r"[{}](?<!{CATALOGO})", sql.replace("{CATALOGO}.{ESQUEMA}", "")), f"{ruta}: llaves sueltas, no cabe en un f-string"
    return "\n".join([
        f"# Fuente: lakeflow/{ruta} (generado: no editar aquí)",
        f'sql = f"""\n{sql}\n"""',
        "spark.sql(sql)",
    ])


def reemplazar(m):
    marca = m.group(1).strip()
    if marca.startswith("ejecutar:"):
        return ejecutable(marca.removeprefix("ejecutar:").strip())
    return bloque(marca)


for carpeta, salida in GUIAS.items():
    plantilla = AQUI / carpeta / "_plantilla.py"
    if not plantilla.exists():
        continue
    texto = re.sub(r"\{\{(.+?)\}\}", reemplazar, plantilla.read_text())
    (AQUI / carpeta / salida).write_text(texto)
    print(f"ok: {carpeta}/{salida}")
