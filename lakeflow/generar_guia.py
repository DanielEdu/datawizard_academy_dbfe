"""Genera 00_paso_a_paso_lakeflow.py desde _plantilla_guia.py, insertando el código de cada archivo de
transformations/ donde la plantilla dice {{archivo}}. Así la guía y el pipeline nunca se desincronizan.
Uso: python generar_guia.py"""
import pathlib, re

aqui = pathlib.Path(__file__).parent
plantilla = (aqui / "_plantilla_guia.py").read_text()


def bloque(m):
    archivo = aqui / "transformations" / m.group(1)
    lenguaje = "sql" if archivo.suffix == ".sql" else "python"
    lineas = [f"# MAGIC ```{lenguaje}"] + [f"# MAGIC {l}".rstrip() for l in archivo.read_text().rstrip().splitlines()] + ["# MAGIC ```"]
    return "\n".join(lineas)


(aqui / "00_paso_a_paso_lakeflow.py").write_text(re.sub(r"\{\{(.+?)\}\}", bloque, plantilla))
print("ok: 00_paso_a_paso_lakeflow.py")
