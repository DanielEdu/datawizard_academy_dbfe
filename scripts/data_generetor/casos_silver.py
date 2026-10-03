"""
═══════════════════════════════════════════════════════════════════════════════
WIZARD BANK · Casos de prueba para Silver en los deltas
Data Wizard Academy — usado por generar_datos_wizard_bank.py y generar_datos_cobranzas.py

Un delta "limpio" (solo filas nuevas y cambios bien ordenados) no ejercita lo
que Silver tiene que resolver (S15/S16). Este módulo agrega a cada delta las
situaciones reales de una carga por archivos:

  invalida            fila que rompe una regla de calidad   → _cuarentena
  duplicado_en_lote   la misma fila dos veces en el archivo → dedup antes del MERGE
  reenvio             una versión ya cargada, sin cambios   → MERGE idempotente: no cambia nada
  llegada_tardia      versión con fecha ANTERIOR a la vigente → la guarda de orden la ignora
  hecho_modificado    un hecho (pago, gestión) reenviado con otro valor → insert-only lo ignora
  correccion          el proveedor reenvía la misma clave corregida → gana el último archivo

Cada caso queda en un manifiesto local (casos_silver_<sufijo>.csv) con la clave
y lo que Silver debería hacer, para verificar el job contra él.
También incluye subir_a_s3, compartido por los dos generadores.
═══════════════════════════════════════════════════════════════════════════════
"""

import csv
import os
import random
import sys
from datetime import datetime, timedelta

ESPERADO = {
    'invalida':          'cuarentena (regla: {regla})',
    'duplicado_en_lote': 'una sola fila en Silver (dedup dentro del lote)',
    'reenvio':           'Silver no cambia (MERGE idempotente)',
    'llegada_tardia':    'Silver no cambia (guarda de orden: fecha anterior a la vigente)',
    'hecho_modificado':  'Silver no cambia (insert-only: un hecho no se actualiza)',
    'correccion':        'Silver toma el valor corregido (gana el último archivo)',
}


def _a_datetime(v):
    return v if isinstance(v, datetime) else datetime.fromisoformat(str(v))


def _clave(fila, campos):
    return '|'.join(str(fila.get(c, '')) for c in campos)


def ultima_version(filas, clave, orden):
    """Versión vigente por clave: la de mayor columna de orden."""
    vig = {}
    for f in filas:
        k = _clave(f, clave)
        if k not in vig or str(f.get(orden) or '') > str(vig[k].get(orden) or ''):
            vig[k] = f
    return list(vig.values())


def ensuciar(filas, reglas, n, tabla=None, manifiesto=None, clave=None):
    """Rompe una regla de calidad en n filas, in-place (reparte las reglas en ronda).
    reglas: lista de (nombre_regla, funcion(fila) que la rompe). Devuelve las filas tocadas."""
    if not reglas or n <= 0 or not filas:
        return []
    # Como mucho la mitad de las filas: el delta sigue siendo mayormente válido
    elegidas = random.sample(range(len(filas)), min(n, max(1, len(filas) // 2)))
    for i, idx in enumerate(elegidas):
        nombre, romper = reglas[i % len(reglas)]
        registro = _registro(tabla, 'invalida', filas[idx], clave, regla=nombre)  # clave antes de romperla
        romper(filas[idx])
        if manifiesto is not None:
            manifiesto.append(registro)
    return [filas[i] for i in elegidas]


def _registro(tabla, caso, fila, clave, regla=''):
    clave_txt = _clave(fila, clave) if clave else ''
    return {'tabla': tabla, 'caso': caso, 'clave': clave_txt,
            'esperado_en_silver': ESPERADO[caso].format(regla=regla)}


def agregar_casos(tabla, filas, vigentes, cfg, n, ahora, manifiesto):
    """Agrega a `filas` (las del delta) los casos de Silver de la tabla y los anota
    en `manifiesto`. Devuelve la lista final para escribir en el CSV.

    cfg: clave (lista), orden (columna), patron ('upsert' | 'insert_only' | 'ultimo_archivo'),
         reglas [(nombre, romper)], mutar (fila → cambia un atributo de negocio).
    vigentes: versión vigente de las filas ya cargadas (carga inicial + deltas previos)."""
    if n <= 0 or not filas:
        return filas
    clave, orden, patron = cfg['clave'], cfg['orden'], cfg['patron']
    ts = ahora.strftime('%Y-%m-%d %H:%M:%S')
    # Las inválidas de deltas anteriores (clave vacía) no sirven de base para otros casos
    vigentes = [v for v in vigentes if all(v.get(c) not in (None, '') for c in clave)]

    # Filas nuevas de este delta (las cambiadas ya existen): las inválidas salen de aquí
    claves_vig = {_clave(v, clave) for v in vigentes}
    nuevas = [f for f in filas if _clave(f, clave) not in claves_vig]
    invalidas = {id(f) for f in ensuciar(nuevas, cfg.get('reglas', []), n, tabla, manifiesto, clave)}
    extra = []
    validas = [f for f in filas if id(f) not in invalidas] or filas
    for f in random.sample(validas, min(n, len(validas))):
        extra.append(dict(f))
        manifiesto.append(_registro(tabla, 'duplicado_en_lote', f, clave))

    # Las versiones vigentes que este delta no toca: base para reenvíos y tardías
    claves_delta = {_clave(f, clave) for f in filas}
    intactas = [v for v in vigentes if _clave(v, clave) not in claves_delta]
    muestra = random.sample(intactas, min(2 * n, len(intactas)))
    reenvios, otras = muestra[:n], muestra[n:]

    for v in reenvios:
        extra.append(dict(v))
        manifiesto.append(_registro(tabla, 'reenvio', v, clave))

    for v in otras:
        f = dict(v)
        cfg['mutar'](f)
        if patron == 'upsert':
            # La versión tardía es 1 día anterior a la vigente (orden, o su respaldo si viene vacío)
            vigente = next((v[c] for c in [orden, *cfg.get('respaldo', [])] if v.get(c)), None)
            if vigente is None:
                continue
            f[orden] = (_a_datetime(vigente) - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
            caso = 'llegada_tardia'
        elif patron == 'insert_only':
            f[orden] = ts
            caso = 'hecho_modificado'
        else:
            caso = 'correccion'
        extra.append(f)
        manifiesto.append(_registro(tabla, caso, f, clave))

    return filas + extra


def escribir_manifiesto(manifiesto, salida, sufijo):
    if not manifiesto:
        return None
    ruta = os.path.join(salida, f'casos_silver_{sufijo}.csv')
    with open(ruta, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['tabla', 'caso', 'clave', 'esperado_en_silver'])
        w.writeheader()
        w.writerows(manifiesto)
    conteo = {}
    for r in manifiesto:
        conteo[r['caso']] = conteo.get(r['caso'], 0) + 1
    print(f'  🧪 casos Silver: ' + ', '.join(f'{c} {k}' for c, k in sorted(conteo.items()))
          + f'  →  {ruta}')
    return ruta


def subir_a_s3(archivos, destino_s3, perfil=None):
    """Sube cada CSV a <destino_s3>/<tabla>/<archivo>, la estructura que espera Auto Loader
    (una carpeta por tabla en landing). archivos: lista de (tabla, ruta_local)."""
    try:
        import boto3
    except ImportError:
        sys.exit('ERROR: falta boto3. Instalar con: pip install "boto3[crt]"')
    if not destino_s3.startswith('s3://'):
        sys.exit(f'ERROR: --s3 debe empezar con s3:// (recibido: {destino_s3})')
    bucket, _, prefijo = destino_s3[len('s3://'):].partition('/')
    prefijo = prefijo.strip('/')
    s3 = boto3.Session(profile_name=perfil).client('s3')
    for tabla, ruta in archivos:
        clave = '/'.join(p for p in (prefijo, tabla, os.path.basename(ruta)) if p)
        s3.upload_file(ruta, bucket, clave)
        print(f'  ☁️  {tabla:<24} →  s3://{bucket}/{clave}')
