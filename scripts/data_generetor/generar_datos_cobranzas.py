#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
WIZARD BANK · Generador de datos del sistema de cobranzas
Data Wizard Academy — Sesión 10 (Azure Data Factory)

Genera cuotas, pagos y gestiones_cobranza para el schema `cobranzas` en la
MISMA base wizardbank (Azure SQL) que ya usas desde la Sesión 8.2 — reutiliza
servidor, firewall y credenciales, sin infraestructura nueva que levantar.

No hay integridad referencial real contra lending.desembolsos (en un caso
real vivirían en sistemas separados) — el vínculo es lógico vía
numero_credito, en el mismo rango que lending.desembolsos.id_desembolso.

Dos modos:
  --modo full         Carga inicial completa (por defecto ~27.000 créditos
                       con su historia completa de cuotas/pagos/gestiones).
                       Usar --truncar para repetirla desde cero.
  --modo incremental   Toca las 3 tablas para simular actividad real desde
                       la última corrida: agrega N créditos NUEVOS (cuotas
                       Pendiente) y además mueve una muestra de cuotas
                       Pendiente YA EXISTENTES a Pagada (crea su pago) o a
                       Vencida (crea su gestión de cobranza). Todo con
                       fecha_modificacion = ahora, para que el watermark del
                       pipeline de ADF lo detecte en la siguiente corrida.
                       Con --destino csv lee el estado de los CSV de --base y
                       escribe <tabla>_delta_<AAAAMMDD_HHMMSS>.csv.

Casos para Silver (S15/S16): todo incremental agrega filas que Silver debe
resolver — inválidas (→ cuarentena) y, en CSV, duplicados en el lote,
reenvíos de versiones ya cargadas, llegadas tardías y hechos modificados.
Ver casos_silver.py. --casos-silver 0 los desactiva.

Uso:
    # Carga inicial en Azure SQL (schema cobranzas de la base wizardbank)
    python generar_datos_cobranzas.py --destino azuresql --modo full \
        --dsn "Driver={ODBC Driver 18 for SQL Server};Server=tcp:<tu-servidor>.database.windows.net,1433;\
Database=wizardbank;Uid=wizadmin;Pwd=***;Encrypt=yes;TrustServerCertificate=no;"

    # Simular la llegada de 25 créditos nuevos (para probar el incremental de ADF)
    python generar_datos_cobranzas.py --destino azuresql --modo incremental \
        --nuevos-creditos 25 --dsn "..."

    # Bonus on-premise (Docker + Self-hosted IR): mismo generador, otro destino
    python generar_datos_cobranzas.py --destino sqlserver-legacy --modo full \
        --dsn "Driver={ODBC Driver 18 for SQL Server};Server=tcp:localhost,1401;\
Database=cobranzas;Uid=sa;Pwd=***;Encrypt=yes;TrustServerCertificate=yes;"

    # Generar CSVs (no requiere base de datos)
    python generar_datos_cobranzas.py --destino csv --salida ./datos_cobranzas

    # Generar CSVs y subirlos directo a landing en S3 (una carpeta por tabla)
    python generar_datos_cobranzas.py --destino csv --salida ./csv_cobranzas \
        --s3 s3://lakehouse-datawizard/landing/wizard_bank_onp --aws-profile datawizard

    # Delta en CSV sobre la carga inicial de ./csv_cobranzas, directo a landing
    python generar_datos_cobranzas.py --destino csv --modo incremental \
        --base ./csv_cobranzas --salida ./csv_cobranzas_delta --seed 7 \
        --s3 s3://lakehouse-datawizard/landing/wizard_bank_onp --aws-profile datawizard

Requisitos:
    pip install pyodbc   (para --destino azuresql o sqlserver-legacy)
    pip install "boto3[crt]"   (para --s3; crt lo pide el perfil de `aws login`)
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import csv
import os
import random
import sys
from datetime import date, datetime, timedelta

from casos_silver import agregar_casos, ensuciar, escribir_manifiesto, subir_a_s3, ultima_version

# ─────────────────────────────────────────────────────────────────────────────
# PARÁMETROS
# ─────────────────────────────────────────────────────────────────────────────

# Coincide con los ~26.960 desembolsos que produce generar_datos_wizard_bank.py
# con el volumen por defecto (50.000 clientes) — así numero_credito "cruza"
# con lending.desembolsos.id_desembolso, como pide el escenario de la Sesión 10.
N_CREDITOS_DEFAULT = 26_960

PLAZO_MIN, PLAZO_MAX = 6, 36
FECHA_INICIO_CREDITOS = date(2024, 8, 1)
FECHA_FIN_CREDITOS    = date(2026, 7, 31)
HOY = date(2026, 8, 15)   # "hoy" simulado para decidir qué cuotas ya vencieron

SEED = 42

PROB_PAGADA_A_TIEMPO = 0.78
PROB_PAGADA_TARDE     = 0.13
# El resto (0.09) queda Vencida

MEDIOS_PAGO      = ['Transferencia', 'Debito Automatico', 'Ventanilla', 'Agente']
PESO_MEDIOS_PAGO = [0.45, 0.35, 0.12, 0.08]

TIPOS_GESTION = ['Llamada', 'SMS', 'Email', 'Visita', 'Carta Notarial']
PESO_TIPOS    = [0.45, 0.25, 0.15, 0.10, 0.05]

RESULTADOS_GESTION = ['Compromiso de pago', 'Sin respuesta', 'Rechazo', 'Pago inmediato']
PESO_RESULTADOS    = [0.35, 0.40, 0.15, 0.10]

NOMBRES_GESTOR = [
    'Ana Beltrán', 'Luis Vega', 'Carla Rojas', 'Miguel Soto', 'Paula Vidal',
    'Jorge Núñez', 'Karen Salas', 'Diego Paredes', 'Rocío Aguirre', 'Iván Cornejo',
]

ORDEN_CARGA = ['cuotas', 'pagos', 'gestiones_cobranza']


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

def fecha_aleatoria(desde, hasta):
    dias = (hasta - desde).days
    return desde + timedelta(days=random.randint(0, max(dias, 0)))


def sumar_meses(f, n):
    mes = f.month - 1 + n
    anio = f.year + mes // 12
    mes = mes % 12 + 1
    dia = min(f.day, 28)
    return date(anio, mes, dia)


def esquema_de(destino):
    """azuresql -> schema 'cobranzas' en la base wizardbank.
       sqlserver-legacy -> schema 'dbo' en su propia base 'cobranzas' (bonus Docker)."""
    return 'cobranzas' if destino == 'azuresql' else 'dbo'


# ─────────────────────────────────────────────────────────────────────────────
# GENERACIÓN — CARGA INICIAL (full)
# ─────────────────────────────────────────────────────────────────────────────

def generar_credito_historico(numero_credito):
    """Un crédito con su historia completa: cuotas, pagos y gestiones si aplica."""
    cuotas, pagos, gestiones = [], [], []

    fecha_desembolso = fecha_aleatoria(FECHA_INICIO_CREDITOS, FECHA_FIN_CREDITOS)
    plazo = random.randint(PLAZO_MIN, PLAZO_MAX)
    monto_cuota_base = round(random.uniform(150, 2500), 2)

    cuotas_vencidas_fechas = []

    for n in range(1, plazo + 1):
        fecha_vencimiento = sumar_meses(fecha_desembolso, n)
        monto_interes = round(monto_cuota_base * 0.18, 2)
        monto_capital = round(monto_cuota_base - monto_interes, 2)

        if fecha_vencimiento > HOY:
            estado = 'Pendiente'
        else:
            r = random.random()
            if r < PROB_PAGADA_A_TIEMPO + PROB_PAGADA_TARDE:
                estado = 'Pagada'
            else:
                estado = 'Vencida'
                cuotas_vencidas_fechas.append(fecha_vencimiento)

        f_creacion = datetime.combine(fecha_desembolso, datetime.min.time())
        f_mod_fecha = fecha_vencimiento if estado != 'Pendiente' else fecha_desembolso
        f_mod = datetime.combine(f_mod_fecha, datetime.min.time())

        cuotas.append({
            'numero_credito': numero_credito, 'numero_cuota': n,
            'fecha_vencimiento': fecha_vencimiento.isoformat(),
            'monto_cuota': monto_cuota_base, 'monto_capital': monto_capital,
            'monto_interes': monto_interes, 'estado_cuota': estado,
            'fecha_creacion': f_creacion.isoformat(sep=' '),
            'fecha_modificacion': f_mod.isoformat(sep=' '),
        })

        if estado == 'Pagada':
            atraso = 0 if random.random() < 0.75 else random.randint(1, 20)
            fecha_pago = datetime.combine(fecha_vencimiento, datetime.min.time()) + timedelta(days=atraso)
            pagos.append({
                'numero_credito': numero_credito,
                # No hay id_cuota (autogenerado por la BD, no lo conocemos acá):
                # el cruce cuotas↔pagos es por (numero_credito, numero_cuota) —
                # exactamente el tipo de join que Silver resuelve en la Sesión 16.
                'numero_cuota': n,
                'fecha_pago': fecha_pago.isoformat(sep=' '),
                'monto_pagado': monto_cuota_base,
                'medio_pago': random.choices(MEDIOS_PAGO, weights=PESO_MEDIOS_PAGO)[0],
                'fecha_creacion': fecha_pago.isoformat(sep=' '),
                'fecha_modificacion': fecha_pago.isoformat(sep=' '),
            })

    if cuotas_vencidas_fechas:
        primera_vencida = min(cuotas_vencidas_fechas)
        dias_mora = (HOY - primera_vencida).days
        for _ in range(random.randint(1, 4)):
            fecha_gestion = datetime.combine(fecha_aleatoria(primera_vencida, HOY), datetime.min.time())
            gestiones.append({
                'numero_credito': numero_credito,
                'fecha_gestion': fecha_gestion.isoformat(sep=' '),
                'tipo_gestion': random.choices(TIPOS_GESTION, weights=PESO_TIPOS)[0],
                'resultado': random.choices(RESULTADOS_GESTION, weights=PESO_RESULTADOS)[0],
                'dias_mora_al_momento': max(dias_mora, 1),
                'gestor': random.choice(NOMBRES_GESTOR),
                'fecha_creacion': fecha_gestion.isoformat(sep=' '),
                'fecha_modificacion': fecha_gestion.isoformat(sep=' '),
            })

    return cuotas, pagos, gestiones


def generar_full(n_creditos, seed):
    random.seed(seed)
    datos = {'cuotas': [], 'pagos': [], 'gestiones_cobranza': []}
    for numero_credito in range(1, n_creditos + 1):
        c, p, g = generar_credito_historico(numero_credito)
        datos['cuotas'].extend(c)
        datos['pagos'].extend(p)
        datos['gestiones_cobranza'].extend(g)
    return datos


# ─────────────────────────────────────────────────────────────────────────────
# GENERACIÓN — INCREMENTAL (créditos nuevos, recién desembolsados)
# ─────────────────────────────────────────────────────────────────────────────

def generar_incremental(numero_credito_inicial, n_nuevos, seed):
    """Créditos que "acaban de desembolsarse": todas sus cuotas en Pendiente,
    sin pagos ni gestiones (son demasiado nuevos para tener historia todavía).
    fecha_modificacion = ahora, para que el watermark del pipeline los detecte."""
    random.seed(seed)
    ahora = datetime.utcnow()
    cuotas = []

    for i in range(n_nuevos):
        numero_credito = numero_credito_inicial + i
        plazo = random.randint(PLAZO_MIN, PLAZO_MAX)
        monto_cuota_base = round(random.uniform(150, 2500), 2)

        for n in range(1, plazo + 1):
            fecha_vencimiento = sumar_meses(HOY, n)
            monto_interes = round(monto_cuota_base * 0.18, 2)
            monto_capital = round(monto_cuota_base - monto_interes, 2)
            cuotas.append({
                'numero_credito': numero_credito, 'numero_cuota': n,
                'fecha_vencimiento': fecha_vencimiento.isoformat(),
                'monto_cuota': monto_cuota_base, 'monto_capital': monto_capital,
                'monto_interes': monto_interes, 'estado_cuota': 'Pendiente',
                'fecha_creacion': ahora.isoformat(sep=' '),
                'fecha_modificacion': ahora.isoformat(sep=' '),
            })

    return {'cuotas': cuotas, 'pagos': [], 'gestiones_cobranza': []}


def actualizar_cuotas_existentes(dsn, esquema, n_pagar, n_vencer, n_invalidos=0):
    """Toca las OTRAS dos tablas del delta: agarra cuotas 'Pendiente' ya
    cargadas (de la corrida full o de un incremental anterior) y las mueve
    de estado — algunas a 'Pagada' (con su pago) y otras a 'Vencida' (con
    su gestión de cobranza). Sin esto, --modo incremental solo ejercita
    `cuotas`; con esto, la corrida toca las 3 tablas, como pide probar el
    pipeline de punta a punta."""
    import pyodbc
    conn = pyodbc.connect(dsn, autocommit=False)
    cur = conn.cursor()
    ahora = datetime.utcnow()

    cur.execute(
        f"SELECT TOP {n_pagar + n_vencer} id_cuota, numero_credito, numero_cuota, monto_cuota "
        f"FROM {esquema}.cuotas WHERE estado_cuota = 'Pendiente' ORDER BY NEWID()")
    filas = cur.fetchall()
    a_pagar, a_vencer = filas[:n_pagar], filas[n_pagar:n_pagar + n_vencer]

    # Las primeras n_invalidos de cada lista rompen una regla de Silver (→ cuarentena):
    # pago con monto 0 y gestión con días de mora negativos.
    for i, (id_cuota, numero_credito, numero_cuota, monto_cuota) in enumerate(a_pagar):
        if i < n_invalidos:
            monto_cuota = 0
        cur.execute(
            f"UPDATE {esquema}.cuotas SET estado_cuota = 'Pagada', fecha_modificacion = ? "
            f"WHERE id_cuota = ?", ahora, id_cuota)
        cur.execute(
            f"INSERT INTO {esquema}.pagos "
            f"(numero_credito, numero_cuota, fecha_pago, monto_pagado, medio_pago, fecha_creacion, fecha_modificacion) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            numero_credito, numero_cuota, ahora, monto_cuota,
            random.choices(MEDIOS_PAGO, weights=PESO_MEDIOS_PAGO)[0], ahora, ahora)

    for i, (id_cuota, numero_credito, numero_cuota, monto_cuota) in enumerate(a_vencer):
        cur.execute(
            f"UPDATE {esquema}.cuotas SET estado_cuota = 'Vencida', fecha_modificacion = ? "
            f"WHERE id_cuota = ?", ahora, id_cuota)
        cur.execute(
            f"INSERT INTO {esquema}.gestiones_cobranza "
            f"(numero_credito, fecha_gestion, tipo_gestion, resultado, dias_mora_al_momento, gestor, "
            f"fecha_creacion, fecha_modificacion) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            numero_credito, ahora, random.choices(TIPOS_GESTION, weights=PESO_TIPOS)[0],
            random.choices(RESULTADOS_GESTION, weights=PESO_RESULTADOS)[0],
            -random.randint(1, 30) if i < n_invalidos else random.randint(1, 10),
            random.choice(NOMBRES_GESTOR), ahora, ahora)

    conn.commit()
    cur.close()
    conn.close()
    return len(a_pagar), len(a_vencer)


# ─────────────────────────────────────────────────────────────────────────────
# IDS Y CASOS PARA SILVER
# ─────────────────────────────────────────────────────────────────────────────

# En la base, id_cuota / id_pago / id_gestion son IDENTITY. En el CSV hay que
# materializarlos: son la clave de negocio de Silver (MERGE por id).
CAMPO_ID = {'cuotas': 'id_cuota', 'pagos': 'id_pago', 'gestiones_cobranza': 'id_gestion'}


def asignar_ids(datos, desde=None):
    """Pone el id como primera columna, continuando desde `desde` (máximos ya cargados)."""
    desde = desde or {}
    for tabla, campo in CAMPO_ID.items():
        siguiente = desde.get(tabla, 0) + 1
        filas = []
        for f in datos[tabla]:
            if f.get(campo) in (None, ''):
                f = {campo: siguiente, **{k: v for k, v in f.items() if k != campo}}
                siguiente += 1
            filas.append(f)
        datos[tabla] = filas
    return datos


def _sumar(f, col, delta):
    f[col] = round(float(f[col]) + delta, 2)


# Reglas de silver.cobranzas (las mismas del YAML de Silver) y cómo romperlas.
CASOS = {
    'cuotas': {
        'clave': ['id_cuota'], 'orden': 'fecha_modificacion', 'patron': 'upsert',
        'reglas': [
            ('estado_valido', lambda f: f.update(estado_cuota='PAGADO')),
            ('cuota_cuadra',  lambda f: _sumar(f, 'monto_capital', 10)),   # cuota != capital + interés
            ('clave',         lambda f: f.update(id_cuota='')),
        ],
        'mutar': lambda f: f.update(estado_cuota='Pendiente' if f['estado_cuota'] != 'Pendiente' else 'Vencida'),
    },
    'pagos': {
        'clave': ['id_pago'], 'orden': 'fecha_modificacion', 'patron': 'insert_only',
        'reglas': [
            ('monto_positivo', lambda f: f.update(monto_pagado=random.choice([0, -round(float(f['monto_pagado']), 2)]))),
            ('clave',          lambda f: f.update(id_pago='')),
        ],
        'mutar': lambda f: _sumar(f, 'monto_pagado', 50),
    },
    'gestiones_cobranza': {
        'clave': ['id_gestion'], 'orden': 'fecha_modificacion', 'patron': 'insert_only',
        'reglas': [
            ('dias_mora_no_negativos', lambda f: f.update(dias_mora_al_momento=-random.randint(1, 30))),
            ('clave',                  lambda f: f.update(id_gestion='')),
        ],
        'mutar': lambda f: f.update(resultado='Pago inmediato'),
    },
}


def leer_estado_csv(carpetas):
    """Lee los CSV de la carga inicial y de los deltas previos (<tabla>*.csv en cada carpeta)."""
    import glob
    base = carpetas[0]
    filas = {}
    for tabla in ORDEN_CARGA:
        rutas = sorted({r for c in carpetas for r in glob.glob(os.path.join(c, f'{tabla}*.csv'))})
        if not rutas:
            sys.exit(f'ERROR: no encontré {tabla}*.csv en {base} — genera primero la carga '
                     f'inicial con --modo full --destino csv --salida {base}')
        filas[tabla] = []
        for ruta in rutas:
            with open(ruta, newline='', encoding='utf-8') as f:
                filas[tabla].extend(csv.DictReader(f))
        campo = CAMPO_ID[tabla]
        if filas[tabla] and campo not in filas[tabla][0]:
            sys.exit(f'ERROR: {tabla} en {base} no trae {campo}. Esos CSV son de una versión '
                     f'anterior del generador: regenera la carga inicial con --modo full --destino csv.')
    return filas


def generar_incremental_csv(base_filas, n_nuevos, n_pagar, n_vencer, seed, ahora):
    """Equivalente CSV de --modo incremental: créditos nuevos + cuotas Pendiente
    existentes que pasan a Pagada (con su pago) o Vencida (con su gestión).
    Las cuotas que cambian salen como VERSIÓN NUEVA (mismo id_cuota, otra
    fecha_modificacion): Bronze guarda ambas y Silver se queda con la última."""
    ts = ahora.isoformat(sep=' ', timespec='seconds')
    maximos = {t: max((int(f[c]) for f in base_filas[t] if f[c]), default=0) for t, c in CAMPO_ID.items()}
    max_credito = max(int(f['numero_credito']) for f in base_filas['cuotas'])

    datos = generar_incremental(max_credito + 1, n_nuevos, seed)
    for f in datos['cuotas']:
        f.update(fecha_creacion=ts, fecha_modificacion=ts)
    asignar_ids(datos, maximos)

    vigentes = ultima_version(base_filas['cuotas'], ['id_cuota'], 'fecha_modificacion')
    pendientes = [c for c in vigentes if c['estado_cuota'] == 'Pendiente']
    elegidas = random.sample(pendientes, min(n_pagar + n_vencer, len(pendientes)))
    sig_pago, sig_gestion = maximos['pagos'] + 1, maximos['gestiones_cobranza'] + 1
    for i, c in enumerate(elegidas):
        pagar = i < n_pagar
        datos['cuotas'].append({**c, 'estado_cuota': 'Pagada' if pagar else 'Vencida',
                                'fecha_modificacion': ts})
        if pagar:
            datos['pagos'].append({
                'id_pago': sig_pago, 'numero_credito': c['numero_credito'], 'numero_cuota': c['numero_cuota'],
                'fecha_pago': ts, 'monto_pagado': c['monto_cuota'],
                'medio_pago': random.choices(MEDIOS_PAGO, weights=PESO_MEDIOS_PAGO)[0],
                'fecha_creacion': ts, 'fecha_modificacion': ts})
            sig_pago += 1
        else:
            datos['gestiones_cobranza'].append({
                'id_gestion': sig_gestion, 'numero_credito': c['numero_credito'], 'fecha_gestion': ts,
                'tipo_gestion': random.choices(TIPOS_GESTION, weights=PESO_TIPOS)[0],
                'resultado': random.choices(RESULTADOS_GESTION, weights=PESO_RESULTADOS)[0],
                'dias_mora_al_momento': random.randint(1, 10), 'gestor': random.choice(NOMBRES_GESTOR),
                'fecha_creacion': ts, 'fecha_modificacion': ts})
            sig_gestion += 1
    return datos, sum(1 for i in range(len(elegidas)) if i < n_pagar), max(0, len(elegidas) - n_pagar)


def escribir_csv_delta(datos, base_filas, salida, ahora, n_casos):
    """Un <tabla>_delta_<sufijo>.csv por tabla, con los casos de Silver agregados,
    más el manifiesto local casos_silver_<sufijo>.csv (no se sube a landing)."""
    os.makedirs(salida, exist_ok=True)
    sufijo = ahora.strftime('%Y%m%d_%H%M%S')
    manifiesto, escritos = [], []
    for tabla in ORDEN_CARGA:
        cfg = CASOS[tabla]
        vigentes = ultima_version(base_filas[tabla], cfg['clave'], cfg['orden'])
        filas = agregar_casos(tabla, datos[tabla], vigentes, cfg, n_casos, ahora, manifiesto)
        if not filas:
            continue
        ruta = os.path.join(salida, f'{tabla}_delta_{sufijo}.csv')
        columnas = list(base_filas[tabla][0].keys())
        with open(ruta, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columnas, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(filas)
        print(f'  ✅ {tabla:<20} {len(filas):>9,} filas  →  {ruta}')
        escritos.append((tabla, ruta))
    escribir_manifiesto(manifiesto, salida, sufijo)
    return escritos


# ─────────────────────────────────────────────────────────────────────────────
# ESCRITURA
# ─────────────────────────────────────────────────────────────────────────────

def escribir_csv(datos, salida, sufijo=''):
    os.makedirs(salida, exist_ok=True)
    escritos = []
    for tabla in ORDEN_CARGA:
        filas = datos[tabla]
        if not filas:
            continue
        ruta = os.path.join(salida, f'{tabla}{sufijo}.csv')
        with open(ruta, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
            writer.writeheader()
            writer.writerows(filas)
        print(f'  ✅ {tabla:<20} {len(filas):>9,} filas  →  {ruta}')
        escritos.append((tabla, ruta))
    return escritos


def obtener_max_credito(conn, esquema):
    cur = conn.cursor()
    cur.execute(f'SELECT ISNULL(MAX(numero_credito), 0) FROM {esquema}.cuotas')
    return cur.fetchone()[0]


def escribir_sql(datos, dsn, esquema, truncar):
    try:
        import pyodbc
    except ImportError:
        sys.exit('ERROR: falta pyodbc. Instalar con: pip install pyodbc\n'
                 '       (requiere el ODBC Driver 18 for SQL Server)')

    conn = pyodbc.connect(dsn, autocommit=False)
    cur = conn.cursor()
    cur.fast_executemany = True

    if truncar:
        for tabla in reversed(ORDEN_CARGA):
            cur.execute(f'DELETE FROM {esquema}.{tabla}')
        conn.commit()
        print('  🧹 Tablas vaciadas')

    # Las cuotas se insertan con el schema real de columnas (id lo asigna la BD).
    for tabla in ORDEN_CARGA:
        filas = datos[tabla]
        if not filas:
            continue
        columnas = list(filas[0].keys())
        marcadores = ', '.join('?' * len(columnas))
        sql = f'INSERT INTO {esquema}.{tabla} ({", ".join(columnas)}) VALUES ({marcadores})'
        valores = [tuple(f[c] for c in columnas) for f in filas]

        for ini in range(0, len(valores), 5_000):
            cur.executemany(sql, valores[ini:ini + 5_000])
        conn.commit()
        print(f'  ✅ {tabla:<20} {len(filas):>9,} filas insertadas')

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Genera datos sintéticos de cobranzas para el lab de ADF (Sesión 10).')
    ap.add_argument('--destino', choices=['csv', 'azuresql', 'sqlserver-legacy'], default='csv')
    ap.add_argument('--modo', choices=['full', 'incremental'], default='full')
    ap.add_argument('--salida', default='./datos_cobranzas')
    ap.add_argument('--base', default='./datos_cobranzas',
                    help='--modo incremental --destino csv: carpeta con los CSV ya generados '
                         '(carga inicial + deltas previos); de ahí salen los ids y las cuotas a mover')
    ap.add_argument('--sufijo', nargs='?', const='auto', default='',
                    help='--modo full --destino csv: sufijo del nombre (tabla_<sufijo>.csv). Sin valor usa '
                         'la fecha-hora. Auto Loader ignora nombres que ya leyó.')
    ap.add_argument('--dsn', default=os.getenv('COBRANZAS_DSN'))
    ap.add_argument('--s3', default=os.getenv('COBRANZAS_S3'),
                    help='Con --destino csv: además sube los CSV a este prefijo de S3, una carpeta '
                         'por tabla (p. ej. s3://lakehouse-datawizard/landing/wizard_bank_onp). '
                         'También por variable COBRANZAS_S3.')
    ap.add_argument('--aws-profile', default=os.getenv('AWS_PROFILE'),
                    help='Perfil de la AWS CLI para --s3 (default: AWS_PROFILE o credenciales por defecto)')
    ap.add_argument('--n-creditos', type=int, default=N_CREDITOS_DEFAULT,
                     help='Solo --modo full: cantidad de créditos históricos a generar')
    ap.add_argument('--nuevos-creditos', type=int, default=25,
                     help='Solo --modo incremental: cuántos créditos nuevos agregar')
    ap.add_argument('--cuotas-pagar', type=int, default=30,
                     help='Solo --modo incremental: cuántas cuotas Pendiente existentes marcar '
                          'Pagada (genera su pago) — 0 para omitir. Default: 30')
    ap.add_argument('--cuotas-vencer', type=int, default=15,
                     help='Solo --modo incremental: cuántas cuotas Pendiente existentes marcar '
                          'Vencida (genera su gestión de cobranza) — 0 para omitir. Default: 15')
    ap.add_argument('--casos-silver', type=int, default=5,
                     help='Solo --modo incremental: filas por caso de Silver y tabla (inválidas, '
                          'duplicados, reenvíos, tardías…; ver casos_silver.py). 0 = delta limpio. Default: 5')
    ap.add_argument('--truncar', action='store_true',
                     help='Solo --modo full: vaciar las tablas antes de insertar')
    ap.add_argument('--seed', type=int, default=SEED,
                     help=f'Semilla (default: {SEED}). En --modo incremental usa una distinta a la del full.')
    args = ap.parse_args()

    if args.s3 and args.destino != 'csv':
        sys.exit('ERROR: --s3 solo aplica con --destino csv')

    esquema = esquema_de(args.destino) if args.destino != 'csv' else None

    print('═' * 68)
    print('  WIZARD BANK · Cobranzas (Sesión 10 — Azure Data Factory)')
    print('═' * 68)
    print(f'  Modo: {args.modo}   ·   Destino: {args.destino}'
          + (f' (schema {esquema})' if esquema else ''))

    escritos = []
    if args.modo == 'incremental' and args.destino == 'csv':
        ahora = datetime.now()
        base_filas = leer_estado_csv([args.base, args.salida])
        random.seed(args.seed)
        datos, n_pagadas, n_vencidas = generar_incremental_csv(
            base_filas, args.nuevos_creditos, args.cuotas_pagar, args.cuotas_vencer, args.seed, ahora)
        print(f'  Base: {args.base}   ·   Créditos nuevos: {args.nuevos_creditos}   ·   '
              f'cuotas → Pagada {n_pagadas} / Vencida {n_vencidas}\n')
        print('── Escribiendo (csv delta) ───────────────────────────────────')
        escritos = escribir_csv_delta(datos, base_filas, args.salida, ahora, args.casos_silver)

    elif args.modo == 'full':
        print(f'  Créditos: {args.n_creditos:,}   ·   Semilla: {args.seed}\n')
        datos = generar_full(args.n_creditos, args.seed)
        print('── Generando ─────────────────────────────────────────────────')
        for tabla in ORDEN_CARGA:
            print(f'  {tabla:<20} {len(datos[tabla]):>9,}')
        print('\n── Escribiendo ───────────────────────────────────────────────')
        if args.destino == 'csv':
            sufijo = datetime.now().strftime('%Y%m%d_%H%M%S') if args.sufijo == 'auto' else args.sufijo
            escritos = escribir_csv(asignar_ids(datos), args.salida, f'_{sufijo}' if sufijo else '')
        else:
            if not args.dsn:
                sys.exit('ERROR: falta --dsn (o la variable de entorno COBRANZAS_DSN)')
            escribir_sql(datos, args.dsn, esquema, args.truncar)

    else:
        if not args.dsn:
            sys.exit('ERROR: falta --dsn (o la variable de entorno COBRANZAS_DSN)')
        try:
            import pyodbc
        except ImportError:
            sys.exit('ERROR: falta pyodbc. Instalar con: pip install pyodbc')
        conn_check = pyodbc.connect(args.dsn, autocommit=True)
        max_actual = obtener_max_credito(conn_check, esquema)
        conn_check.close()
        numero_inicial = max_actual + 1
        print(f'  Último numero_credito en la base: {max_actual:,}')
        print(f'  Generando {args.nuevos_creditos} créditos nuevos desde el {numero_inicial:,}\n')
        datos = generar_incremental(numero_inicial, args.nuevos_creditos, args.seed)
        # En la base no hay duplicados ni llegadas tardías (eso nace al mover archivos);
        # sí filas que rompen las reglas de Silver. La regla de clave no aplica: el id es IDENTITY.
        reglas = [r for r in CASOS['cuotas']['reglas'] if r[0] != 'clave']
        n_inv = len(ensuciar(datos['cuotas'], reglas, args.casos_silver))
        print('── Escribiendo ───────────────────────────────────────────────')
        escribir_sql(datos, args.dsn, esquema, False)
        print(f'  🧪 cuotas inválidas para Silver: {n_inv}')
        if args.cuotas_pagar > 0 or args.cuotas_vencer > 0:
            print(f'\n── Tocando cuotas existentes (pagos y gestiones) ──────────────')
            n_pagadas, n_vencidas = actualizar_cuotas_existentes(
                args.dsn, esquema, args.cuotas_pagar, args.cuotas_vencer, args.casos_silver)
            print(f'  ✅ cuotas → Pagada       {n_pagadas:>9,}  (+ su pago en cobranzas.pagos)')
            print(f'  ✅ cuotas → Vencida      {n_vencidas:>9,}  (+ su gestión en cobranzas.gestiones_cobranza)')

    if args.s3 and escritos:
        print('\n── Subiendo a S3 ─────────────────────────────────────────────')
        subir_a_s3(escritos, args.s3, args.aws_profile)

    print()


if __name__ == '__main__':
    main()
