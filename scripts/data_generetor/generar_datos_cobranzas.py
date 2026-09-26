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

Requisitos:
    pip install pyodbc   (para --destino azuresql o sqlserver-legacy)
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import csv
import os
import random
import sys
from datetime import date, datetime, timedelta

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


def actualizar_cuotas_existentes(dsn, esquema, n_pagar, n_vencer):
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

    for id_cuota, numero_credito, numero_cuota, monto_cuota in a_pagar:
        cur.execute(
            f"UPDATE {esquema}.cuotas SET estado_cuota = 'Pagada', fecha_modificacion = ? "
            f"WHERE id_cuota = ?", ahora, id_cuota)
        cur.execute(
            f"INSERT INTO {esquema}.pagos "
            f"(numero_credito, numero_cuota, fecha_pago, monto_pagado, medio_pago, fecha_creacion, fecha_modificacion) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            numero_credito, numero_cuota, ahora, monto_cuota,
            random.choices(MEDIOS_PAGO, weights=PESO_MEDIOS_PAGO)[0], ahora, ahora)

    for id_cuota, numero_credito, numero_cuota, monto_cuota in a_vencer:
        cur.execute(
            f"UPDATE {esquema}.cuotas SET estado_cuota = 'Vencida', fecha_modificacion = ? "
            f"WHERE id_cuota = ?", ahora, id_cuota)
        cur.execute(
            f"INSERT INTO {esquema}.gestiones_cobranza "
            f"(numero_credito, fecha_gestion, tipo_gestion, resultado, dias_mora_al_momento, gestor, "
            f"fecha_creacion, fecha_modificacion) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            numero_credito, ahora, random.choices(TIPOS_GESTION, weights=PESO_TIPOS)[0],
            random.choices(RESULTADOS_GESTION, weights=PESO_RESULTADOS)[0],
            random.randint(1, 10), random.choice(NOMBRES_GESTOR), ahora, ahora)

    conn.commit()
    cur.close()
    conn.close()
    return len(a_pagar), len(a_vencer)


# ─────────────────────────────────────────────────────────────────────────────
# ESCRITURA
# ─────────────────────────────────────────────────────────────────────────────

def escribir_csv(datos, salida):
    os.makedirs(salida, exist_ok=True)
    for tabla in ORDEN_CARGA:
        filas = datos[tabla]
        if not filas:
            continue
        ruta = os.path.join(salida, f'{tabla}.csv')
        with open(ruta, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
            writer.writeheader()
            writer.writerows(filas)
        print(f'  ✅ {tabla:<20} {len(filas):>9,} filas  →  {ruta}')


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
    ap.add_argument('--dsn', default=os.getenv('COBRANZAS_DSN'))
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
    ap.add_argument('--truncar', action='store_true',
                     help='Solo --modo full: vaciar las tablas antes de insertar')
    ap.add_argument('--seed', type=int, default=SEED)
    args = ap.parse_args()

    esquema = esquema_de(args.destino) if args.destino != 'csv' else None

    print('═' * 68)
    print('  WIZARD BANK · Cobranzas (Sesión 10 — Azure Data Factory)')
    print('═' * 68)
    print(f'  Modo: {args.modo}   ·   Destino: {args.destino}'
          + (f' (schema {esquema})' if esquema else ''))

    if args.modo == 'full':
        print(f'  Créditos: {args.n_creditos:,}   ·   Semilla: {args.seed}\n')
        datos = generar_full(args.n_creditos, args.seed)
    else:
        if args.destino == 'csv':
            sys.exit('ERROR: --modo incremental requiere --destino azuresql o sqlserver-legacy '
                      '(necesita leer el máximo numero_credito ya cargado)')
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

    print('── Generando ─────────────────────────────────────────────────')
    for tabla in ORDEN_CARGA:
        print(f'  {tabla:<20} {len(datos[tabla]):>9,}')

    print('\n── Escribiendo ───────────────────────────────────────────────')
    if args.destino == 'csv':
        escribir_csv(datos, args.salida)
    else:
        if not args.dsn:
            sys.exit('ERROR: falta --dsn (o la variable de entorno COBRANZAS_DSN)')
        escribir_sql(datos, args.dsn, esquema, args.truncar and args.modo == 'full')

    if args.modo == 'incremental' and (args.cuotas_pagar > 0 or args.cuotas_vencer > 0):
        print(f'\n── Tocando cuotas existentes (pagos y gestiones) ──────────────')
        n_pagadas, n_vencidas = actualizar_cuotas_existentes(
            args.dsn, esquema, args.cuotas_pagar, args.cuotas_vencer)
        print(f'  ✅ cuotas → Pagada       {n_pagadas:>9,}  (+ su pago en cobranzas.pagos)')
        print(f'  ✅ cuotas → Vencida      {n_vencidas:>9,}  (+ su gestión en cobranzas.gestiones_cobranza)')

    print()


if __name__ == '__main__':
    main()
