#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
WIZARD BANK · Generador de datos sintéticos del módulo de Lending
Data Wizard Academy — proyecto integrador

Genera las 8 tablas de la fuente transaccional respetando:
  · Integridad referencial completa (ninguna FK huérfana)
  · Coherencia de país: un cliente peruano solo recibe productos peruanos
  · Montos realistas por moneda (PEN ≈ 3,75 / COP ≈ 4.100 / BOB ≈ 6,91 por USD)
  · Orden cronológico: registro < oferta < solicitud < resolución < desembolso
  · Las tasas de conversión del funnel documentado

Uso:
    # Cargar a Azure SQL — FUENTE PRINCIPAL DEL CURSO
    python generar_datos_wizard_bank.py --destino azuresql \
        --dsn "Driver={ODBC Driver 18 for SQL Server};\
Server=tcp:srv.database.windows.net,1433;Database=wizardbank;\
Uid=wizadmin;Pwd=***;Encrypt=yes;TrustServerCertificate=no;"

    # Cargar a PostgreSQL — fallback sin Azure (Docker local)
    python generar_datos_wizard_bank.py --destino postgres \
        --dsn "host=localhost dbname=wizardbank user=wizadmin password=***"

    # Generar CSVs (no requiere base de datos)
    python generar_datos_wizard_bank.py --destino csv --salida ./datos

    # Volumen reducido para pruebas rápidas
    python generar_datos_wizard_bank.py --destino csv --clientes 1000

Requisitos:
    pip install pyodbc              (para --destino azuresql)
    pip install psycopg2-binary     (para --destino postgres)

Nota sobre el motor:
    Azure SQL es la fuente principal porque el conector de SQL Server en
    Lakeflow Connect está GA y soporta Change Tracking, lo que habilita el
    lab de la Sesión 9. PostgreSQL queda como fallback para quien no tenga
    presupuesto de Azure, pero con él no se puede hacer ese lab.
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import csv
import os
import random
import sys
import unicodedata
from datetime import date, datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# PARÁMETROS DEL FUNNEL
# Las proporciones vienen del caso de uso documentado en la Sesión 8.
# ─────────────────────────────────────────────────────────────────────────────

CLIENTES_DEFAULT = 50_000

RATIO_OFERTAS_POR_CLIENTE = 2.4    # 50.000 clientes  → 120.000 ofertas
RATIO_SOLICITUD_POR_OFERTA = 0.50  # 120.000 ofertas  →  60.000 solicitudes
RATIO_APROBADA = 0.50              #  60.000 solicit. →  30.000 aprobadas
RATIO_RECHAZADA = 0.25             #                     15.000 rechazadas
                                   #                     15.000 desistidas (resto)
RATIO_DESEMBOLSO = 0.90            #  30.000 aprobadas →  27.000 desembolsos

# Ventana temporal de los datos
FECHA_INICIO = date(2024, 8, 1)
FECHA_FIN    = date(2026, 7, 31)

SEED = 42

# ─────────────────────────────────────────────────────────────────────────────
# CATÁLOGOS · textos con sentido de negocio
# ─────────────────────────────────────────────────────────────────────────────

PAISES = [
    # id, iso, nombre, moneda, peso_poblacion, tipo_doc, largo_doc
    (1, 'PE', 'Perú',     'PEN', 0.40, 'DNI', 8),
    (2, 'CO', 'Colombia', 'COP', 0.45, 'CC',  10),
    (3, 'BO', 'Bolivia',  'BOB', 0.15, 'CI',  7),
]

CIUDADES = {
    1: ['Lima', 'Arequipa', 'Trujillo', 'Chiclayo', 'Piura', 'Cusco', 'Huancayo'],
    2: ['Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena', 'Bucaramanga', 'Pereira'],
    3: ['La Paz', 'Santa Cruz de la Sierra', 'Cochabamba', 'El Alto', 'Oruro', 'Sucre', 'Tarija'],
}

NOMBRES_M = [
    'Carlos', 'José', 'Luis', 'Juan', 'Miguel', 'Jorge', 'Pedro', 'Andrés',
    'Diego', 'Fernando', 'Ricardo', 'Javier', 'Alberto', 'Manuel', 'Sergio',
    'Raúl', 'Óscar', 'Mario', 'Eduardo', 'Rodrigo', 'Iván', 'Julio', 'Marco',
]
NOMBRES_F = [
    'María', 'Ana', 'Rosa', 'Carmen', 'Patricia', 'Laura', 'Claudia', 'Sandra',
    'Mónica', 'Adriana', 'Gabriela', 'Verónica', 'Paola', 'Silvia', 'Andrea',
    'Natalia', 'Lucía', 'Elena', 'Daniela', 'Valeria', 'Karina', 'Fiorella',
]
# Apellidos comunes en la región; los andinos (Quispe, Mamani, Condori, Huamán)
# aparecen sobre todo en Perú y Bolivia, como en la realidad.
APELLIDOS_GENERALES = [
    'García', 'Rodríguez', 'Martínez', 'López', 'González', 'Pérez', 'Sánchez',
    'Ramírez', 'Torres', 'Flores', 'Rivera', 'Gómez', 'Díaz', 'Vargas',
    'Castillo', 'Rojas', 'Mendoza', 'Guerrero', 'Medina', 'Herrera', 'Cárdenas',
    'Salazar', 'Núñez', 'Peña', 'Delgado', 'Ríos', 'Campos', 'Fuentes',
]
APELLIDOS_ANDINOS = ['Quispe', 'Mamani', 'Condori', 'Huamán', 'Chávez', 'Apaza', 'Ticona']

SITUACION_LABORAL = ['Dependiente', 'Independiente', 'Desempleado', 'Jubilado']
PESO_SITUACION    = [0.55, 0.32, 0.08, 0.05]

CANALES        = ['APP_ANDROID', 'APP_IOS', 'WEB']
PESO_CANALES   = [0.55, 0.30, 0.15]

MOTORES        = ['modelo_v1', 'modelo_v2']
PESO_MOTORES   = [0.45, 0.55]

BANCOS_DESTINO = {
    1: ['BCP', 'Interbank', 'BBVA Perú', 'Scotiabank Perú'],
    2: ['Bancolombia', 'Davivienda', 'Banco de Bogotá', 'BBVA Colombia'],
    3: ['Banco Nacional de Bolivia', 'Banco Mercantil Santa Cruz', 'BancoSol'],
}

MOTIVOS_RECHAZO = [
    'Score crediticio insuficiente',
    'Ratio deuda/ingreso excede el máximo permitido',
    'Ingreso declarado no verificable',
    'Cliente con mora vigente en el sistema financiero',
    'Antigüedad laboral insuficiente',
    'Situación laboral no elegible para el producto',
    'Documento de identidad no validado',
]
PESO_RECHAZO = [0.30, 0.22, 0.15, 0.14, 0.09, 0.06, 0.04]

# ── Productos: 3 por país, con montos coherentes con cada moneda ────────────
# (id, id_pais, nombre, tipo, monto_min, monto_max, plazo_min, plazo_max, TEA)
PRODUCTOS = [
    # Perú · PEN
    (1, 1, 'Préstamo Personal Wizard',   'Libre disponibilidad',  1_000,     30_000,   6, 36, 28.500),
    (2, 1, 'Crédito Efectivo Express',   'Consumo',                 500,     15_000,   3, 24, 42.000),
    (3, 1, 'Crédito Educativo Wizard',   'Educativo',             2_000,     50_000,  12, 60, 18.900),
    # Colombia · COP
    (4, 2, 'Préstamo Personal Wizard',   'Libre disponibilidad',  1_000_000, 30_000_000,  6, 36, 26.900),
    (5, 2, 'Crédito Efectivo Express',   'Consumo',                 500_000, 15_000_000,  3, 24, 39.500),
    (6, 2, 'Crédito Educativo Wizard',   'Educativo',             2_000_000, 50_000_000, 12, 60, 17.500),
    # Bolivia · BOB
    (7, 3, 'Préstamo Personal Wizard',   'Libre disponibilidad',  2_000,     55_000,   6, 36, 24.000),
    (8, 3, 'Crédito Efectivo Express',   'Consumo',               1_000,     28_000,   3, 24, 36.000),
    (9, 3, 'Crédito Educativo Wizard',   'Educativo',             4_000,     90_000,  12, 60, 16.500),
]

# Redondeo del monto según la moneda: nadie pide 4.837, pide 5.000
REDONDEO_MONEDA = {'PEN': 100, 'COP': 100_000, 'BOB': 100}

# Tipo de cambio base contra USD. El BOB está anclado al dólar, por eso
# su banda de variación es mínima — igual que en la realidad.
TIPO_CAMBIO_BASE = {
    'PEN': (3.75, 0.045),   # (tasa media, volatilidad)
    'COP': (4_100.0, 130.0),
    'BOB': (6.91, 0.015),
}

# (nombre, tipo, meses_validos)
# meses_validos = None → la campaña puede arrancar en cualquier mes.
# Las campañas estacionales SÍ están atadas a su temporada real: no tendría
# sentido una campaña de "Verano" lanzada en mayo (el verano austral va de
# diciembre a febrero) ni un "Aguinaldo" fuera de diciembre.
NOMBRES_CAMPANIA = [
    ('Verano Wizard',               'Captacion',    (12, 1, 2)),  # verano austral
    ('Vuelta al Cole',              'Captacion',    (1, 2, 3)),   # inicio del año escolar
    ('Aguinaldo Wizard',            'Captacion',    (12,)),       # pago de aguinaldo
    ('Black Friday Créditos',       'Captacion',    (11,)),
    ('Préstamo Express',            'Captacion',    None),
    ('Reactivación Inactivos',      'Reactivacion', None),
    ('Vuelve a Wizard',             'Reactivacion', None),
    ('Cross-sell Cuenta a Crédito', 'Cross-sell',   None),
    ('Upgrade de Línea',            'Cross-sell',   None),
    ('Referidos Wizard',            'Captacion',    None),
]


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

def sin_tildes(texto):
    """Normaliza para construir emails: 'Óscar Núñez' → 'oscar.nunez'."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    ).lower()


def fecha_aleatoria(desde, hasta):
    """Fecha uniforme entre dos fechas (inclusive)."""
    dias = (hasta - desde).days
    return desde + timedelta(days=random.randint(0, max(dias, 0)))


def timestamp_aleatorio(desde, hasta):
    """Timestamp uniforme entre dos fechas, con hora del día realista.

    Sesga las horas hacia la franja 08:00–23:00: nadie pide un préstamo
    por la app a las 4 de la mañana.
    """
    d = fecha_aleatoria(desde, hasta)
    hora = random.choices(
        population=list(range(24)),
        weights=[1, 1, 1, 1, 1, 2, 4, 8, 12, 14, 15, 14, 13, 14, 15, 16,
                 17, 18, 20, 22, 20, 15, 9, 4],
    )[0]
    return datetime(d.year, d.month, d.day, hora,
                    random.randint(0, 59), random.randint(0, 59))


def redondear_monto(monto, moneda):
    """Redondea al múltiplo típico de la moneda."""
    paso = REDONDEO_MONEDA[moneda]
    return float(max(paso, round(monto / paso) * paso))


def elegir(poblacion, pesos):
    return random.choices(poblacion, weights=pesos)[0]


def nivel_riesgo_desde_score(score):
    """Traduce el score interno (0–1000) a la escala A–E del banco."""
    if score >= 800: return 'A'
    if score >= 700: return 'B'
    if score >= 600: return 'C'
    if score >= 500: return 'D'
    return 'E'


# ─────────────────────────────────────────────────────────────────────────────
# GENERADORES POR TABLA
# ─────────────────────────────────────────────────────────────────────────────

def fecha_inicio_campania(meses):
    """Fecha de arranque dentro de la ventana, restringida a ciertos meses.

    Si `meses` es None la campaña puede empezar cualquier día; si no, solo
    en los meses indicados, para que el nombre de la campaña sea coherente
    con la temporada (ver NOMBRES_CAMPANIA).
    """
    limite = FECHA_FIN - timedelta(days=60)
    candidatas, d = [], FECHA_INICIO
    while d <= limite:
        if meses is None or d.month in meses:
            candidatas.append(d)
        d += timedelta(days=1)
    # Si la ventana no cubre esos meses, cae a cualquier fecha válida.
    return random.choice(candidatas) if candidatas else fecha_aleatoria(FECHA_INICIO, limite)


def generar_campanias():
    """~30 campañas: cada nombre base se instancia por país."""
    campanias = []
    id_campania = 1
    for nombre_base, tipo, meses in NOMBRES_CAMPANIA:
        for id_pais, _, _, moneda, _, _, _ in PAISES:
            inicio = fecha_inicio_campania(meses)
            fin = inicio + timedelta(days=random.choice([30, 45, 60, 90]))
            # El presupuesto va en moneda local: COP maneja órdenes de
            # magnitud distintos a PEN/BOB.
            base = 400_000 if moneda == 'COP' else 120
            campanias.append({
                'id_campania':     id_campania,
                'id_pais':         id_pais,
                'nombre_campania': f'{nombre_base} {inicio.year} - {PAISES[id_pais-1][2]}',
                'tipo_campania':   tipo,
                'fecha_inicio':    inicio,
                'fecha_fin':       min(fin, FECHA_FIN),
                'presupuesto':     round(base * random.uniform(80, 500), 2),
            })
            id_campania += 1
    return campanias


def generar_productos():
    productos = []
    for (pid, id_pais, nombre, tipo, mn, mx, pmin, pmax, tea) in PRODUCTOS:
        productos.append({
            'id_producto':        pid,
            'id_pais':            id_pais,
            'nombre_producto':    nombre,
            'tipo_producto':      tipo,
            'monto_minimo':       float(mn),
            'monto_maximo':       float(mx),
            'plazo_min_meses':    pmin,
            'plazo_max_meses':    pmax,
            'tasa_interes_anual': tea,
            'es_activo':          True,
        })
    return productos


def generar_clientes(n, campanias):
    """Clientes distribuidos por país según el peso de cada mercado."""
    pesos_pais = [p[4] for p in PAISES]
    ids_pais   = [p[0] for p in PAISES]

    # Índice de campañas por país para asignar captación coherente
    camp_por_pais = {}
    for c in campanias:
        camp_por_pais.setdefault(c['id_pais'], []).append(c['id_campania'])

    documentos_usados = set()
    clientes = []

    for i in range(1, n + 1):
        id_pais = elegir(ids_pais, pesos_pais)
        _, _, _, _, _, tipo_doc, largo_doc = PAISES[id_pais - 1]

        # Documento único dentro del país
        while True:
            numero_doc = ''.join(str(random.randint(0, 9)) for _ in range(largo_doc))
            if (id_pais, numero_doc) not in documentos_usados:
                documentos_usados.add((id_pais, numero_doc))
                break

        es_hombre = random.random() < 0.5
        nombre = random.choice(NOMBRES_M if es_hombre else NOMBRES_F)
        # Los apellidos andinos pesan más en Perú y Bolivia
        pool = APELLIDOS_GENERALES + (APELLIDOS_ANDINOS * (3 if id_pais in (1, 3) else 0))
        apellido = f'{random.choice(pool)} {random.choice(pool)}'

        edad = int(random.triangular(21, 68, 33))   # sesgado a población joven
        fecha_nac = date(FECHA_FIN.year - edad,
                         random.randint(1, 12), random.randint(1, 28))

        situacion = elegir(SITUACION_LABORAL, PESO_SITUACION)

        # El ingreso depende de la moneda y de la situación laboral.
        if id_pais == 2:      # COP
            ingreso_base = random.triangular(1_300_000, 12_000_000, 2_600_000)
        elif id_pais == 1:    # PEN
            ingreso_base = random.triangular(1_100, 12_000, 2_200)
        else:                 # BOB
            ingreso_base = random.triangular(2_500, 20_000, 4_500)

        if situacion == 'Desempleado':
            ingreso_base *= 0.25
        elif situacion == 'Jubilado':
            ingreso_base *= 0.60

        # El score correlaciona con la situación laboral: no es aleatorio puro.
        score_base = {'Dependiente': 690, 'Independiente': 640,
                      'Jubilado': 660, 'Desempleado': 520}[situacion]
        score = int(max(300, min(1000, random.gauss(score_base, 95))))

        fecha_registro = timestamp_aleatorio(
            FECHA_INICIO, FECHA_FIN - timedelta(days=15))

        clientes.append({
            'id_cliente':                i,
            'id_pais':                   id_pais,
            'tipo_documento':            tipo_doc,
            'numero_documento':          numero_doc,
            'nombres':                   nombre,
            'apellidos':                 apellido,
            'fecha_nacimiento':          fecha_nac,
            'email':                     f'{sin_tildes(nombre)}.{sin_tildes(apellido.split()[0])}{i}@correo.com',
            'ciudad':                    random.choice(CIUDADES[id_pais]),
            'situacion_laboral':         situacion,
            'ingreso_mensual_declarado': round(ingreso_base, 2),
            'score_interno':             score,
            'nivel_riesgo':              nivel_riesgo_desde_score(score),
            'id_campania_captacion':     random.choice(camp_por_pais[id_pais]),
            'fecha_registro':            fecha_registro,
        })
    return clientes


def generar_ofertas(clientes, productos, campanias, n_ofertas):
    """Ofertas preaprobadas.

    Reglas de integridad:
      · El producto debe pertenecer al MISMO país del cliente.
      · La oferta se genera DESPUÉS del registro del cliente.
      · El monto respeta el rango del producto y se ajusta al ingreso.
    """
    prod_por_pais = {}
    for p in productos:
        prod_por_pais.setdefault(p['id_pais'], []).append(p)

    camp_por_pais = {}
    for c in campanias:
        camp_por_pais.setdefault(c['id_pais'], []).append(c['id_campania'])

    moneda_por_pais = {p[0]: p[3] for p in PAISES}

    # Algunos clientes reciben más ofertas que otros (los de mejor score).
    # Se muestrea TODO de una sola vez: random.choices() recalcula la
    # distribución acumulada en cada llamada, así que invocarlo 120.000
    # veces por separado es O(n_clientes) cada vez y domina el runtime.
    pesos = [1 + (c['score_interno'] - 300) / 250 for c in clientes]
    muestra_clientes = random.choices(clientes, weights=pesos, k=n_ofertas)

    ofertas = []
    for i, cli in enumerate(muestra_clientes, start=1):
        prod = random.choice(prod_por_pais[cli['id_pais']])
        moneda = moneda_por_pais[cli['id_pais']]

        # El monto ofertado escala con el ingreso y el score, acotado
        # al rango del producto.
        factor_score = 0.4 + (cli['score_interno'] - 300) / 700 * 1.1
        monto = cli['ingreso_mensual_declarado'] * random.uniform(2.5, 9.0) * factor_score
        monto = min(max(monto, prod['monto_minimo']), prod['monto_maximo'])
        monto = redondear_monto(monto, moneda)

        fecha_gen = timestamp_aleatorio(
            max(cli['fecha_registro'].date() + timedelta(days=1), FECHA_INICIO),
            FECHA_FIN)
        vigencia = fecha_gen.date() + timedelta(days=random.choice([15, 30, 45, 60]))

        # La tasa ofertada parte de la del producto y mejora con el score.
        ajuste = (700 - cli['score_interno']) / 700 * 6.0
        tasa = round(max(8.0, prod['tasa_interes_anual'] + ajuste), 3)

        ofertas.append({
            'id_oferta':            i,
            'id_cliente':           cli['id_cliente'],
            'id_producto':          prod['id_producto'],
            'id_campania':          random.choice(camp_por_pais[cli['id_pais']])
                                    if random.random() < 0.75 else None,
            'monto_ofertado':       monto,
            'plazo_meses_ofertado': random.choice(
                [m for m in (6, 12, 18, 24, 36, 48, 60)
                 if prod['plazo_min_meses'] <= m <= prod['plazo_max_meses']]),
            'tasa_ofertada':        tasa,
            'motor_asignacion':     elegir(MOTORES, PESO_MOTORES),
            'fecha_generacion':     fecha_gen,
            'fecha_vigencia_fin':   vigencia,
            'estado_oferta':        'Vigente',   # se recalcula tras las solicitudes
        })
    return ofertas


def generar_solicitudes(ofertas, clientes, productos):
    """Solicitudes derivadas de un subconjunto de ofertas.

    Máquina de estados:
        Desistida  (25%) → el cliente abandona, nunca se evalúa
        Rechazada  (25%) → evaluada y rechazada
        Aprobada   (50%) → evaluada y aprobada
    """
    cli_por_id  = {c['id_cliente']: c for c in clientes}
    prod_por_id = {p['id_producto']: p for p in productos}
    moneda_por_pais = {p[0]: p[3] for p in PAISES}

    n_solicitudes = int(len(ofertas) * RATIO_SOLICITUD_POR_OFERTA)
    # Una oferta genera como máximo UNA solicitud → muestreo sin reemplazo.
    ofertas_elegidas = random.sample(ofertas, n_solicitudes)

    solicitudes = []
    for i, of in enumerate(ofertas_elegidas, start=1):
        cli    = cli_por_id[of['id_cliente']]
        prod   = prod_por_id[of['id_producto']]
        moneda = moneda_por_pais[cli['id_pais']]

        # La solicitud ocurre dentro de la vigencia de la oferta.
        fecha_sol = timestamp_aleatorio(
            of['fecha_generacion'].date(), of['fecha_vigencia_fin'])
        if fecha_sol < of['fecha_generacion']:
            fecha_sol = of['fecha_generacion'] + timedelta(minutes=random.randint(1, 600))

        # El cliente puede pedir menos de lo ofertado (rara vez más).
        monto_sol = redondear_monto(
            of['monto_ofertado'] * random.uniform(0.45, 1.0), moneda)
        monto_sol = min(max(monto_sol, prod['monto_minimo']), prod['monto_maximo'])

        r = random.random()
        if r < RATIO_APROBADA:
            estado = 'Aprobada'
        elif r < RATIO_APROBADA + RATIO_RECHAZADA:
            estado = 'Rechazada'
        else:
            estado = 'Desistida'

        fila = {
            'id_solicitud':           i,
            'id_oferta':              of['id_oferta'],
            'id_cliente':             cli['id_cliente'],
            'id_producto':            prod['id_producto'],
            'canal':                  elegir(CANALES, PESO_CANALES),
            'monto_solicitado':       monto_sol,
            'plazo_meses_solicitado': of['plazo_meses_ofertado'],
            'fecha_hora_solicitud':   fecha_sol,
            'estado_solicitud':       estado,
            'score_evaluacion':       None,
            'decision_motor':         None,
            'monto_aprobado':         None,
            'tasa_aprobada':          None,
            'motivo_rechazo':         None,
            'fecha_hora_resolucion':  None,
        }

        if estado == 'Desistida':
            # Abandonó el flujo: no hay evaluación de riesgo.
            solicitudes.append(fila)
            continue

        # Evaluada: la resolución llega entre minutos y 3 días después.
        fila['fecha_hora_resolucion'] = fecha_sol + timedelta(
            minutes=random.choices([3, 20, 120, 1440, 4320],
                                   weights=[35, 30, 20, 10, 5])[0])
        fila['score_evaluacion'] = cli['score_interno']

        if estado == 'Aprobada':
            fila['decision_motor'] = 'Aprobar'
            # Puede aprobarse por menos de lo solicitado.
            aprobado = monto_sol * random.choices(
                [1.0, 0.85, 0.70, 0.50], weights=[62, 20, 12, 6])[0]
            fila['monto_aprobado'] = redondear_monto(aprobado, moneda)
            fila['tasa_aprobada']  = of['tasa_ofertada']
        else:
            fila['decision_motor']  = 'Rechazar'
            fila['motivo_rechazo']  = elegir(MOTIVOS_RECHAZO, PESO_RECHAZO)

        solicitudes.append(fila)

    # Las ofertas que derivaron en solicitud quedan en estado 'Aceptada';
    # el resto expira o sigue vigente según su fecha de vigencia.
    aceptadas = {s['id_oferta'] for s in solicitudes}
    hoy = FECHA_FIN
    for of in ofertas:
        if of['id_oferta'] in aceptadas:
            of['estado_oferta'] = 'Aceptada'
        elif of['fecha_vigencia_fin'] < hoy:
            of['estado_oferta'] = 'Expirada'
        else:
            of['estado_oferta'] = 'Vigente'

    return solicitudes


def generar_desembolsos(solicitudes, clientes):
    """Desembolsos: solo para solicitudes aprobadas (90% de ellas)."""
    cli_por_id = {c['id_cliente']: c for c in clientes}
    moneda_por_pais = {p[0]: p[3] for p in PAISES}

    aprobadas = [s for s in solicitudes if s['estado_solicitud'] == 'Aprobada']
    elegidas  = random.sample(aprobadas, int(len(aprobadas) * RATIO_DESEMBOLSO))

    desembolsos = []
    for i, sol in enumerate(elegidas, start=1):
        cli    = cli_por_id[sol['id_cliente']]
        moneda = moneda_por_pais[cli['id_pais']]

        # El desembolso ocurre entre minutos y 2 días tras la aprobación.
        fecha_des = sol['fecha_hora_resolucion'] + timedelta(
            minutes=random.choices([5, 45, 300, 2880],
                                   weights=[45, 30, 18, 7])[0])

        estado = random.choices(['Completado', 'Fallido', 'Reversado'],
                                weights=[96, 2, 2])[0]

        desembolsos.append({
            'id_desembolso':         i,
            'id_solicitud':          sol['id_solicitud'],
            'id_cliente':            cli['id_cliente'],
            'monto_desembolsado':    sol['monto_aprobado'],
            'moneda':                moneda,
            'tasa_aplicada':         sol['tasa_aprobada'],
            'plazo_meses':           sol['plazo_meses_solicitado'],
            'comision_cobrada':      round(sol['monto_aprobado'] * random.uniform(0.005, 0.03), 2),
            'cuenta_destino_masked': f'****{random.randint(1000, 9999)}',
            'fecha_hora_desembolso': fecha_des,
            'estado_desembolso':     estado,
            'referencia_bancaria':   f"{random.choice(BANCOS_DESTINO[cli['id_pais']])[:3].upper()}"
                                     f"-{random.randint(10**9, 10**10 - 1)}",
        })
    return desembolsos


def generar_tipos_cambio():
    """Serie diaria por moneda con caminata aleatoria suave.

    El BOB está anclado al dólar, por eso su volatilidad es casi nula.
    """
    filas = []
    idx = 1
    for moneda, (base, vol) in TIPO_CAMBIO_BASE.items():
        tasa = base
        dia = FECHA_INICIO
        while dia <= FECHA_FIN:
            # Caminata aleatoria con reversión a la media
            tasa += random.gauss(0, vol) - (tasa - base) * 0.05
            spread = tasa * random.uniform(0.004, 0.012)
            filas.append({
                'id_tipo_cambio': idx,
                'fecha':          dia,
                'moneda_origen':  moneda,
                'moneda_destino': 'USD',
                'tasa_compra':    round(tasa - spread / 2, 6),
                'tasa_venta':     round(tasa + spread / 2, 6),
            })
            idx += 1
            dia += timedelta(days=1)
    return filas


# ─────────────────────────────────────────────────────────────────────────────
# DELTA · agregar actividad nueva sin recargar todo desde cero
#
# Pensado para volver a correr el generador entre corridas del pipeline
# (Lakeflow Connect, Sesión 9) y ver que solo se ingesta lo nuevo/cambiado,
# no la tabla completa. Reutiliza EXACTAMENTE las mismas funciones de
# generación que --modo full — la única diferencia es que los IDs de las
# filas nuevas continúan desde el máximo ya existente en la base (nunca
# desde 1), para no colisionar con la carga inicial.
# ─────────────────────────────────────────────────────────────────────────────

N_CLIENTES_DELTA_DEFAULT = 100
DIAS_TIPO_CAMBIO_DELTA_DEFAULT = 3
N_UPDATES_DELTA_DEFAULT = 50

TABLAS_CON_ID = {
    'clientes': 'id_cliente',
    'ofertas_preaprobadas': 'id_oferta',
    'solicitudes_prestamo': 'id_solicitud',
    'desembolsos': 'id_desembolso',
    'tipos_cambio': 'id_tipo_cambio',
}


def _remapear_ids(filas, campo, offset):
    """Suma offset al campo id de cada fila, in-place. Las FK hacia estas
    filas (ej. id_cliente en ofertas) ya quedan correctas porque las
    funciones de generación las toman del objeto padre YA remapeado."""
    for f in filas:
        f[campo] += offset
    return filas


def obtener_max_ids(dsn):
    """Lee el máximo id actual de cada tabla — punto de partida del delta."""
    try:
        import pyodbc
    except ImportError:
        sys.exit('ERROR: falta pyodbc. Instalar con: pip install pyodbc')

    conn = pyodbc.connect(dsn, autocommit=True)
    cur = conn.cursor()
    maximos = {}
    for tabla, campo_id in TABLAS_CON_ID.items():
        cur.execute(f'SELECT ISNULL(MAX({campo_id}), 0) FROM lending.{tabla}')
        maximos[tabla] = cur.fetchone()[0]

    # Última tasa conocida por moneda, para continuar la caminata aleatoria
    # de tipos_cambio en vez de reiniciarla desde TIPO_CAMBIO_BASE.
    ultimas_tasas = {}
    ultima_fecha = None
    for moneda in TIPO_CAMBIO_BASE:
        cur.execute(
            "SELECT TOP 1 fecha, tasa_compra, tasa_venta FROM lending.tipos_cambio "
            "WHERE moneda_origen = ? ORDER BY fecha DESC", moneda)
        row = cur.fetchone()
        if row:
            ultimas_tasas[moneda] = (row[1] + row[2]) / 2   # punto medio compra/venta
            ultima_fecha = row[0] if ultima_fecha is None else max(ultima_fecha, row[0])
    cur.close()
    conn.close()
    return maximos, ultimas_tasas, ultima_fecha


def generar_tipos_cambio_delta(dias, ultimas_tasas, desde_fecha, offset_id):
    """Continúa la caminata aleatoria de tipos_cambio desde la última tasa
    conocida — no reinicia desde TIPO_CAMBIO_BASE como hace el modo full."""
    filas = []
    idx = offset_id + 1
    for moneda, (base, vol) in TIPO_CAMBIO_BASE.items():
        tasa = ultimas_tasas.get(moneda, base)
        dia = desde_fecha + timedelta(days=1) if desde_fecha else FECHA_FIN + timedelta(days=1)
        for _ in range(dias):
            tasa += random.gauss(0, vol) - (tasa - base) * 0.05
            spread = tasa * random.uniform(0.004, 0.012)
            filas.append({
                'id_tipo_cambio': idx,
                'fecha':          dia,
                'moneda_origen':  moneda,
                'moneda_destino': 'USD',
                'tasa_compra':    round(tasa - spread / 2, 6),
                'tasa_venta':     round(tasa + spread / 2, 6),
            })
            idx += 1
            dia += timedelta(days=1)
    return filas


def generar_delta(n_clientes_delta, dias_tc_delta, maximos, ultimas_tasas, ultima_fecha):
    """Arma el paquete de datos NUEVOS (clientes + su cascada de ofertas/
    solicitudes/desembolsos, más días nuevos de tipos_cambio), con IDs que
    continúan desde lo ya cargado. Las fechas de negocio de lo nuevo caen
    en los últimos 7 días de la ventana del curso, simulando "actividad
    de ayer/hoy" en vez de mezclarse con el histórico."""
    campanias = generar_campanias()
    productos = generar_productos()

    hoy_simulado = FECHA_FIN
    ventana_reciente = hoy_simulado - timedelta(days=7)

    clientes = generar_clientes(n_clientes_delta, campanias)
    for c in clientes:
        c['fecha_registro'] = timestamp_aleatorio(ventana_reciente, hoy_simulado)
    _remapear_ids(clientes, 'id_cliente', maximos['clientes'])

    n_ofertas = max(1, int(n_clientes_delta * RATIO_OFERTAS_POR_CLIENTE))
    ofertas = generar_ofertas(clientes, productos, campanias, n_ofertas)
    _remapear_ids(ofertas, 'id_oferta', maximos['ofertas_preaprobadas'])

    solicitudes = generar_solicitudes(ofertas, clientes, productos)
    _remapear_ids(solicitudes, 'id_solicitud', maximos['solicitudes_prestamo'])

    desembolsos = generar_desembolsos(solicitudes, clientes)
    _remapear_ids(desembolsos, 'id_desembolso', maximos['desembolsos'])

    tipos_cambio = generar_tipos_cambio_delta(
        dias_tc_delta, ultimas_tasas, ultima_fecha, maximos['tipos_cambio'])

    return {
        'clientes':             clientes,
        'ofertas_preaprobadas': ofertas,
        'solicitudes_prestamo': solicitudes,
        'desembolsos':          desembolsos,
        'tipos_cambio':         tipos_cambio,
    }


def actualizar_filas_existentes(dsn, n_updates):
    """Simula lo que en el lab de la Sesión 9 se hace a mano: cambiar el
    ingreso/score de clientes ya existentes y el estado de algunos
    desembolsos. El trigger de la base mantiene fecha_actualizacion sola —
    por eso alcanza con el UPDATE, sin tocar esa columna aquí.

    Esto es lo que prueba que el conector detecta MODIFICACIONES, no solo
    filas nuevas — justo la parte que --modo incremental por sí solo no cubre."""
    import pyodbc
    conn = pyodbc.connect(dsn, autocommit=False)
    cur = conn.cursor()

    cur.execute(f'SELECT TOP {n_updates} id_cliente FROM lending.clientes ORDER BY NEWID()')
    ids_cliente = [r[0] for r in cur.fetchall()]
    for id_cliente in ids_cliente:
        nuevo_ingreso = round(random.uniform(1_100, 12_000_000), 2)  # rango amplio, cruza monedas a propósito
        nuevo_score = int(max(300, min(1000, random.gauss(650, 100))))
        cur.execute(
            'UPDATE lending.clientes SET ingreso_mensual_declarado = ?, score_interno = ?, '
            'nivel_riesgo = ? WHERE id_cliente = ?',
            nuevo_ingreso, nuevo_score, nivel_riesgo_desde_score(nuevo_score), id_cliente)

    cur.execute(f"SELECT TOP {max(1, n_updates // 5)} id_desembolso FROM lending.desembolsos "
                "WHERE estado_desembolso = 'Completado' ORDER BY NEWID()")
    ids_desembolso = [r[0] for r in cur.fetchall()]
    for id_desembolso in ids_desembolso:
        cur.execute(
            "UPDATE lending.desembolsos SET estado_desembolso = 'Reversado' WHERE id_desembolso = ?",
            id_desembolso)

    conn.commit()
    cur.close()
    conn.close()
    return len(ids_cliente), len(ids_desembolso)


def leer_estado_csv(base):
    """Equivalente CSV de obtener_max_ids: lee los CSV ya generados en `base`
    (tabla*.csv, incluidos deltas previos) para continuar IDs y tasas."""
    import glob
    filas_por_tabla = {}
    for tabla in TABLAS_CON_ID:
        filas = []
        for ruta in sorted(glob.glob(os.path.join(base, f'{tabla}*.csv'))):
            with open(ruta, newline='', encoding='utf-8') as f:
                filas.extend(csv.DictReader(f))
        if not filas:
            sys.exit(f'ERROR: no encontré {tabla}*.csv en {base} — genera primero la carga '
                     f'inicial con --modo full --destino csv --salida {base}')
        filas_por_tabla[tabla] = filas

    maximos = {t: max(int(f[c]) for f in filas_por_tabla[t]) for t, c in TABLAS_CON_ID.items()}

    ultimas_tasas, ultima_fecha = {}, None
    ultima_por_moneda = {}
    for f in filas_por_tabla['tipos_cambio']:
        fecha = date.fromisoformat(f['fecha'])
        m = f['moneda_origen']
        if m not in ultima_por_moneda or fecha > ultima_por_moneda[m][0]:
            ultima_por_moneda[m] = (fecha, (float(f['tasa_compra']) + float(f['tasa_venta'])) / 2)
        ultima_fecha = fecha if ultima_fecha is None else max(ultima_fecha, fecha)
    ultimas_tasas = {m: t for m, (_f, t) in ultima_por_moneda.items()}
    return maximos, ultimas_tasas, ultima_fecha, filas_por_tabla


def actualizar_filas_csv(filas_por_tabla, n_updates, ahora):
    """Equivalente CSV de actualizar_filas_existentes: devuelve versiones NUEVAS
    de filas ya existentes (mismo id, atributos cambiados, fecha_actualizacion
    = ahora). Es lo que en Silver se resuelve con MERGE / SCD2."""
    clientes = random.sample(filas_por_tabla['clientes'], min(n_updates, len(filas_por_tabla['clientes'])))
    for c in clientes:
        c['ingreso_mensual_declarado'] = round(random.uniform(1_100, 12_000_000), 2)
        c['score_interno'] = int(max(300, min(1000, random.gauss(650, 100))))
        c['nivel_riesgo'] = nivel_riesgo_desde_score(c['score_interno'])
    completados = [d for d in filas_por_tabla['desembolsos'] if d['estado_desembolso'] == 'Completado']
    desembolsos = random.sample(completados, min(max(1, n_updates // 5), len(completados)))
    for d in desembolsos:
        d['estado_desembolso'] = 'Reversado'
    salida = {'clientes': clientes, 'desembolsos': desembolsos}
    for tabla, filas in salida.items():
        for f in filas:
            if 'fecha_creacion' not in f:
                f.update(agregar_auditoria(tabla, [f])[0])
            f['fecha_actualizacion'] = ahora.strftime('%Y-%m-%d %H:%M:%S')
    return salida


def escribir_csv_delta(datos_nuevos, actualizados, salida, ahora):
    """Un CSV por tabla con las filas nuevas + las versiones actualizadas.
    Todas llevan fecha_actualizacion = ahora (la cursor column del incremental)."""
    os.makedirs(salida, exist_ok=True)
    sufijo = ahora.strftime('%Y%m%d_%H%M%S')
    ts = ahora.strftime('%Y-%m-%d %H:%M:%S')
    for tabla in ORDEN_CARGA:
        nuevas = [{**f, 'fecha_actualizacion': ts}
                  for f in agregar_auditoria(tabla, datos_nuevos.get(tabla, []))]
        cambiadas = actualizados.get(tabla, [])
        filas = nuevas + cambiadas
        if not filas:
            continue
        ruta = os.path.join(salida, f'{tabla}_delta_{sufijo}.csv')
        with open(ruta, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
            writer.writeheader()
            writer.writerows(filas)
        print(f'  ✅ {tabla:<24} {len(nuevas):>7,} nuevas + {len(cambiadas):>4,} actualizadas  →  {ruta}')


# ─────────────────────────────────────────────────────────────────────────────
# ESCRITURA
# ─────────────────────────────────────────────────────────────────────────────

ORDEN_CARGA = [
    'paises', 'campanias', 'productos_prestamo', 'clientes',
    'ofertas_preaprobadas', 'solicitudes_prestamo', 'desembolsos', 'tipos_cambio',
]


# Columna de negocio de la que se deriva fecha_creacion en el CSV, y la
# columna (si existe) que marca el último cambio de estado de la fila.
FECHA_CREACION_DESDE = {
    'campanias':            'fecha_inicio',
    'clientes':             'fecha_registro',
    'ofertas_preaprobadas': 'fecha_generacion',
    'solicitudes_prestamo': 'fecha_hora_solicitud',
    'desembolsos':          'fecha_hora_desembolso',
    'tipos_cambio':         'fecha',
}
FECHA_ACTUALIZACION_DESDE = {
    'solicitudes_prestamo': 'fecha_hora_resolucion',   # si no hay resolución, = creación
}


def _a_datetime(v):
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    return datetime.fromisoformat(str(v))


def agregar_auditoria(tabla, filas):
    """Agrega fecha_creacion y fecha_actualizacion a las filas de un CSV.

    En Azure SQL esas columnas las llena el DEFAULT y el trigger de la base,
    por eso el generador no las produce. En el CSV hay que materializarlas:
    son la cursor column de la carga incremental y la base de las ventanas
    de Silver. Se derivan de la fecha de negocio para que sean coherentes
    (creación = cuándo ocurrió el hecho; actualización = último cambio de
    estado, o la misma creación si la fila nunca cambió)."""
    col_cre = FECHA_CREACION_DESDE.get(tabla)
    col_act = FECHA_ACTUALIZACION_DESDE.get(tabla)
    salida = []
    for f in filas:
        cre = _a_datetime(f[col_cre]) if col_cre else datetime.combine(FECHA_INICIO, datetime.min.time())
        act = _a_datetime(f[col_act]) if col_act and f.get(col_act) else cre
        salida.append({**f, 'fecha_creacion': cre, 'fecha_actualizacion': act})
    return salida


def escribir_csv(datos, salida, sufijo=''):
    os.makedirs(salida, exist_ok=True)
    for tabla in ORDEN_CARGA:
        filas = agregar_auditoria(tabla, datos[tabla])
        ruta = os.path.join(salida, f'{tabla}{sufijo}.csv')
        with open(ruta, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
            writer.writeheader()
            writer.writerows(filas)
        print(f'  ✅ {tabla:<24} {len(filas):>9,} filas  →  {ruta}')


def escribir_azuresql(datos, dsn, truncar):
    """Carga a Azure SQL con pyodbc.

    Detalles que importan:
      · fast_executemany acelera el insert por lotes en órdenes de magnitud.
      · Los booleanos de Python van a columnas BIT: pyodbc los convierte solo,
        pero se normalizan a 1/0 para evitar sorpresas entre drivers.
      · No se puede TRUNCATE una tabla referenciada por una FK, así que el
        vaciado se hace con DELETE en orden inverso al de carga.
    """
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
            cur.execute(f'DELETE FROM lending.{tabla}')
        conn.commit()
        print('  🧹 Tablas vaciadas')

    for tabla in ORDEN_CARGA:
        filas = datos[tabla]
        if not filas:
            continue
        columnas = list(filas[0].keys())
        marcadores = ', '.join('?' * len(columnas))
        sql = (f'INSERT INTO lending.{tabla} ({", ".join(columnas)}) '
               f'VALUES ({marcadores})')

        valores = [
            tuple(
                (1 if f[c] else 0) if isinstance(f[c], bool) else f[c]
                for c in columnas
            )
            for f in filas
        ]

        # Lotes de 5.000 para no agotar memoria del driver en tablas grandes
        for ini in range(0, len(valores), 5_000):
            cur.executemany(sql, valores[ini:ini + 5_000])
        conn.commit()
        print(f'  ✅ {tabla:<24} {len(filas):>9,} filas insertadas')

    cur.close()
    conn.close()


def escribir_postgres(datos, dsn, truncar):
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        sys.exit('ERROR: falta psycopg2. Instalar con: pip install psycopg2-binary')

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute('SET search_path TO lending')

    if truncar:
        # Orden inverso para respetar las FK
        for tabla in reversed(ORDEN_CARGA):
            cur.execute(f'TRUNCATE TABLE lending.{tabla} CASCADE')
        print('  🧹 Tablas truncadas')

    for tabla in ORDEN_CARGA:
        filas = datos[tabla]
        if not filas:
            continue
        columnas = list(filas[0].keys())
        valores = [tuple(f[c] for c in columnas) for f in filas]
        sql = (f'INSERT INTO lending.{tabla} ({", ".join(columnas)}) VALUES %s '
               f'ON CONFLICT DO NOTHING')
        execute_values(cur, sql, valores, page_size=5_000)
        conn.commit()
        print(f'  ✅ {tabla:<24} {len(filas):>9,} filas insertadas')

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# VALIDACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def validar(datos):
    """Comprueba integridad referencial y coherencia del funnel."""
    print('\n── Validación ────────────────────────────────────────────────')
    errores = []

    ids_pais    = {p['id_pais']    for p in datos['paises']}
    ids_camp    = {c['id_campania'] for c in datos['campanias']}
    ids_prod    = {p['id_producto'] for p in datos['productos_prestamo']}
    ids_cli     = {c['id_cliente']  for c in datos['clientes']}
    ids_oferta  = {o['id_oferta']   for o in datos['ofertas_preaprobadas']}
    ids_sol     = {s['id_solicitud'] for s in datos['solicitudes_prestamo']}

    pais_de_cliente  = {c['id_cliente']: c['id_pais'] for c in datos['clientes']}
    pais_de_producto = {p['id_producto']: p['id_pais'] for p in datos['productos_prestamo']}

    # ── FK huérfanas ────────────────────────────────────────────────────
    if any(c['id_pais'] not in ids_pais for c in datos['clientes']):
        errores.append('clientes.id_pais huérfano')
    if any(c['id_campania_captacion'] not in ids_camp for c in datos['clientes']):
        errores.append('clientes.id_campania_captacion huérfano')
    if any(o['id_cliente'] not in ids_cli for o in datos['ofertas_preaprobadas']):
        errores.append('ofertas.id_cliente huérfano')
    if any(o['id_producto'] not in ids_prod for o in datos['ofertas_preaprobadas']):
        errores.append('ofertas.id_producto huérfano')
    if any(s['id_oferta'] not in ids_oferta for s in datos['solicitudes_prestamo']):
        errores.append('solicitudes.id_oferta huérfano')
    if any(d['id_solicitud'] not in ids_sol for d in datos['desembolsos']):
        errores.append('desembolsos.id_solicitud huérfano')

    # ── Coherencia de país: producto y cliente del mismo mercado ────────
    inconsistentes = sum(
        1 for o in datos['ofertas_preaprobadas']
        if pais_de_producto[o['id_producto']] != pais_de_cliente[o['id_cliente']]
    )
    if inconsistentes:
        errores.append(f'{inconsistentes} ofertas con producto de otro país')

    # ── Cronología ──────────────────────────────────────────────────────
    sol_por_id = {s['id_solicitud']: s for s in datos['solicitudes_prestamo']}
    fuera_orden = sum(
        1 for d in datos['desembolsos']
        if d['fecha_hora_desembolso'] < sol_por_id[d['id_solicitud']]['fecha_hora_solicitud']
    )
    if fuera_orden:
        errores.append(f'{fuera_orden} desembolsos anteriores a su solicitud')

    # ── Solo se desembolsan solicitudes aprobadas ───────────────────────
    no_aprobadas = sum(
        1 for d in datos['desembolsos']
        if sol_por_id[d['id_solicitud']]['estado_solicitud'] != 'Aprobada'
    )
    if no_aprobadas:
        errores.append(f'{no_aprobadas} desembolsos sobre solicitudes no aprobadas')

    # ── Una oferta genera como máximo una solicitud ─────────────────────
    ofertas_usadas = [s['id_oferta'] for s in datos['solicitudes_prestamo']]
    if len(ofertas_usadas) != len(set(ofertas_usadas)):
        errores.append('hay ofertas con más de una solicitud')

    # ── Resumen del funnel ──────────────────────────────────────────────
    n_of  = len(datos['ofertas_preaprobadas'])
    n_sol = len(datos['solicitudes_prestamo'])
    n_apr = sum(1 for s in datos['solicitudes_prestamo'] if s['estado_solicitud'] == 'Aprobada')
    n_des = len(datos['desembolsos'])

    print(f'  Ofertas          {n_of:>9,}')
    print(f'  Solicitudes      {n_sol:>9,}   ({n_sol/n_of*100:5.1f}% de ofertas)')
    print(f'  Aprobadas        {n_apr:>9,}   ({n_apr/n_sol*100:5.1f}% de solicitudes)')
    print(f'  Desembolsos      {n_des:>9,}   ({n_des/n_of*100:5.1f}% end-to-end)')

    print('\n  Distribución por país:')
    for id_pais, iso, nombre, moneda, _, _, _ in PAISES:
        n = sum(1 for c in datos['clientes'] if c['id_pais'] == id_pais)
        print(f'    {iso} {nombre:<10} {n:>7,} clientes   moneda {moneda}')

    if errores:
        print('\n  ❌ ERRORES DE INTEGRIDAD:')
        for e in errores:
            print(f'     · {e}')
        return False

    print('\n  ✅ Integridad referencial y cronología correctas')
    return True


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Genera los datos sintéticos de Wizard Bank · módulo Lending')
    ap.add_argument('--modo', choices=['full', 'delta'], default='full',
                    help='full = carga inicial completa (default). '
                         'delta = agrega actividad nueva (y actualiza filas existentes) sobre lo ya generado '
                         '(--destino azuresql o csv) — para probar ingesta incremental.')
    ap.add_argument('--destino', choices=['csv', 'azuresql', 'postgres'], default='csv',
                    help='Dónde escribir los datos (default: csv). '
                         'azuresql es la fuente principal del curso.')
    ap.add_argument('--salida', default='./datos_wizard_bank',
                    help='Carpeta de salida para --destino csv')
    ap.add_argument('--sufijo', nargs='?', const='auto', default='',
                    help='Sufijo para el nombre de los CSV (tabla_<sufijo>.csv). Sin valor usa '
                         'la fecha-hora actual (AAAAMMDD_HHMMSS). Sirve para dejar en landing '
                         'archivos "nuevos": Auto Loader ignora nombres que ya leyó.')
    ap.add_argument('--base', default='./datos_wizard_bank',
                    help='--modo delta --destino csv: carpeta con los CSV de la carga inicial '
                         '(de ahí toma los IDs máximos y las filas a actualizar)')
    ap.add_argument('--dsn', default=os.getenv('WIZARD_BANK_DSN'),
                    help='Cadena de conexión (o variable WIZARD_BANK_DSN). '
                         'ODBC para azuresql, libpq para postgres.')
    ap.add_argument('--clientes', type=int, default=CLIENTES_DEFAULT,
                    help=f'--modo full: número de clientes a generar (default: {CLIENTES_DEFAULT:,})')
    ap.add_argument('--clientes-delta', type=int, default=N_CLIENTES_DELTA_DEFAULT,
                    help=f'--modo delta: clientes nuevos a agregar (default: {N_CLIENTES_DELTA_DEFAULT})')
    ap.add_argument('--dias-delta', type=int, default=DIAS_TIPO_CAMBIO_DELTA_DEFAULT,
                    help=f'--modo delta: días nuevos de tipos_cambio a agregar (default: {DIAS_TIPO_CAMBIO_DELTA_DEFAULT})')
    ap.add_argument('--updates-delta', type=int, default=N_UPDATES_DELTA_DEFAULT,
                    help=f'--modo delta: cuántos clientes/desembolsos existentes modificar '
                         f'para simular cambios reales (default: {N_UPDATES_DELTA_DEFAULT}, 0 para omitir)')
    ap.add_argument('--truncar', action='store_true',
                    help='--modo full: vaciar las tablas antes de insertar (azuresql | postgres)')
    ap.add_argument('--seed', type=int, default=SEED,
                    help=f'Semilla aleatoria para reproducibilidad (default: {SEED}). '
                         'En --modo delta, usa una semilla DISTINTA a la de la carga full '
                         'si no quieres que la "nueva" actividad salga idéntica a la ya cargada.')
    args = ap.parse_args()

    if args.destino in ('azuresql', 'postgres') and not args.dsn:
        sys.exit(f'ERROR: --destino {args.destino} requiere --dsn '
                 f'o la variable de entorno WIZARD_BANK_DSN')

    if args.modo == 'delta' and args.destino not in ('azuresql', 'csv'):
        sys.exit('ERROR: --modo delta solo soporta --destino azuresql o csv '
                  '(necesita leer el estado actual: la base, o los CSV de --base)')

    random.seed(args.seed)

    if args.modo == 'delta' and args.destino == 'csv':
        ahora = datetime.now()
        print('═' * 66)
        print('  WIZARD BANK · Delta CSV — filas nuevas + actualizadas')
        print('═' * 66)
        maximos, ultimas_tasas, ultima_fecha, base_filas = leer_estado_csv(args.base)
        for tabla, campo in TABLAS_CON_ID.items():
            print(f'  {tabla:<22} max {campo} = {maximos[tabla]:,}')
        print('\n── Generando delta ──────────────────────────────────────────')
        datos = generar_delta(args.clientes_delta, args.dias_delta,
                               maximos, ultimas_tasas, ultima_fecha)
        actualizados = actualizar_filas_csv(base_filas, args.updates_delta, ahora) \
            if args.updates_delta > 0 else {}
        print('\n── Escribiendo (csv) ────────────────────────────────────────')
        escribir_csv_delta(datos, actualizados, args.salida, ahora)
        print('\n✅ Delta listo — sube estos archivos a landing y vuelve a correr la ingesta.\n')
        return

    if args.modo == 'delta':
        print('═' * 66)
        print('  WIZARD BANK · Delta — agregar actividad nueva')
        print('═' * 66)
        print(f'  Clientes nuevos: {args.clientes_delta}   ·   Días de tipos_cambio: {args.dias_delta}')
        print(f'  Semilla: {args.seed}\n')

        print('── Leyendo estado actual de la base ────────────────────────────')
        maximos, ultimas_tasas, ultima_fecha = obtener_max_ids(args.dsn)
        for tabla, campo in TABLAS_CON_ID.items():
            print(f'  {tabla:<22} max {campo} = {maximos[tabla]:,}')

        print('\n── Generando delta ──────────────────────────────────────────')
        datos = generar_delta(args.clientes_delta, args.dias_delta,
                               maximos, ultimas_tasas, ultima_fecha)
        for tabla in ('clientes', 'ofertas_preaprobadas', 'solicitudes_prestamo',
                      'desembolsos', 'tipos_cambio'):
            print(f'  {tabla:<22} {len(datos[tabla]):>9,} filas nuevas')

        print('\n── Escribiendo (azuresql) ───────────────────────────────────')
        escribir_azuresql(datos, args.dsn, truncar=False)

        if args.updates_delta > 0:
            print(f'\n── Modificando {args.updates_delta} filas existentes ─────────────────')
            n_cli, n_des = actualizar_filas_existentes(args.dsn, args.updates_delta)
            print(f'  ✅ clientes             {n_cli:>9,} filas actualizadas (ingreso/score/riesgo)')
            print(f'  ✅ desembolsos          {n_des:>9,} filas actualizadas (estado → Reversado)')

        print('\n✅ Delta listo — vuelve a correr el pipeline de ingesta para ver que solo trae esto.\n')
        return

    print('═' * 66)
    print('  WIZARD BANK · Generador de datos del módulo de Lending')
    print('═' * 66)
    print(f'  Clientes: {args.clientes:,}   ·   Ventana: {FECHA_INICIO} → {FECHA_FIN}')
    print(f'  Semilla:  {args.seed}   ·   Destino: {args.destino}\n')

    n_ofertas = int(args.clientes * RATIO_OFERTAS_POR_CLIENTE)

    print('── Generando ─────────────────────────────────────────────────')
    campanias   = generar_campanias();                  print(f'  campanias            {len(campanias):>9,}')
    productos   = generar_productos();                  print(f'  productos_prestamo   {len(productos):>9,}')
    clientes    = generar_clientes(args.clientes, campanias)
    print(f'  clientes             {len(clientes):>9,}')
    ofertas     = generar_ofertas(clientes, productos, campanias, n_ofertas)
    print(f'  ofertas_preaprobadas {len(ofertas):>9,}')
    solicitudes = generar_solicitudes(ofertas, clientes, productos)
    print(f'  solicitudes_prestamo {len(solicitudes):>9,}')
    desembolsos = generar_desembolsos(solicitudes, clientes)
    print(f'  desembolsos          {len(desembolsos):>9,}')
    tipos_cambio = generar_tipos_cambio()
    print(f'  tipos_cambio         {len(tipos_cambio):>9,}')

    datos = {
        'paises': [{'id_pais': p[0], 'codigo_iso': p[1], 'nombre_pais': p[2],
                    'moneda_codigo': p[3]} for p in PAISES],
        'campanias':            campanias,
        'productos_prestamo':   productos,
        'clientes':             clientes,
        'ofertas_preaprobadas': ofertas,
        'solicitudes_prestamo': solicitudes,
        'desembolsos':          desembolsos,
        'tipos_cambio':         tipos_cambio,
    }

    if not validar(datos):
        sys.exit('\nAbortado: los datos generados no pasaron la validación.')

    print(f'\n── Escribiendo ({args.destino}) ───────────────────────────────')
    if args.destino == 'csv':
        sufijo = args.sufijo
        if sufijo == 'auto':
            sufijo = datetime.now().strftime('%Y%m%d_%H%M%S')
        escribir_csv(datos, args.salida, f'_{sufijo}' if sufijo else '')
    elif args.destino == 'azuresql':
        escribir_azuresql(datos, args.dsn, args.truncar)
    else:
        escribir_postgres(datos, args.dsn, args.truncar)

    print('\n✅ Listo.\n')


if __name__ == '__main__':
    main()
