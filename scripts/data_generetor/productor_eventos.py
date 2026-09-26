#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
WIZARD BANK · Productor de eventos de telemetría (Kafka / Event Hubs)
Data Wizard Academy — Sesión 11 (Kafka y Structured Streaming)

Publica directo al topic `wizard.lending.eventos-app` los cuatro tipos de
evento de la telemetría de la app: oferta_vista, oferta_click,
simulacion_realizada, solicitud_iniciada — el payload y el esquema son los
que se leen y parsean en la Sesión 11.

Lee los clientes y ofertas YA CARGADOS en Azure SQL (por generar_datos_wizard_
bank.py) para que cada evento referencie un id_cliente/id_oferta real — no
inventa clientes nuevos.

Requiere que el namespace de Event Hubs y el event hub (topic) ya existan:
ver "Provisionar Event Hubs" en la Sesión 11 antes de correr este script.

Uso:
    # Carga histórica — 20.000 eventos por defecto (tamaño de lab; el volumen
    # documentado en la Sesión 8.2, ~2.027.000, es la referencia de producción)
    python productor_eventos.py --modo historico \
        --dsn "Driver={ODBC Driver 18 for SQL Server};\
Server=tcp:<tu-servidor>.database.windows.net,1433;Database=wizardbank;\
Uid=wizadmin;Pwd=<tu-password>;Encrypt=yes;TrustServerCertificate=no;" \
        --bootstrap-servers <tu-namespace>.servicebus.windows.net:9093 \
        --connection-string "Endpoint=sb://<tu-namespace>.servicebus.windows.net/;\
SharedAccessKeyName=<...>;SharedAccessKey=<...>"

    # Volumen reducido, para una prueba rápida
    python productor_eventos.py --modo historico --eventos 2000 --dsn "..." \
        --bootstrap-servers ... --connection-string "..."

    # Emisión continua — para el lab de streaming en vivo (Ctrl+C para detener)
    python productor_eventos.py --modo realtime --eps 5 --dsn "..." \
        --bootstrap-servers ... --connection-string "..."

    # Variables de entorno equivalentes a --dsn / --bootstrap-servers / --connection-string:
    #   WIZARD_BANK_DSN, EVENTHUB_BOOTSTRAP_SERVERS, EVENTHUB_CONNECTION_STRING

Requisitos:
    pip install confluent-kafka pyodbc
    (+ ODBC Driver 18 for SQL Server instalado en el SO — igual que para
    generar_datos_wizard_bank.py)

Nota: confluent-kafka trae ruedas (wheels) precompiladas para macOS/Linux/
Windows con librdkafka incluido — normalmente `pip install confluent-kafka`
basta. Si el build falla, es casi siempre por un pip/wheel desactualizado
(`pip install --upgrade pip` antes de reintentar).
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────────────────────
# El topic y las proporciones del funnel documentado en la Sesión 8.2
# (2.027.000 eventos reales: 1.800.000 + 95.000 + 72.000 + 60.000)
# ─────────────────────────────────────────────────────────────────────────────

TOPIC_DEFAULT = "wizard.lending.eventos-app"

PESO_EVENTOS = {
    "oferta_vista":         1_800_000,
    "oferta_click":            95_000,
    "simulacion_realizada":    72_000,
    "solicitud_iniciada":      60_000,
}

N_EVENTOS_HISTORICO_DEFAULT = 20_000   # tamaño de lab — sube con --eventos
EPS_DEFAULT = 5

CANALES              = ["APP_IOS", "APP_ANDROID", "WEB"]
VERSIONES_APP         = ["4.11.0", "4.12.1", "4.13.0"]
SISTEMAS_OPERATIVOS   = ["Android 14", "iOS 17", "iOS 18"]
DISPOSITIVOS          = ["Samsung Galaxy S22", "Samsung Galaxy A54",
                          "iPhone 15", "iPhone 14", "Motorola Edge 40"]
UBICACIONES_PANTALLA  = ["home_banner", "home_card", "push", "detalle_oferta"]

PLAZOS_SIMULADOS = [6, 12, 18, 24, 36, 48]


# ─────────────────────────────────────────────────────────────────────────────
# Leer clientes y ofertas ya existentes en Azure SQL
# ─────────────────────────────────────────────────────────────────────────────

def cargar_clientes_y_ofertas(dsn):
    """No genera datos nuevos: lee lo que generar_datos_wizard_bank.py ya cargó."""
    import pyodbc
    conn = pyodbc.connect(dsn, autocommit=True)
    cur = conn.cursor()

    cur.execute('SELECT id_cliente, id_pais FROM lending.clientes')
    clientes = {r[0]: {'id_cliente': r[0], 'id_pais': r[1]} for r in cur.fetchall()}

    cur.execute('SELECT id_oferta, id_cliente FROM lending.ofertas_preaprobadas')
    ofertas = [{'id_oferta': r[0], 'id_cliente': r[1]} for r in cur.fetchall()]

    cur.close()
    conn.close()

    if not clientes or not ofertas:
        sys.exit(
            "No hay clientes/ofertas en Azure SQL todavía. Corre primero:\n"
            "  python generar_datos_wizard_bank.py --destino azuresql --dsn \"...\""
        )
    return clientes, ofertas


# ─────────────────────────────────────────────────────────────────────────────
# Construcción del evento — mismo contrato que el esquema explícito de la Sesión 11
# ─────────────────────────────────────────────────────────────────────────────

def construir_evento(tipo, cliente, oferta):
    """Arma el payload con la estructura anidada (sesion/contexto) del contrato."""
    contexto = {
        'ubicacion_pantalla': random.choice(UBICACIONES_PANTALLA),
        'posicion':           random.randint(1, 4),
        'tiempo_visible_seg': round(random.uniform(0.5, 12.0), 2),
        'monto_simulado':     None,
        'plazo_simulado':     None,
    }
    if tipo == 'simulacion_realizada':
        contexto['monto_simulado'] = round(random.uniform(1_000, 50_000), 2)
        contexto['plazo_simulado'] = random.choice(PLAZOS_SIMULADOS)

    return {
        'id_evento':        str(uuid.uuid4()),
        'tipo_evento':      tipo,
        'timestamp_evento': datetime.now(timezone.utc).isoformat(),
        'id_cliente':       cliente['id_cliente'],
        'id_oferta':        oferta['id_oferta'],
        'id_pais':          cliente['id_pais'],
        'canal':            random.choice(CANALES),
        'sesion': {
            'id_sesion':          str(uuid.uuid4()),
            'version_app':        random.choice(VERSIONES_APP),
            'sistema_operativo':  random.choice(SISTEMAS_OPERATIVOS),
            'modelo_dispositivo': random.choice(DISPOSITIVOS),
        },
        'contexto': contexto,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer (Event Hubs vía su endpoint compatible con Kafka)
# ─────────────────────────────────────────────────────────────────────────────

def hacer_producer(bootstrap_servers, connection_string):
    from confluent_kafka import Producer
    return Producer({
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism':    'PLAIN',
        'sasl.username':     '$ConnectionString',
        'sasl.password':     connection_string,
        'client.id':         'productor-eventos-wizardbank',
    })


def _reporte_entrega(err, msg):
    if err is not None:
        print(f'  ⚠ entrega fallida: {err}', file=sys.stderr)


def publicar(producer, topic, evento):
    producer.produce(
        topic,
        key=str(evento['id_cliente']).encode(),
        value=json.dumps(evento).encode(),
        callback=_reporte_entrega,
    )
    producer.poll(0)   # procesa callbacks pendientes sin bloquear el loop


def _elegir_tipo():
    tipos = list(PESO_EVENTOS.keys())
    pesos = list(PESO_EVENTOS.values())
    return random.choices(tipos, weights=pesos, k=1)[0]


def generar_historico(producer, topic, clientes, ofertas, n_eventos):
    """Publica n_eventos respetando las proporciones reales del funnel."""
    publicados = 0
    for _ in range(n_eventos):
        oferta = random.choice(ofertas)
        cliente = clientes[oferta['id_cliente']]

        publicar(producer, topic, construir_evento(_elegir_tipo(), cliente, oferta))
        publicados += 1

        if publicados % 2000 == 0:
            producer.flush(10)
            print(f'  {publicados:,} / {n_eventos:,} eventos publicados')

    producer.flush(30)
    print(f"✔ Histórico completo: {publicados:,} eventos publicados en '{topic}'")


def generar_realtime(producer, topic, clientes, ofertas, eps):
    """Emite eventos de forma continua hasta Ctrl+C — para el lab de streaming."""
    intervalo = 1.0 / eps
    publicados = 0

    print(f"Emitiendo ~{eps} eventos/seg a '{topic}' — Ctrl+C para detener")
    try:
        while True:
            oferta = random.choice(ofertas)
            cliente = clientes[oferta['id_cliente']]

            publicar(producer, topic, construir_evento(_elegir_tipo(), cliente, oferta))
            publicados += 1

            if publicados % 25 == 0:
                producer.flush(2)
                print(f'  {publicados:,} eventos emitidos...')
            time.sleep(intervalo)
    except KeyboardInterrupt:
        producer.flush(10)
        print(f'\n✔ Detenido. {publicados:,} eventos emitidos en total.')


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Publica telemetría de Wizard Bank a Kafka / Event Hubs.')
    ap.add_argument('--modo', choices=['historico', 'realtime'], default='historico',
                    help='historico = carga N eventos y termina. '
                         'realtime = emite continuo hasta Ctrl+C.')
    ap.add_argument('--eventos', type=int, default=N_EVENTOS_HISTORICO_DEFAULT,
                    help=f'Solo --modo historico. Default {N_EVENTOS_HISTORICO_DEFAULT:,} '
                         '(tamaño de lab; la Sesión 8.2 documenta ~2.027.000 a escala de producción).')
    ap.add_argument('--eps', type=int, default=EPS_DEFAULT,
                    help=f'Solo --modo realtime. Eventos por segundo. Default {EPS_DEFAULT}.')
    ap.add_argument('--topic', default=TOPIC_DEFAULT,
                    help=f'Default {TOPIC_DEFAULT}.')
    ap.add_argument('--dsn', default=os.getenv('WIZARD_BANK_DSN'),
                    help='Cadena de conexión ODBC a Azure SQL (o variable WIZARD_BANK_DSN). '
                         'Debe tener clientes/ofertas ya cargados.')
    ap.add_argument('--bootstrap-servers', default=os.getenv('EVENTHUB_BOOTSTRAP_SERVERS'),
                    help='<namespace>.servicebus.windows.net:9093 (o variable EVENTHUB_BOOTSTRAP_SERVERS).')
    ap.add_argument('--connection-string', default=os.getenv('EVENTHUB_CONNECTION_STRING'),
                    help='Connection string del namespace, con SharedAccessKey '
                         '(o variable EVENTHUB_CONNECTION_STRING).')
    ap.add_argument('--seed', type=int, default=None,
                    help='Fija la semilla aleatoria (por defecto no se fija: cada '
                         'corrida genera tráfico distinto, como una app real).')
    args = ap.parse_args()

    if not args.dsn:
        sys.exit('Falta --dsn (o la variable de entorno WIZARD_BANK_DSN)')
    if not args.bootstrap_servers:
        sys.exit('Falta --bootstrap-servers (o la variable EVENTHUB_BOOTSTRAP_SERVERS)')
    if not args.connection_string:
        sys.exit('Falta --connection-string (o la variable EVENTHUB_CONNECTION_STRING)')

    if args.seed is not None:
        random.seed(args.seed)

    print(f'Leyendo clientes y ofertas desde Azure SQL...')
    clientes, ofertas = cargar_clientes_y_ofertas(args.dsn)
    print(f'  {len(clientes):,} clientes · {len(ofertas):,} ofertas disponibles')

    producer = hacer_producer(args.bootstrap_servers, args.connection_string)

    if args.modo == 'historico':
        generar_historico(producer, args.topic, clientes, ofertas, args.eventos)
    else:
        generar_realtime(producer, args.topic, clientes, ofertas, args.eps)


if __name__ == '__main__':
    main()
