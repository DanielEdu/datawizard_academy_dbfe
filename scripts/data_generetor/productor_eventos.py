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

    # Sin Kafka: escribir un .parquet con la MISMA estructura que entrega la fuente
    # Kafka de Spark (key, value binario con el JSON, topic, partition, offset,
    # timestamp, timestampType) y subirlo a landing. Clientes/ofertas desde CSV.
    python productor_eventos.py --destino parquet --base-csv ./csv_wizard_bank \
        --eventos 20000 --salida ./parquet_eventos_app \
        --s3 s3://lakehouse-datawizard/landing/wizard_bank_kafka --aws-profile datawizard

Destino parquet:
  · Un archivo por corrida: <salida>/eventos_app_<AAAAMMDD_HHMMSS>.parquet, subido a
    <s3>/eventos_app/. Los offsets continúan desde los .parquet previos de --salida.
  · timestamp_evento se reparte en las últimas --horas; timestamp (de Kafka) es el
    momento de llegada al broker, unos segundos después.
  · Casos para Silver (S17), --casos-silver por caso: duplicado (at-least-once),
    reenvío de una corrida anterior, llegada tardía dentro del watermark, evento
    fuera del watermark, payload corrupto y tipo de evento desconocido. Quedan en
    el manifiesto local casos_silver_<sufijo>.csv.

Requisitos:
    pip install confluent-kafka pyodbc       (destino kafka)
    pip install pyarrow "boto3[crt]"         (destino parquet; boto3 solo con --s3)
    (+ ODBC Driver 18 for SQL Server instalado en el SO — igual que para
    generar_datos_wizard_bank.py)

Nota: confluent-kafka trae ruedas (wheels) precompiladas para macOS/Linux/
Windows con librdkafka incluido — normalmente `pip install confluent-kafka`
basta. Si el build falla, es casi siempre por un pip/wheel desactualizado
(`pip install --upgrade pip` antes de reintentar).
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import csv
import glob
import json
import os
import random
import sys
import time
import uuid
import zlib
from datetime import datetime, timedelta, timezone

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


def cargar_clientes_y_ofertas_csv(carpeta):
    """Igual que cargar_clientes_y_ofertas, pero desde los CSV de generar_datos_wizard_bank.py
    (clientes*.csv y ofertas_preaprobadas*.csv: carga inicial y deltas)."""
    def leer(patron):
        filas = []
        for ruta in sorted(glob.glob(os.path.join(carpeta, patron))):
            with open(ruta, newline='', encoding='utf-8') as f:
                filas.extend(csv.DictReader(f))
        return filas

    clientes = {int(r['id_cliente']): {'id_cliente': int(r['id_cliente']), 'id_pais': int(r['id_pais'])}
                for r in leer('clientes*.csv') if r.get('id_cliente') and r.get('id_pais')}
    ofertas = [{'id_oferta': int(r['id_oferta']), 'id_cliente': int(r['id_cliente'])}
               for r in leer('ofertas_preaprobadas*.csv')
               if r.get('id_oferta') and r.get('id_cliente') and int(r['id_cliente']) in clientes]
    if not clientes or not ofertas:
        sys.exit(f'No encontré clientes*.csv / ofertas_preaprobadas*.csv en {carpeta}. Genera antes:\n'
                 f'  python generar_datos_wizard_bank.py --destino csv --salida {carpeta}')
    return clientes, ofertas


# ─────────────────────────────────────────────────────────────────────────────
# Construcción del evento — mismo contrato que el esquema explícito de la Sesión 11
# ─────────────────────────────────────────────────────────────────────────────

def construir_evento(tipo, cliente, oferta, ts_evento=None):
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
        'timestamp_evento': (ts_evento or datetime.now(timezone.utc)).isoformat(),
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
# Destino parquet: los registros tal como los entrega la fuente Kafka de Spark
# ─────────────────────────────────────────────────────────────────────────────

ESPERADO_EVENTOS = {
    'duplicado':          'una sola fila en Silver (dropDuplicatesWithinWatermark por id_evento)',
    'reenvio_anterior':   'se cuela en Silver si su lote anterior ya salió del watermark de 2 h (dedup con estado acotado)',
    'llegada_tardia':     'entra a Silver (retraso < watermark de 2 h); retraso_seg alto',
    'fuera_watermark':    'Silver lo descarta si ya procesó un lote más nuevo (más viejo que el watermark)',
    'payload_corrupto':   'Bronze guarda _payload_crudo con id_evento NULL; Silver lo filtra (cuarentena)',
    'tipo_desconocido':   'entra a Silver: un tipo nuevo es una señal, no un error',
}


def _particion(clave, n):
    # Kafka usa murmur2(key) % particiones; crc32 da el mismo efecto: misma key → misma partición
    return zlib.crc32(clave) % n


def _offsets_previos(salida):
    """Último offset por partición en los .parquet ya generados (para continuar la secuencia)."""
    import pyarrow.parquet as pq
    maximos, valores = {}, []
    for ruta in sorted(glob.glob(os.path.join(salida, 'eventos_app_*.parquet'))):
        t = pq.read_table(ruta, columns=['partition', 'offset', 'value']).to_pylist()
        for r in t:
            maximos[r['partition']] = max(maximos.get(r['partition'], -1), r['offset'])
        valores.extend(r['value'] for r in t)
    return maximos, valores


def generar_parquet(clientes, ofertas, n_eventos, horas, topic, particiones, salida, n_casos):
    """Escribe un .parquet con el schema de la fuente Kafka de Spark:
    key BINARY, value BINARY (JSON utf-8), topic STRING, partition INT, offset BIGINT,
    timestamp TIMESTAMP, timestampType INT (0 = CreateTime)."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        sys.exit('ERROR: falta pyarrow. Instalar con: pip install pyarrow')

    ahora = datetime.now(timezone.utc).replace(microsecond=0)
    desde = ahora - timedelta(hours=horas)
    manifiesto = []

    def nuevo(ts_evento=None, tipo=None):
        oferta = random.choice(ofertas)
        ts_evento = ts_evento or desde + timedelta(seconds=random.uniform(0, horas * 3600))
        return construir_evento(tipo or _elegir_tipo(), clientes[oferta['id_cliente']], oferta, ts_evento)

    def registro(ev, ts_kafka, value=None):
        return {'key': str(ev['id_cliente']).encode(),
                'value': value if value is not None else json.dumps(ev).encode(),
                'ts': ts_kafka}

    # Tráfico normal: llega al broker 0,2–5 s después del evento
    registros = []
    for _ in range(n_eventos):
        ev = nuevo()
        ts = datetime.fromisoformat(ev['timestamp_evento'])
        registros.append(registro(ev, min(ts + timedelta(seconds=random.uniform(0.2, 5)), ahora)))

    def anotar(caso, clave):
        manifiesto.append({'tabla': 'eventos_app', 'caso': caso, 'clave': clave,
                           'esperado_en_silver': ESPERADO_EVENTOS[caso]})

    if n_casos > 0:
        # 1 · Duplicado: el productor reintenta y el mismo mensaje entra con otro offset
        for r in random.sample(registros, min(n_casos, len(registros))):
            registros.append({**r, 'ts': min(r['ts'] + timedelta(seconds=random.uniform(1, 30)), ahora)})
            anotar('duplicado', json.loads(r['value'])['id_evento'])
        # 2 · Reenvío de un evento de una corrida anterior (horas después)
        maximos, previos = _offsets_previos(salida)
        for v in random.sample(previos, min(n_casos, len(previos))):
            ev = json.loads(v) if v and v[:1] == b'{' and v.endswith(b'}') else None
            if ev:
                registros.append({'key': str(ev['id_cliente']).encode(), 'value': v, 'ts': ahora})
                anotar('reenvio_anterior', ev['id_evento'])
        # 3 · Llegada tardía: el teléfono estuvo sin red 30–90 min (dentro del watermark de 2 h)
        for _ in range(n_casos):
            ev = nuevo(ahora - timedelta(minutes=random.uniform(30, 90)))
            registros.append(registro(ev, ahora))
            anotar('llegada_tardia', ev['id_evento'])
        # 4 · Fuera del watermark: el evento es de hace 3–6 h
        for _ in range(n_casos):
            ev = nuevo(ahora - timedelta(hours=random.uniform(3, 6)))
            registros.append(registro(ev, ahora))
            anotar('fuera_watermark', ev['id_evento'])
        # 5 · Payload corrupto: JSON cortado a la mitad
        for _ in range(n_casos):
            ev = nuevo()
            crudo = json.dumps(ev).encode()
            registros.append(registro(ev, ahora, value=crudo[:len(crudo) // 2]))
            anotar('payload_corrupto', ev['id_evento'])
        # 6 · Tipo de evento que la app empezó a mandar y nadie avisó
        for _ in range(n_casos):
            ev = nuevo(tipo='banner_cerrado')
            registros.append(registro(ev, ahora))
            anotar('tipo_desconocido', ev['id_evento'])
    else:
        maximos, _ = _offsets_previos(salida)

    # Offsets: secuencia por partición en orden de llegada al broker
    registros.sort(key=lambda r: r['ts'])
    siguiente = {p: maximos.get(p, -1) + 1 for p in range(particiones)}
    filas = {'key': [], 'value': [], 'topic': [], 'partition': [], 'offset': [],
             'timestamp': [], 'timestampType': []}
    for r in registros:
        p = _particion(r['key'], particiones)
        for col, val in (('key', r['key']), ('value', r['value']), ('topic', topic), ('partition', p),
                         ('offset', siguiente[p]), ('timestamp', r['ts']), ('timestampType', 0)):
            filas[col].append(val)
        siguiente[p] += 1

    schema = pa.schema([
        ('key', pa.binary()), ('value', pa.binary()), ('topic', pa.string()),
        ('partition', pa.int32()), ('offset', pa.int64()),
        ('timestamp', pa.timestamp('us', tz='UTC')), ('timestampType', pa.int32()),
    ])
    os.makedirs(salida, exist_ok=True)
    sufijo = ahora.strftime('%Y%m%d_%H%M%S')
    ruta = os.path.join(salida, f'eventos_app_{sufijo}.parquet')
    pq.write_table(pa.table(filas, schema=schema), ruta)
    print(f'  ✅ eventos_app {len(registros):>9,} registros ({n_eventos:,} normales)  →  {ruta}')

    if manifiesto:
        from casos_silver import escribir_manifiesto
        escribir_manifiesto(manifiesto, salida, sufijo)
    return [('eventos_app', ruta)]


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Publica telemetría de Wizard Bank a Kafka / Event Hubs, o la escribe como .parquet '
                    'con la estructura de los registros de Kafka.')
    ap.add_argument('--destino', choices=['kafka', 'parquet'], default='kafka',
                    help='kafka = publica en el topic (default). parquet = escribe un archivo con el '
                         'schema de la fuente Kafka de Spark, sin necesitar broker.')
    ap.add_argument('--modo', choices=['historico', 'realtime'], default='historico',
                    help='historico = carga N eventos y termina. '
                         'realtime = emite continuo hasta Ctrl+C (solo destino kafka).')
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
    ap.add_argument('--base-csv', default=None,
                    help='En lugar de --dsn: carpeta con clientes*.csv y ofertas_preaprobadas*.csv '
                         '(salida de generar_datos_wizard_bank.py --destino csv).')
    ap.add_argument('--bootstrap-servers', default=os.getenv('EVENTHUB_BOOTSTRAP_SERVERS'),
                    help='<namespace>.servicebus.windows.net:9093 (o variable EVENTHUB_BOOTSTRAP_SERVERS).')
    ap.add_argument('--connection-string', default=os.getenv('EVENTHUB_CONNECTION_STRING'),
                    help='Connection string del namespace, con SharedAccessKey '
                         '(o variable EVENTHUB_CONNECTION_STRING).')
    ap.add_argument('--salida', default='./parquet_eventos_app',
                    help='Destino parquet: carpeta local de los archivos (y de los offsets previos).')
    ap.add_argument('--horas', type=float, default=6,
                    help='Destino parquet: ventana de tiempo hacia atrás en la que caen los eventos. Default 6.')
    ap.add_argument('--particiones', type=int, default=3,
                    help='Destino parquet: particiones simuladas del topic. Default 3.')
    ap.add_argument('--casos-silver', type=int, default=20,
                    help='Destino parquet: registros por caso de Silver (duplicados, tardíos, corruptos…). '
                         '0 = sin casos. Default 20.')
    ap.add_argument('--s3', default=os.getenv('EVENTOS_S3'),
                    help='Destino parquet: además sube el archivo a <s3>/eventos_app/ '
                         '(p. ej. s3://lakehouse-datawizard/landing/wizard_bank_kafka). Variable EVENTOS_S3.')
    ap.add_argument('--aws-profile', default=os.getenv('AWS_PROFILE'),
                    help='Perfil de la AWS CLI para --s3 (default: AWS_PROFILE o credenciales por defecto)')
    ap.add_argument('--seed', type=int, default=None,
                    help='Fija la semilla aleatoria (por defecto no se fija: cada '
                         'corrida genera tráfico distinto, como una app real).')
    args = ap.parse_args()

    if not args.dsn and not args.base_csv:
        sys.exit('Falta --dsn (o WIZARD_BANK_DSN) o --base-csv para leer clientes y ofertas')
    if args.destino == 'kafka':
        if not args.bootstrap_servers:
            sys.exit('Falta --bootstrap-servers (o la variable EVENTHUB_BOOTSTRAP_SERVERS)')
        if not args.connection_string:
            sys.exit('Falta --connection-string (o la variable EVENTHUB_CONNECTION_STRING)')
    elif args.modo != 'historico':
        sys.exit('--destino parquet solo admite --modo historico (un archivo por corrida)')
    if args.s3 and args.destino != 'parquet':
        sys.exit('--s3 solo aplica con --destino parquet')

    if args.seed is not None:
        random.seed(args.seed)

    if args.base_csv:
        print(f'Leyendo clientes y ofertas desde {args.base_csv}...')
        clientes, ofertas = cargar_clientes_y_ofertas_csv(args.base_csv)
    else:
        print(f'Leyendo clientes y ofertas desde Azure SQL...')
        clientes, ofertas = cargar_clientes_y_ofertas(args.dsn)
    print(f'  {len(clientes):,} clientes · {len(ofertas):,} ofertas disponibles')

    if args.destino == 'parquet':
        escritos = generar_parquet(clientes, ofertas, args.eventos, args.horas, args.topic,
                                   args.particiones, args.salida, args.casos_silver)
        if args.s3:
            from casos_silver import subir_a_s3
            subir_a_s3(escritos, args.s3, args.aws_profile)
        return

    producer = hacer_producer(args.bootstrap_servers, args.connection_string)

    if args.modo == 'historico':
        generar_historico(producer, args.topic, clientes, ofertas, args.eventos)
    else:
        generar_realtime(producer, args.topic, clientes, ofertas, args.eps)


if __name__ == '__main__':
    main()
