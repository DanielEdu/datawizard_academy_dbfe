"""Emisor continuo de la telemetría de la app (topic Kafka simulado) hacia landing.

Cada `--segundos-por-archivo` escribe un .parquet pequeño con el schema de la fuente Kafka de Spark
(key, value, topic, partition, offset, timestamp, timestampType) en la carpeta de landing que lee
job_bronze_app_ingest. Los eventos se generan "en vivo": su timestamp_evento cae en la ventana
transcurrida desde el archivo anterior. Mezcla, con baja probabilidad, los casos de prueba de Silver
(duplicados, reenvíos, llegadas tardías, fuera de watermark, payload corrupto, tipo desconocido).

Es un port de scripts/data_generetor/productor_eventos.py (destino parquet): el contrato del evento
y los casos son los mismos; cambia que aquí corre en Databricks, en continuo, y lee clientes y ofertas
de Bronze. Si cambia el contrato en uno, cambiarlo en el otro (ver specs/datagen_spec.md).

El run dura `--minutos` y termina; el job es `continuous` y lo relanza. Los offsets se retoman del
landing al arrancar cada run, así la secuencia por partición no se rompe entre runs.
"""

import argparse
import json
import logging
import random
import sys
import time
import uuid
import zlib
from collections import deque
from datetime import datetime, timedelta, timezone

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("emit_eventos_app")

TOPIC = "wizard.lending.eventos-app"

# Proporciones del funnel (Sesión 8.2)
PESO_EVENTOS = {
    "oferta_vista":         1_800_000,
    "oferta_click":            95_000,
    "simulacion_realizada":    72_000,
    "solicitud_iniciada":      60_000,
}

CANALES = ["APP_IOS", "APP_ANDROID", "WEB"]
VERSIONES_APP = ["4.11.0", "4.12.1", "4.13.0"]
SISTEMAS_OPERATIVOS = ["Android 14", "iOS 17", "iOS 18"]
DISPOSITIVOS = ["Samsung Galaxy S22", "Samsung Galaxy A54", "iPhone 15", "iPhone 14", "Motorola Edge 40"]
UBICACIONES_PANTALLA = ["home_banner", "home_card", "push", "detalle_oferta"]
PLAZOS_SIMULADOS = [6, 12, 18, 24, 36, 48]

# Probabilidad por evento normal de generar cada caso (se multiplica por --factor-casos)
PROB_CASOS = {
    "duplicado":        0.020,
    "reenvio_anterior": 0.005,
    "llegada_tardia":   0.010,
    "fuera_watermark":  0.005,
    "payload_corrupto": 0.005,
    "tipo_desconocido": 0.005,
}

ESPERADO_EVENTOS = {
    "duplicado":        "una sola fila en Silver (dropDuplicatesWithinWatermark por id_evento)",
    "reenvio_anterior": "se cuela en Silver si su lote anterior ya salió del watermark de 2 h (dedup con estado acotado)",
    "llegada_tardia":   "entra a Silver (retraso < watermark de 2 h); retraso_seg alto",
    "fuera_watermark":  "Silver lo descarta si ya procesó un lote más nuevo (más viejo que el watermark)",
    "payload_corrupto": "Bronze guarda _payload_crudo con id_evento NULL; Silver lo filtra (cuarentena)",
    "tipo_desconocido": "entra a Silver: un tipo nuevo es una señal, no un error",
}

PARTICIONES = 3

# Schema del registro de Kafka tal como lo entrega spark.readStream.format("kafka")
SCHEMA_KAFKA = ("key BINARY, value BINARY, topic STRING, partition INT, offset BIGINT, "
                "timestamp TIMESTAMP, timestampType INT")
MAX_RECIENTES = 5000   # payloads de archivos anteriores disponibles para 'reenvio_anterior'


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Emisor continuo de eventos de la app → landing (parquet estilo Kafka)")
    p.add_argument("--catalog", required=True, help="Catálogo Bronze de donde se leen clientes y ofertas")
    p.add_argument("--landing", required=True, help="Carpeta de landing destino (s3://.../eventos_app)")
    p.add_argument("--manifiestos", default="", help="Carpeta del manifiesto de casos; vacío = no escribirlo")
    p.add_argument("--staging", required=True, help="Carpeta temporal (fuera de landing) para armar cada archivo")
    p.add_argument("--eps", type=float, default=5, help="Eventos normales por segundo (±30 % por archivo)")
    p.add_argument("--segundos-por-archivo", type=int, default=30, help="Cada cuántos segundos sube un archivo (≥ 1)")
    p.add_argument("--minutos", type=float, default=50, help="Duración del run; 0 = hasta que lo cancelen")
    p.add_argument("--factor-casos", type=float, default=1.0, help="Multiplica la probabilidad de los casos de Silver; 0 = ninguno")
    p.add_argument("--max-ofertas", type=int, default=100_000, help="Tope de ofertas que se cargan en memoria")
    p.add_argument("--seed", type=int, default=None, help="Semilla (reproducible)")
    args = p.parse_args(argv)
    if args.segundos_por_archivo < 1:
        p.error("--segundos-por-archivo debe ser ≥ 1 (el nombre del archivo lleva segundos)")
    if args.eps <= 0:
        p.error("--eps debe ser > 0")
    return args


# ─────────────────────────────────────────────────────────────────────────────
# Generación (sin Spark ni dbutils: se puede probar en local)
# ─────────────────────────────────────────────────────────────────────────────

def elegir_tipo():
    return random.choices(list(PESO_EVENTOS), weights=list(PESO_EVENTOS.values()), k=1)[0]


def construir_evento(tipo, cliente, oferta, ts_evento):
    """Payload con la estructura anidada (sesion/contexto) del contrato de la Sesión 11."""
    contexto = {
        "ubicacion_pantalla": random.choice(UBICACIONES_PANTALLA),
        "posicion":           random.randint(1, 4),
        "tiempo_visible_seg": round(random.uniform(0.5, 12.0), 2),
        "monto_simulado":     None,
        "plazo_simulado":     None,
    }
    if tipo == "simulacion_realizada":
        contexto["monto_simulado"] = round(random.uniform(1_000, 50_000), 2)
        contexto["plazo_simulado"] = random.choice(PLAZOS_SIMULADOS)
    return {
        "id_evento":        str(uuid.uuid4()),
        "tipo_evento":      tipo,
        "timestamp_evento": ts_evento.isoformat(),
        "id_cliente":       cliente["id_cliente"],
        "id_oferta":        oferta["id_oferta"],
        "id_pais":          cliente["id_pais"],
        "canal":            random.choice(CANALES),
        "sesion": {
            "id_sesion":          str(uuid.uuid4()),
            "version_app":        random.choice(VERSIONES_APP),
            "sistema_operativo":  random.choice(SISTEMAS_OPERATIVOS),
            "modelo_dispositivo": random.choice(DISPOSITIVOS),
        },
        "contexto": contexto,
    }


def _cuantos(n, prob):
    """Cuántos de n eventos disparan un caso con probabilidad prob."""
    return sum(random.random() < prob for _ in range(n))


def generar_registros(ofertas, clientes, n, desde, hasta, factor_casos, recientes):
    """n eventos normales con timestamp_evento en [desde, hasta] + los casos de Silver.

    Devuelve (registros, manifiesto). Cada registro: key, value (bytes) y ts (llegada al broker).
    `recientes` es el buffer de payloads de archivos anteriores (se actualiza aquí).
    """
    ventana = max((hasta - desde).total_seconds(), 1.0)
    manifiesto = []

    def nuevo(ts_evento=None, tipo=None):
        oferta = random.choice(ofertas)
        ts_evento = ts_evento or desde + timedelta(seconds=random.uniform(0, ventana))
        return construir_evento(tipo or elegir_tipo(), clientes[oferta["id_cliente"]], oferta, ts_evento)

    def registro(ev, ts, value=None):
        return {"key": str(ev["id_cliente"]).encode(),
                "value": value if value is not None else json.dumps(ev).encode(), "ts": ts}

    def anotar(caso, clave):
        manifiesto.append({"tabla": "eventos_app", "caso": caso, "clave": clave,
                           "esperado_en_silver": ESPERADO_EVENTOS[caso]})

    # Tráfico normal: llega al broker 0,2–5 s después del evento
    registros = []
    for _ in range(n):
        ev = nuevo()
        ts = datetime.fromisoformat(ev["timestamp_evento"])
        registros.append(registro(ev, min(ts + timedelta(seconds=random.uniform(0.2, 5)), hasta)))
    normales = list(registros)

    if factor_casos > 0:
        prob = {caso: p * factor_casos for caso, p in PROB_CASOS.items()}
        # 1 · Duplicado: el productor reintenta y el mismo mensaje entra con otro offset
        for r in random.sample(normales, min(_cuantos(n, prob["duplicado"]), len(normales))):
            registros.append({**r, "ts": min(r["ts"] + timedelta(seconds=random.uniform(1, 30)), hasta)})
            anotar("duplicado", json.loads(r["value"])["id_evento"])
        # 2 · Reenvío de un evento de un archivo anterior
        for v in random.sample(list(recientes), min(_cuantos(n, prob["reenvio_anterior"]), len(recientes))):
            ev = json.loads(v)
            registros.append({"key": str(ev["id_cliente"]).encode(), "value": v, "ts": hasta})
            anotar("reenvio_anterior", ev["id_evento"])
        # 3 · Llegada tardía: el teléfono estuvo sin red 30–90 min (dentro del watermark de 2 h)
        for _ in range(_cuantos(n, prob["llegada_tardia"])):
            ev = nuevo(hasta - timedelta(minutes=random.uniform(30, 90)))
            registros.append(registro(ev, hasta))
            anotar("llegada_tardia", ev["id_evento"])
        # 4 · Fuera del watermark: el evento es de hace 3–6 h
        for _ in range(_cuantos(n, prob["fuera_watermark"])):
            ev = nuevo(hasta - timedelta(hours=random.uniform(3, 6)))
            registros.append(registro(ev, hasta))
            anotar("fuera_watermark", ev["id_evento"])
        # 5 · Payload corrupto: JSON cortado a la mitad
        for _ in range(_cuantos(n, prob["payload_corrupto"])):
            ev = nuevo()
            crudo = json.dumps(ev).encode()
            registros.append(registro(ev, hasta, value=crudo[:len(crudo) // 2]))
            anotar("payload_corrupto", ev["id_evento"])
        # 6 · Tipo de evento que la app empezó a mandar y nadie avisó
        for _ in range(_cuantos(n, prob["tipo_desconocido"])):
            ev = nuevo(tipo="banner_cerrado")
            registros.append(registro(ev, hasta))
            anotar("tipo_desconocido", ev["id_evento"])

    recientes.extend(r["value"] for r in normales)
    return registros, manifiesto


def particion(clave, n=PARTICIONES):
    # Kafka usa murmur2(key) % particiones; crc32 da el mismo efecto: misma key → misma partición
    return zlib.crc32(clave) % n


def armar_filas(registros, siguiente):
    """Filas (tuplas) con el schema de Kafka (SCHEMA_KAFKA). Asigna offsets por partición en orden
    de llegada y actualiza `siguiente` (próximo offset libre por partición)."""
    filas = []
    for r in sorted(registros, key=lambda r: r["ts"]):
        p = particion(r["key"])
        filas.append((r["key"], r["value"], TOPIC, p, siguiente[p], r["ts"], 0))
        siguiente[p] += 1
    return filas


# ─────────────────────────────────────────────────────────────────────────────
# Entrada / salida en Databricks
# ─────────────────────────────────────────────────────────────────────────────

def cargar_clientes_y_ofertas(spark, catalog, max_ofertas):
    """Clientes y ofertas que ya existen en Bronze (se generan con generar_datos_wizard_bank.py)."""
    from pyspark.sql import functions as F

    clientes = (spark.table(f"{catalog}.lending.clientes_brz")
                .select(F.col("id_cliente").try_cast("bigint").alias("id_cliente"),
                        F.col("id_pais").try_cast("int").alias("id_pais"))
                .where("id_cliente IS NOT NULL AND id_pais IS NOT NULL")
                .dropDuplicates(["id_cliente"]))
    ofertas = (spark.table(f"{catalog}.lending.ofertas_preaprobadas_brz")
               .select(F.col("id_oferta").try_cast("bigint").alias("id_oferta"),
                       F.col("id_cliente").try_cast("bigint").alias("id_cliente"))
               .where("id_oferta IS NOT NULL AND id_cliente IS NOT NULL")
               .dropDuplicates(["id_oferta"])
               .join(clientes, "id_cliente")
               .limit(max_ofertas)
               .collect())
    if not ofertas:
        sys.exit(f"No hay ofertas con cliente en {catalog}.lending. Ingesta Lending a Bronze antes de emitir.")
    cli = {r["id_cliente"]: {"id_cliente": r["id_cliente"], "id_pais": r["id_pais"]} for r in ofertas}
    ofe = [{"id_oferta": r["id_oferta"], "id_cliente": r["id_cliente"]} for r in ofertas]
    return cli, ofe


def offsets_iniciales(spark, dbutils, landing):
    """Siguiente offset libre por partición según lo que ya hay en landing."""
    siguiente = {p: 0 for p in range(PARTICIONES)}
    if not dbutils.fs.ls(landing):   # si la carpeta no existe, falla: es un error de configuración
        return siguiente
    from pyspark.sql import functions as F
    filas = spark.read.format("parquet").load(landing).groupBy("partition").agg(F.max("offset").alias("m")).collect()
    for r in filas:
        siguiente[r["partition"]] = r["m"] + 1
    return siguiente


def escribir_parquet(spark, dbutils, filas, nombre, landing, staging):
    """Serverless no deja copiar desde el disco local, así que escribe con Spark (1 archivo) a una
    carpeta de staging fuera de landing y mueve el part-*.parquet a landing/<nombre>."""
    carpeta = f"{staging.rstrip('/')}/{nombre}"
    spark.createDataFrame(filas, SCHEMA_KAFKA).coalesce(1).write.mode("overwrite").parquet(carpeta)
    parte = next(f.path for f in dbutils.fs.ls(carpeta) if f.name.startswith("part-") and f.name.endswith(".parquet"))
    dbutils.fs.mv(parte, f"{landing.rstrip('/')}/{nombre}")
    dbutils.fs.rm(carpeta, True)


def escribir_manifiesto(dbutils, manifiesto, nombre, carpeta):
    lineas = [",".join(["tabla", "caso", "clave", "esperado_en_silver"])]
    for m in manifiesto:
        lineas.append(",".join(f'"{m[c]}"' for c in ("tabla", "caso", "clave", "esperado_en_silver")))
    dbutils.fs.put(f"{carpeta.rstrip('/')}/{nombre}", "\n".join(lineas) + "\n", True)


def emitir(spark, dbutils, args):
    clientes, ofertas = cargar_clientes_y_ofertas(spark, args.catalog, args.max_ofertas)
    log.info("%s clientes · %s ofertas cargadas desde %s.lending", f"{len(clientes):,}", f"{len(ofertas):,}", args.catalog)
    siguiente = offsets_iniciales(spark, dbutils, args.landing)
    log.info("Offsets de partida por partición: %s", dict(siguiente))

    recientes = deque(maxlen=MAX_RECIENTES)
    fin = time.monotonic() + args.minutos * 60 if args.minutos > 0 else float("inf")
    ultimo = datetime.now(timezone.utc)
    archivos = total = 0

    while time.monotonic() < fin:
        time.sleep(args.segundos_por_archivo)
        ahora = datetime.now(timezone.utc).replace(microsecond=0)
        n = max(1, round(args.eps * args.segundos_por_archivo * random.uniform(0.7, 1.3)))
        registros, manifiesto = generar_registros(ofertas, clientes, n, ultimo, ahora, args.factor_casos, recientes)
        ultimo = ahora

        sufijo = ahora.strftime("%Y%m%d_%H%M%S")
        escribir_parquet(spark, dbutils, armar_filas(registros, siguiente),
                         f"eventos_app_{sufijo}.parquet", args.landing, args.staging)
        if manifiesto and args.manifiestos:
            escribir_manifiesto(dbutils, manifiesto, f"casos_silver_{sufijo}.csv", args.manifiestos)

        archivos += 1
        total += len(registros)
        log.info("eventos_app_%s.parquet: %d registros (%d normales, %d casos). Acumulado: %d archivos, %s registros",
                 sufijo, len(registros), n, len(manifiesto), archivos, f"{total:,}")
    log.info("Fin del run: %d archivos, %s registros. El job continuo lo relanza.", archivos, f"{total:,}")


def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    from databricks.sdk.runtime import dbutils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    emitir(spark, dbutils, args)


if __name__ == "__main__":
    main()
