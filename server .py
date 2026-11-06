"""Servidor de salas, retransmisión de eventos y monedas para Truco Cordobés.

Corre con:  uvicorn server:app --host 0.0.0.0 --port $PORT
Variables de entorno:
  DATABASE_URL  Postgres (postgresql://...). Sin ella usa SQLite local (se pierde en Render gratis).
  ADMIN_KEY     Clave para recargar monedas (/admin/...). Sin ella las rutas admin quedan apagadas.
  APUESTA_MIN / APUESTA_MAX   Límites de apuesta (por defecto 10 y 100000).

Monedas: el saldo vive en la base. Las apuestas se retienen al entrar a la sala y
se liquidan una sola vez: gana quien llegó a 18 (si los dos celulares informan lo
mismo), abandona = pierde, y si los reportes no coinciden se devuelven las apuestas.
"""
import asyncio
import contextlib
import hmac
import json
import os
import random
import secrets
import time

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

import ranking
from economia import Banco, DIA_LOGRO_ID, LOGROS_PERIODICOS, poderes_ofertas
from social import Social
from torneos import NOMBRES_BOT, TAM_MAX, TAM_MIN, Torneo, nombre_ronda

banco = Banco()


def _sala_espera(codigo):
    """True mientras la sala existe, no empezó y espera a un segundo jugador."""
    tor = torneos.get(codigo)
    if tor is not None:
        return tor["t"].estado == "lobby" and bool(tor["t"].jugadores)
    room = rooms.get(codigo)
    return room is not None and not room["started"] and len(room["players"]) == 1


social = Social(banco, sala_viva=_sala_espera)
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
APUESTA_MIN = int(os.environ.get("APUESTA_MIN", "10"))
APUESTA_MAX = int(os.environ.get("APUESTA_MAX", "100000"))
ESPERA_REPORTE = float(os.environ.get("ESPERA_REPORTE", "20"))  # segundos
ESPERA_TICKET = int(os.environ.get("ESPERA_TICKET", "60"))  # mínimo entre resultados contra la IA
MAX_TORNEOS = int(os.environ.get("MAX_TORNEOS", "200"))
ESPERA_SALA_TORNEO = float(os.environ.get("ESPERA_SALA_TORNEO", "75"))   # s para que los dos entren a su partido
MIN_PARTIDO_IA = float(os.environ.get("MIN_PARTIDO_IA", "15"))
GRACIA_RECONEXION = float(os.environ.get("GRACIA_RECONEXION", "60"))   # igualar con ONLINE_RECONEXION_GRACIA de la app
RANK_MIN_SEG = float(os.environ.get("RANK_MIN_SEG", "45"))          # una partida más corta no suma al ranking
RANK_ABANDONO_SEG = float(os.environ.get("RANK_ABANDONO_SEG", "90"))   # abandono que cuenta como derrota   # una partida contra un bot no puede terminar antes

rooms = {}
torneos = {}   # código -> torneo privado (en memoria; si el servidor se reinicia, las apuestas se devuelven)
waiting = []
rooms_lock = asyncio.Lock()
registros_por_ip = {}


async def db(funcion, *args):
    return await asyncio.to_thread(funcion, *args)


def code_room():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(5))
        if code not in rooms and code not in torneos:
            return code


def nueva_sala(apuesta=0):
    return {"players": [], "lock": asyncio.Lock(), "seq": 0, "bet": int(apuesta),
            "reservas": 0, "started": False, "settled": True,
            "reports": {}, "accounts": {}, "timer": None,
            "rank_reports": {}, "rank_idx": 0, "rank_hecho": False, "rank_timer": None, "inicio": 0.0,
            "tokens": {}, "cseq": {}, "log": [], "code": None}


async def send(ws, payload):
    try:
        await ws.send_text(json.dumps(payload, ensure_ascii=False))
        return True
    except Exception:
        return False


def partida_activa(room):
    """Hay una partida en juego: si alguien se cae, se le guarda el lugar un rato en vez de echarlo."""
    return bool(room["started"] and not room["rank_hecho"] and not (room["bet"] > 0 and room["settled"]))


async def broadcast(room, payload, exclude=None):
    dead = []
    for p in list(room["players"]):
        if p["ws"] is None or p["ws"] is exclude:
            continue          # ausente (reconectando) o el que originó el mensaje
        if not await send(p["ws"], payload):
            dead.append(p)
    for p in dead:
        if p in room["players"]:
            if partida_activa(room):
                await ausentar(room, p)
            else:
                room["players"].remove(p)


async def ausentar(room, p):
    """Se cayó la conexión de `p` en plena partida: se le guarda el lugar GRACIA_RECONEXION segundos."""
    if p.get("ausente"):
        return
    p["ws"] = None
    p["ausente"] = True
    for otro in room["players"]:
        if otro is not p and otro["ws"] is not None:
            await send(otro["ws"], {"type": "opponent_away", "grace": int(GRACIA_RECONEXION)})
    p["tarea"] = asyncio.create_task(vencer_gracia(room, p))


async def vencer_gracia(room, p):
    await asyncio.sleep(GRACIA_RECONEXION)
    codigo = room.get("code")
    if p.get("ausente") and rooms.get(codigo) is room and p in room["players"]:
        await cerrar_jugador(codigo, room, p["slot"], None)   # no volvió: ahora sí abandonó


def sala_llena(room):
    return len(room["players"]) + room["reservas"] >= 2


async def attach(room_code, name, ws, account=None):
    room = rooms.get(room_code)
    if room is None or len(room["players"]) >= 2:
        return None
    existing = next((p for p in room["players"] if p["ws"] is ws), None)
    if existing:
        return existing["slot"]
    usados = {p["slot"] for p in room["players"]}
    slot = 0 if 0 not in usados else 1
    room["players"].append({"name": name[:32] or "Jugador", "ws": ws, "slot": slot, "account": account})
    room["code"] = room_code
    room["tokens"][slot] = secrets.token_urlsafe(16)    # para volver al mismo lugar si se corta la conexión
    if account:
        room["accounts"][slot] = account
    await broadcast(room, {
        "type": "lobby", "room": room_code,
        "players": [p["name"] for p in sorted(room["players"], key=lambda p: p["slot"])],
        "ready": len(room["players"]) == 2, "bet": room["bet"]
    })
    if len(room["players"]) == 2:
        room["started"] = True
        room["settled"] = room["bet"] <= 0
        room["reports"] = {}
        room["inicio"] = time.time()
        room["rank_hecho"] = False
        room["rank_reports"] = {}
        await broadcast(room, {
            "type": "match_found", "room": room_code, "bet": room["bet"],
            "players": [p["name"] for p in sorted(room["players"], key=lambda p: p["slot"])],
            "message": "Rival conectado. La sala esta lista."
        })
    return slot


# ---------------------------------------------------------------- monedas
def cuenta_de(req):
    cuenta = req.get("account") or {}
    if not isinstance(cuenta, dict):
        return None
    return cuenta


async def autenticar(req):
    cuenta = cuenta_de(req)
    if not cuenta:
        return None
    cid = str(cuenta.get("id", "")).strip().upper()
    if await db(banco.autenticar, cid, str(cuenta.get("token", ""))):
        return cid
    return None


def leer_apuesta(req):
    try:
        bet = int(req.get("bet", 0) or 0)
    except (TypeError, ValueError):
        return None
    return bet if bet >= 0 else None


async def validar_apuesta(ws, req):
    """Devuelve (bet, cuenta) o None si ya se respondió con un error."""
    bet = leer_apuesta(req)
    if bet is None:
        await send(ws, {"type": "error", "message": "Apuesta inválida."})
        return None
    if bet == 0:
        return 0, None
    if not APUESTA_MIN <= bet <= APUESTA_MAX:
        await send(ws, {"type": "error", "message": f"La apuesta debe estar entre {APUESTA_MIN} y {APUESTA_MAX}."})
        return None
    cuenta = await autenticar(req)
    if cuenta is None:
        await send(ws, {"type": "error", "message": "Para apostar necesitás una cuenta válida."})
        return None
    return bet, cuenta


async def enviar_saldo(ws, cuenta):
    if cuenta:
        await send(ws, {"type": "wallet", "coins": await db(banco.saldo, cuenta)})


async def liquidar_sala(room_code, room, ganador_slot):
    """Cierra la apuesta de la sala una sola vez. ganador_slot None = devolver."""
    if room["settled"]:
        return
    room["settled"] = True
    timer = room.get("timer")
    if timer is not None and timer is not asyncio.current_task():
        timer.cancel()
    room["timer"] = None
    ganador = room["accounts"].get(ganador_slot) if ganador_slot in (0, 1) else None
    pagos = await db(banco.liquidar, room_code, ganador)
    if not pagos:
        return
    pozo = sum(pagos.values())
    for p in list(room["players"]):
        if not p.get("account"):
            continue
        if ganador is None or ganador not in pagos:
            resultado = "refund"
        elif p["account"] == ganador:
            resultado = "win"
        else:
            resultado = "lose"
        await send(p["ws"], {"type": "bet_result", "result": resultado, "pot": pozo,
                             "coins": await db(banco.saldo, p["account"])})


async def esperar_reporte(room_code, room):
    await asyncio.sleep(ESPERA_REPORTE)
    if room["settled"] or rooms.get(room_code) is not room:
        return
    reportes = list(room["reports"].values())
    if len(reportes) == 1:
        await liquidar_sala(room_code, room, reportes[0])
    else:
        await liquidar_sala(room_code, room, None)


# ------------------------------------------------------- ranking semanal (partidas 1 a 1)
def _marcador_valido(score):
    try:
        a, b = int(score[0]), int(score[1])
    except (TypeError, ValueError, IndexError, KeyError):
        return None
    return (a, b) if 0 <= a <= 30 and 0 <= b <= 30 else None


async def rank_otorgar(room_code, room, ganador_slot, margen=0):
    """Suma los puntos de la partida (una sola vez por partida) y avisa a cada jugador cuánto ganó."""
    if room["rank_hecho"]:
        return
    room["rank_hecho"] = True
    a0, a1 = room["accounts"].get(0), room["accounts"].get(1)
    if not a0 or not a1 or a0 == a1 or ganador_slot not in (0, 1):
        return
    if time.time() - room["inicio"] < RANK_MIN_SEG:
        return
    ganador = a0 if ganador_slot == 0 else a1
    evento = "P:%s:%d" % (room_code, room["rank_idx"])
    pagos = await db(banco.semanal_partida, evento, a0, a1, ganador, margen, room["bet"])
    for p in list(room["players"]):
        r = pagos.get(p.get("account"))
        if r:
            await send(p["ws"], {"type": "rank_points", "points": r["puntos"],
                                 "missions": [{"titulo": m["titulo"], "premio": m["premio"]} for m in r["misiones"]]})


async def rank_esperar(room_code, room):
    """Si solo uno informó el resultado y el otro no aparece, vale lo que dijo el que informó."""
    await asyncio.sleep(ESPERA_REPORTE)
    if room["rank_hecho"] or rooms.get(room_code) is not room or len(room["rank_reports"]) != 1:
        return
    ganador, score = next(iter(room["rank_reports"].values()))
    await rank_otorgar(room_code, room, ganador, 0)


async def rank_reportar(room_code, room, slot, ganador, score):
    if room["rank_hecho"] or not room["started"]:
        return
    room["rank_reports"][slot] = (ganador, _marcador_valido(score))
    if len(room["rank_reports"]) == 2:
        (g0, s0), (g1, s1) = room["rank_reports"][0], room["rank_reports"][1]
        if g0 == g1:
            await rank_otorgar(room_code, room, g0, abs(s0[0] - s0[1]) if s0 and s0 == s1 else 0)
        else:
            room["rank_hecho"] = True      # los celulares no coinciden: no suma nadie
    elif room.get("rank_timer") is None:
        room["rank_timer"] = asyncio.create_task(rank_esperar(room_code, room))


async def recibir_resultado(room_code, room, slot, ganador, score=None):
    if ganador not in (0, 1) or slot not in (0, 1):
        return
    await rank_reportar(room_code, room, slot, ganador, score)
    if room["bet"] <= 0 or room["settled"]:
        return
    room["reports"][slot] = ganador
    if len(room["reports"]) == 2:
        r0, r1 = room["reports"][0], room["reports"][1]
        await liquidar_sala(room_code, room, r0 if r0 == r1 else None)
    elif room.get("timer") is None:
        room["timer"] = asyncio.create_task(esperar_reporte(room_code, room))


async def reiniciar_apuesta(room_code, room):
    """Revancha con apuesta: vuelve a retener a los dos. False si alguno no puede."""
    jugadores = sorted(room["players"], key=lambda p: p["slot"])
    if len(jugadores) != 2 or not all(p.get("account") for p in jugadores):
        return False
    for p in jugadores:
        if not await db(banco.retener, p["account"], room_code, room["bet"]):
            await db(banco.liquidar, room_code, None)
            return False
    room["settled"] = False
    room["reports"] = {}
    for p in jugadores:
        await enviar_saldo(p["ws"], p["account"])
    return True


async def cerrar_jugador(room_code, room, slot, ws):
    """Un jugador se fue: devolver apuesta si no había partida, o perder por abandono."""
    if room["started"] and not room["rank_hecho"]:
        restante = next((p for p in room["players"] if p["slot"] != slot), None)
        if restante is not None:
            if room["rank_reports"]:
                g = next(iter(room["rank_reports"].values()))[0]
                if g == restante["slot"]:
                    await rank_otorgar(room_code, room, g, 0)
            elif time.time() - room["inicio"] >= RANK_ABANDONO_SEG:   # abandonó jugando: gana quien se queda
                await rank_otorgar(room_code, room, restante["slot"], 0)
    saliente = next((p for p in room["players"] if p["slot"] == slot), None)
    if saliente is not None and saliente.get("tarea") is not None:
        saliente["tarea"].cancel()
    room["players"][:] = [p for p in room["players"] if p["slot"] != slot]
    if room["players"]:
        await broadcast(room, {"type": "opponent_left", "message": "El rival se desconecto."})
    if room["bet"] > 0:
        if not room["started"]:
            await db(banco.liquidar, room_code, None)
            room["settled"] = True
        elif not room["settled"]:
            if slot in room["reports"]:
                # Ya había informado su resultado: se espera el reporte del rival.
                if room["players"] and room.get("timer") is None:
                    room["timer"] = asyncio.create_task(esperar_reporte(room_code, room))
                elif not room["players"]:
                    await liquidar_sala(room_code, room, room["reports"][slot])
            elif room["players"]:
                await liquidar_sala(room_code, room, room["players"][0]["slot"])
            else:
                await liquidar_sala(room_code, room, None)
    if not room["players"]:
        timer = room.get("timer")
        if timer is not None:
            timer.cancel()
        rooms.pop(room_code, None)


# ----------------------------------------------------------- torneos privados
# Un torneo es un cuadro de eliminación directa de 4 a 16 jugadores. Los lugares vacíos los
# completan bots. Cada humano mantiene un WebSocket propio con el torneo (/ws_torneo); cada
# partido entre humanos se juega en una sala común de 2 (la misma de siempre, sin apuesta propia)
# y la apuesta de inscripción queda retenida en "TOR-<código>" hasta que hay campeón.
def tor_sala(t):
    return "TOR-" + t.codigo


def nombre_unico(t, nombre):
    usados = {j["nombre"].lower() for j in t.jugadores} | {b.lower() for b in NOMBRES_BOT}
    nuevo, n = nombre, 2
    while nuevo.lower() in usados:
        nuevo = "%s %d" % (nombre[:28], n)
        n += 1
    return nuevo


def tor_tarea(tor, corrutina):
    tarea = asyncio.create_task(corrutina)
    tor["tareas"].add(tarea)
    tarea.add_done_callback(tor["tareas"].discard)
    return tarea


async def tor_enviar_a(tor, pid, payload):
    ws = tor["ws"].get(pid)
    if ws is not None:
        await send(ws, payload)


async def tor_difundir(tor, payload):
    for ws in list(tor["ws"].values()):
        await send(ws, payload)


def tor_payload_lobby(tor):
    t = tor["t"]
    return {"type": "torneo_lobby", "room": t.codigo, "size": t.tam, "bet": t.apuesta, "powers": t.poderes,
            "host": tor["host"], "players": [{"pid": j["pid"], "name": j["nombre"]} for j in t.jugadores]}


def tor_payload_cuadro(tor, tipo="torneo_cuadro"):
    t = tor["t"]
    return {"type": tipo, "room": t.codigo, "cuadro": t.cuadro_publico(), "ronda": t.ronda_actual(),
            "campeon": t.campeon, "estado": t.estado,
            "jugadores": [{"pid": j["pid"], "name": j["nombre"], "humano": j["humano"]} for j in t.jugadores]}


def tor_limpiar_sala(tor, partido_id):
    codigo = tor["salas"].pop(partido_id, None)
    room = rooms.get(codigo) if codigo else None
    if room is not None and not room["players"]:
        rooms.pop(codigo, None)


async def torneo_empezar(tor):
    t = tor["t"]
    async with tor["lock"]:
        if t.estado != "lobby" or not t.jugadores:
            return
        t.empezar()
    await tor_difundir(tor, tor_payload_cuadro(tor, "torneo_inicio"))
    await torneo_avanzar(tor)


async def torneo_avanzar(tor):
    """Lanza los partidos que ya tienen a sus dos jugadores (siempre de a una ronda)."""
    t = tor["t"]
    if t.estado != "en_juego":
        return
    listos = t.partidos_listos()
    for p in listos:          # se marcan todos antes de cualquier await: nadie los lanza dos veces
        p["estado"] = "jugando"
    for p in listos:
        if t.estado == "en_juego":
            await torneo_lanzar(tor, p)


async def torneo_lanzar(tor, p):
    t = tor["t"]
    ja, jb = t.jugador(p["a"]), t.jugador(p["b"])
    r = int(p["id"][1:p["id"].index("m")])
    ronda = nombre_ronda(r, len(t.rondas))
    ausentes = [j for j in (ja, jb) if j["humano"] and j["pid"] not in tor["ws"]]
    if ausentes:   # quien no está conectado pierde por ausencia
        if len(ausentes) == 2:
            ganador = random.choice([p["a"], p["b"]])
        else:
            ganador = jb["pid"] if ausentes[0] is ja else ja["pid"]
        await torneo_cerrar(tor, p["id"], ganador)
        return
    humanos = [j for j in (ja, jb) if j["humano"]]
    tor["inicio"][p["id"]] = time.time()
    tor["reportes"].pop(p["id"], None)
    tor["esperas"].discard(p["id"])
    base = {"type": "torneo_partido", "match": p["id"], "ronda": ronda, "powers": t.poderes}
    if not humanos:
        tor_tarea(tor, torneo_simular(tor, p["id"]))
    elif len(humanos) == 1:
        h = humanos[0]
        rival = jb if h is ja else ja
        await tor_enviar_a(tor, h["pid"], dict(base, modo="ia", rival=rival["nombre"], rival_pid=rival["pid"]))
    else:
        tor_limpiar_sala(tor, p["id"])
        codigo = code_room()
        rooms[codigo] = nueva_sala(0)
        tor["salas"][p["id"]] = codigo
        for j, otro in ((ja, jb), (jb, ja)):
            await tor_enviar_a(tor, j["pid"], dict(base, modo="sala", room=codigo, rival=otro["nombre"],
                                                   rival_pid=otro["pid"]))
        tor_tarea(tor, torneo_vigilar_sala(tor, p["id"], codigo))


async def torneo_simular(tor, partido_id):
    await asyncio.sleep(random.uniform(3, 7))
    p = tor["t"].partido(partido_id)
    if p is not None and p["estado"] != "fin":
        await torneo_cerrar(tor, partido_id, random.choice([p["a"], p["b"]]))


async def torneo_vigilar_sala(tor, partido_id, codigo):
    """Si pasado un rato alguno no entró a la sala del partido, pierde por ausencia."""
    await asyncio.sleep(ESPERA_SALA_TORNEO)
    t = tor["t"]
    p = t.partido(partido_id)
    if p is None or p["estado"] != "jugando" or tor["salas"].get(partido_id) != codigo:
        return
    room = rooms.get(codigo)
    if room is not None and room["started"]:
        return
    presentes = {pl["name"] for pl in room["players"]} if room is not None else set()
    a_ok, b_ok = t.nombre_de(p["a"]) in presentes, t.nombre_de(p["b"]) in presentes
    if a_ok and not b_ok:
        ganador = p["a"]
    elif b_ok and not a_ok:
        ganador = p["b"]
    else:
        ganador = random.choice([p["a"], p["b"]])
    await torneo_cerrar(tor, partido_id, ganador)


async def torneo_cerrar(tor, partido_id, ganador):
    t = tor["t"]
    p = t.registrar(partido_id, ganador)
    if p is None:
        return
    tor["reportes"].pop(partido_id, None)
    tor["esperas"].discard(partido_id)
    tor_limpiar_sala(tor, partido_id)
    await tor_difundir(tor, dict(tor_payload_cuadro(tor), match=partido_id, winner=ganador))
    if t.estado == "terminado":
        await torneo_terminar(tor)
    else:
        await torneo_avanzar(tor)


async def torneo_resultado(tor, pid, partido_id, ganador):
    t = tor["t"]
    p = t.partido(partido_id)
    if p is None or p["estado"] != "jugando" or pid not in (p["a"], p["b"]) or ganador not in (p["a"], p["b"]):
        return
    humanos = [x for x in (p["a"], p["b"]) if t.jugador(x)["humano"]]
    reportes = tor["reportes"].setdefault(partido_id, {})
    reportes[pid] = ganador
    if len(humanos) == 1:   # contra un bot: se confía en el celular, pero no puede terminar al instante
        falta = MIN_PARTIDO_IA - (time.time() - tor["inicio"].get(partido_id, 0))
        if falta > 0:
            await asyncio.sleep(falta)
        await torneo_cerrar(tor, partido_id, ganador)
    elif len(reportes) >= 2:
        if len(set(reportes.values())) == 1:
            await torneo_cerrar(tor, partido_id, ganador)
        elif tor["repetidos"].get(partido_id, 0) < 1:   # no coinciden: se juega de nuevo, una vez
            tor["repetidos"][partido_id] = tor["repetidos"].get(partido_id, 0) + 1
            await tor_difundir(tor, {"type": "torneo_aviso", "message": "Los celulares no coincidieron en el resultado. Se vuelve a jugar el partido."})
            await torneo_lanzar(tor, p)
        else:
            await torneo_cerrar(tor, partido_id, random.choice([p["a"], p["b"]]))
    elif partido_id not in tor["esperas"]:
        tor["esperas"].add(partido_id)
        tor_tarea(tor, torneo_esperar_reporte(tor, partido_id))


async def torneo_esperar_reporte(tor, partido_id):
    await asyncio.sleep(ESPERA_REPORTE)
    p = tor["t"].partido(partido_id)
    reportes = tor["reportes"].get(partido_id)
    if p is not None and p["estado"] == "jugando" and reportes:
        await torneo_cerrar(tor, partido_id, next(iter(reportes.values())))


async def torneo_terminar(tor):
    """Hay campeón: si es humano se lleva todo el pozo; si ganó un bot, el pozo se pierde (no se devuelve nada)."""
    t = tor["t"]
    campeon = t.jugador(t.campeon)
    pagos = {}
    if t.apuesta > 0:
        if campeon["humano"]:
            pagos = await db(banco.liquidar, tor_sala(t), campeon["cuenta"])
        else:
            pagos = await db(banco.perder_retenciones, tor_sala(t))
    pozo = sum(pagos.values())
    puntos_rank = {}   # ranking semanal: solo si jugaron suficientes humanos con cuenta
    misiones_rank = {}
    humanos = [j for j in t.jugadores if j["humano"] and j["cuenta"]]
    if len(humanos) >= ranking.TORNEO_MIN_HUMANOS:
        podio = t.podio()
        for j in humanos:
            if j["pid"] in podio:
                puntos_rank[j["pid"]] = await db(banco.semanal_sumar, j["cuenta"], ranking.PTS_TORNEO[podio[j["pid"]]],
                                                 "T:%s:%s" % (t.codigo, j["cuenta"]))
            m = await db(banco.semanal_mision, j["cuenta"], "torneo1", 1)   # terminar un torneo: misión semanal
            if m:
                misiones_rank[j["pid"]] = [{"titulo": m["titulo"], "premio": m["premio"]}]
                puntos_rank[j["pid"]] = puntos_rank.get(j["pid"], 0) + m["premio"]
    for pid, ws in list(tor["ws"].items()):
        j = t.jugador(pid)
        if not pagos:
            res = ""
        elif campeon["humano"] and campeon["cuenta"] in pagos:
            res = "win" if pid == t.campeon else "lose"
        else:
            res = "lose"
        saldo = await db(banco.saldo, j["cuenta"]) if j and j["cuenta"] else None
        await send(ws, {"type": "torneo_fin", "campeon": campeon["nombre"], "campeon_pid": t.campeon,
                        "pot": pozo, "result": res, "coins": saldo, "rank": puntos_rank.get(pid, 0),
                        "missions": misiones_rank.get(pid, [])})


async def torneo_salida(codigo, pid, ws):
    """Un jugador cerró su conexión con el torneo."""
    tor = torneos.get(codigo)
    if tor is None or pid is None:
        return
    t = tor["t"]
    if tor["ws"].get(pid) is ws:
        del tor["ws"][pid]
    if t.estado == "lobby":
        j = t.jugador(pid)
        if t.apuesta > 0 and j and j["cuenta"]:
            await db(banco.devolver_cuenta, tor_sala(t), j["cuenta"])
        t.quitar_humano(pid)
        if not tor["ws"]:
            torneos.pop(codigo, None)
            return
        if tor["host"] == pid:
            tor["host"] = next(iter(tor["ws"]))
        await tor_difundir(tor, tor_payload_lobby(tor))
    elif t.estado == "en_juego":
        if not tor["ws"]:   # no queda ningún humano: se cancela y se devuelven las apuestas
            if t.apuesta > 0:
                await db(banco.liquidar, tor_sala(t), None)
            for partido_id in list(tor["salas"]):
                tor_limpiar_sala(tor, partido_id)
            for tarea in list(tor["tareas"]):
                tarea.cancel()
            torneos.pop(codigo, None)
            return
        afectados = [p["id"] for ronda in t.rondas for p in ronda
                     if p["estado"] == "jugando" and pid in (p["a"], p["b"])]
        for partido_id in afectados:   # abandonó en plena partida: gana el rival
            p = t.partido(partido_id)
            if p is None or p["estado"] != "jugando":
                continue
            otro = p["b"] if p["a"] == pid else p["a"]
            oj = t.jugador(otro)
            ganador = otro if not (oj and oj["humano"] and otro not in tor["ws"]) else random.choice([p["a"], p["b"]])
            await torneo_cerrar(tor, partido_id, ganador)
    else:
        if not tor["ws"]:
            torneos.pop(codigo, None)


async def torneo_error(ws, mensaje):
    await send(ws, {"type": "torneo_error", "message": mensaje})


async def torneo_endpoint(ws: WebSocket):
    await ws.accept()
    codigo = None
    pid = None
    try:
        raw = await ws.receive_text()
        try:
            req = json.loads(raw)
        except Exception:
            await torneo_error(ws, "Formato de mensaje inválido.")
            return
        action = req.get("action")
        nombre = str(req.get("name", "Jugador"))[:32].strip() or "Jugador"
        if action == "crear":
            try:
                tam = int(req.get("size"))
            except (TypeError, ValueError):
                tam = 0
            if not TAM_MIN <= tam <= TAM_MAX:
                await torneo_error(ws, "El torneo es de %d a %d jugadores." % (TAM_MIN, TAM_MAX))
                return
            if len(torneos) >= MAX_TORNEOS:
                await torneo_error(ws, "Hay demasiados torneos abiertos. Probá en unos minutos.")
                return
            validada = await validar_apuesta(ws, req)
            if validada is None:
                return
            bet, cuenta = validada
            cuenta = cuenta or await autenticar(req)   # sin apuesta también: el ranking semanal usa la cuenta
            codigo = code_room()
            t = Torneo(codigo, tam, bet, bool(req.get("powers")))
            tor = {"t": t, "ws": {}, "host": None, "lock": asyncio.Lock(), "tareas": set(), "reportes": {},
                   "inicio": {}, "esperas": set(), "repetidos": {}, "salas": {},
                   "duenio": cuenta or await autenticar(req)}
            torneos[codigo] = tor
            if bet > 0 and not await db(banco.retener, cuenta, tor_sala(t), bet):
                torneos.pop(codigo, None)
                codigo = None
                await torneo_error(ws, "Saldo insuficiente para esa apuesta.")
                return
            pid = t.agregar_humano(nombre_unico(t, nombre), cuenta)
            tor["host"] = pid
            tor["ws"][pid] = ws
            await send(ws, {"type": "torneo_creado", "room": codigo, "pid": pid, "name": t.jugador(pid)["nombre"],
                            "size": t.tam, "bet": t.apuesta, "powers": t.poderes})
            await enviar_saldo(ws, cuenta)
            await tor_difundir(tor, tor_payload_lobby(tor))
        elif action == "unirse":
            pedido = str(req.get("room", "")).strip().upper()
            tor = torneos.get(pedido)
            if tor is None or tor["t"].estado != "lobby" or not tor["t"].jugadores:
                await torneo_error(ws, "Código inexistente o el torneo ya empezó.")
                return
            t = tor["t"]
            async with tor["lock"]:
                if t.estado != "lobby" or t.lugares_libres() <= 0:
                    await torneo_error(ws, "El torneo ya está completo o empezó.")
                    return
                cuenta = await autenticar(req)   # sin apuesta también: el ranking semanal usa la cuenta
                if t.apuesta > 0 and cuenta is None:
                    await torneo_error(ws, "Para entrar a un torneo con apuesta necesitás una cuenta válida.")
                    return
                if cuenta and any(j["cuenta"] == cuenta for j in t.jugadores):
                    await torneo_error(ws, "Ya estás anotado en este torneo.")
                    return
                if t.apuesta > 0:
                    if not await db(banco.retener, cuenta, tor_sala(t), t.apuesta):
                        await torneo_error(ws, "Saldo insuficiente: la inscripción es de %d monedas." % t.apuesta)
                        return
                pid = t.agregar_humano(nombre_unico(t, nombre), cuenta)
            codigo = pedido
            tor["ws"][pid] = ws
            await send(ws, {"type": "torneo_unido", "room": codigo, "pid": pid, "name": t.jugador(pid)["nombre"],
                            "size": t.tam, "bet": t.apuesta, "powers": t.poderes})
            await enviar_saldo(ws, cuenta)
            await tor_difundir(tor, tor_payload_lobby(tor))
        else:
            await torneo_error(ws, "Acción desconocida. Usá crear o unirse.")
            return

        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            accion = msg.get("action")
            if accion == "salir":
                break
            if accion == "ping":
                await send(ws, {"type": "pong"})
                continue
            tor = torneos.get(codigo)
            if tor is None:
                continue
            if accion == "empezar":
                if pid != tor["host"]:
                    await torneo_error(ws, "Solo quien creó el torneo puede empezarlo.")
                    continue
                await torneo_empezar(tor)
            elif accion == "resultado":
                tor_tarea(tor, torneo_resultado(tor, pid, str(msg.get("match", "")), str(msg.get("winner", ""))))
            elif accion == "estado":   # el cliente volvió a la pantalla: se le reenvía lo que se perdió
                if tor["t"].estado == "lobby":
                    await send(ws, tor_payload_lobby(tor))
                else:
                    await send(ws, tor_payload_cuadro(tor, "torneo_inicio"))
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await torneo_error(ws, str(exc)[:160])
        except Exception:
            pass
    finally:
        if codigo:
            await torneo_salida(codigo, pid, ws)


# ------------------------------------------------------------------- HTTP
async def health(request):
    return JSONResponse({"status": "ok", "service": "Truco Cordobes Online", "rooms": len(rooms), "torneos": len(torneos),
                         "db": banco.tipo, "persistente": banco.pg,
                         "database_url_cargada": banco.url_cargada,
                         "poderes_comprar": True, "gira_descuento": hasattr(banco, "gira_descuento"),
                         "poderes_ofertas_hoy": list(poderes_ofertas())})


async def leer_json(request):
    try:
        datos = await request.json()
        return datos if isinstance(datos, dict) else {}
    except Exception:
        return {}


def ip_cliente(request):
    reenviada = request.headers.get("x-forwarded-for", "")
    if reenviada:
        return reenviada.split(",")[0].strip()
    return request.client.host if request.client else "?"


async def registrar(request):
    ip = ip_cliente(request)
    ahora = time.time()
    recientes = [t for t in registros_por_ip.get(ip, []) if ahora - t < 3600]
    if len(recientes) >= 10:
        return JSONResponse({"error": "Demasiados registros desde esta conexión."}, status_code=429)
    recientes.append(ahora)
    registros_por_ip[ip] = recientes
    datos = await leer_json(request)
    cid, token = await db(banco.crear_cuenta, str(datos.get("name", "Jugador")), datos.get("token"),
                          str(datos.get("club", "")))
    return JSONResponse({"id": cid, "token": token, "coins": 0})


async def billetera(request):
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        return JSONResponse({"error": "Cuenta inválida."}, status_code=401)
    return JSONResponse({"id": cid, "coins": await db(banco.saldo, cid)})


# ---------------------------------------------------------------- amigos
async def amigos_auth(request):
    """Autentica la cuenta y, de paso, la marca como en línea (señal de vida)."""
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        return None, datos, JSONResponse({"error": "Cuenta inválida."}, status_code=401)
    social.latido(cid)
    return cid, datos, None


def _respuesta_social(r):
    return JSONResponse(r, status_code=400 if "error" in r else 200)


async def amigos_latido(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return JSONResponse({"ok": True, "sin_leer": await db(social.total_sin_leer, cid),
                         "invitaciones": await db(social.pendientes, cid)})


async def amigos_listar(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    r = await db(social.listar, cid)
    # Agrega el club de cada uno para que la app muestre su escudo.
    grupos = [r.get(k) or [] for k in ("amigos", "recibidas", "enviadas")]
    clubes = await db(banco.clubes_de, [x.get("id") for g in grupos for x in g])
    for g in grupos:
        for x in g:
            x["club"] = clubes.get(str(x.get("id", "")).upper(), "")
    return JSONResponse(r)


async def cuenta_perfil(request):
    """El celular informa su nombre y club para que los amigos vean el escudo y lo encuentren."""
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return JSONResponse(await db(banco.actualizar_perfil, cid, datos.get("nombre"), datos.get("club")))


async def amigos_buscar(request):
    """Busca jugadores por nombre, apellido y/o club para mandarles solicitud de amistad."""
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    palabras = (str(datos.get("nombre", "")) + " " + str(datos.get("apellido", ""))).split()[:6]
    club = str(datos.get("club", "")).strip()
    if not club and not any(len(p) >= 2 for p in palabras):
        return JSONResponse({"error": "Escribí al menos 2 letras del nombre o apellido, o elegí un club."}, status_code=400)
    encontrados = await db(banco.buscar_cuentas, cid, palabras, club)
    rel = await db(social.listar, cid)
    estado = {}
    for clave, nombre_rel in (("amigos", "amigo"), ("enviadas", "enviada"), ("recibidas", "recibida")):
        for x in rel.get(clave) or []:
            estado[str(x.get("id", "")).upper()] = nombre_rel
    for x in encontrados:
        x["relacion"] = estado.get(x["id"], "")
    return JSONResponse({"resultados": encontrados})


async def amigos_pedir(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return _respuesta_social(await db(social.pedir, cid, datos.get("otro")))


async def amigos_responder(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return _respuesta_social(await db(social.responder, cid, datos.get("otro"), bool(datos.get("aceptar"))))


async def amigos_eliminar(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return _respuesta_social(await db(social.eliminar, cid, datos.get("otro")))


async def chat_enviar(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return _respuesta_social(await db(social.enviar, cid, datos.get("otro"), datos.get("texto")))


async def chat_invitar(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    sala = str(datos.get("sala", "")).strip().upper()
    tor = torneos.get(sala)
    if tor is not None:   # torneo privado en su lobby: solo quien lo creó puede invitar
        if tor.get("duenio") != cid or not _sala_espera(sala):
            return JSONResponse({"error": "Ese torneo ya no está disponible."}, status_code=400)
        return _respuesta_social(await db(social.invitar, cid, datos.get("otro"), sala, tor["t"].apuesta))
    room = rooms.get(sala)
    # La sala tiene que ser tuya (la creaste vos), seguir esperando y no estar llena.
    if room is None or room.get("duenio") != cid or not _sala_espera(sala):
        return JSONResponse({"error": "Esa sala ya no está disponible."}, status_code=400)
    return _respuesta_social(await db(social.invitar, cid, datos.get("otro"), sala, room["bet"]))


async def chat_invitacion(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return _respuesta_social(await db(social.responder_invitacion, cid, datos.get("invitacion"), bool(datos.get("aceptar"))))


async def chat_leer(request):
    cid, datos, error = await amigos_auth(request)
    if error:
        return error
    return _respuesta_social(await db(social.leer, cid, datos.get("otro"), datos.get("desde", 0)))


async def info_sala(request):
    codigo = request.path_params["code"].strip().upper()
    tor = torneos.get(codigo)
    if tor is not None:
        t = tor["t"]
        return JSONResponse({"exists": t.estado == "lobby" and bool(t.jugadores), "full": t.lugares_libres() <= 0,
                             "bet": t.apuesta, "torneo": True, "size": t.tam, "powers": t.poderes})
    room = rooms.get(codigo)
    if room is None:
        return JSONResponse({"exists": False, "full": False, "bet": 0})
    return JSONResponse({"exists": True, "full": sala_llena(room), "bet": room["bet"]})


async def apuesta_abrir(request):
    """Apuesta contra la IA ('ia') o en el campeonato ('torneo'): retiene el monto."""
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        return JSONResponse({"error": "Cuenta inválida."}, status_code=401)
    tipo = datos.get("tipo")
    try:
        bet = int(datos.get("bet"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "Apuesta inválida."}, status_code=400)
    if tipo not in ("ia", "torneo") or not APUESTA_MIN <= bet <= APUESTA_MAX:
        return JSONResponse({"error": f"Apuesta inválida (entre {APUESTA_MIN} y {APUESTA_MAX})."}, status_code=400)
    abierto = await db(banco.abrir_ticket, cid, tipo, bet)
    if abierto is None:
        return JSONResponse({"error": "Saldo insuficiente para esa apuesta."}, status_code=400)
    ticket, saldo = abierto
    return JSONResponse({"ticket": ticket, "coins": saldo})


async def apuesta_reclamar(request):
    """Informa un resultado: ganar/perder (ia) o clasificar/octavos/.../final/perder (torneo)."""
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        return JSONResponse({"error": "Cuenta inválida."}, status_code=401)
    resultado = await db(banco.reclamar_ticket, str(datos.get("ticket", "")), cid,
                         str(datos.get("evento", "")), ESPERA_TICKET)
    if "error" in resultado:
        return JSONResponse(resultado, status_code=400)
    return JSONResponse(resultado)


async def poderes_comprar(request):
    """Compra de un poder: el servidor calcula el precio (con la oferta del día) y lo cobra una sola vez por `evento`."""
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        return JSONResponse({"error": "Cuenta inválida."}, status_code=401)
    try:
        visto = int(datos["precio"]) if datos.get("precio") is not None else None
    except (TypeError, ValueError):
        visto = None
    r = await db(banco.comprar_poder, cid, str(datos.get("poder", "")), str(datos.get("evento", "")), visto)
    return JSONResponse(r, status_code=400 if "error" in r else 200)


def admin_ok(request):
    if not ADMIN_KEY:
        return JSONResponse({"error": "Administración apagada: falta ADMIN_KEY en el servidor."}, status_code=503)
    if not hmac.compare_digest(request.headers.get("x-admin-key", ""), ADMIN_KEY):
        return JSONResponse({"error": "Clave de administrador incorrecta."}, status_code=403)
    return None


async def admin_recargar(request):
    error = admin_ok(request)
    if error:
        return error
    datos = await leer_json(request)
    try:
        monto = int(datos.get("amount"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "amount inválido."}, status_code=400)
    if monto == 0 or abs(monto) > 10_000_000:
        return JSONResponse({"error": "amount fuera de rango."}, status_code=400)
    cid = str(datos.get("id", "")).strip().upper()
    nuevo = await db(banco.ajustar, cid, monto, "recarga" if monto > 0 else "correccion")
    if nuevo is None:
        return JSONResponse({"error": "Cuenta inexistente o saldo insuficiente para restar."}, status_code=404)
    return JSONResponse({"id": cid, "coins": nuevo})


async def admin_cuenta(request):
    error = admin_ok(request)
    if error:
        return error
    cid = request.path_params["cid"].strip().upper()
    saldo = await db(banco.saldo, cid)
    if saldo is None:
        return JSONResponse({"error": "Cuenta inexistente."}, status_code=404)
    return JSONResponse({"id": cid, "coins": saldo, "movimientos": await db(banco.movimientos, cid, 30)})


# -------------------------------------------------------------- WebSocket
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    room_code = None
    player_name = "Jugador"
    slot = None
    ticket = None
    salio_a_proposito = False
    try:
        raw = await ws.receive_text()
        try:
            req = json.loads(raw)
        except Exception:
            await send(ws, {"type": "error", "message": "Formato de mensaje invalido."})
            return
        action = req.get("action")
        player_name = str(req.get("name", "Jugador"))[:32].strip() or "Jugador"

        if action == "create":
            validada = await validar_apuesta(ws, req)
            if validada is None:
                return
            bet, cuenta = validada
            room_code = code_room()
            rooms[room_code] = nueva_sala(bet)
            cuenta = cuenta or await autenticar(req)   # sin apuesta también: el ranking semanal necesita la cuenta
            rooms[room_code]["duenio"] = cuenta   # dueño de la sala (solo para invitaciones)
            if bet > 0 and not await db(banco.retener, cuenta, room_code, bet):
                rooms.pop(room_code, None)
                room_code = None
                await send(ws, {"type": "error", "message": "Saldo insuficiente para esa apuesta."})
                return
            slot = await attach(room_code, player_name, ws, cuenta)
            await send(ws, {"type": "created", "room": room_code, "slot": slot, "bet": bet,
                            "resume_token": rooms[room_code]["tokens"].get(slot),
                            "message": "Sala creada. Compartile el codigo a tu rival."})
            await enviar_saldo(ws, cuenta)
        elif action == "join":
            room_code = str(req.get("room", "")).strip().upper()
            room = rooms.get(room_code)
            if room is None or sala_llena(room):
                room_code = None
                await send(ws, {"type": "error", "message": "Codigo inexistente o sala completa."})
                return
            cuenta = None
            if room["bet"] > 0:
                cuenta = await autenticar(req)
                if cuenta is None:
                    room_code = None
                    await send(ws, {"type": "error", "message": "Para entrar a una sala con apuesta necesitás una cuenta válida."})
                    return
                if cuenta in room["accounts"].values():
                    room_code = None
                    await send(ws, {"type": "error", "message": "No podés jugar contra vos mismo."})
                    return
                room["reservas"] += 1
                try:
                    retenida = await db(banco.retener, cuenta, room_code, room["bet"])
                finally:
                    room["reservas"] -= 1
                if not retenida:
                    room_code = None
                    await send(ws, {"type": "error", "message": f"Saldo insuficiente: la sala apuesta {room['bet']} monedas."})
                    return
                if rooms.get(room_code) is not room or not room["players"]:
                    await db(banco.liquidar, room_code, None)
                    room_code = None
                    await send(ws, {"type": "error", "message": "La sala ya no existe. Se te devolvió la apuesta."})
                    return
            if cuenta is None:
                cuenta = await autenticar(req)   # sin apuesta también: el ranking semanal necesita la cuenta
            slot = await attach(room_code, player_name, ws, cuenta)
            if slot is None:
                if cuenta:
                    await db(banco.liquidar, room_code, None)
                room_code = None
                await send(ws, {"type": "error", "message": "Codigo inexistente o sala completa."})
                return
            await send(ws, {"type": "joined", "room": room_code, "slot": slot, "bet": room["bet"],
                            "resume_token": room["tokens"].get(slot)})
            await enviar_saldo(ws, cuenta)
        elif action == "search":
            validada = await validar_apuesta(ws, req)
            if validada is None:
                return
            bet, cuenta = validada
            if cuenta is None:
                cuenta = await autenticar(req)   # sin apuesta también: el ranking semanal necesita la cuenta
            if bet > 0 and await db(banco.saldo, cuenta) < bet:
                await send(ws, {"type": "error", "message": "Saldo insuficiente para esa apuesta."})
                return
            opponent = None
            async with rooms_lock:
                waiting[:] = [t for t in waiting if t["ws"] is not ws]
                for t in waiting:
                    if (not t["matched"] and t["bet"] == bet
                            and (bet == 0 or t["account"] != cuenta)):
                        opponent = t
                        break
                if opponent:
                    waiting.remove(opponent)
                    opponent["matched"] = True
                else:
                    ticket = {"ws": ws, "name": player_name, "matched": False, "listo": False,
                              "bet": bet, "account": cuenta, "room": None, "error": None}
                    waiting.append(ticket)
            if opponent:
                codigo = code_room()
                rooms[codigo] = nueva_sala(bet)
                if bet > 0:
                    ok_a = await db(banco.retener, opponent["account"], codigo, bet)
                    ok_b = ok_a and await db(banco.retener, cuenta, codigo, bet)
                    if not (ok_a and ok_b):
                        await db(banco.liquidar, codigo, None)
                        rooms.pop(codigo, None)
                        opponent["error"] = "No se pudo retener la apuesta. Buscá de nuevo."
                        opponent["listo"] = True
                        await send(ws, {"type": "error", "message": "No se pudo retener la apuesta. Buscá de nuevo."})
                        return
                room_code = codigo
                # Primer jugador conserva slot 0; el buscador queda slot 1.
                await attach(room_code, opponent["name"], opponent["ws"], opponent["account"])
                slot = await attach(room_code, player_name, ws, cuenta)
                sala = rooms[room_code]
                if len(sala["players"]) < 2:
                    await db(banco.liquidar, room_code, None)
                    opponent["listo"] = True
                    await send(ws, {"type": "error", "message": "El rival se desconectó. Buscá de nuevo."})
                    return
                nombres = [opponent["name"], player_name]
                await send(ws, {"type": "match_found", "room": room_code, "slot": slot,
                                "players": nombres, "bet": bet, "resume_token": sala["tokens"].get(slot)})
                await send(opponent["ws"], {"type": "match_found", "room": room_code, "slot": 0,
                                            "players": nombres, "bet": bet, "resume_token": sala["tokens"].get(0)})
                opponent["room"] = room_code
                opponent["listo"] = True
                await enviar_saldo(ws, cuenta)
                await enviar_saldo(opponent["ws"], opponent["account"])
            else:
                await send(ws, {"type": "searching", "message": "Buscando rival; deja la conexion abierta."})
                esperado = 0.0
                while not ticket["listo"]:
                    await asyncio.sleep(0.25)
                    esperado += 0.25
                    if esperado >= 5:  # aviso de vida: detecta si el buscador se fue
                        esperado = 0.0
                        if not await send(ws, {"type": "pong"}):
                            raise WebSocketDisconnect()
                if ticket["error"] or ticket["room"] is None:
                    await send(ws, {"type": "error", "message": ticket["error"] or "No se pudo armar la partida."})
                    return
                room_code = ticket["room"]
                slot = 0
        elif action == "rejoin":
            pedido = str(req.get("room", "")).strip().upper()
            room = rooms.get(pedido)
            try:
                slot_pedido = int(req.get("slot"))
            except (TypeError, ValueError):
                slot_pedido = None
            jugador = next((p for p in room["players"] if p["slot"] == slot_pedido), None) if room else None
            esperado = room["tokens"].get(slot_pedido, "") if room else ""
            if (room is None or jugador is None or not esperado
                    or not secrets.compare_digest(str(req.get("resume_token", "")), esperado)):
                await send(ws, {"type": "rejoin_failed", "message": "La sala ya no existe o se venció el tiempo."})
                return
            anterior = jugador["ws"]
            jugador["ws"] = ws
            jugador["ausente"] = False
            if jugador.get("tarea") is not None:
                jugador["tarea"].cancel()
                jugador["tarea"] = None
            if anterior is not None and anterior is not ws:
                try:
                    await anterior.close()      # conexión vieja que quedó colgada
                except Exception:
                    pass
            room_code = pedido
            slot = slot_pedido
            try:
                visto = int(req.get("last_seq", 0) or 0)
            except (TypeError, ValueError):
                visto = 0
            await send(ws, {"type": "rejoined", "room": room_code, "slot": slot, "bet": room["bet"],
                            "last_cseq": room["cseq"].get(slot, 0), "resume_token": esperado})
            for sobre in list(room["log"]):      # lo que el rival mandó mientras estabas afuera
                if sobre["seq"] > visto and sobre["from_slot"] != slot:
                    await send(ws, sobre)
            for otro in room["players"]:
                if otro is not jugador and otro["ws"] is not None:
                    await send(otro["ws"], {"type": "opponent_back"})
            await enviar_saldo(ws, jugador.get("account"))
        else:
            await send(ws, {"type": "error", "message": "Accion desconocida. Usa create, join, search o rejoin."})
            return

        # Retransmisión persistente. Incluye el slot autoritativo y la secuencia.
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            accion = msg.get("action")
            if accion == "leave":
                salio_a_proposito = True
                break
            if accion == "ping":
                await send(ws, {"type": "pong"})
                continue
            room = rooms.get(room_code) if room_code else None
            if room is None:
                continue
            if accion == "result":
                await recibir_resultado(room_code, room, slot, msg.get("winner_slot"), msg.get("score"))
                continue
            if accion == "game_event":
                payload = msg.get("payload", {})
                evento = payload.get("event") if isinstance(payload, dict) else None
                cseq = msg.get("cseq")
                if isinstance(cseq, int) and cseq > 0:      # tras reconectar el cliente reenvía: no se repite
                    if cseq <= room["cseq"].get(slot, 0):
                        continue
                    room["cseq"][slot] = cseq
                if evento == "rematch_start" and slot == 0:   # revancha: otra partida para el ranking
                    room["rank_reports"] = {}
                    room["rank_hecho"] = False
                    room["rank_timer"] = None
                    room["rank_idx"] += 1
                    room["inicio"] = time.time()
                if evento == "rematch_start" and room["bet"] > 0:
                    if slot != 0 or not room["settled"]:
                        continue
                    if not await reiniciar_apuesta(room_code, room):
                        await broadcast(room, {"type": "bet_error",
                                               "message": "No hay saldo para la revancha."})
                        continue
                async with room["lock"]:
                    room["seq"] += 1
                    envelope = {
                        "type": "game_event",
                        "room": room_code,
                        "from_slot": slot,
                        "seq": room["seq"],
                        "payload": payload
                    }
                    room["log"].append(envelope)
                    del room["log"][:-400]
                    await broadcast(room, envelope, exclude=ws)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await send(ws, {"type": "error", "message": str(exc)[:160]})
        except Exception:
            pass
    finally:
        waiting[:] = [t for t in waiting if t.get("ws") is not ws]
        if ticket is not None and not ticket["listo"]:
            ticket["matched"] = True
        if room_code and room_code in rooms:
            sala_actual = rooms[room_code]
            yo = next((p for p in sala_actual["players"] if p["slot"] == slot), None)
            if yo is not None and yo["ws"] is ws:      # (si volvió por otra conexión, esta ya no cuenta)
                if salio_a_proposito or not partida_activa(sala_actual):
                    await cerrar_jugador(room_code, sala_actual, slot, ws)
                else:
                    await ausentar(sala_actual, yo)    # corte en plena partida: se le guarda el lugar


async def gira_auth(request):
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        return None, datos, JSONResponse({"error": "Cuenta inválida."}, status_code=401)
    return cid, datos, None


async def gira_victoria(request):
    cid, datos, error = await gira_auth(request)
    if error:
        return error
    r = await db(banco.gira_victoria, cid, str(datos.get("evento", "")), str(datos.get("region", "")),
                 str(datos.get("nombre", "")), str(datos.get("club", "")))
    return JSONResponse(r, status_code=400 if "error" in r else 200)


async def gira_logro(request):
    cid, datos, error = await gira_auth(request)
    if error:
        return error
    if str(datos.get("logro", "")) == DIA_LOGRO_ID:
        # Truco del día: se puede repetir una vez por día; el premio y la fecha los calcula el servidor
        r = await db(banco.gira_truco_dia, cid, str(datos.get("evento", "")),
                     str(datos.get("nombre", "")), str(datos.get("club", "")))
        return JSONResponse(r, status_code=400 if "error" in r else 200)
    if str(datos.get("logro", "")) in LOGROS_PERIODICOS:
        # Ruleta, personaje del día y desafíos semanales: los puntos y el tope por día/semana los fija el servidor
        r = await db(banco.gira_periodico, cid, str(datos.get("evento", "")), str(datos.get("logro", "")),
                     str(datos.get("nombre", "")), str(datos.get("club", "")))
        return JSONResponse(r, status_code=400 if "error" in r else 200)
    r = await db(banco.gira_logro, cid, str(datos.get("evento", "")), str(datos.get("logro", "")),
                 str(datos.get("nombre", "")), str(datos.get("club", "")))
    return JSONResponse(r, status_code=400 if "error" in r else 200)


async def gira_descuento(request):
    # Puntos que se pierden en un Desafío: se descuentan del ranking online (una sola vez por evento).
    cid, datos, error = await gira_auth(request)
    if error:
        return error
    try:
        puntos = int(datos.get("puntos", 0))
    except (TypeError, ValueError):
        puntos = 0
    r = await db(banco.gira_descuento, cid, str(datos.get("evento", "")), puntos, str(datos.get("motivo", "")),
                 str(datos.get("nombre", "")), str(datos.get("club", "")))
    return JSONResponse(r, status_code=400 if "error" in r else 200)


async def gira_sincronizar(request):
    cid, datos, error = await gira_auth(request)
    if error:
        return error
    regiones = datos.get("regiones") if isinstance(datos.get("regiones"), dict) else {}
    return JSONResponse(await db(banco.gira_sincronizar, cid, regiones,
                                 str(datos.get("nombre", "")), str(datos.get("club", ""))))


async def gira_mi_estado(request):
    cid, datos, error = await gira_auth(request)
    if error:
        return error
    return JSONResponse(await db(banco.gira_estado, cid))


async def gira_podio(request):
    # ?limite=10 pide el Top 10; sin parámetro (apps viejas) sigue devolviendo el podio de 3
    try:
        limite = int(request.query_params.get("limite", 3))
    except (TypeError, ValueError):
        limite = 3
    return JSONResponse(await db(banco.gira_podio, max(1, min(limite, 10))))


async def semanal_ranking(request):
    """Ranking semanal: top 10, mi puesto (si mandás id y token) y el podio de la semana pasada."""
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not cid or not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        cid = None
    return JSONResponse(await db(banco.semanal_ranking, cid, 10))


async def semanal_ia(request):
    """+1 punto semanal por ganarle a la IA (con tope diario)."""
    datos = await leer_json(request)
    cid = str(datos.get("id", "")).strip().upper()
    if not await db(banco.autenticar, cid, str(datos.get("token", ""))):
        return JSONResponse({"error": "Cuenta inválida."}, status_code=401)
    r = await db(banco.semanal_ia, cid, "IA:%s:%s" % (cid, str(datos.get("evento", ""))[:40]), ESPERA_TICKET)
    return JSONResponse(r, status_code=400 if "error" in r else 200)


async def lifespan(app):
    if not banco.pg:
        print("[truco] ATENCIÓN: sin DATABASE_URL se usa SQLite local: los saldos y cuentas "
              "se pierden cuando Render reinicia o duerme el servicio.")
    devueltas = await db(banco.devolver_abiertas)
    if devueltas:
        print(f"[truco] Se devolvieron {devueltas} apuestas que habían quedado abiertas.")
    yield


app = Starlette(
    routes=[
        Route("/", health),
        Route("/register", registrar, methods=["POST"]),
        Route("/wallet", billetera, methods=["POST"]),
        Route("/room/{code}", info_sala),
        Route("/amigos/latido", amigos_latido, methods=["POST"]),
        Route("/amigos/listar", amigos_listar, methods=["POST"]),
        Route("/amigos/buscar", amigos_buscar, methods=["POST"]),
        Route("/cuenta/perfil", cuenta_perfil, methods=["POST"]),
        Route("/amigos/pedir", amigos_pedir, methods=["POST"]),
        Route("/amigos/responder", amigos_responder, methods=["POST"]),
        Route("/amigos/eliminar", amigos_eliminar, methods=["POST"]),
        Route("/chat/enviar", chat_enviar, methods=["POST"]),
        Route("/chat/leer", chat_leer, methods=["POST"]),
        Route("/chat/invitar", chat_invitar, methods=["POST"]),
        Route("/chat/invitacion", chat_invitacion, methods=["POST"]),
        Route("/apuesta/abrir", apuesta_abrir, methods=["POST"]),
        Route("/apuesta/reclamar", apuesta_reclamar, methods=["POST"]),
        Route("/poderes/comprar", poderes_comprar, methods=["POST"]),
        Route("/gira/victoria", gira_victoria, methods=["POST"]),
        Route("/gira/logro", gira_logro, methods=["POST"]),
        Route("/gira/descuento", gira_descuento, methods=["POST"]),
        Route("/gira/sincronizar", gira_sincronizar, methods=["POST"]),
        Route("/gira/mi_estado", gira_mi_estado, methods=["POST"]),
        Route("/gira/podio", gira_podio),
        Route("/semanal/ranking", semanal_ranking, methods=["POST"]),
        Route("/semanal/ia", semanal_ia, methods=["POST"]),
        Route("/admin/credit", admin_recargar, methods=["POST"]),
        Route("/admin/account/{cid}", admin_cuenta),
        WebSocketRoute("/ws", websocket_endpoint),
        WebSocketRoute("/ws_torneo", torneo_endpoint),
    ],
    lifespan=lifespan,
)
