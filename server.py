"""Servidor de salas, retransmisión de eventos y monedas para Truco Cordobés.

Corre con:  uvicorn server:app --host 0.0.0.0 --port $PORT
Variables de entorno:
  DATABASE_URL  Postgres (postgresql://...). Sin ella usa SQLite local (se pierde en Render gratis).
  ADMIN_KEY     Clave para recargar monedas (/admin/...). Sin ella las rutas admin quedan apagadas.
  APUESTA_MIN / APUESTA_MAX   Límites de apuesta (por defecto 10 y 100000).

Monedas: el saldo vive en la base. Las apuestas se retienen al entrar a la sala y
se liquidan una sola vez: gana quien llegó a 30 (si los dos celulares informan lo
mismo), abandona = pierde, y si los reportes no coinciden se devuelven las apuestas.
"""
import asyncio
import contextlib
import hmac
import json
import os
import secrets
import time

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from economia import Banco
from social import Social

banco = Banco()


def _sala_espera(codigo):
    """True mientras la sala existe, no empezó y espera a un segundo jugador."""
    room = rooms.get(codigo)
    return room is not None and not room["started"] and len(room["players"]) == 1


social = Social(banco, sala_viva=_sala_espera)
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
APUESTA_MIN = int(os.environ.get("APUESTA_MIN", "10"))
APUESTA_MAX = int(os.environ.get("APUESTA_MAX", "100000"))
ESPERA_REPORTE = float(os.environ.get("ESPERA_REPORTE", "20"))  # segundos
ESPERA_TICKET = int(os.environ.get("ESPERA_TICKET", "60"))  # mínimo entre resultados contra la IA

rooms = {}
waiting = []
rooms_lock = asyncio.Lock()
registros_por_ip = {}


async def db(funcion, *args):
    return await asyncio.to_thread(funcion, *args)


def code_room():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(5))
        if code not in rooms:
            return code


def nueva_sala(apuesta=0):
    return {"players": [], "lock": asyncio.Lock(), "seq": 0, "bet": int(apuesta),
            "reservas": 0, "started": False, "settled": True,
            "reports": {}, "accounts": {}, "timer": None}


async def send(ws, payload):
    try:
        await ws.send_text(json.dumps(payload, ensure_ascii=False))
        return True
    except Exception:
        return False


async def broadcast(room, payload, exclude=None):
    dead = []
    for p in list(room["players"]):
        if p["ws"] is exclude:
            continue
        if not await send(p["ws"], payload):
            dead.append(p)
    for p in dead:
        if p in room["players"]:
            room["players"].remove(p)


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


async def recibir_resultado(room_code, room, slot, ganador):
    if room["bet"] <= 0 or room["settled"] or ganador not in (0, 1) or slot not in (0, 1):
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
    room["players"][:] = [p for p in room["players"] if p["ws"] is not ws]
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


# ------------------------------------------------------------------- HTTP
async def health(request):
    return JSONResponse({"status": "ok", "service": "Truco Cordobes Online", "rooms": len(rooms),
                         "db": banco.tipo, "persistente": banco.pg,
                         "database_url_cargada": banco.url_cargada})


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
    cid, token = await db(banco.crear_cuenta, str(datos.get("name", "Jugador")), datos.get("token"))
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
    return JSONResponse(await db(social.listar, cid))


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
    room = rooms.get(request.path_params["code"].strip().upper())
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
            rooms[room_code]["duenio"] = cuenta or await autenticar(req)   # dueño de la sala (solo para invitaciones)
            if bet > 0 and not await db(banco.retener, cuenta, room_code, bet):
                rooms.pop(room_code, None)
                room_code = None
                await send(ws, {"type": "error", "message": "Saldo insuficiente para esa apuesta."})
                return
            slot = await attach(room_code, player_name, ws, cuenta)
            await send(ws, {"type": "created", "room": room_code, "slot": slot, "bet": bet,
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
            slot = await attach(room_code, player_name, ws, cuenta)
            if slot is None:
                if cuenta:
                    await db(banco.liquidar, room_code, None)
                room_code = None
                await send(ws, {"type": "error", "message": "Codigo inexistente o sala completa."})
                return
            await send(ws, {"type": "joined", "room": room_code, "slot": slot, "bet": room["bet"]})
            await enviar_saldo(ws, cuenta)
        elif action == "search":
            validada = await validar_apuesta(ws, req)
            if validada is None:
                return
            bet, cuenta = validada
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
                                "players": nombres, "bet": bet})
                await send(opponent["ws"], {"type": "match_found", "room": room_code, "slot": 0,
                                            "players": nombres, "bet": bet})
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
        else:
            await send(ws, {"type": "error", "message": "Accion desconocida. Usa create, join o search."})
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
                break
            if accion == "ping":
                await send(ws, {"type": "pong"})
                continue
            room = rooms.get(room_code) if room_code else None
            if room is None:
                continue
            if accion == "result":
                await recibir_resultado(room_code, room, slot, msg.get("winner_slot"))
                continue
            if accion == "game_event":
                payload = msg.get("payload", {})
                evento = payload.get("event") if isinstance(payload, dict) else None
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
            await cerrar_jugador(room_code, rooms[room_code], slot, ws)


@contextlib.asynccontextmanager
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
        Route("/amigos/pedir", amigos_pedir, methods=["POST"]),
        Route("/amigos/responder", amigos_responder, methods=["POST"]),
        Route("/amigos/eliminar", amigos_eliminar, methods=["POST"]),
        Route("/chat/enviar", chat_enviar, methods=["POST"]),
        Route("/chat/leer", chat_leer, methods=["POST"]),
        Route("/chat/invitar", chat_invitar, methods=["POST"]),
        Route("/chat/invitacion", chat_invitacion, methods=["POST"]),
        Route("/apuesta/abrir", apuesta_abrir, methods=["POST"]),
        Route("/apuesta/reclamar", apuesta_reclamar, methods=["POST"]),
        Route("/gira/victoria", gira_victoria, methods=["POST"]),
        Route("/gira/sincronizar", gira_sincronizar, methods=["POST"]),
        Route("/gira/mi_estado", gira_mi_estado, methods=["POST"]),
        Route("/gira/podio", gira_podio),
        Route("/admin/credit", admin_recargar, methods=["POST"]),
        Route("/admin/account/{cid}", admin_cuenta),
        WebSocketRoute("/ws", websocket_endpoint),
    ],
    lifespan=lifespan,
)
