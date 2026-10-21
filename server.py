"""Servidor de salas online para Truco Cordobes (MVP de lobby).
Deploy gratuito en Render: Python + FastAPI + WebSockets.
Esta primera entrega administra salas, invitaciones y matchmaking; no arbitra aun las bazas/apuestas.
"""
import asyncio, json, secrets, string
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI(title="Truco Cordobes Online")
rooms = {}      # codigo -> {players:[{name,ws}], lock:asyncio.Lock}
waiting = []    # sockets en busqueda rapida


def code_room():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(5))
        if code not in rooms:
            return code

async def send(ws, payload):
    try:
        await ws.send_text(json.dumps(payload, ensure_ascii=False))
        return True
    except Exception:
        return False

async def broadcast(room, payload):
    dead = []
    for p in list(room["players"]):
        if not await send(p["ws"], payload):
            dead.append(p)
    for p in dead:
        if p in room["players"]:
            room["players"].remove(p)

async def attach(room_code, name, ws):
    room = rooms.get(room_code)
    if room is None:
        return False
    if len(room["players"]) >= 2:
        return False
    if any(p["ws"] is ws for p in room["players"]):
        return True
    room["players"].append({"name": name[:32] or "Jugador", "ws": ws})
    await broadcast(room, {"type":"lobby", "room":room_code,
                           "players":[p["name"] for p in room["players"]],
                           "ready":len(room["players"]) == 2})
    if len(room["players"]) == 2:
        await broadcast(room, {"type":"match_found", "room":room_code,
                               "players":[p["name"] for p in room["players"]],
                               "message":"Rival conectado. La sala esta lista."})
    return True

@app.get("/")
def health():
    return {"status":"ok", "service":"Truco Cordobes Online", "rooms":len(rooms)}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    room_code = None
    player_name = "Jugador"
    try:
        raw = await ws.receive_text()
        try:
            req = json.loads(raw)
        except Exception:
            await send(ws, {"type":"error", "message":"Formato de mensaje invalido."})
            return
        action = req.get("action")
        player_name = str(req.get("name", "Jugador"))[:32].strip() or "Jugador"

        if action == "create":
            room_code = code_room()
            rooms[room_code] = {"players":[], "lock":asyncio.Lock()}
            await attach(room_code, player_name, ws)
            await send(ws, {"type":"created", "room":room_code,
                           "message":"Sala creada. Compartile el codigo a tu rival."})
        elif action == "join":
            room_code = str(req.get("room", "")).strip().upper()
            if not await attach(room_code, player_name, ws):
                await send(ws, {"type":"error", "message":"Codigo inexistente o sala completa."})
                return
            await send(ws, {"type":"joined", "room":room_code})
        elif action == "search":
            # Toma al primer rival conectado que siga esperando.
            opponent = None
            while waiting and opponent is None:
                candidate = waiting.pop(0)
                if candidate.get("ws") is not ws and not candidate.get("matched"):
                    opponent = candidate
            if opponent:
                room_code = code_room()
                rooms[room_code] = {"players":[], "lock":asyncio.Lock()}
                opponent["matched"] = True
                opponent["room"] = room_code
                await attach(room_code, opponent["name"], opponent["ws"])
                await attach(room_code, player_name, ws)
                await send(ws, {"type":"match_found", "room":room_code,
                               "players":[opponent["name"], player_name]})
                await send(opponent["ws"], {"type":"match_found", "room":room_code,
                                              "players":[opponent["name"], player_name]})
            else:
                ticket = {"ws":ws, "name":player_name, "matched":False}
                waiting.append(ticket)
                await send(ws, {"type":"searching", "message":"Buscando rival; deja la conexion abierta."})
                # Espera a que el socket se cierre o se empareje.
                while not ticket["matched"]:
                    await asyncio.sleep(0.5)
        else:
            await send(ws, {"type":"error", "message":"Accion desconocida. Usa create, join o search."})
            return

        # Mantener el canal abierto y retransmitir eventos de lobby.
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("action") == "leave":
                break
            if room_code and msg.get("action") == "ping":
                await send(ws, {"type":"pong"})
            # Canal de eventos listo para la siguiente etapa de juego sincronizado.
            elif room_code and msg.get("action") == "game_event":
                room = rooms.get(room_code)
                if room:
                    await broadcast(room, {"type":"game_event", "from":player_name,
                                           "payload":msg.get("payload", {})})
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try: await send(ws, {"type":"error", "message":str(exc)[:160]})
        except Exception: pass
    finally:
        waiting[:] = [t for t in waiting if t.get("ws") is not ws]
        if room_code and room_code in rooms:
            room = rooms[room_code]
            room["players"][:] = [p for p in room["players"] if p["ws"] is not ws]
            if room["players"]:
                await broadcast(room, {"type":"opponent_left", "message":"El rival se desconecto."})
            else:
                rooms.pop(room_code, None)
