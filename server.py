"""Servidor de salas y retransmisión de eventos para Truco Cordobés.
Deploy gratuito en Render: Python + FastAPI + WebSockets.
"""
import asyncio
import json
import secrets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI(title="Truco Cordobes Online")
rooms = {}
waiting = []
rooms_lock = asyncio.Lock()


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


async def attach(room_code, name, ws):
    room = rooms.get(room_code)
    if room is None or len(room["players"]) >= 2:
        return None
    existing = next((p for p in room["players"] if p["ws"] is ws), None)
    if existing:
        return existing["slot"]
    slot = len(room["players"])
    room["players"].append({"name": name[:32] or "Jugador", "ws": ws, "slot": slot})
    await broadcast(room, {
        "type": "lobby", "room": room_code,
        "players": [p["name"] for p in room["players"]],
        "ready": len(room["players"]) == 2
    })
    if len(room["players"]) == 2:
        await broadcast(room, {
            "type": "match_found", "room": room_code,
            "players": [p["name"] for p in room["players"]],
            "message": "Rival conectado. La sala esta lista."
        })
    return slot


@app.get("/")
def health():
    return {"status": "ok", "service": "Truco Cordobes Online", "rooms": len(rooms)}


@app.websocket("/ws")
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
            room_code = code_room()
            rooms[room_code] = {"players": [], "lock": asyncio.Lock(), "seq": 0}
            slot = await attach(room_code, player_name, ws)
            await send(ws, {"type": "created", "room": room_code, "slot": slot,
                            "message": "Sala creada. Compartile el codigo a tu rival."})
        elif action == "join":
            room_code = str(req.get("room", "")).strip().upper()
            slot = await attach(room_code, player_name, ws)
            if slot is None:
                await send(ws, {"type": "error", "message": "Codigo inexistente o sala completa."})
                return
            await send(ws, {"type": "joined", "room": room_code, "slot": slot})
        elif action == "search":
            async with rooms_lock:
                while waiting and waiting[0].get("ws") is ws:
                    waiting.pop(0)
                opponent = None
                while waiting and opponent is None:
                    candidate = waiting.pop(0)
                    if candidate["ws"] is not ws and not candidate.get("matched"):
                        opponent = candidate
                if opponent:
                    room_code = code_room()
                    rooms[room_code] = {"players": [], "lock": asyncio.Lock(), "seq": 0}
                    opponent["matched"] = True
                    opponent["room"] = room_code
                else:
                    ticket = {"ws": ws, "name": player_name, "matched": False}
                    waiting.append(ticket)
            if opponent:
                room_code = opponent["room"]
                # Primer jugador conserva slot 0; el buscador queda slot 1.
                await attach(room_code, opponent["name"], opponent["ws"])
                slot = await attach(room_code, player_name, ws)
                await send(ws, {"type": "match_found", "room": room_code, "slot": slot,
                                "players": [opponent["name"], player_name]})
                await send(opponent["ws"], {"type": "match_found", "room": room_code, "slot": 0,
                                            "players": [opponent["name"], player_name]})
            else:
                await send(ws, {"type": "searching", "message": "Buscando rival; deja la conexion abierta."})
                while not ticket["matched"]:
                    await asyncio.sleep(0.25)
                room_code = ticket["room"]
                slot = 0
        else:
            await send(ws, {"type": "error", "message": "Accion desconocida. Usa create, join o search."})
            return

        # Persistent event relay. Include authoritative sender slot and sequence.
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("action") == "leave":
                break
            if msg.get("action") == "ping":
                await send(ws, {"type": "pong"})
                continue
            if msg.get("action") == "game_event" and room_code:
                room = rooms.get(room_code)
                if room is None:
                    continue
                async with room["lock"]:
                    room["seq"] += 1
                    envelope = {
                        "type": "game_event",
                        "room": room_code,
                        "from_slot": slot,
                        "seq": room["seq"],
                        "payload": msg.get("payload", {})
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
        if room_code and room_code in rooms:
            room = rooms[room_code]
            room["players"][:] = [p for p in room["players"] if p["ws"] is not ws]
            if room["players"]:
                await broadcast(room, {"type": "opponent_left", "message": "El rival se desconecto."})
            else:
                rooms.pop(room_code, None)
