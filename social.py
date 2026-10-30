"""Amigos de Truco Cordobés (Etapa 1: amigos y estado en línea. Etapa 2: chats directos).

Usa la misma base que `economia.py` (Postgres o SQLite) a través de `banco._transaccion`.
Las amistades se guardan una sola vez por pareja, con los IDs ordenados (a < b).
El estado "en línea" no se guarda: se deduce de la última señal de vida (`latido`) del celular.
"""
import time

MAX_RELACIONES = 200      # amigos + solicitudes por cuenta
MAX_ENVIADAS = 30         # solicitudes pendientes enviadas a la vez
EN_LINEA_SEG = 45         # sin señal de vida en 45 s = desconectado
MAX_TEXTO = 500           # largo máximo de un mensaje
GUARDAR_MENSAJES = 200    # mensajes que se conservan por conversación
RAFAGA_MAX = 8            # mensajes permitidos...
RAFAGA_SEG = 10           # ...cada 10 segundos por cuenta


class Social:
    def __init__(self, banco):
        self.b = banco
        self.visto = {}   # cuenta -> última señal de vida (time.time())
        self.envios = {}  # cuenta -> horas de sus últimos mensajes (límite de ráfaga)
        self._crear_tablas()

    def _crear_tablas(self):
        def trabajo(q):
            q("""CREATE TABLE IF NOT EXISTS amistades (
                    a TEXT NOT NULL, b TEXT NOT NULL, pidio TEXT NOT NULL,
                    aceptada INTEGER NOT NULL DEFAULT 0, fecha BIGINT NOT NULL,
                    PRIMARY KEY (a, b))""")
            q("CREATE INDEX IF NOT EXISTS ix_amistades_b ON amistades (b)")
            serial = "BIGSERIAL PRIMARY KEY" if self.b.pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
            q(f"""CREATE TABLE IF NOT EXISTS mensajes (
                    id {serial}, de TEXT NOT NULL, para TEXT NOT NULL, texto TEXT NOT NULL,
                    fecha BIGINT NOT NULL, leido INTEGER NOT NULL DEFAULT 0)""")
            q("CREATE INDEX IF NOT EXISTS ix_mensajes_par ON mensajes (para, de, leido)")
        self.b._transaccion(trabajo)

    # ---------- presencia ----------
    def latido(self, cid):
        self.visto[cid] = time.time()
        if len(self.visto) > 5000:   # limpieza simple de cuentas que hace rato no aparecen
            corte = time.time() - 3600
            for k in [k for k, t in self.visto.items() if t < corte]:
                self.visto.pop(k, None)

    def en_linea(self, cid):
        return time.time() - self.visto.get(cid, 0) < EN_LINEA_SEG

    @staticmethod
    def _par(x, y):
        return (x, y) if x < y else (y, x)

    @staticmethod
    def _id(valor):
        return str(valor or "").strip().upper()[:16]

    # ---------- acciones ----------
    def pedir(self, cid, otro):
        otro = self._id(otro)
        if not otro:
            return {"error": "Escribí el ID de tu amigo."}
        if otro == cid:
            return {"error": "Ese es tu propio ID."}
        a, b = self._par(cid, otro)

        def trabajo(q):
            existe = q("SELECT nombre FROM cuentas WHERE id = ?", (otro,)).fetchone()
            if existe is None:
                return {"error": "No existe ninguna cuenta con ese ID."}
            fila = q("SELECT pidio, aceptada FROM amistades WHERE a = ? AND b = ?", (a, b)).fetchone()
            if fila is not None:
                pidio, aceptada = fila[0], int(fila[1])
                if aceptada:
                    return {"error": "Ya son amigos."}
                if pidio == cid:
                    return {"error": "Ya le mandaste una solicitud."}
                # El otro ya te había pedido amistad: se acepta directo.
                q("UPDATE amistades SET aceptada = 1 WHERE a = ? AND b = ?", (a, b))
                return {"ok": True, "estado": "aceptada", "nombre": existe[0]}
            total = q("SELECT COUNT(*) FROM amistades WHERE a = ? OR b = ?", (cid, cid)).fetchone()[0]
            if int(total) >= MAX_RELACIONES:
                return {"error": "Llegaste al máximo de amigos y solicitudes."}
            enviadas = q("SELECT COUNT(*) FROM amistades WHERE pidio = ? AND aceptada = 0", (cid,)).fetchone()[0]
            if int(enviadas) >= MAX_ENVIADAS:
                return {"error": "Tenés demasiadas solicitudes pendientes."}
            q("INSERT INTO amistades (a, b, pidio, aceptada, fecha) VALUES (?, ?, ?, 0, ?)",
              (a, b, cid, int(time.time())))
            return {"ok": True, "estado": "pendiente", "nombre": existe[0]}
        return self.b._transaccion(trabajo)

    def responder(self, cid, otro, aceptar):
        otro = self._id(otro)
        a, b = self._par(cid, otro)

        def trabajo(q):
            fila = q("SELECT pidio, aceptada FROM amistades WHERE a = ? AND b = ?", (a, b)).fetchone()
            if fila is None or int(fila[1]) or fila[0] != otro:
                return {"error": "Esa solicitud ya no existe."}
            if aceptar:
                q("UPDATE amistades SET aceptada = 1 WHERE a = ? AND b = ?", (a, b))
                return {"ok": True, "estado": "aceptada"}
            q("DELETE FROM amistades WHERE a = ? AND b = ?", (a, b))
            return {"ok": True, "estado": "rechazada"}
        return self.b._transaccion(trabajo)

    def eliminar(self, cid, otro):
        """Borra un amigo o cancela una solicitud (enviada o recibida)."""
        otro = self._id(otro)
        a, b = self._par(cid, otro)

        def trabajo(q):
            q("DELETE FROM amistades WHERE a = ? AND b = ?", (a, b))
            q("DELETE FROM mensajes WHERE (de = ? AND para = ?) OR (de = ? AND para = ?)", (cid, otro, otro, cid))
            return {"ok": True}
        return self.b._transaccion(trabajo)

    def son_amigos(self, cid, otro):
        a, b = self._par(cid, self._id(otro))

        def trabajo(q):
            fila = q("SELECT aceptada FROM amistades WHERE a = ? AND b = ?", (a, b)).fetchone()
            return bool(fila and int(fila[0]))
        return self.b._transaccion(trabajo)

    def listar(self, cid):
        def trabajo(q):
            filas = q("SELECT a, b, pidio, aceptada FROM amistades WHERE a = ? OR b = ? ORDER BY fecha DESC",
                      (cid, cid)).fetchall()
            sin_leer = {de: int(n) for de, n in q(
                "SELECT de, COUNT(*) FROM mensajes WHERE para = ? AND leido = 0 GROUP BY de", (cid,)).fetchall()}
            nombres = {}
            for a, b, _p, _ac in filas:
                otro = b if a == cid else a
                fila = q("SELECT nombre FROM cuentas WHERE id = ?", (otro,)).fetchone()
                nombres[otro] = fila[0] if fila else "Jugador"
            return filas, nombres, sin_leer
        filas, nombres, sin_leer = self.b._transaccion(trabajo)
        amigos, recibidas, enviadas = [], [], []
        for a, b, pidio, aceptada in filas:
            otro = b if a == cid else a
            item = {"id": otro, "nombre": nombres.get(otro, "Jugador")}
            if int(aceptada):
                item["en_linea"] = self.en_linea(otro)
                item["sin_leer"] = sin_leer.get(otro, 0)
                amigos.append(item)
            elif pidio == cid:
                enviadas.append(item)
            else:
                recibidas.append(item)
        # En línea primero, después por nombre.
        amigos.sort(key=lambda x: (not x["en_linea"], x["nombre"].lower()))
        return {"amigos": amigos, "recibidas": recibidas, "enviadas": enviadas}

    # ---------- chats directos ----------
    def total_sin_leer(self, cid):
        def trabajo(q):
            return int(q("SELECT COUNT(*) FROM mensajes WHERE para = ? AND leido = 0", (cid,)).fetchone()[0])
        return self.b._transaccion(trabajo)

    def enviar(self, cid, otro, texto):
        otro = self._id(otro)
        texto = " ".join(str(texto or "").split())[:MAX_TEXTO]   # sin saltos ni espacios de más
        if not texto:
            return {"error": "Escribí un mensaje."}
        ahora = time.time()
        recientes = [t for t in self.envios.get(cid, []) if ahora - t < RAFAGA_SEG]
        if len(recientes) >= RAFAGA_MAX:
            self.envios[cid] = recientes
            return {"error": "Estás mandando mensajes muy rápido."}
        a, b = self._par(cid, otro)

        def trabajo(q):
            fila = q("SELECT aceptada FROM amistades WHERE a = ? AND b = ?", (a, b)).fetchone()
            if not fila or not int(fila[0]):
                return {"error": "Solo podés chatear con tus amigos."}
            q("INSERT INTO mensajes (de, para, texto, fecha, leido) VALUES (?, ?, ?, ?, 0)",
              (cid, otro, texto, int(ahora)))
            nuevo = q("SELECT id FROM mensajes WHERE de = ? AND para = ? ORDER BY id DESC LIMIT 1",
                      (cid, otro)).fetchone()[0]
            # Conserva solo los últimos GUARDAR_MENSAJES de la conversación.
            corte = q("SELECT id FROM mensajes WHERE (de = ? AND para = ?) OR (de = ? AND para = ?) "
                      "ORDER BY id DESC LIMIT 1 OFFSET ?", (cid, otro, otro, cid, GUARDAR_MENSAJES)).fetchone()
            if corte:
                q("DELETE FROM mensajes WHERE ((de = ? AND para = ?) OR (de = ? AND para = ?)) AND id <= ?",
                  (cid, otro, otro, cid, int(corte[0])))
            return {"ok": True, "id": int(nuevo)}
        r = self.b._transaccion(trabajo)
        if r.get("ok"):
            recientes.append(ahora)
            self.envios[cid] = recientes
        return r

    def leer(self, cid, otro, desde=0, limite=60):
        """Mensajes de la conversación con id > desde (los últimos `limite` si desde = 0).
        Los que te mandó el otro quedan marcados como leídos."""
        otro = self._id(otro)
        try:
            desde = max(0, int(desde))
        except (TypeError, ValueError):
            desde = 0
        a, b = self._par(cid, otro)

        def trabajo(q):
            fila = q("SELECT aceptada FROM amistades WHERE a = ? AND b = ?", (a, b)).fetchone()
            if not fila or not int(fila[0]):
                return {"error": "Solo podés chatear con tus amigos."}
            filas = q("SELECT id, de, texto, fecha FROM mensajes WHERE id > ? AND "
                      "((de = ? AND para = ?) OR (de = ? AND para = ?)) ORDER BY id DESC LIMIT ?",
                      (desde, cid, otro, otro, cid, int(limite))).fetchall()
            filas.reverse()
            if filas:
                q("UPDATE mensajes SET leido = 1 WHERE para = ? AND de = ? AND leido = 0", (cid, otro))
            return {"mensajes": [{"id": int(i), "mio": de == cid, "texto": t, "fecha": int(f)}
                                 for i, de, t, f in filas]}
        return self.b._transaccion(trabajo)
