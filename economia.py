"""Cuentas, monedas y apuestas retenidas (escrow) para Truco Cordobés.

Guarda todo en una base de datos:
  * DATABASE_URL = postgres://...  -> Postgres (Neon, Supabase, etc.)
  * sin DATABASE_URL               -> SQLite local (solo para probar; en Render
                                      gratis se borra en cada reinicio).

Garantías:
  * Los saldos nunca bajan de 0 (el débito es condicional y atómico).
  * Cada apuesta retenida queda registrada y se liquida UNA sola vez
    (se paga al ganador o se devuelve); si el servidor se reinicia con
    apuestas abiertas, se devuelven al arrancar.
  * Todo movimiento queda en la tabla `movimientos`.
"""
import hashlib
import hmac
import os
import secrets
import sqlite3
import threading
import time


# Apuestas contra la IA / campeonato: el premio lo fija el servidor, no el celular.
PREMIO_IA = 2  # gana la partida: cobra el doble de lo apostado
PREMIOS_TORNEO = [("clasificar", 1), ("octavos", 1), ("cuartos", 2), ("semifinal", 3), ("final", 5)]


class Banco:
    def __init__(self, url=None, ruta_sqlite="truco.db"):
        self.url = url if url is not None else os.environ.get("DATABASE_URL", "")
        self.pg = self.url.startswith(("postgres://", "postgresql://"))
        self.ruta_sqlite = ruta_sqlite
        self._mem = None
        if not self.pg and ruta_sqlite == ":memory:":
            self._mem = sqlite3.connect(":memory:", check_same_thread=False)
        self._lock = threading.Lock()  # SQLite: una escritura a la vez
        self._crear_tablas()

    # ---------- infraestructura ----------
    def _conectar(self):
        if self.pg:
            import psycopg  # solo hace falta con Postgres
            return psycopg.connect(self.url)
        if self._mem is not None:
            return self._mem
        con = sqlite3.connect(self.ruta_sqlite, timeout=15)
        return con

    def _sql(self, sql):
        return sql.replace("?", "%s") if self.pg else sql

    def _transaccion(self, trabajo):
        """Ejecuta trabajo(cur) en una transacción y devuelve su resultado."""
        with self._lock:
            con = self._conectar()
            try:
                cur = con.cursor()
                resultado = trabajo(lambda sql, args=(): cur.execute(self._sql(sql), args) or cur)
                con.commit()
                return resultado
            except Exception:
                con.rollback()
                raise
            finally:
                if self._mem is None:
                    con.close()

    def _crear_tablas(self):
        serial = "BIGSERIAL PRIMARY KEY" if self.pg else "INTEGER PRIMARY KEY AUTOINCREMENT"

        def trabajo(q):
            q("""CREATE TABLE IF NOT EXISTS cuentas (
                    id TEXT PRIMARY KEY, token_hash TEXT NOT NULL, nombre TEXT NOT NULL,
                    monedas BIGINT NOT NULL DEFAULT 0 CHECK (monedas >= 0), creada BIGINT NOT NULL)""")
            q(f"""CREATE TABLE IF NOT EXISTS movimientos (
                    id {serial}, cuenta TEXT NOT NULL, delta BIGINT NOT NULL,
                    motivo TEXT NOT NULL, ref TEXT, fecha BIGINT NOT NULL)""")
            q("""CREATE TABLE IF NOT EXISTS tickets (
                    id TEXT PRIMARY KEY, cuenta TEXT NOT NULL, tipo TEXT NOT NULL,
                    monto BIGINT NOT NULL, creado BIGINT NOT NULL, ultimo BIGINT NOT NULL,
                    etapas TEXT NOT NULL DEFAULT '', abierto INTEGER NOT NULL DEFAULT 1)""")
            q(f"""CREATE TABLE IF NOT EXISTS retenciones (
                    id {serial}, sala TEXT NOT NULL, cuenta TEXT NOT NULL,
                    monto BIGINT NOT NULL, abierta INTEGER NOT NULL DEFAULT 1, fecha BIGINT NOT NULL)""")
        self._transaccion(trabajo)

    @staticmethod
    def _hash_token(token):
        return hashlib.sha256(token.encode()).hexdigest()

    # ---------- cuentas ----------
    def crear_cuenta(self, nombre):
        nombre = (nombre or "Jugador").strip()[:32] or "Jugador"
        token = secrets.token_urlsafe(24)

        def trabajo(q):
            for _ in range(20):
                cid = secrets.token_hex(4).upper()
                if q("SELECT 1 FROM cuentas WHERE id = ?", (cid,)).fetchone() is None:
                    q("INSERT INTO cuentas (id, token_hash, nombre, monedas, creada) VALUES (?, ?, ?, 0, ?)",
                      (cid, self._hash_token(token), nombre, int(time.time())))
                    return cid
            raise RuntimeError("no se pudo generar un ID")
        return self._transaccion(trabajo), token

    def autenticar(self, cuenta_id, token):
        if not cuenta_id or not token:
            return False

        def trabajo(q):
            fila = q("SELECT token_hash FROM cuentas WHERE id = ?", (str(cuenta_id).upper(),)).fetchone()
            return fila[0] if fila else None
        guardado = self._transaccion(trabajo)
        return guardado is not None and hmac.compare_digest(guardado, self._hash_token(str(token)))

    def saldo(self, cuenta_id):
        def trabajo(q):
            fila = q("SELECT monedas FROM cuentas WHERE id = ?", (str(cuenta_id).upper(),)).fetchone()
            return int(fila[0]) if fila else None
        return self._transaccion(trabajo)

    def ajustar(self, cuenta_id, monto, motivo="admin", ref=None):
        """Recarga (monto > 0) o corrección (monto < 0). Devuelve el saldo nuevo o None."""
        monto = int(monto)
        cuenta_id = str(cuenta_id).upper()

        def trabajo(q):
            cur = q("UPDATE cuentas SET monedas = monedas + ? WHERE id = ? AND monedas + ? >= 0",
                    (monto, cuenta_id, monto))
            if cur.rowcount != 1:
                return None
            q("INSERT INTO movimientos (cuenta, delta, motivo, ref, fecha) VALUES (?, ?, ?, ?, ?)",
              (cuenta_id, monto, motivo, ref, int(time.time())))
            return int(q("SELECT monedas FROM cuentas WHERE id = ?", (cuenta_id,)).fetchone()[0])
        return self._transaccion(trabajo)

    def movimientos(self, cuenta_id, limite=20):
        def trabajo(q):
            filas = q("SELECT delta, motivo, ref, fecha FROM movimientos WHERE cuenta = ? ORDER BY id DESC LIMIT ?",
                      (str(cuenta_id).upper(), int(limite))).fetchall()
            return [{"delta": int(f[0]), "motivo": f[1], "ref": f[2], "fecha": int(f[3])} for f in filas]
        return self._transaccion(trabajo)

    # ---------- apuestas retenidas ----------
    def retener(self, cuenta_id, sala, monto):
        """Descuenta `monto` y lo deja retenido en `sala`. False si no alcanza el saldo."""
        monto = int(monto)
        cuenta_id = str(cuenta_id).upper()
        if monto <= 0:
            return False

        def trabajo(q):
            cur = q("UPDATE cuentas SET monedas = monedas - ? WHERE id = ? AND monedas >= ?",
                    (monto, cuenta_id, monto))
            if cur.rowcount != 1:
                return False
            now = int(time.time())
            q("INSERT INTO retenciones (sala, cuenta, monto, abierta, fecha) VALUES (?, ?, ?, 1, ?)",
              (sala, cuenta_id, monto, now))
            q("INSERT INTO movimientos (cuenta, delta, motivo, ref, fecha) VALUES (?, ?, 'apuesta', ?, ?)",
              (cuenta_id, -monto, sala, now))
            return True
        return self._transaccion(trabajo)

    def liquidar(self, sala, ganador=None):
        """Cierra las retenciones abiertas de la sala, una sola vez.

        Con ganador: él cobra todo el pozo. Sin ganador: cada uno recupera lo suyo.
        Devuelve {cuenta: delta_cobrado} (vacío si no había nada abierto).
        """
        ganador_pedido = str(ganador).upper() if ganador else None

        def trabajo(q):
            ganador = ganador_pedido
            filas = q("SELECT id, cuenta, monto FROM retenciones WHERE sala = ? AND abierta = 1",
                      (sala,)).fetchall()
            if not filas:
                return {}
            now = int(time.time())
            cuentas = {f[1] for f in filas}
            if ganador is not None and ganador not in cuentas:
                ganador = None  # un ganador que no apostó no puede cobrar: se devuelve
            pagos = {}
            if ganador:
                pagos[ganador] = sum(int(f[2]) for f in filas)
                motivo = "premio"
            else:
                for f in filas:
                    pagos[f[1]] = pagos.get(f[1], 0) + int(f[2])
                motivo = "devolucion"
            for cid, monto in pagos.items():
                q("UPDATE cuentas SET monedas = monedas + ? WHERE id = ?", (monto, cid))
                q("INSERT INTO movimientos (cuenta, delta, motivo, ref, fecha) VALUES (?, ?, ?, ?, ?)",
                  (cid, monto, motivo, sala, now))
            for f in filas:
                q("UPDATE retenciones SET abierta = 0 WHERE id = ?", (f[0],))
            return pagos
        return self._transaccion(trabajo)

    def devolver_abiertas(self):
        """Al arrancar: devuelve toda apuesta que quedó abierta por un reinicio."""
        def trabajo(q):
            return [f[0] for f in q("SELECT DISTINCT sala FROM retenciones WHERE abierta = 1").fetchall()]
        salas = self._transaccion(trabajo)
        for sala in salas:
            if sala.startswith("T-"):
                self._devolver_ticket(sala[2:])
            else:
                self.liquidar(sala, None)
        return len(salas)

    # ---------- apuestas contra la IA y campeonato (tickets) ----------
    def _devolver_ticket(self, ticket):
        def trabajo(q):
            fila = q("SELECT cuenta, monto, etapas, abierto FROM tickets WHERE id = ?", (ticket,)).fetchone()
            q("UPDATE tickets SET abierto = 0 WHERE id = ?", (ticket,))
            q("UPDATE retenciones SET abierta = 0 WHERE sala = ?", ("T-" + ticket,))
            if fila and fila[3] == 1 and not fila[2]:  # sin premios cobrados: se devuelve la apuesta
                q("UPDATE cuentas SET monedas = monedas + ? WHERE id = ?", (int(fila[1]), fila[0]))
                q("INSERT INTO movimientos (cuenta, delta, motivo, ref, fecha) VALUES (?, ?, 'devolucion', ?, ?)",
                  (fila[0], int(fila[1]), "T-" + ticket, int(time.time())))
        self._transaccion(trabajo)

    def abrir_ticket(self, cuenta_id, tipo, monto):
        """Retiene la apuesta y abre un ticket. Cierra (perdido) cualquier ticket abierto anterior."""
        cuenta_id = str(cuenta_id).upper()
        monto = int(monto)
        ticket = secrets.token_hex(6).upper()

        def trabajo(q):
            cur = q("UPDATE cuentas SET monedas = monedas - ? WHERE id = ? AND monedas >= ?",
                    (monto, cuenta_id, monto))
            if cur.rowcount != 1:
                return None
            now = int(time.time())
            for (previo,) in q("SELECT id FROM tickets WHERE cuenta = ? AND abierto = 1", (cuenta_id,)).fetchall():
                q("UPDATE tickets SET abierto = 0 WHERE id = ?", (previo,))
                q("UPDATE retenciones SET abierta = 0 WHERE sala = ?", ("T-" + previo,))
            q("INSERT INTO tickets (id, cuenta, tipo, monto, creado, ultimo, etapas, abierto) VALUES (?, ?, ?, ?, ?, ?, '', 1)",
              (ticket, cuenta_id, tipo, monto, now, now))
            q("INSERT INTO retenciones (sala, cuenta, monto, abierta, fecha) VALUES (?, ?, ?, 1, ?)",
              ("T-" + ticket, cuenta_id, monto, now))
            q("INSERT INTO movimientos (cuenta, delta, motivo, ref, fecha) VALUES (?, ?, ?, ?, ?)",
              (cuenta_id, -monto, "apuesta_" + tipo, ticket, now))
            return int(q("SELECT monedas FROM cuentas WHERE id = ?", (cuenta_id,)).fetchone()[0])
        saldo = self._transaccion(trabajo)
        return (ticket, saldo) if saldo is not None else None

    def reclamar_ticket(self, ticket, cuenta_id, evento, minimo_segundos=60):
        """Aplica un resultado informado por el celular. El monto del premio lo decide el servidor."""
        cuenta_id = str(cuenta_id).upper()
        ticket = str(ticket).upper()

        def trabajo(q):
            fila = q("SELECT cuenta, tipo, monto, ultimo, etapas, abierto FROM tickets WHERE id = ?",
                     (ticket,)).fetchone()
            if not fila or fila[0] != cuenta_id or fila[5] != 1:
                return {"error": "Apuesta inexistente o ya cerrada."}
            _, tipo, monto, ultimo, etapas, _ = fila
            monto, ultimo = int(monto), int(ultimo)
            now = int(time.time())
            hechas = [e for e in etapas.split(",") if e]

            def cerrar():
                q("UPDATE tickets SET abierto = 0 WHERE id = ?", (ticket,))
                q("UPDATE retenciones SET abierta = 0 WHERE sala = ?", ("T-" + ticket,))

            def pagar(importe, motivo):
                q("UPDATE cuentas SET monedas = monedas + ? WHERE id = ?", (importe, cuenta_id))
                q("INSERT INTO movimientos (cuenta, delta, motivo, ref, fecha) VALUES (?, ?, ?, ?, ?)",
                  (cuenta_id, importe, motivo, ticket, now))

            pago, cerrado = 0, False
            if evento == "perder":
                cerrar()
                cerrado = True
            elif tipo == "ia" and evento == "ganar":
                if now - ultimo < minimo_segundos:
                    return {"error": "Resultado demasiado rápido para ser válido."}
                pago = PREMIO_IA * monto
                pagar(pago, "premio_ia")
                cerrar()
                cerrado = True
            elif tipo == "torneo":
                orden = [e for e, _ in PREMIOS_TORNEO]
                if len(hechas) >= len(orden) or evento != orden[len(hechas)]:
                    return {"error": "Etapa fuera de orden."}
                if now - ultimo < minimo_segundos:
                    return {"error": "Resultado demasiado rápido para ser válido."}
                pago = dict(PREMIOS_TORNEO)[evento] * monto
                pagar(pago, "premio_torneo")
                q("UPDATE tickets SET ultimo = ?, etapas = ? WHERE id = ?",
                  (now, ",".join(hechas + [evento]), ticket))
                if evento == orden[-1]:
                    cerrar()
                    cerrado = True
            else:
                return {"error": "Evento inválido."}
            saldo = int(q("SELECT monedas FROM cuentas WHERE id = ?", (cuenta_id,)).fetchone()[0])
            return {"coins": saldo, "pago": pago, "cerrado": cerrado}
        return self._transaccion(trabajo)
