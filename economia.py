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
import unicodedata


# Apuestas contra la IA / campeonato: el premio lo fija el servidor, no el celular.
PREMIO_IA = 2  # gana la partida: cobra el doble de lo apostado
PREMIOS_TORNEO = [("clasificar", 1), ("octavos", 1), ("cuartos", 2), ("semifinal", 3), ("final", 5)]

# Gira Nacional: puntos de cada región (espejo del cliente). Premio por victoria = pt // 2.
GIRA_PT = {
    "arg.buenos_aires": 400, "arg.patagonia": 600, "arg.cuyo": 800, "arg.mesopotamia": 1000,
    "arg.norte": 1200, "arg.malvinas": 1600,
    "chi.norte_grande": 1000, "chi.norte_chico": 1200, "chi.zona_central": 1400,
    "chi.sur": 1600, "chi.austral": 2000,
    "uru.norte": 1600, "uru.centro": 1800, "uru.costa": 2400,
}
GIRA_VICTORIAS_REGION = 1  # igual que el juego: con un campeonato ganado se conquista la región

# Gira Nacional: puntos de cada logro personal (espejo del cliente). Se cobran UNA sola vez por cuenta.
GIRA_LOGROS_PT = {
    "primera_victoria": 50, "ganador10": 100, "imparable": 200,
    "primer_destino": 50, "explorador": 150, "cruce_internacional": 250,
}

# Jugadores simulados para que el podio y el Top 10 no estén vacíos.
# (nombre, club, puntos al arrancar, puntos que suman por día, regiones conquistadas)
# Los puntos crecen solos con los días (hasta GIRA_BOTS_TOPE sobre el inicial) para que parezcan jugadores activos.
# Para apagarlos: variable de entorno TRUCO_BOTS=0 en Render (se borran del ranking al reiniciar).
GIRA_BOTS = [
    ("Facu_Cba", "Talleres de Córdoba", 4300, 70, 9),
    ("ElTanoRC", "Rosario Central", 3800, 60, 8),
    ("Maxi Truco", "Boca Juniors", 3300, 80, 7),
    ("Lucho_10", "River Plate", 2900, 55, 6),
    ("NachoRacing", "Racing Club", 2500, 65, 6),
    ("Gringo_Bel", "Belgrano de Córdoba", 2100, 50, 5),
    ("PabloVelez", "Vélez Sarsfield", 1700, 45, 4),
    ("Cande_Pincha", "Estudiantes de La Plata", 1300, 40, 3),
    ("Mati_Lanús", "Lanús", 900, 35, 2),
    ("Rodri_Funes", "Newell's Old Boys", 600, 30, 1),
]
GIRA_BOTS_TOPE = 1500
GIRA_ESPERA = 60  # segundos mínimos entre dos victorias informadas


class Banco:
    def __init__(self, url=None, ruta_sqlite="truco.db"):
        crudo = url if url is not None else os.environ.get("DATABASE_URL", "")
        self.url_cargada = bool(str(crudo).strip())
        self.url = str(crudo).strip().strip("\"'").strip()  # tolera comillas/espacios al pegar
        self.pg = self.url.startswith(("postgres://", "postgresql://"))
        self.ruta_sqlite = ruta_sqlite
        self._mem = None
        if not self.pg and ruta_sqlite == ":memory:":
            self._mem = sqlite3.connect(":memory:", check_same_thread=False)
        self._lock = threading.Lock()  # SQLite: una escritura a la vez
        self._crear_tablas()
        if os.environ.get("TRUCO_BOTS", "1").strip() != "0":
            self.gira_sembrar_bots()
        else:
            self.gira_quitar_bots()
        self.gira_recalcular_conquistadas()

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
            q("""CREATE TABLE IF NOT EXISTS gira (
                    cuenta TEXT PRIMARY KEY, nombre TEXT NOT NULL, club TEXT NOT NULL DEFAULT '',
                    puntos BIGINT NOT NULL DEFAULT 0, torneos INTEGER NOT NULL DEFAULT 0,
                    regiones TEXT NOT NULL DEFAULT '', conquistadas INTEGER NOT NULL DEFAULT 0,
                    ultimo BIGINT NOT NULL DEFAULT 0, desde BIGINT NOT NULL DEFAULT 0)""")
            q("""CREATE TABLE IF NOT EXISTS gira_eventos (
                    id TEXT PRIMARY KEY, cuenta TEXT NOT NULL, fecha BIGINT NOT NULL)""")
            q("CREATE UNIQUE INDEX IF NOT EXISTS ix_cuentas_token ON cuentas (token_hash)")
        self._transaccion(trabajo)
        self._asegurar_columna_club()

    def _asegurar_columna_club(self):
        """Cuentas viejas no tenían club: se agrega la columna sin perder nada."""
        def trabajo(q):
            if self.pg:
                q("ALTER TABLE cuentas ADD COLUMN IF NOT EXISTS club TEXT NOT NULL DEFAULT ''")
            else:
                columnas = [f[1] for f in q("PRAGMA table_info(cuentas)").fetchall()]
                if "club" not in columnas:
                    q("ALTER TABLE cuentas ADD COLUMN club TEXT NOT NULL DEFAULT ''")
        self._transaccion(trabajo)

    @property
    def tipo(self):
        return "postgres" if self.pg else "sqlite (se borra al reiniciar el servidor)"

    @staticmethod
    def _hash_token(token):
        return hashlib.sha256(token.encode()).hexdigest()

    # ---------- cuentas ----------
    def crear_cuenta(self, nombre, token=None, club=""):
        """Crea una cuenta. Si el celular manda su propio token y ya existe, devuelve esa misma
        cuenta (así un reintento por mala conexión no crea una segunda cuenta)."""
        nombre = (nombre or "Jugador").strip()[:32] or "Jugador"
        club = str(club or "").strip()[:40]
        token = str(token) if token is not None else None
        if token is not None and not 20 <= len(token) <= 100:
            token = None
        propio = token is not None
        token = token or secrets.token_urlsafe(24)
        th = self._hash_token(token)

        def trabajo(q):
            if propio:
                fila = q("SELECT id FROM cuentas WHERE token_hash = ?", (th,)).fetchone()
                if fila:
                    if club:
                        q("UPDATE cuentas SET club = ? WHERE id = ?", (club, fila[0]))
                    return fila[0]
            for _ in range(20):
                cid = secrets.token_hex(4).upper()
                if q("SELECT 1 FROM cuentas WHERE id = ?", (cid,)).fetchone() is None:
                    q("INSERT INTO cuentas (id, token_hash, nombre, club, monedas, creada) VALUES (?, ?, ?, ?, 0, ?)",
                      (cid, th, nombre, club, int(time.time())))
                    return cid
            raise RuntimeError("no se pudo generar un ID")
        return self._transaccion(trabajo), token

    # ---------- perfil público y búsqueda de jugadores ----------
    @staticmethod
    def _norm(texto):
        """Minúsculas y sin tildes, para buscar 'Martín' escribiendo 'martin'."""
        t = unicodedata.normalize("NFD", str(texto or "").lower())
        return "".join(c for c in t if unicodedata.category(c) != "Mn").strip()

    def actualizar_perfil(self, cuenta_id, nombre=None, club=None):
        """Guarda nombre y club visibles para los amigos y la búsqueda."""
        cuenta_id = str(cuenta_id).upper()
        nombre = (str(nombre).strip()[:32] if nombre else "")
        club = (str(club).strip()[:40] if club else "")

        def trabajo(q):
            if nombre:
                q("UPDATE cuentas SET nombre = ? WHERE id = ?", (nombre, cuenta_id))
            if club:
                q("UPDATE cuentas SET club = ? WHERE id = ?", (club, cuenta_id))
        self._transaccion(trabajo)
        return {"ok": True}

    def clubes_de(self, ids):
        """{id: club} para una lista de cuentas (la lista de amigos muestra el escudo)."""
        ids = [str(i).upper() for i in ids if i]
        if not ids:
            return {}

        def trabajo(q):
            filas = q("SELECT id, club FROM cuentas WHERE id IN (%s)" % ",".join("?" * len(ids)),
                      tuple(ids)).fetchall()
            return {f[0]: f[1] or "" for f in filas}
        return self._transaccion(trabajo)

    def buscar_cuentas(self, excluir, palabras, club="", limite=20):
        """Busca jugadores por nombre/apellido (cada palabra debe empezar alguna palabra del nombre) y club."""
        excluir = str(excluir).upper()
        palabras = [self._norm(p) for p in palabras if self._norm(p)]
        club_n = self._norm(club)

        def trabajo(q):
            return q("SELECT id, nombre, club FROM cuentas WHERE id <> ? ORDER BY creada DESC LIMIT 5000",
                     (excluir,)).fetchall()
        encontrados = []
        for cid, nombre, club_c in self._transaccion(trabajo):
            tokens = self._norm(nombre).split()
            if any(not any(t.startswith(p) for t in tokens) for p in palabras):
                continue
            if club_n and self._norm(club_c) != club_n:
                continue
            encontrados.append({"id": cid, "nombre": nombre, "club": club_c or ""})
            if len(encontrados) >= limite:
                break
        return encontrados

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
        salas = [x for x in self._transaccion(trabajo) if not x.startswith("T-")]
        # Los tickets (IA / copa) NO se tocan: viven en la base de datos y el reinicio del servidor
        # (Render gratis se duerme a los 15 min) no debe cerrarle la apuesta a quien está jugando.
        for sala in salas:
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


    # ---------- Gira Nacional: ranking online ----------
    @staticmethod
    def _gira_leer(texto):
        out = {}
        for par in (texto or "").split(","):
            if ":" in par:
                clave, v = par.rsplit(":", 1)
                try: out[clave] = int(v)
                except ValueError: pass
        return out

    @staticmethod
    def _gira_escribir(regiones):
        return ",".join(f"{k}:{v}" for k, v in sorted(regiones.items()))

    def _gira_guardar(self, q, cuenta, nombre, club, puntos, torneos, regiones, ultimo):
        conq = sum(1 for v in regiones.values() if v >= GIRA_VICTORIAS_REGION)
        existe = q("SELECT 1 FROM gira WHERE cuenta = ?", (cuenta,)).fetchone()
        datos = (nombre[:24], club[:40], puntos, torneos, self._gira_escribir(regiones), conq, ultimo)
        if existe:
            q("UPDATE gira SET nombre=?, club=?, puntos=?, torneos=?, regiones=?, conquistadas=?, ultimo=? "
              "WHERE cuenta=?", datos + (cuenta,))
        else:
            q("INSERT INTO gira (nombre, club, puntos, torneos, regiones, conquistadas, ultimo, cuenta, desde) "
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", datos + (cuenta, int(time.time())))

    def gira_victoria(self, cuenta_id, evento_id, region_clave, nombre="", club=""):
        """Suma una victoria de mini campeonato. El premio lo fija el servidor."""
        cuenta_id = str(cuenta_id).upper()
        evento_id = str(evento_id)[:64]
        if region_clave not in GIRA_PT or not evento_id:
            return {"error": "Región inválida."}

        def trabajo(q):
            if q("SELECT 1 FROM gira_eventos WHERE id = ?", (evento_id,)).fetchone():
                return {"repetido": True}
            now = int(time.time())
            fila = q("SELECT nombre, club, puntos, torneos, regiones, ultimo FROM gira WHERE cuenta = ?",
                     (cuenta_id,)).fetchone()
            nom, clb, puntos, torneos, regs, ultimo = (fila if fila else ("", "", 0, 0, "", 0))
            if now - int(ultimo) < GIRA_ESPERA:
                return {"error": "Victoria demasiado rápida; se reintentará."}
            regiones = self._gira_leer(regs)
            regiones[region_clave] = min(GIRA_VICTORIAS_REGION, regiones.get(region_clave, 0) + 1)
            premio = GIRA_PT[region_clave] // 2
            self._gira_guardar(q, cuenta_id, nombre or nom or "Jugador", club or clb, int(puntos) + premio,
                               int(torneos) + 1, regiones, now)
            q("INSERT INTO gira_eventos (id, cuenta, fecha) VALUES (?, ?, ?)", (evento_id, cuenta_id, now))
            return {"ok": True, "premio": premio}
        return self._transaccion(trabajo)

    def gira_logro(self, cuenta_id, evento_id, logro_id, nombre="", club=""):
        """Suma los puntos de un logro cumplido. El premio lo fija el servidor y cada logro se cobra una sola vez por cuenta."""
        cuenta_id = str(cuenta_id).upper()
        evento_id = str(evento_id)[:64]
        logro_id = str(logro_id)[:40]
        if logro_id not in GIRA_LOGROS_PT or not evento_id:
            return {"error": "Logro inválido."}
        clave = f"logro:{cuenta_id}:{logro_id}"  # evita cobrar dos veces el mismo logro aunque cambie el id del evento

        def trabajo(q):
            if (q("SELECT 1 FROM gira_eventos WHERE id = ?", (evento_id,)).fetchone()
                    or q("SELECT 1 FROM gira_eventos WHERE id = ?", (clave,)).fetchone()):
                return {"repetido": True}
            now = int(time.time())
            fila = q("SELECT nombre, club, puntos, torneos, regiones, ultimo FROM gira WHERE cuenta = ?",
                     (cuenta_id,)).fetchone()
            nom, clb, puntos, torneos, regs, ultimo = (fila if fila else ("", "", 0, 0, "", 0))
            premio = GIRA_LOGROS_PT[logro_id]
            # "ultimo" no se toca: los logros no cuentan para el tiempo mínimo entre victorias
            self._gira_guardar(q, cuenta_id, nombre or nom or "Jugador", club or clb, int(puntos) + premio,
                               int(torneos), self._gira_leer(regs), int(ultimo))
            q("INSERT INTO gira_eventos (id, cuenta, fecha) VALUES (?, ?, ?)", (evento_id, cuenta_id, now))
            q("INSERT INTO gira_eventos (id, cuenta, fecha) VALUES (?, ?, ?)", (clave, cuenta_id, now))
            return {"ok": True, "premio": premio}
        return self._transaccion(trabajo)

    def gira_recalcular_conquistadas(self):
        """Al arrancar: vuelve a contar las regiones conquistadas de cada jugador con la regla actual
        (así quien ya tenía victorias guardadas con una regla anterior aparece en el ranking sin esperar a su próxima jugada)."""
        def trabajo(q):
            cambiadas = 0
            for cuenta, regs, conq in q("SELECT cuenta, regiones, conquistadas FROM gira").fetchall():
                nuevo = sum(1 for v in self._gira_leer(regs).values() if v >= GIRA_VICTORIAS_REGION)
                if nuevo != int(conq):
                    q("UPDATE gira SET conquistadas = ? WHERE cuenta = ?", (nuevo, cuenta))
                    cambiadas += 1
            return cambiadas
        try:
            cambiadas = self._transaccion(trabajo)
            if cambiadas:
                print(f"[truco] Gira: se actualizaron las regiones conquistadas de {cambiadas} jugadores.")
        except Exception as error:  # nunca debe impedir que el servidor arranque
            print("[truco] Gira: no se pudo recalcular las regiones conquistadas:", error)

    def gira_sincronizar(self, cuenta_id, regiones, nombre="", club=""):
        """Alta única de quien ya venía jugando: reconstruye un mínimo de puntos desde sus regiones."""
        cuenta_id = str(cuenta_id).upper()

        def trabajo(q):
            if q("SELECT 1 FROM gira WHERE cuenta = ?", (cuenta_id,)).fetchone():
                return {"ok": True, "ya": True}
            limpio, puntos, torneos = {}, 0, 0
            for clave, v in (regiones or {}).items():
                try: v = max(0, min(GIRA_VICTORIAS_REGION, int(v)))
                except (TypeError, ValueError): continue
                if clave in GIRA_PT and v:
                    limpio[clave] = v
                    puntos += v * (GIRA_PT[clave] // 2)
                    torneos += v
            if not limpio:
                return {"ok": True, "vacio": True}
            self._gira_guardar(q, cuenta_id, nombre or "Jugador", club or "", puntos, torneos, limpio, 0)
            return {"ok": True}
        return self._transaccion(trabajo)

    @staticmethod
    def _bots_ids():
        return ["BOT%02d" % (i + 1) for i in range(len(GIRA_BOTS))]

    def gira_sembrar_bots(self):
        """Crea (una sola vez) los jugadores simulados del ranking."""
        def trabajo(q):
            ahora = int(time.time())
            for i, (nombre, club, base, _ritmo, n) in enumerate(GIRA_BOTS):
                cuenta = self._bots_ids()[i]
                if q("SELECT 1 FROM gira WHERE cuenta = ?", (cuenta,)).fetchone():
                    continue
                regiones = {k: GIRA_VICTORIAS_REGION for k in list(GIRA_PT)[:n]}
                q("INSERT INTO gira (nombre, club, puntos, torneos, regiones, conquistadas, ultimo, cuenta, desde) "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (nombre[:24], club[:40], base, n * 2, self._gira_escribir(regiones), n, 0, cuenta, ahora - i))
        self._transaccion(trabajo)

    def gira_quitar_bots(self):
        ids = self._bots_ids()
        self._transaccion(lambda q: q("DELETE FROM gira WHERE cuenta IN (%s)" % ",".join("?" * len(ids)), tuple(ids)))

    def _bots_avanzar(self, q):
        """Los simulados suman puntos con el paso de los días (cuenta desde que se crearon)."""
        ids = self._bots_ids()
        filas = q("SELECT cuenta, desde, puntos FROM gira WHERE cuenta IN (%s)" % ",".join("?" * len(ids)),
                  tuple(ids)).fetchall()
        ahora = time.time()
        for cuenta, desde, puntos in filas:
            i = ids.index(cuenta)
            _n, _c, base, ritmo, _r = GIRA_BOTS[i]
            nuevo = base + min(GIRA_BOTS_TOPE, int((ahora - int(desde)) / 86400.0 * ritmo))
            if nuevo != int(puntos):
                q("UPDATE gira SET puntos = ? WHERE cuenta = ?", (nuevo, cuenta))

    def gira_podio(self, limite=3):
        """Los mejores por puntos entre quienes conquistaron al menos 1 región."""
        def trabajo(q):
            self._bots_avanzar(q)
            filas = q("SELECT nombre, club, puntos, conquistadas FROM gira WHERE conquistadas >= 1 "
                      "ORDER BY puntos DESC, desde ASC LIMIT ?", (int(limite),)).fetchall()
            total = q("SELECT COUNT(*) FROM gira WHERE conquistadas >= 1").fetchone()[0]
            return {"podio": [{"puesto": i + 1, "nombre": f[0], "club": f[1], "puntos": int(f[2]),
                               "regiones": int(f[3])} for i, f in enumerate(filas)],
                    "total": int(total)}
        return self._transaccion(trabajo)

    def gira_estado(self, cuenta_id):
        cuenta_id = str(cuenta_id).upper()

        def trabajo(q):
            self._bots_avanzar(q)
            fila = q("SELECT puntos, conquistadas, torneos, desde FROM gira WHERE cuenta = ?", (cuenta_id,)).fetchone()
            total = int(q("SELECT COUNT(*) FROM gira WHERE conquistadas >= 1").fetchone()[0])
            if not fila:
                return {"registrado": False, "clasificado": False, "puesto": None, "total": total, "puntos": 0, "regiones": 0}
            puntos, conq, torneos, desde = int(fila[0]), int(fila[1]), int(fila[2]), int(fila[3])
            puesto = None
            if conq >= 1:
                puesto = 1 + int(q("SELECT COUNT(*) FROM gira WHERE conquistadas >= 1 AND "
                                   "(puntos > ? OR (puntos = ? AND desde < ?))", (puntos, puntos, desde)).fetchone()[0])
            return {"registrado": True, "clasificado": conq >= 1, "puesto": puesto, "total": total,
                    "puntos": puntos, "regiones": conq, "torneos": torneos}
        return self._transaccion(trabajo)
