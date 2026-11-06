"""Lógica del torneo privado (cuadro de eliminación directa de 4 a 16 jugadores).

No sabe nada de red ni de monedas: solo arma el cuadro, resuelve los pases directos
(byes) y avanza ganadores. server.py usa esto y se ocupa de los sockets y de las apuestas.
"""
import random

TAM_MIN = 4
TAM_MAX = 16
NOMBRES_BOT = [
    "Chino Paredes", "Tano Ferraro", "Beto Salvatierra", "Cacho Medina", "Lalo Ibarra",
    "Pocho Villarreal", "Nene Quiroga", "Gringo Bustos", "Rulo Arias", "Facha Montenegro",
    "Cabezón Luna", "Mono Cabrera", "Tito Ledesma", "Flaco Ocampo", "Colo Barrionuevo",
    "Turco Aguirre",
]


def potencia_dos(n):
    p = 1
    while p < n:
        p *= 2
    return p


def nombre_ronda(indice, total):
    """Nombre de la ronda `indice` (0 = primera) en un cuadro de `total` rondas."""
    faltan = total - indice
    return {1: "Final", 2: "Semifinal", 3: "Cuartos de final", 4: "Octavos de final"}.get(
        faltan, "Ronda %d" % (indice + 1))


class Torneo:
    def __init__(self, codigo, tam, apuesta=0, poderes=False):
        self.codigo = codigo
        self.tam = max(TAM_MIN, min(TAM_MAX, int(tam)))
        self.apuesta = int(apuesta)
        self.poderes = bool(poderes)
        self.estado = "lobby"          # lobby | en_juego | terminado
        self.jugadores = []            # [{"pid", "nombre", "humano", "cuenta"}]
        self.rondas = []               # [[partido, ...], ...]
        self.campeon = None
        self._n_humanos = 0
        self._n_bots = 0

    # ---- lobby ----
    def lugares_libres(self):
        return self.tam - len(self.jugadores)

    def agregar_humano(self, nombre, cuenta=None):
        if self.estado != "lobby" or self.lugares_libres() <= 0:
            return None
        pid = "H%d" % self._n_humanos
        self._n_humanos += 1
        self.jugadores.append({"pid": pid, "nombre": nombre, "humano": True, "cuenta": cuenta})
        return pid

    def quitar_humano(self, pid):
        self.jugadores = [j for j in self.jugadores if j["pid"] != pid]

    def completar_con_bots(self):
        usados = {j["nombre"] for j in self.jugadores}
        libres = [n for n in NOMBRES_BOT if n not in usados]
        random.shuffle(libres)
        while self.lugares_libres() > 0:
            nombre = libres.pop() if libres else "Bot %d" % (self._n_bots + 1)
            self.jugadores.append({"pid": "B%d" % self._n_bots, "nombre": nombre, "humano": False, "cuenta": None})
            self._n_bots += 1

    def jugador(self, pid):
        return next((j for j in self.jugadores if j["pid"] == pid), None)

    # ---- cuadro ----
    def empezar(self):
        """Completa con bots, sortea posiciones y arma todas las rondas (pases directos incluidos)."""
        self.completar_con_bots()
        pids = [j["pid"] for j in self.jugadores]
        random.shuffle(pids)
        p = potencia_dos(len(pids))
        pases = p - len(pids)
        pares = [[None, None] for _ in range(p // 2)]
        it = iter(pids)
        for i, par in enumerate(pares):
            par[0] = next(it)
            if i >= pases:                 # los primeros `pases` cruces tienen un solo jugador
                par[1] = next(it)
        total = 0
        t = p
        while t > 1:
            t //= 2
            total += 1
        self.rondas = []
        for r in range(total):
            cant = p // (2 ** (r + 1))
            self.rondas.append([{"id": "r%dm%d" % (r, m), "a": None, "b": None, "w": None, "estado": "pend"}
                                for m in range(cant)])
        for m, par in enumerate(pares):
            self.rondas[0][m]["a"], self.rondas[0][m]["b"] = par
        self.estado = "en_juego"
        self._resolver_pases(0)

    def _resolver_pases(self, r):
        """En la ronda r, un cruce con un solo jugador pasa solo."""
        for m, partido in enumerate(self.rondas[r]):
            if partido["estado"] == "pend" and partido["w"] is None:
                if partido["a"] and not partido["b"] and self._completo(r, m):
                    self._cerrar(r, m, partido["a"])

    def _completo(self, r, m):
        """True si ya no puede llegar otro jugador a este cruce (solo la ronda 0 tiene pases)."""
        return r == 0

    def _cerrar(self, r, m, ganador):
        partido = self.rondas[r][m]
        partido["w"] = ganador
        partido["estado"] = "fin"
        if r + 1 < len(self.rondas):
            sig = self.rondas[r + 1][m // 2]
            sig["a" if m % 2 == 0 else "b"] = ganador
        else:
            self.campeon = ganador
            self.estado = "terminado"

    def partido(self, partido_id):
        for ronda in self.rondas:
            for p in ronda:
                if p["id"] == partido_id:
                    return p
        return None

    def registrar(self, partido_id, ganador):
        """Anota el ganador de un partido. Devuelve el partido o None si no era válido."""
        p = self.partido(partido_id)
        if p is None or p["estado"] == "fin" or ganador not in (p["a"], p["b"]) or ganador is None:
            return None
        r = int(partido_id[1:partido_id.index("m")])
        m = int(partido_id[partido_id.index("m") + 1:])
        self._cerrar(r, m, ganador)
        return p

    def ronda_actual(self):
        for r, ronda in enumerate(self.rondas):
            if any(p["estado"] != "fin" for p in ronda):
                return r
        return len(self.rondas) - 1 if self.rondas else 0

    def partidos_listos(self):
        """Partidos de la ronda actual con los dos jugadores definidos y sin jugar."""
        if not self.rondas:
            return []
        r = self.ronda_actual()
        return [p for p in self.rondas[r] if p["estado"] == "pend" and p["a"] and p["b"]]

    def nombre_de(self, pid):
        j = self.jugador(pid) if pid else None
        return j["nombre"] if j else ""

    def cuadro_publico(self):
        """Lo que ven los jugadores: rondas con nombres, y quién ganó."""
        total = len(self.rondas)
        salida = []
        for r, ronda in enumerate(self.rondas):
            salida.append({
                "nombre": nombre_ronda(r, total),
                "partidos": [{"id": p["id"], "a": self.nombre_de(p["a"]), "b": self.nombre_de(p["b"]),
                              "a_pid": p["a"], "b_pid": p["b"], "w": p["w"], "estado": p["estado"],
                              "pase": bool(p["a"] and not p["b"] and r == 0)}
                             for p in ronda]})
        return salida
