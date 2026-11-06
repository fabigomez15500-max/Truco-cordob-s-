"""Ranking semanal: reglas de puntos y calendario (semana argentina: de lunes 00:00 a domingo 23:59).

Solo cuentan partidas contra personas (más un puntito por ganarle a la IA, con tope diario) y los
premios de los torneos privados. No da monedas: es solo prestigio.
"""
import time

DESFASE_ARG = 3 * 3600          # Argentina = UTC-3 (sin horario de verano)
SEMANA_SEG = 7 * 86400

PTS_GANAR = 10
PTS_PERDER = 2
MARGEN_CADA = 3                 # +1 punto cada 3 de diferencia al ganar...
MARGEN_MAX = 5                  # ...hasta +5
RACHA_CADA = 3                  # cada 3 victorias seguidas...
PTS_RACHA = 5                   # ...+5
PTS_RIVAL_MEJOR = 3             # ganarle a alguien que va arriba en la semana
APUESTA_CADA = 50               # +1 por cada 50 monedas apostadas al ganar...
APUESTA_MAX = 6                 # ...hasta +6
PTS_IA = 1
IA_TOPE_DIA = 5
PTS_TORNEO = {"campeon": 30, "finalista": 15, "semifinalista": 8}
TORNEO_MIN_HUMANOS = 3          # un torneo con menos humanos no da puntos (evita armarlos a medida)
MAX_CRUCES_SEMANA = 3           # contra el mismo rival cuentan 3 partidas por semana
MIN_PARTIDAS_VISIBLE = 5        # para figurar en la tabla


def _local(ahora=None):
    return time.gmtime((time.time() if ahora is None else ahora) - DESFASE_ARG)


def semana_clave(ahora=None):
    """'AAAA-MM-DD' del lunes de la semana (hora argentina)."""
    t = _local(ahora)
    lunes = (time.time() if ahora is None else ahora) - DESFASE_ARG - t.tm_wday * 86400
    return time.strftime("%Y-%m-%d", time.gmtime(lunes))


def segundos_restantes(ahora=None):
    """Segundos hasta el próximo lunes 00:00 (hora argentina)."""
    t = _local(ahora)
    pasados = t.tm_wday * 86400 + t.tm_hour * 3600 + t.tm_min * 60 + t.tm_sec
    return SEMANA_SEG - pasados


def dia_clave(ahora=None):
    return time.strftime("%Y-%m-%d", _local(ahora))


def puntos_partida(gano, margen=0, racha=0, rival_mejor=False, apuesta=0):
    """Puntos de UNA partida contra una persona. `racha` = victorias seguidas contando esta."""
    if not gano:
        return PTS_PERDER
    p = PTS_GANAR + min(MARGEN_MAX, max(0, int(margen)) // MARGEN_CADA)
    if racha > 0 and racha % RACHA_CADA == 0:
        p += PTS_RACHA
    if rival_mejor:
        p += PTS_RIVAL_MEJOR
    if apuesta and apuesta > 0:
        p += min(APUESTA_MAX, int(apuesta) // APUESTA_CADA)
    return p


MIN_PARTIDAS_DIARIO = 1         # el ranking diario muestra a quien jugó al menos una partida contra una persona

# Misiones semanales: clave -> (meta, premio en puntos del ranking, texto). Se renuevan cada lunes.
MISIONES = {
    "jugar10": (10, 10, "Jugá 10 partidas contra personas"),
    "ganar5": (5, 15, "Ganá 5 partidas contra personas"),
    "racha3": (3, 10, "Conseguí 3 victorias seguidas"),
    "apuesta1": (1, 10, "Ganá una partida apostando"),
    "torneo1": (1, 10, "Terminá un torneo privado"),
}
MISIONES_ORDEN = ("jugar10", "ganar5", "racha3", "apuesta1", "torneo1")


def segundos_restantes_dia(ahora=None):
    """Segundos hasta la medianoche argentina."""
    t = _local(ahora)
    return 86400 - (t.tm_hour * 3600 + t.tm_min * 60 + t.tm_sec)
