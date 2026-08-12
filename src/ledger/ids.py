"""UUIDv7 (RFC 9562).

Postgres 18 traz uuidv7() nativo; em 16/17 geramos aqui. O ponto de usar v7 em vez
de v4 e a localidade de insercao: v4 e aleatorio e espalha cada INSERT por uma pagina
random do B-tree, causando page split e inflacao de WAL sob carga. v7 tem prefixo
temporal, entao mantem o comportamento de insercao de um serial sem ser enumeravel.

Layout (128 bits):
    unix_ts_ms  48 | ver 4 (=7) | rand_a 12 | var 2 (=0b10) | rand_b 62
`rand_a` carrega a fracao de sub-milissegundo, o que garante monotonicidade dentro
do mesmo milissegundo.
"""

from __future__ import annotations

import os
import time
import uuid

_VERSION = 0x7
_VARIANT = 0b10


def uuid7() -> uuid.UUID:
    ns = time.time_ns()
    ms, rest_ns = divmod(ns, 1_000_000)
    sub_ms = (rest_ns * 4096) // 1_000_000  # 12 bits
    rand_b = int.from_bytes(os.urandom(8), "big") & ((1 << 62) - 1)

    value = (ms & ((1 << 48) - 1)) << 80
    value |= _VERSION << 76
    value |= (sub_ms & 0xFFF) << 64
    value |= _VARIANT << 62
    value |= rand_b
    return uuid.UUID(int=value)


def timestamp_ms(value: uuid.UUID) -> int:
    """Extrai o timestamp de um UUIDv7 (util para debug e particionamento)."""
    if value.version != 7:
        raise ValueError(f"expected UUIDv7, got v{value.version}")
    return value.int >> 80
