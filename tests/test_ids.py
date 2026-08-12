"""UUIDv7 -- ver src/ledger/ids.py para o racional de nao usar v4."""

from __future__ import annotations

from ledger.ids import timestamp_ms, uuid7


def test_uuid7_layout_and_monotonicity():
    values = [uuid7() for _ in range(1000)]
    assert all(v.version == 7 for v in values)
    assert all((v.int >> 62) & 0b11 == 0b10 for v in values), "variante RFC 9562"
    assert len(set(values)) == 1000, "sem colisao"
    # prefixo temporal -> ordenacao por insercao (o motivo de nao usar v4)
    assert values == sorted(values)
    assert abs(timestamp_ms(values[0]) - timestamp_ms(values[-1])) < 1000
