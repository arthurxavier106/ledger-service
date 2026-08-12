"""Hash canonico do payload -- e o que distingue replay legitimo de reuso de chave."""

from __future__ import annotations

import uuid

from ledger.idempotency import fingerprint


def test_is_order_independent():
    """A ordem das chaves no JSON nao pode mudar o hash, senao o mesmo pedido
    contaria como pedido diferente."""
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})


def test_detects_amount_change():
    assert fingerprint({"amount": 100}) != fingerprint({"amount": 500})


def test_handles_non_json_native_types():
    assert fingerprint({"k": uuid.uuid4()})


def test_nested_structures_are_canonicalized():
    left = {"meta": {"b": 2, "a": 1}, "amount": 10}
    right = {"amount": 10, "meta": {"a": 1, "b": 2}}
    assert fingerprint(left) == fingerprint(right)
