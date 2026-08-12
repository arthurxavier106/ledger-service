"""Assinatura HMAC e backoff -- funcoes puras, sem banco."""

from __future__ import annotations

import datetime as dt
import json

from ledger.outbox import backoff_delay, sign, verify


def test_backoff_grows_and_is_capped():
    assert all(backoff_delay(1) < backoff_delay(6) for _ in range(20))
    assert backoff_delay(50) <= 3600 * 1.5, "teto de 1 hora (mais jitter)"


def test_backoff_has_jitter():
    """Sem jitter, todos os eventos pendentes retentam no mesmo instante e derrubam
    o cliente de novo assim que ele volta."""
    assert len({round(backoff_delay(5), 6) for _ in range(50)}) > 1


def test_signature_round_trip():
    secret = b"super-secret-key"
    body = json.dumps({"amount": 100})
    timestamp = int(dt.datetime.now(dt.UTC).timestamp())
    assert verify(secret, sign(secret, timestamp, body), body) is True


def test_signature_rejects_tampered_body():
    secret = b"super-secret-key"
    timestamp = int(dt.datetime.now(dt.UTC).timestamp())
    header = sign(secret, timestamp, json.dumps({"amount": 100}))
    assert verify(secret, header, json.dumps({"amount": 999})) is False


def test_signature_rejects_wrong_secret():
    timestamp = int(dt.datetime.now(dt.UTC).timestamp())
    header = sign(b"right", timestamp, "body")
    assert verify(b"wrong", header, "body") is False


def test_signature_rejects_replay_of_old_delivery():
    """O timestamp entra no que e assinado justamente para isso."""
    secret = b"super-secret-key"
    old = int(dt.datetime.now(dt.UTC).timestamp()) - 3600
    assert verify(secret, sign(secret, old, "body"), "body", tolerance_s=300) is False


def test_signature_rejects_malformed_header():
    assert verify(b"k", "lixo", "body") is False


