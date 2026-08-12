"""/metrics no formato Prometheus."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="session")

EXPECTED_SERIES = [
    "ledger_transactions_total",
    "ledger_transaction_duration_seconds",
    "ledger_lock_wait_seconds",
    "ledger_serialization_failures_total",
    "ledger_deadlocks_total",
    "ledger_reconciliation_drift",
]


async def test_metrics_endpoint_exposes_prometheus_format(client):
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text
    for series in EXPECTED_SERIES:
        assert series in body, f"metrica ausente: {series}"
    assert "# HELP" in body and "# TYPE" in body


async def test_lock_wait_is_observed_on_transfer(client):
    from ledger.metrics import REGISTRY

    funding = await _account(client, allow_negative=True)
    payer = await _account(client)
    merchant = await _account(client)
    await client.post("/v1/transfers", json={
        "from_account_id": funding["id"], "to_account_id": payer["id"],
        "amount": 1_000, "currency": "BRL"})

    before = REGISTRY.get_sample_value(
        "ledger_lock_wait_seconds_count", {"strategy": "row_lock"}) or 0
    await client.post("/v1/transfers", json={
        "from_account_id": payer["id"], "to_account_id": merchant["id"],
        "amount": 100, "currency": "BRL"})
    after = REGISTRY.get_sample_value(
        "ledger_lock_wait_seconds_count", {"strategy": "row_lock"}) or 0

    assert after > before, "a espera de lock precisa ser medida no write path"


async def _account(client, *, allow_negative: bool = False) -> dict:
    import uuid

    response = await client.post("/v1/accounts", json={
        "external_id": f"acct-{uuid.uuid4()}", "owner_id": str(uuid.uuid4()),
        "currency": "BRL", "type": "equity" if allow_negative else "liability",
        "allow_negative": allow_negative})
    return response.json()
