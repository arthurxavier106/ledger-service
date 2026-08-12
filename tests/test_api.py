"""Smoke end-to-end da borda HTTP contra Postgres real."""

from __future__ import annotations

import uuid

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def _create_account(client, *, allow_negative: bool = False) -> dict:
    response = await client.post("/v1/accounts", json={
        "external_id": f"acct-{uuid.uuid4()}",
        "owner_id": str(uuid.uuid4()),
        "currency": "BRL",
        "type": "equity" if allow_negative else "liability",
        "allow_negative": allow_negative,
    })
    assert response.status_code == 201, response.text
    return response.json()


async def test_transfer_produces_double_entry(client):
    funding = await _create_account(client, allow_negative=True)
    payer = await _create_account(client)
    merchant = await _create_account(client)

    # dinheiro entra debitando a conta de sistema -- nao aparece do nada
    await client.post("/v1/transfers", json={
        "from_account_id": funding["id"], "to_account_id": payer["id"],
        "amount": 10_000, "currency": "BRL",
    })

    response = await client.post("/v1/transfers", json={
        "from_account_id": payer["id"], "to_account_id": merchant["id"],
        "amount": 2_500, "currency": "BRL", "external_ref": "pix-0001",
    })
    assert response.status_code == 201, response.text
    body = response.json()

    assert body["kind"] == "transfer"
    assert len(body["entries"]) == 2
    directions = {e["direction"]: e for e in body["entries"]}
    assert directions["debit"]["amount"] == directions["credit"]["amount"] == 2_500
    assert directions["debit"]["balance_after"] == 7_500
    assert directions["credit"]["balance_after"] == 2_500

    payer_after = (await client.get(f"/v1/accounts/{payer['id']}")).json()
    assert payer_after["balance"] == 7_500


async def test_insufficient_funds_returns_problem_json(client):
    payer = await _create_account(client)
    merchant = await _create_account(client)

    response = await client.post("/v1/transfers", json={
        "from_account_id": payer["id"], "to_account_id": merchant["id"],
        "amount": 100, "currency": "BRL",
    })
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    problem = response.json()
    assert problem["type"] == "https://ledger.dev/errors/insufficient-funds"
    assert problem["available"] == 0
    assert problem["requested"] == 100


async def test_statement_keyset_pagination(client):
    funding = await _create_account(client, allow_negative=True)
    payer = await _create_account(client)
    merchant = await _create_account(client)
    await client.post("/v1/transfers", json={
        "from_account_id": funding["id"], "to_account_id": payer["id"],
        "amount": 10_000, "currency": "BRL"})

    for _ in range(7):
        await client.post("/v1/transfers", json={
            "from_account_id": payer["id"], "to_account_id": merchant["id"],
            "amount": 100, "currency": "BRL"})

    seen: list[int] = []
    cursor, pages = None, 0
    while True:
        params = {"limit": 3} | ({"cursor": cursor} if cursor else {})
        page = (await client.get(f"/v1/accounts/{merchant['id']}/entries",
                                 params=params)).json()
        seen += [e["id"] for e in page["data"]]
        pages += 1
        if not page["has_more"]:
            break
        cursor = page["next_cursor"]
        assert cursor, "has_more=true exige next_cursor"

    assert len(seen) == 7, "nenhum lancamento perdido ou duplicado entre paginas"
    assert len(set(seen)) == 7
    assert seen == sorted(seen, reverse=True), "ordem estavel, mais recente primeiro"
    assert pages == 3


async def test_unknown_account_returns_404_problem(client):
    response = await client.get(f"/v1/accounts/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.json()["type"] == "https://ledger.dev/errors/account-not-found"


async def test_openapi_documents_examples(client):
    schema = (await client.get("/openapi.json")).json()
    transfer = schema["components"]["schemas"]["TransferRequestBody"]
    assert "examples" in transfer, "OpenAPI sem exemplo de request"
    assert schema["paths"]["/v1/transfers"]["post"]["responses"]["201"]


# --- idempotencia na borda HTTP -------------------------------------------
async def test_idempotency_key_replays_original_response(client):
    funding = await _create_account(client, allow_negative=True)
    payer = await _create_account(client)
    merchant = await _create_account(client)
    await client.post("/v1/transfers", json={
        "from_account_id": funding["id"], "to_account_id": payer["id"],
        "amount": 10_000, "currency": "BRL"})

    body = {"from_account_id": payer["id"], "to_account_id": merchant["id"],
            "amount": 3_000, "currency": "BRL"}
    headers = {"Idempotency-Key": "req-abc-123"}

    first = await client.post("/v1/transfers", json=body, headers=headers)
    second = await client.post("/v1/transfers", json=body, headers=headers)

    assert first.status_code == second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    assert "idempotency-replayed" not in first.headers
    assert second.headers["idempotency-replayed"] == "true"

    payer_after = (await client.get(f"/v1/accounts/{payer['id']}")).json()
    assert payer_after["balance"] == 7_000, "o replay nao pode debitar de novo"


async def test_idempotency_key_reuse_with_other_body_is_422(client):
    funding = await _create_account(client, allow_negative=True)
    payer = await _create_account(client)
    merchant = await _create_account(client)
    await client.post("/v1/transfers", json={
        "from_account_id": funding["id"], "to_account_id": payer["id"],
        "amount": 10_000, "currency": "BRL"})

    headers = {"Idempotency-Key": "req-xyz-789"}
    base = {"from_account_id": payer["id"], "to_account_id": merchant["id"],
            "currency": "BRL"}

    assert (await client.post("/v1/transfers", json=base | {"amount": 100},
                              headers=headers)).status_code == 201
    clash = await client.post("/v1/transfers", json=base | {"amount": 9_000},
                              headers=headers)

    assert clash.status_code == 422
    assert clash.json()["type"] == "https://ledger.dev/errors/idempotency-key-reuse"


# --- estorno na borda HTTP -------------------------------------------------
async def test_reversal_endpoint(client):
    funding = await _create_account(client, allow_negative=True)
    payer = await _create_account(client)
    merchant = await _create_account(client)
    await client.post("/v1/transfers", json={
        "from_account_id": funding["id"], "to_account_id": payer["id"],
        "amount": 10_000, "currency": "BRL"})

    txn = (await client.post("/v1/transfers", json={
        "from_account_id": payer["id"], "to_account_id": merchant["id"],
        "amount": 2_000, "currency": "BRL"})).json()

    reversal = await client.post(f"/v1/transactions/{txn['id']}/reversal",
                                 json={"external_ref": "chargeback-1"})
    assert reversal.status_code == 201, reversal.text
    assert reversal.json()["kind"] == "reversal"
    assert reversal.json()["reverses_transaction_id"] == txn["id"]

    payer_after = (await client.get(f"/v1/accounts/{payer['id']}")).json()
    assert payer_after["balance"] == 10_000

    original = (await client.get(f"/v1/transactions/{txn['id']}")).json()
    assert original["status"] == "reversed"

    again = await client.post(f"/v1/transactions/{txn['id']}/reversal", json={})
    assert again.status_code == 409
    assert again.json()["type"] == "https://ledger.dev/errors/already-reversed"


async def test_openapi_documents_idempotency_header(client):
    schema = (await client.get("/openapi.json")).json()
    params = schema["paths"]["/v1/transfers"]["post"].get("parameters", [])
    names = {p["name"] for p in params}
    assert "Idempotency-Key" in names
    assert "/v1/transactions/{transaction_id}/reversal" in schema["paths"]
