"""Semeia contas para o load test e grava os ids em accounts.json."""

from __future__ import annotations

import json
import os
import pathlib
import uuid

import httpx

BASE_URL = os.environ.get("LEDGER_BASE_URL", "http://127.0.0.1:8000")
PAYERS = int(os.environ.get("LOADTEST_PAYERS", "200"))
OPENING_BALANCE = 10**12  # alto o bastante para saldo insuficiente nao virar ruido


def create_account(client: httpx.Client, *, allow_negative: bool = False) -> str:
    response = client.post(f"{BASE_URL}/v1/accounts", json={
        "external_id": f"load-{uuid.uuid4()}",
        "owner_id": str(uuid.uuid4()),
        "currency": "BRL",
        "type": "equity" if allow_negative else "liability",
        "allow_negative": allow_negative,
    })
    response.raise_for_status()
    return response.json()["id"]


def main() -> None:
    with httpx.Client(timeout=30) as client:
        funding = create_account(client, allow_negative=True)
        payers = [create_account(client) for _ in range(PAYERS)]
        merchant = create_account(client)

        for payer in payers:
            client.post(f"{BASE_URL}/v1/transfers", json={
                "from_account_id": funding, "to_account_id": payer,
                "amount": OPENING_BALANCE, "currency": "BRL",
            }).raise_for_status()

    path = pathlib.Path(__file__).parent / "accounts.json"
    path.write_text(json.dumps({"funding": funding, "payers": payers,
                                "merchant": merchant}, indent=2))
    print(f"{len(payers)} pagadores + 1 merchant -> {path}")


if __name__ == "__main__":
    main()
