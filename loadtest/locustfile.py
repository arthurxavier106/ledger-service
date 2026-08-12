"""Load test do ledger.

Tres cenarios, porque uma media unica esconde exatamente o que interessa:

  baseline          pares aleatorios, contencao ~zero -> mede o teto do stack
  hot_account       90% das transferencias mirando UMA conta destino. E o caso real
                    (merchant recebendo pagamentos) e e onde row_lock e serializable
                    divergem.
  idempotent_replay 30% de replays de chave ja usada -> mede o custo do caminho de
                    replay e valida que ele e mais barato que o de escrita.

    locust -f loadtest/locustfile.py --headless -u 50 -r 50 -t 60s \\
           -H http://127.0.0.1:8000
"""

from __future__ import annotations

import json
import os
import pathlib
import random
import uuid

from locust import FastHttpUser, constant, events, task

SCENARIO = os.environ.get("LOADTEST_SCENARIO", "baseline")
HOT_SHARE = float(os.environ.get("LOADTEST_HOT_SHARE", "0.9"))
REPLAY_SHARE = float(os.environ.get("LOADTEST_REPLAY_SHARE", "0.3"))

_data = json.loads((pathlib.Path(__file__).parent / "accounts.json").read_text())
PAYERS: list[str] = _data["payers"]
MERCHANT: str = _data["merchant"]

# chaves ja usadas, compartilhadas entre usuarios, para o cenario de replay
_used_keys: list[tuple[str, dict]] = []


@events.test_start.add_listener
def _announce(**_kwargs) -> None:
    print(f"[loadtest] cenario={SCENARIO} pagadores={len(PAYERS)}")


class LedgerUser(FastHttpUser):
    wait_time = constant(0)
    network_timeout = 30.0
    connection_timeout = 30.0

    def _transfer_body(self) -> dict:
        if SCENARIO == "hot_account" and random.random() < HOT_SHARE:  # noqa: S311
            source, destination = random.choice(PAYERS), MERCHANT  # noqa: S311
        else:
            source, destination = random.sample(PAYERS, 2)
        return {"from_account_id": source, "to_account_id": destination,
                "amount": 100, "currency": "BRL"}

    @task
    def transfer(self) -> None:
        if SCENARIO == "idempotent_replay" and _used_keys and \
                random.random() < REPLAY_SHARE:  # noqa: S311
            key, body = random.choice(_used_keys)  # noqa: S311
            name = "POST /v1/transfers [replay]"
        else:
            key, body = str(uuid.uuid4()), self._transfer_body()
            name = "POST /v1/transfers"

        with self.client.post("/v1/transfers", json=body,
                              headers={"Idempotency-Key": key},
                              name=name, catch_response=True) as response:
            if response.status_code == 201:
                if SCENARIO == "idempotent_replay" and len(_used_keys) < 500:
                    _used_keys.append((key, body))
                response.success()
            else:
                response.failure(f"{response.status_code}: {response.text[:200]}")
