"""GoCardless Bank Account Data (formerly Nordigen) calls for one set of credentials.

GoCardless counts calls per account, endpoint and day, and for some banks allows only a
few. So a call is retried only when the answer never arrived (connection error or
timeout) or the server failed (5xx). A 4xx is raised at once: repeating a 429 spends
more of the day's calls, and a 401/403 needs the bank to be authorised again.
"""
import time

import requests
from nordigen import NordigenClient

ATTEMPTS = 3
MAX_WAIT_SECONDS = 60


def call(fn, *args, **kwargs):
    for attempt in range(1, ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status is None or status < 500 or attempt == ATTEMPTS:
                raise
            retry_after = e.response.headers.get("Retry-After", "")
            wait = int(retry_after) if retry_after.isdigit() else 2 ** attempt
        except (requests.ConnectionError, requests.Timeout):
            if attempt == ATTEMPTS:
                raise
            wait = 2 ** attempt
        time.sleep(min(wait, MAX_WAIT_SECONDS))


class Client:
    def __init__(self, secret_id, secret_key):
        self.api = NordigenClient(secret_id=secret_id, secret_key=secret_key)
        call(self.api.generate_token)

    def linked_account_ids(self):
        """Account ids of the requisitions whose bank authorisation is still valid (status LN).
        An expired or removed requisition needs the bank to be authorised again, so this
        fails instead of creating a new one."""
        requisitions = call(self.api.requisition.get_requisitions)["results"]
        linked = [r for r in requisitions if r["status"] == "LN"]
        if not linked:
            statuses = sorted({r["status"] for r in requisitions})
            raise RuntimeError(
                f"No linked requisition (statuses found: {statuses}). The bank must be authorised again."
            )
        return [account_id for r in linked for account_id in r["accounts"]]

    def fetch(self, account_id, endpoint):
        """endpoint is metadata, details, balances or transactions. One call per run each."""
        account = self.api.account_api(id=account_id)
        return call(getattr(account, f"get_{endpoint}"))
