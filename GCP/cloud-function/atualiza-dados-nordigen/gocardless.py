"""GoCardless Bank Account Data (formerly Nordigen) calls for one set of credentials.

GoCardless counts calls per account, endpoint and day, and for some banks allows only a
few. So a call is retried only when the answer never arrived (connection error or
timeout) or the server failed (5xx). A 4xx is raised at once: repeating a 429 spends
more of the day's calls, and a 401/403 needs the bank to be authorised again.

Every successful details, balances or transactions answer carries the account's allowance in
three headers (finance-backend plans/bank-refresh.md §1.2): the scope's limit, what is left of
it, and the seconds until it renews. The nordigen library returns only the body, so the account
calls are made here with requests; the library still issues the token and lists requisitions.
"""
import time

import requests
from nordigen import NordigenClient

ATTEMPTS = 3
MAX_WAIT_SECONDS = 60
TIMEOUT_SECONDS = 10  # as the nordigen library's own calls

# The account success headers, by the names GoCardless sends them (read on a live call 2026-10-31).
ALLOWANCE_HEADERS = {
    "limit": "http_x_ratelimit_account_success_limit",
    "remaining": "http_x_ratelimit_account_success_remaining",
    "reset_seconds": "http_x_ratelimit_account_success_reset",
}


def allowance(headers):
    """The account success headers of an answer as integers; {} when it has none (metadata, an
    error)."""
    return {key: int(headers[name]) for key, name in ALLOWANCE_HEADERS.items()
            if str(headers.get(name, "")).strip().isdigit()}


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
        self._requisitions = None

    def requisitions(self):
        """Every requisition of these credentials, as GoCardless lists them: one per bank
        authorisation, expired ones included. Asked once per client."""
        if self._requisitions is None:
            self._requisitions = call(self.api.requisition.get_requisitions)["results"]
        return self._requisitions

    def linked_account_ids(self, registered):
        """Account ids of the registered requisitions whose bank authorisation is still valid
        (status LN). registered is the requisition ids the registry holds as linked
        (finance-backend plans/bank-integrations.md D-21); none registered reads nothing. An
        expired or removed requisition needs the bank to be authorised again, so this fails
        instead of creating a new one."""
        if not registered:
            return []
        requisitions = [r for r in self.requisitions() if r["id"] in registered]
        linked = [r for r in requisitions if r["status"] == "LN"]
        if not linked:
            statuses = sorted({r["status"] for r in requisitions})
            raise RuntimeError(
                f"No linked requisition (statuses found: {statuses}). The bank must be authorised again."
            )
        # One account two registered requisitions hold (two households, or a renewal not yet
        # replaced, D-24) is read once, so its daily allowance is not spent twice.
        return list(dict.fromkeys(account_id for r in linked for account_id in r["accounts"]))

    def fetch(self, account_id, endpoint):
        """endpoint is metadata, details, balances or transactions; one call each. Returns the
        answer's body and the allowance it reported (see allowance)."""
        path = f"accounts/{account_id}/" + ("" if endpoint == "metadata" else f"{endpoint}/")
        answer = call(self.get, path)
        return answer.json(), allowance(answer.headers)

    def agreement(self, agreement_id):
        """The end user agreement a requisition was created with: when it was accepted and for
        how many days (finance-backend plans/bank-integrations.md D-16). Not counted against an
        account's allowance."""
        return call(self.get, f"agreements/enduser/{agreement_id}/").json()

    def institution(self, institution_id):
        """The bank a requisition is with: its name, as GoCardless gives it."""
        return call(self.get, f"institutions/{institution_id}/").json()

    def get(self, path):
        """One GET, raised as the nordigen library raises it: an HTTPError carrying GoCardless's
        answer, so the logs read the same."""
        answer = requests.get(f"{self.api.base_url}/{path}", timeout=TIMEOUT_SECONDS, headers={
            "accept": "application/json", "Authorization": f"Bearer {self.api.token}"})
        if not answer.ok:
            try:
                body = answer.json()
            except ValueError:
                body = answer.text[:300]
            raise requests.HTTPError({"response": body, "status": answer.status_code}, response=answer)
        return answer
