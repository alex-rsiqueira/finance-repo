"""Pluggy API calls for the Meu Pluggy connections of one application.

A call is retried only when the answer never arrived (connection error or timeout) or the
server failed (5xx), honouring Retry-After. A 4xx is raised at once: a 401/403 needs the
credentials or the bank authorisation fixed, and repeating a 429 only adds to the limit.
"""
import time

import requests

BASE_URL = "https://api.pluggy.ai"
TIMEOUT = 60
ATTEMPTS = 3
MAX_WAIT_SECONDS = 60


class Client:
    def __init__(self, client_id, client_secret):
        self.session = requests.Session()
        # The API key lasts 2 hours, far longer than a run.
        self.session.headers["X-API-KEY"] = self.call(
            "POST", "/auth", json={"clientId": client_id, "clientSecret": client_secret})["apiKey"]

    def call(self, method, path, **kwargs):
        for attempt in range(1, ATTEMPTS + 1):
            try:
                response = self.session.request(method, f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)
                response.raise_for_status()
                return response.json()
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

    def item(self, item_id):
        return self.call("GET", f"/items/{item_id}")

    def accounts(self, item_id):
        return self.call("GET", "/accounts", params={"itemId": item_id})["results"]

    def transactions(self, account_id):
        """Every transaction of the account, no date filter. Cursor-paged: next is the query
        string of the following page and is appended as it comes."""
        records = []
        answer = self.call("GET", "/v2/transactions", params={"accountId": account_id})
        records += answer["results"]
        while answer.get("next"):
            answer = self.call("GET", f"/v2/transactions{answer['next']}")
            records += answer["results"]
        return records

    def bills(self, account_id):
        return self.pages("/bills", accountId=account_id)

    def investments(self, item_id):
        return self.pages("/investments", itemId=item_id)

    def investment_transactions(self, investment_id):
        return self.pages(f"/investments/{investment_id}/transactions")

    def pages(self, path, **params):
        records = []
        page = 1
        while True:
            answer = self.call("GET", path, params={**params, "page": page})
            records += answer["results"]
            if page >= answer.get("totalPages", 1):
                return records
            page += 1
