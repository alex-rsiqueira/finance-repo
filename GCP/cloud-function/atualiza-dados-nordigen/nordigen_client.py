import time
from functools import wraps
from typing import Any, Dict, List

import requests
from nordigen import NordigenClient

from config import config
from logger import logger


class RateLimiter:
    """Rate limiter for API calls"""

    def __init__(self, max_calls_per_second: int):
        self.min_interval = 1.0 / max_calls_per_second
        self.last_call_time = 0

    def wait_if_needed(self):
        """Wait if necessary to respect rate limit"""
        time_since_last_call = time.time() - self.last_call_time

        if time_since_last_call < self.min_interval:
            time.sleep(self.min_interval - time_since_last_call)

        self.last_call_time = time.time()


def retry_with_backoff(max_retries: int = None, initial_delay: float = None):
    """Decorator for retrying functions with exponential backoff"""
    max_retries = max_retries or config.nordigen_max_retries
    initial_delay = initial_delay or config.nordigen_retry_delay

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ConnectionError) as e:
                    if attempt == max_retries - 1:
                        logger.error(
                            f"All {max_retries} attempts failed",
                            function=func.__name__,
                            error=str(e)
                        )
                        raise

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}",
                        function=func.__name__,
                        delay_seconds=delay
                    )
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff

        return wrapper
    return decorator


class EnhancedNordigenClient:
    """Nordigen (GoCardless Bank Account Data) client with retry logic and rate limiting"""

    def __init__(self, secret_id: str, secret_key: str):
        self.client = NordigenClient(secret_id=secret_id, secret_key=secret_key)
        self.rate_limiter = RateLimiter(config.nordigen_rate_limit_per_second)
        self._generate_token()

    @retry_with_backoff()
    def _generate_token(self):
        """Generate the access token, valid for the whole run"""
        logger.info("Generating Nordigen access token")

        try:
            self.client.generate_token()
        except Exception as e:
            # The client hides the HTTP status when the body is not JSON, so the request is
            # repeated once to record what the API actually answered.
            probe = requests.post(
                f"{self.client.base_url}/token/new/",
                json={"secret_id": self.client.secret_id, "secret_key": self.client.secret_key},
                headers={"accept": "application/json"},
                timeout=30
            )
            logger.error(
                "Token request failed",
                error=str(e),
                status_code=probe.status_code,
                content_type=probe.headers.get("Content-Type"),
                body="<token omitted>" if probe.ok else probe.text[:300]
            )
            raise

        logger.info("Token generated successfully")

    @retry_with_backoff()
    def get_linked_requisitions(self) -> List[Dict[str, Any]]:
        """
        Get the requisitions whose bank authorisation is still valid (status LN).
        A requisition that expired or was removed needs the user to authorise the bank
        again, so this fails instead of creating a new one.
        """
        self.rate_limiter.wait_if_needed()
        requisitions = self.client.requisition.get_requisitions()

        linked = [r for r in requisitions.get("results", []) if r["status"] == "LN"]

        if not linked:
            statuses = sorted({r["status"] for r in requisitions.get("results", [])})
            raise RuntimeError(
                f"No linked requisition found (statuses found: {statuses}). "
                "The bank connection must be authorised again."
            )

        logger.info(
            f"Found {len(linked)} linked requisitions",
            requisition_ids=[r["id"] for r in linked]
        )

        return linked

    def get_account_api(self, account_id: str) -> "EnhancedAccountAPI":
        """Get enhanced account API instance"""
        return EnhancedAccountAPI(self.client.account_api(id=account_id), self.rate_limiter, account_id)


class EnhancedAccountAPI:
    """Account API with retry logic. Each endpoint is called once per run to stay inside the daily limits."""

    def __init__(self, account_api: Any, rate_limiter: RateLimiter, account_id: str):
        self.account_api = account_api
        self.rate_limiter = rate_limiter
        self.account_id = account_id

    @retry_with_backoff()
    def fetch(self, endpoint: str) -> Dict[str, Any]:
        """Call metadata, details, balances or transactions"""
        self.rate_limiter.wait_if_needed()
        return getattr(self.account_api, f"get_{endpoint}")()
