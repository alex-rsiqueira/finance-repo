import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config import config
from logger import logger, ProcessingMetrics
from nordigen_client import EnhancedAccountAPI
from db_manager import DatabaseManager

# Raw table -> (API endpoint, path to the records inside the response)
SOURCES = {
    "tb_nordigen_meta": ("metadata", ()),
    "tb_nordigen_details": ("details", ("account",)),
    "tb_nordigen_balances": ("balances", ("balances",)),
    "tb_nordigen_transactions": ("transactions", ("transactions", "booked")),
}


class DataProcessor:
    """Process Nordigen data with parallel execution and error handling"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.metrics = ProcessingMetrics(logger)

    def process_account(self, user_id: str, account: EnhancedAccountAPI) -> Dict[str, Any]:
        """Load every table for a single account. A failing table does not stop the others."""
        logger.info(
            f"Processing account for user {user_id}",
            user_id=user_id,
            account_id=account.account_id
        )

        results = {}

        for table_id in SOURCES:
            try:
                results[table_id] = self._process_table(user_id, account, table_id)
                self.metrics.record_processing(
                    success=True,
                    record_count=results[table_id]["records"],
                    entity_type="table"
                )
            except Exception as e:
                logger.error(
                    f"Failed to process {table_id}",
                    table=table_id,
                    user_id=user_id,
                    account_id=account.account_id,
                    error=str(e),
                    traceback=traceback.format_exc()
                )

                results[table_id] = {"status": "error", "error": str(e), "error_type": type(e).__name__}
                self.metrics.record_processing(success=False, entity_type="table")

                self.db_manager.log_ingestion_event(
                    event_type="table_processing_error",
                    status="error",
                    details={
                        "table_id": table_id,
                        "user_id": user_id,
                        "account_id": account.account_id,
                        "error": str(e),
                        "error_type": type(e).__name__
                    }
                )

        successful_tables = sum(1 for r in results.values() if r["status"] == "success")
        account_status = "success" if successful_tables == len(results) else "error"

        self.metrics.record_processing(success=account_status == "success", entity_type="account")

        return {
            "user_id": user_id,
            "account_id": account.account_id,
            "status": account_status,
            "tables_processed": len(results),
            "tables_successful": successful_tables,
            "table_results": results
        }

    def _process_table(self, user_id: str, account: EnhancedAccountAPI, table_id: str) -> Dict[str, Any]:
        """Fetch one endpoint and merge its records into the raw table"""
        endpoint, path = SOURCES[table_id]

        response = account.fetch(endpoint)

        # An error answer (rate limit, expired access, ...) comes back as a dict without the expected keys
        data = response
        for key in path:
            if not isinstance(data, dict) or key not in data:
                raise ValueError(f"Unexpected {endpoint} response from the API: {str(response)[:300]}")
            data = data[key]

        df = pd.json_normalize(data)

        if df.empty:
            logger.info(f"No records returned for {table_id}", table=table_id, account_id=account.account_id)
            return {"status": "success", "records": 0}

        df.columns = [c.replace(".", "_") for c in df.columns]
        df["client_id"] = user_id
        df["dtinsert"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # The API names the transaction reference entryReference; the raw table and the
        # Dataform models call it transactionId, with the same value
        if table_id == "tb_nordigen_transactions" and "transactionId" not in df.columns:
            df["transactionId"] = df["entryReference"]

        result = self.db_manager.upsert_dataframe(df, table_id)

        return {"records": len(df), **result}

    def process_accounts_parallel(self, accounts_data: List[Tuple[str, EnhancedAccountAPI]],
                                  max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """Process multiple accounts in parallel, accounts_data being (user_id, account_api) tuples"""
        max_workers = max_workers or config.max_workers
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_user = {
                executor.submit(self.process_account, user_id, account): user_id
                for user_id, account in accounts_data
            }

            for future in as_completed(future_to_user):
                user_id = future_to_user[future]

                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(
                        "Account processing failed",
                        user_id=user_id,
                        error=str(e),
                        traceback=traceback.format_exc()
                    )
                    results.append({
                        "user_id": user_id,
                        "status": "error",
                        "error": str(e),
                        "error_type": type(e).__name__
                    })
                    self.metrics.record_processing(success=False, entity_type="account")

        return results

    def get_processing_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary of processing results"""
        successful_accounts = sum(1 for r in results if r["status"] == "success")

        table_stats = {}
        for result in results:
            for table_id, table_result in result.get("table_results", {}).items():
                stats = table_stats.setdefault(table_id, {"success": 0, "error": 0, "records": 0})

                if table_result["status"] == "success":
                    stats["success"] += 1
                    stats["records"] += table_result.get("records", 0)
                else:
                    stats["error"] += 1

        return {
            "total_accounts": len(results),
            "successful_accounts": successful_accounts,
            "failed_accounts": len(results) - successful_accounts,
            "table_statistics": table_stats,
            "metrics": self.metrics.get_summary()
        }
