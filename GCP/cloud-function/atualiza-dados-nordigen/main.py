import traceback
import uuid
from typing import Any, Dict, List

import connection
from config import config
from logger import logger, ProcessingMetrics
from nordigen_client import EnhancedNordigenClient
from db_manager import DatabaseManager
from data_processor import DataProcessor


def main(event: Dict[str, Any], context: Any) -> str:
    """
    Main entry point for Nordigen data ingestion Cloud Function

    Args:
        event: Cloud Function event data
        context: Cloud Function context

    Returns:
        Status message. Any failure raises, so the execution is reported as failed.
    """
    correlation_id = str(uuid.uuid4())
    logger.set_correlation_id(correlation_id)

    config.validate()

    db_manager = DatabaseManager()
    data_processor = DataProcessor(db_manager)
    metrics = ProcessingMetrics(logger)
    failures: List[str] = []

    logger.info("Starting Nordigen data ingestion", correlation_id=correlation_id, event=event)

    try:
        nordigen_accounts = connection.get_nordigen_accounts()

        if not nordigen_accounts:
            raise RuntimeError("No active Nordigen accounts found in trusted.tb_sheet_nordigen_account")

        accounts_to_process = []

        for account_config in nordigen_accounts:
            user_id = account_config["id"]

            try:
                secret_id = account_config["secret_ID"]
                secret_key = connection.read_secret(f"Nordigen_{secret_id}")
                client = EnhancedNordigenClient(secret_id, secret_key)

                for requisition in client.get_linked_requisitions():
                    for account_id in requisition["accounts"]:
                        accounts_to_process.append((user_id, client.get_account_api(account_id)))
                        logger.info("Added account to processing queue", user_id=user_id, account_id=account_id)

            except Exception as e:
                logger.error(
                    "Failed to initialize client for user",
                    user_id=user_id,
                    error=str(e),
                    traceback=traceback.format_exc()
                )
                db_manager.log_ingestion_event(
                    event_type="client_initialization_error",
                    status="error",
                    details={"user_id": user_id, "error": str(e), "error_type": type(e).__name__}
                )
                metrics.increment("accounts_failed")
                failures.append(f"user {user_id}: {e}")

        results = data_processor.process_accounts_parallel(accounts_to_process) if accounts_to_process else []

        summary = data_processor.get_processing_summary(results)
        logger.info("Processing completed", **summary)

        if config.enable_metrics:
            db_manager.log_metrics(summary["metrics"])

        failed_tables = sum(s["error"] for s in summary["table_statistics"].values())
        if summary["failed_accounts"] or failed_tables:
            failures.append(
                f"{summary['failed_accounts']} accounts and {failed_tables} tables failed, see {config.log_table}"
            )

        db_manager.log_ingestion_event(
            event_type="ingestion_complete",
            status="error" if failures else "success",
            details=summary
        )

        if failures:
            raise RuntimeError("Nordigen ingestion finished with errors: " + "; ".join(failures))

        return (
            f"Nordigen Ingestion Complete - "
            f"Processed: {summary['total_accounts']} accounts, "
            f"Successful: {summary['successful_accounts']}, "
            f"Failed: {summary['failed_accounts']}"
        )

    except Exception as e:
        logger.error(
            "Nordigen ingestion failed",
            error=str(e),
            traceback=traceback.format_exc()
        )
        db_manager.log_ingestion_event(
            event_type="fatal_error",
            status="error",
            details={"error": str(e), "error_type": type(e).__name__, "traceback": traceback.format_exc()}
        )
        raise

    finally:
        metrics.log_summary()


if __name__ == "__main__":
    # For local testing
    print(main(event={}, context=None))
