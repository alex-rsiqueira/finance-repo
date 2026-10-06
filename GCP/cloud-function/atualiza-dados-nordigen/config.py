import os
from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class Config:
    """Configuration management for Nordigen data ingestion"""

    # Environment
    project_id: str = field(default_factory=lambda: os.environ.get("PROJECT_ID", ""))
    dataset_id: str = "raw"
    environment: str = field(default_factory=lambda: os.environ.get("ENVIRONMENT", "development"))

    # Nordigen API
    nordigen_max_retries: int = 3
    nordigen_retry_delay: float = 1.0
    nordigen_rate_limit_per_second: int = 10

    # Processing
    max_workers: int = field(default_factory=lambda: int(os.environ.get("MAX_WORKERS", "4")))

    # Tables. Every column is STRING, as in the existing raw tables. "keys" identify a row
    # in the MERGE, "update_existing" decides whether a matched row is overwritten.
    tables: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        "tb_nordigen_transactions": {
            "columns": [
                "transactionId", "bookingDate", "valueDate", "remittanceInformationUnstructured",
                "internalTransactionId", "transactionAmount_amount", "transactionAmount_currency",
                "client_id", "dtinsert",
            ],
            "keys": ["internalTransactionId", "client_id"],
            "update_existing": False,
        },
        "tb_nordigen_meta": {
            "columns": [
                "id", "created", "last_accessed", "iban", "institution_id", "status",
                "owner_name", "bban", "client_id", "dtinsert",
            ],
            "keys": ["id", "client_id"],
            "update_existing": True,
        },
        "tb_nordigen_balances": {
            "columns": [
                "balanceType", "balanceAmount_amount", "balanceAmount_currency",
                "lastChangeDateTime", "client_id", "dtinsert",
            ],
            "keys": ["balanceType", "client_id"],
            "update_existing": True,
        },
        "tb_nordigen_details": {
            "columns": ["iban", "bban", "currency", "name", "cashAccountType", "bic", "client_id", "dtinsert"],
            "keys": ["iban", "client_id"],
            "update_existing": True,
        },
    })

    # Logging
    log_level: str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))
    log_table: str = "tb_nordigen_ingestion_log"
    metrics_table: str = "tb_nordigen_metrics"

    # Monitoring
    enable_metrics: bool = field(default_factory=lambda: os.environ.get("ENABLE_METRICS", "true").lower() == "true")

    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get configuration for a specific table"""
        return self.tables.get(table_name, {})

    def validate(self) -> None:
        """Validate required configuration"""
        if not self.project_id:
            raise ValueError("PROJECT_ID environment variable is required")

        if self.max_workers < 1:
            raise ValueError("MAX_WORKERS must be at least 1")


# Global config instance
config = Config()
