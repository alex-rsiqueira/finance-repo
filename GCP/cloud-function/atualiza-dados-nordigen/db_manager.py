import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd
from google.cloud import bigquery

from config import config
from logger import logger


class DatabaseManager:
    """Manage BigQuery loads (MERGE on the table keys) and the ingestion log"""

    def __init__(self, project_id: str = None):
        self.project_id = project_id or config.project_id
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_id = config.dataset_id

    def upsert_dataframe(self, df: pd.DataFrame, table_id: str) -> Dict[str, Any]:
        """
        Merge a DataFrame into a raw table using the keys set in config.tables.
        Columns that are not part of the table are dropped, missing ones are loaded as NULL.
        """
        table_config = config.get_table_config(table_id)
        columns = table_config["columns"]
        keys = table_config["keys"]

        dropped = [c for c in df.columns if c not in columns]
        if dropped:
            logger.warning(f"Columns not in {table_id} were ignored", table=table_id, columns=dropped)

        df = df.reindex(columns=columns).astype("string")

        if df[keys].isna().any(axis=None):
            raise ValueError(f"Rows without a value for the key columns {keys} in {table_id}")

        df = df.drop_duplicates(subset=keys)

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        temp_ref = f"{table_ref}_tmp_{uuid.uuid4().hex[:8]}"

        join_condition = " AND ".join(f"target.`{k}` = source.`{k}`" for k in keys)
        matched = ""
        if table_config["update_existing"]:
            update_set = ", ".join(f"target.`{c}` = source.`{c}`" for c in columns if c not in keys)
            matched = f"WHEN MATCHED THEN UPDATE SET {update_set}"
        insert_columns = ", ".join(f"`{c}`" for c in columns)
        insert_values = ", ".join(f"source.`{c}`" for c in columns)

        merge_query = f"""
        MERGE `{table_ref}` target
        USING `{temp_ref}` source
        ON {join_condition}
        {matched}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """

        try:
            load_config = bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField(c, "STRING") for c in columns],
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            self.client.load_table_from_dataframe(df, temp_ref, job_config=load_config).result()

            merge_job = self.client.query(merge_query)
            merge_job.result()
        finally:
            self.client.delete_table(temp_ref, not_found_ok=True)

        result = {
            "status": "success",
            "rows_processed": len(df),
            "rows_written": merge_job.num_dml_affected_rows
        }

        logger.info(f"Data merged into {table_id}", table=table_id, **result)

        return result

    def log_ingestion_event(self, event_type: str, status: str, details: Dict[str, Any]):
        """Append an event to the ingestion log table, using its existing columns"""
        row = {
            "ingestion_dt": datetime.now(timezone.utc).isoformat(),
            "type": "Error" if status == "error" else "Info",
            "error_code": event_type,
            "message": str(details.get("error", status))[:1000],
            "description": json.dumps(details, default=str)[:5000],
            "end_point": details.get("table_id"),
        }

        table_ref = f"{self.project_id}.{self.dataset_id}.{config.log_table}"

        try:
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            self.client.load_table_from_json([row], table_ref, job_config=job_config).result()
        except Exception as e:
            logger.error("Failed to log ingestion event", error=str(e), event_type=event_type)

    def log_metrics(self, metrics: Dict[str, Any]):
        """Log processing metrics"""
        metrics_data = pd.DataFrame([{
            "timestamp": datetime.now(),
            "project_id": self.project_id,
            "environment": config.environment,
            **metrics
        }])

        table_ref = f"{self.project_id}.{self.dataset_id}.{config.metrics_table}"

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            self.client.load_table_from_dataframe(metrics_data, table_ref, job_config=job_config).result()
        except Exception as e:
            logger.error("Failed to log metrics", error=str(e))
