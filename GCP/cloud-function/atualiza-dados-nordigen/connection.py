from typing import List, Dict, Any

from google.cloud import bigquery, secretmanager

from config import config
from logger import logger, log_execution

PROJECT_ID = config.project_id


@log_execution(logger)
def get_nordigen_accounts() -> List[Dict[str, Any]]:
    """
    Get list of Nordigen accounts to process from BigQuery

    Returns:
        List of account configurations (client id, person id, secret id)
    """
    bq_client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT b.id, person_ID, secret_ID
    FROM `{PROJECT_ID}.trusted.tb_sheet_nordigen_account` a
    INNER JOIN `{PROJECT_ID}.refined.dim_client` b ON a.person_ID = b.cpf
    WHERE a.active_FLG = 1
    """

    logger.info("Fetching Nordigen accounts from BigQuery")

    secret_list = [dict(row) for row in bq_client.query(query).result()]

    logger.info(
        f"Found {len(secret_list)} active Nordigen accounts",
        accounts_count=len(secret_list)
    )

    return secret_list


@log_execution(logger)
def read_secret(secret_name: str) -> str:
    """
    Read secret from Google Secret Manager

    Args:
        secret_name: Name of the secret to read

    Returns:
        Secret value as string
    """
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(PROJECT_ID, secret_name, "latest")

    logger.info("Reading secret from Secret Manager", secret_name=secret_name)

    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode("UTF-8")
