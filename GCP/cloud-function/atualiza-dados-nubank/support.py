import os
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery

PROJECT_ID = os.environ.get("PROJECT_ID")
bucket_name = os.environ.get("BUCKET")

def get_nubank_accounts():
   
    bq_client = bigquery.Client(project=PROJECT_ID)
    query_job = bq_client.query(f"""SELECT b.id, person_ID
                                    FROM `{PROJECT_ID}.trusted.tb_sheet_nubank_account` a
                                    INNER JOIN `{PROJECT_ID}.refined.dim_client` b ON a.person_ID = b.cpf
                                    WHERE a.active_FLG = 1
                                """)
    result = query_job.result()  # Waits for job to complete.
    account_list = [dict(row) for row in query_job]
   
    return account_list

def read_secret(secret_name):

    # Instantiate Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build secret path
    name = client.secret_version_path(PROJECT_ID, secret_name, 'latest')

    # Get secret content
    response = client.access_secret_version(request={"name": name})

    # Decode secret content
    secret_value = response.payload.data.decode("UTF-8")

    return secret_value

def download_blob(source_blob_name):
    """Downloads a blob (file) from the bucket."""

    # Instantiate GCS client
    storage_client = storage.Client()

    # Connect to GCS bucket
    bucket = storage_client.bucket(bucket_name)

    # Download file content
    blob = bucket.blob(source_blob_name)

    # Get root path for GCF directory
    root = os.path.dirname(os.path.abspath(__file__))
    
    # Save file inside /tmp directory to avoid read-only error
    file_path = '/tmp/' + source_blob_name
    file_path = os.path.join(root, file_path)
    
    # Save file
    blob.download_to_filename(file_path)