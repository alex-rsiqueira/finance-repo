import os
from google.cloud import secretmanager
from google.cloud import storage

project_id = os.environ.get("PROJECT_ID")
bucket_name = os.environ.get("BUCKET")

def read_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_name, 'latest')
    response = client.access_secret_version(request={"name": name})
    secret_value = response.payload.data.decode("UTF-8")

    return secret_value

def download_blob(source_blob_name):
    """Downloads a blob (file) from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(source_blob_name)