import os
from google.cloud import secretmanager
from google.cloud import storage

project_id = os.environ.get("PROJECT_ID")
bucket_name = os.environ.get("BUCKET")

def read_secret(secret_name):

    # Instantiate Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build secret path
    name = client.secret_version_path(project_id, secret_name, 'latest')

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