import os
from uuid import uuid4
from google.cloud import bigquery
from nordigen import NordigenClient
from google.cloud import secretmanager

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = 'raw'

def get_nordigen_accounts():
   
    bq_client = bigquery.Client(project=PROJECT_ID)
    query_job = bq_client.query(f"""SELECT b.id, person_ID, secret_ID
                                    FROM `{PROJECT_ID}.trusted.tb_sheet_nordigen_account` a
                                    INNER JOIN `{PROJECT_ID}.refined.dim_client` b ON a.person_ID = b.cpf
                                    WHERE a.active_FLG = 1
                                """)
    result = query_job.result()  # Waits for job to complete.
    secret_list = [dict(row) for row in query_job]
   
    return secret_list

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

def get_nordigen_client(secret_id,secret_key):

    # initialize Nordigen client and pass SECRET_ID and SECRET_KEY
    print('Initialize Nordigen client')
    client = NordigenClient(
        secret_id=secret_id,
        secret_key=secret_key
    )

    # Create new access and refresh token
    # Parameters can be loaded from .env or passed as a string
    # Note: access_token is automatically injected to other requests after you successfully obtain it
    print('Generate token')
    token_data = client.generate_token()

    # Exchange refresh token for new access token
    new_token = client.exchange_token(token_data["refresh"])

    # Get existing requisitions
    print('Check for existing requisitions')
    requisitions = client.requisition.get_requisitions()

    if not any([x['status'] == 'LN' for x in requisitions['results']]): # Check if any existing requisition has the status LN - Linked

        print('Open new requisition')

        # Get institution id by bank name and country
        institution_id = client.institution.get_institution_id_by_name(
            country="PT",
            institution="Millennium BCP"
        )

        # Initialize bank session
        print('Initialize bank session')
        init = client.initialize_session(
            # institution id
            institution_id=institution_id,
            # redirect url after successful authentication
            redirect_uri="https://gocardless.com",
            # additional layer of unique ID defined by you
            reference_id=str(uuid4())
        )

        # Get account id after you have completed authorization with a bank
        # requisition_id can be gathered from initialize_session response
        print('Get new requisition ID')
        accounts = client.requisition.get_requisition_by_id(
            requisition_id=init.requisition_id
        )

    else:

        print('Get active requisition ID')
        # Get account list from last authenticated requisition
        accounts = client.requisition.get_requisition_by_id(
            requisition_id = [x['id'] for x in requisitions['results'] if x['status'] == 'LN'][0]
        )

    return client, accounts
