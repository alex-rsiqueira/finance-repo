from pynubank.utils.certificate_generator import CertificateGenerator
from pynubank import Nubank, MockHttpClient, NuException
from support import read_secret, download_blob
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO
import pandas as pd
import numpy as np
import datetime
import base64
import json
import os

project_id = os.environ.get("PROJECT_ID")
bq_client = bigquery.Client(project=project_id)
storage_client = storage.Client()

def main(event_data, context):
    print(type(event_data),event_data)
    print(type(base64.b64decode(event_data["data"]).decode()),base64.b64decode(event_data["data"]).decode())

    # Get credentials
    username = base64.b64decode(event_data["data"]).decode()['username']
    password = read_secret(username)

    # Get certificate file from Cloud Storage
    cert_name = 'cert-' + str(username)[-5:] + '.p12'
    download_blob(cert_name)

    nu = Nubank()
    nu.authenticate_with_cert(username, password, '/tmp/'+cert_name)
    print('Nubank authentication - OK')
    
    client = nu.get_customer()
    del client['_links']
    del client['devices']
    del client['primary_device']
    del client['external_ids']
    del client['channels']

    # Convert dict to json
    client_json = json.dumps(client,ensure_ascii=False)

    df_client = pd.read_json(StringIO(client_json))
    dtinsert = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    df_client['dtinsert'] = dtinsert
    df_client = df_client.drop('documents',axis=1)
    #display(df_client)

    ## Insere registros do arquivo no BigQuery
    dataset = bq_client.dataset(dataset_id='raw').table('tb_nubank_client')
    table = bq_client.get_table(dataset)
    insert_row = bq_client.insert_rows_from_dataframe(table=table, dataframe=df_client)
    print(f"Tabela populada com sucesso: tb_nubank_client")

    client_id = df_client['id'][0]

    """
    ### Credit Card
    #### Retrieve credit card transactions
    """
    transactions = nu.get_card_statements()

    #Convert list to json
    transactions_json = json.dumps(transactions)

    """#### Create dataframe - df_credit"""
    df_credit = pd.read_json(StringIO(transactions_json))

    """#### Delete unnecessary fields and replace NaN values"""
    del df_credit['_links']
    #df_credit = df_credit.where(pd.notnull(df_credit), None)
    df_credit = df_credit.astype(object).replace(np.nan, 'None')

    """#### Insert dataframe into BigQuery"""
    dataset = bq_client.dataset(dataset_id='raw').table('tb_nubank_credit')
    table = bq_client.get_table(dataset)
    insert_row = bq_client.insert_rows_from_dataframe(table=table, dataframe=df_credit)
    print(f"Tabela populada com sucesso: tb_nubank_credit")

    """#### Retrieve bills information """
    # Lista de dicionários contendo todas as faturas do seu cartão de crédito
    bills = nu.get_bills()

    # Get last open bill on trusted layer
    query_job = bq_client.query(f"""SELECT COALESCE(MIN(bill_competence),'2000-01') bill_competence
                                    FROM `{project_id}.trusted.tb_nubank_bills`
                                    WHERE state = 'open'
                                    AND id_client = '{client_id}'
                                """)
    result = query_job.result()  # Waits for job to complete.
    last_open_bill = [dict(row) for row in query_job][0]['bill_competence']

    for i in range(0,len(bills)):
        bill_state = bills[i]['state']
        if bill_state != 'future':
            if bill_state in ('closed','overdue'):
                bill_href = bills[i]['_links']['self']['href']
                bill_id = bill_href[::-1][:bill_href[::-1].find('/')][::-1]
            elif bill_state == 'open':
                bill_href = bills[i]['_links']['self']['href']
                bill_id = bill_href[::-1][bill_href[::-1].find('sllib/')+6:bill_href[::-1].find('/stnuocca')][::-1]
            else:
                bill_id = None

            bill_competence = bills[i]['summary']['open_date'][:-3]

            if int(bill_competence.replace('-','')) >= int(last_open_bill.replace('-',''))-1:
                df_bills = pd.DataFrame(bills[i]['summary'], index=[0])
                df_bills = df_bills[['open_date','due_date','total_balance','minimum_payment']]
                df_bills['bill_id'] = bill_id
                df_bills['bill_competence'] = bill_competence
                df_bills['state'] = bill_state
                df_bills['id_client'] = client_id
                dtinsert = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                df_bills['dtinsert'] = dtinsert
                
                ## Insere registros no BigQuery
                dataset = bq_client.dataset(dataset_id='raw').table('tb_nubank_bills')
                table = bq_client.get_table(dataset)
                insert_row = bq_client.insert_rows_from_dataframe(table=table, dataframe=df_bills)
                print(f"Tabela populada com sucesso: tb_nubank_bills")

                bill_details = nu.get_bill_details(bills[i]) 

                # Cria dataframe com lançamentos de cada fatura
                df_bill_statements = pd.DataFrame(bill_details['bill']['line_items'])
                df_bill_statements['bill_id'] = bill_id
                df_bill_statements['bill_competence'] = bill_competence
                
                dtinsert = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                df_bill_statements['dtinsert'] = dtinsert
                df_bill_statements['id_client'] = client_id
                df_bill_statements = df_bill_statements.where(pd.notnull(df_bill_statements), None)
                df_bill_statements = df_bill_statements.astype(object).replace(np.nan, 'None')

                ## Insere registros no BigQuery
                dataset = bq_client.dataset(dataset_id='raw').table('tb_nubank_bill_statements')
                table = bq_client.get_table(dataset)
                insert_row = bq_client.insert_rows_from_dataframe(table=table, dataframe=df_bill_statements)
                print(f"Tabela populada com sucesso: tb_nubank_bill_statements")
        
        if bill_state == 'future':
            bill_id = None
            bill_competence = bills[i]['summary']['open_date'][:-3]

            df_bills = pd.DataFrame(bills[i]['summary'], index=[0])
            df_bills = df_bills[['open_date','due_date','total_balance','minimum_payment']]
            df_bills['bill_id'] = bill_id
            df_bills['bill_competence'] = bill_competence
            df_bills['state'] = bill_state
            dtinsert = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            df_bills['dtinsert'] = dtinsert
            df_bills['id_client'] = client_id
            
            ## Insere registros no BigQuery
            dataset = bq_client.dataset(dataset_id='raw').table('tb_nubank_bills')
            table = bq_client.get_table(dataset)
            insert_row = bq_client.insert_rows_from_dataframe(table=table, dataframe=df_bills)
            print(f"Tabela populada com sucesso: tb_nubank_bills")

    #"""#### Retrieve account transactions"""
    #account_statement = nu.get_account_feed()
    #account_statement_json = json.dumps(account_statement)

    #"""#### Create dataframe - df_account_statement"""
    #df_account_statement = pd.read_json(StringIO(account_statement_json))
    #
    #"""#### Treat dataframe"""
    ## rename column
    #df_account_statement = df_account_statement.rename(columns={'__typename': 'typename'})

    ## replace NaN values
    ##df_account_statement = df_account_statement.where(pd.notnull(df_account_statement), None)
    #df_account_statement = df_account_statement.astype(object).replace(np.nan, 'None')
    #print(df_account_statement)

    ## retrieve the name from the dict value
    #df_account_statement['originAccount'] = df_account_statement['originAccount'].apply(lambda x: x.get('name') if str(x) != 'None' else x)
    #df_account_statement['destinationAccount'] = df_account_statement['destinationAccount'].apply(lambda x: x.get('name') if str(x) != 'None' else x)

    #dtinsert = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    #df_account_statement['dtinsert'] = dtinsert
    #df_account_statement['id_client'] = client_id

    #"""#### Insert dataframe into BigQuery"""
    ### Insere registros do arquivo no BigQuery
    #bq_client = bigquery.Client(project='finances-314506')
    #dataset = bq_client.dataset(dataset_id='raw').table('tb_nubank_account')
    #table = bq_client.get_table(dataset)
    #insert_row = bq_client.insert_rows_from_dataframe(table=table, dataframe=df_account_statement)
    #print(f"Tabela populada com sucesso: tb_nubank_account")

    return 'Finished'
