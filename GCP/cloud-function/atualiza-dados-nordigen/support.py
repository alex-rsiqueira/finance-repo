import os
import json 
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = 'raw'

df = pd.DataFrame()

def log_error(project_id, dataset_id, table_id, error_code, message, desc_e):

    print(f"[ERROR] {desc_e} - {message}")

    log_table = pd.DataFrame(columns=['ingestion_dt', 'type', 'error_code', 'message', 'description', 'line_no', 'file_name', 'gateway', 'end_point', 'url', 'page'])
    ingestion_dt = datetime.now()
    new_line = [{'ingestion_dt': ingestion_dt, 'type': 'Error', 'error_code': error_code, 'message': message, 'description': desc_e, 'end_point':table_id }]#, 'line_no': linha_erro}]
    log_table = pd.concat([log_table, pd.DataFrame(new_line, index=[0])], ignore_index=True)
    
     # Configurar o nome completo da tabela
    log_table_name = 'tb_nordigen_ingestion_log'

    insert_db(log_table,log_table_name,dataset_id,project_id)

##ADICIONAR TESTE DE ERRO DE BANCO

def identify_error(table_id,e,dataset_id,project_id):
    
    print('Registrando erro: ',e)
    
    if isinstance(e, json.JSONDecodeError):
        desc_e= 'Erro de decodificação JSON'
        log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    elif isinstance(e, requests.HTTPError):
        desc_e= 'Erro de requisição HTTP'
        log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    # elif isinstance(e, pyodbc.Error):
    #     desc_e= 'Erro de banco'
    #     # status_code = response.status_code
    #     log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    # elif isinstance(e, requests.RequestException):
        # pag=-1
        # desc_e= 'Erro de excessão da classe request'
        # # status_code = e.response.status_code if e.response is not None else 'Desconhecido'
        # log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    else:
        desc_e= 'Erro desconhecido'
        log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)

def insert_db(df,table_id,dataset_id,project_id):
     # Configurar o cliente do BigQuery
    try:
        # bq_client = bigquery.Client(project=project_id)

        # Configurar o nome completo da tabela
        table_ref = f'{project_id}.{dataset_id}.{table_id}'

        # Inserir o DataFrame na tabela (cria a tabela se não existir, trunca se existir)
        pd.io.gbq.to_gbq(df, destination_table=table_ref, if_exists='replace', project_id=project_id)

        print(f"Tabela populada com sucesso: {table_id}")
    except Exception as e:
        identify_error(table_id,e,dataset_id,project_id)

    # identify_error(table_id,e,dataset_id,project_id)

def get_data(user_id,account,table_id):

    # Check if the table was already loaded today
    # bq_client = bigquery.Client(project=PROJECT_ID)
    # query_job = bq_client.query(f"""SELECT MAX(dtinsert) as last_load_date
    #                                 FROM `{PROJECT_ID}.raw.{table_id}`
    #                             """)
    # result = query_job.result()  # Waits for job to complete.
    # last_load_date = [dict(row) for row in query_job][0]['last_load_date']

    # if last_load_date < datetime.today().strftime('%Y-%m-%d'):

    if table_id == 'tb_nordigen_meta':
      
      # Fetch account metadata
      try:
        meta_data = account.get_metadata()
        meta_data = pd.json_normalize(meta_data)
        df = meta_data
        df['client_id'] = user_id
        df['dtinsert'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        df.rename(columns=lambda x: x.replace('.', '_'), inplace=True)
        insert_db(df,table_id,DATASET_ID,PROJECT_ID)

      except Exception as e:
        identify_error(table_id,e,DATASET_ID,PROJECT_ID)

    elif table_id =='tb_nordigen_details':
      
      # Fetch details
      try:
        details = account.get_details()
        details = pd.json_normalize(details['account'])
        df = details
        df['client_id'] = user_id
        df['dtinsert'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        df.rename(columns=lambda x: x.replace('.', '_'), inplace=True)
        insert_db(df,table_id,DATASET_ID,PROJECT_ID)

      except Exception as e:
        identify_error(table_id,e,DATASET_ID,PROJECT_ID)

    elif table_id =='tb_nordigen_balances':
      
      # Fetch balances
      try:
        balances = account.get_balances()
        balances = pd.json_normalize(balances['balances'])
        df = balances
        df['client_id'] = user_id
        df['dtinsert'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        df.rename(columns=lambda x: x.replace('.', '_'), inplace=True)
        insert_db(df,table_id,DATASET_ID,PROJECT_ID)

      except Exception as e:
        identify_error(table_id,e,DATASET_ID,PROJECT_ID)

    elif table_id =='tb_nordigen_transactions':
      
      # Fetch transactions
      try:
        transactions = account.get_transactions()
        transactions = pd.json_normalize(transactions['transactions']['booked'])

        df = transactions
        df['client_id'] = user_id
        df['dtinsert'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        df.rename(columns=lambda x: x.replace('.', '_'), inplace=True)
        insert_db(df,table_id,DATASET_ID,PROJECT_ID)

        # Filter transactions by specific date range
        # transactions = account.get_transactions(date_from="2022-10-01", date_to="2022-01-21")

      except Exception as e:
        identify_error(table_id,e,DATASET_ID,PROJECT_ID)