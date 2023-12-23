import support
import connection

TABLES_ID = ['tb_nordigen_transactions', 'tb_nordigen_meta', 'tb_nordigen_balances', 'tb_nordigen_details']

def main(context):
    
    nordigen_accounts = connection.get_nordigen_accounts()

    for row in nordigen_accounts:
        user_id = row['id']
        secret_id = row['secret_ID']
        secret_key = connection.read_secret('Nordigen_' + secret_id)
        client, accounts = connection.get_nordigen_client(secret_id,secret_key)

        for i in range(0,len(accounts["accounts"])-1):
            account_id = accounts["accounts"][i]
            account = client.account_api(id=account_id)

            for table_id in TABLES_ID:
                print('<<>> Iniciando: ', table_id, ' <<>>')
                support.get_data(user_id, account, table_id)
                print('<<>> ',  table_id, ' Finalizada <<>>')
