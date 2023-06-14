import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import storage

def processing_data(df):
    # Normaliza as colunas entre Nome e Total
    df_normalized = pd.melt(df, id_vars=['Nome', 'Total'], 
                                var_name='Mes e Unidade', 
                                value_name='Valor')
    
    # Extrai o mês e a loja e cria a coluna DataRef e Unidade
    df_normalized['Unidade'] = df_normalized['Mes e Unidade'].str.split('(').str[1]
    df_normalized['Unidade'] = df_normalized['Unidade'].str.replace(')', '')
    df_normalized['DataRef'] = df_normalized['Mes e Unidade'].str.split('(').str[0]
    df_normalized['DataRef'] = pd.to_datetime(df_normalized['DataRef'], format='%m/%Y')
    df_normalized['DataRef'] = df_normalized['DataRef'] + pd.DateOffset(days=1)
    df_normalized['DataRef'] = pd.to_datetime(df_normalized['DataRef'], format='%d/%m/%Y').dt.strftime('%m/%d/%Y')

    # Cria a coluna Id da Conta e trata ela
    df_normalized['IdConta'] = df_normalized['Nome'].str.split('-').str[0]
    df_normalized['IdConta'] = df_normalized['IdConta'].str.strip()
    df_normalized['IdConta'] = pd.to_numeric(df_normalized['IdConta']) if len(df_normalized['IdConta'])==1 else df_normalized['IdConta']
    
    # Identifica grupos, subgrupos e contas e os trata
    df_normalized['Tipo'] = ''
    for index, row in df_normalized.iterrows():
        id_conta = row['IdConta']
        tipo = ''
        if '.' not in id_conta:
            tipo = 'Grupo'
        elif id_conta + '.' not in df_normalized['IdConta'].iloc[index+1:]:
            tipo = 'Conta'
        else:
            tipo = 'Subgrupo'
        df_normalized.at[index, 'Tipo'] = tipo
    df_normalized['Grupo'] = np.where(df_normalized['Tipo'] == 'Grupo', True, False)
    df_normalized['Subgrupo'] = np.where(df_normalized['Tipo'] == 'Subgrupo', True, False)
    df_normalized['Conta'] = np.where(df_normalized['Tipo'] != 'Grupo', np.where(df_normalized['Tipo'] != 'Subgrupo', True, False), False)

    # Aplica trim no nome
    df_normalized['Nome'] = df_normalized['Nome'].str.strip()

    # Remove colunas em excesso
    final_df = df_normalized[['Nome', 'Valor', 'DataRef', 'Unidade', 'IdConta', 'Conta', 'Grupo', 'Subgrupo', 'Tipo']]
    return final_df


def upload_files(dataframe, filename):
    # Conectando com o bucket para envio dos arquivos
    project_name = "teste-gcp-py-chrystian"
    file_key = 'data\\teste-gcp-py-chrystian-f4cffb90d1ae.json'
    credentials = service_account.Credentials.from_service_account_file(file_key)
    storage_client = storage.Client(credentials=credentials, project=project_name)
    bucket_name = 'bucket-testetidados-chrystian_formove'
    bucket = storage_client.get_bucket(bucket_name)

    # Renomeando os arquivos para csv
    new_filename = filename.replace('.xlsx', '.csv')
    file_csv = bucket.blob('dre/' + new_filename)
    file_csv.upload_from_string(dataframe.to_csv(index=False), 'text/csv')
    print(f"Arquivo {new_filename} salvo e enviado com sucesso!" )


def read_files(files):
    # Loop para ler os arquivos Excel e tratar os dados
    for file in files:
        if file.name.endswith('.xlsx'):
            # Lê os nomes dos arquivos um por um
            filename = file.name.split('/')[1] 
            print(f'Arquivo: {filename}')
            # leituras dos arquivos sem baixar
            byte = file.download_as_bytes()
            df = pd.read_excel(byte)
            # Tratamento dos dados
            dataframe = processing_data(df)
            print(f'Tratamento de dados do arquivo {filename} finalizado')
            # Enviando arquivos para o novo bucket
            upload_files(dataframe, filename)


def connection(project_name, bucket_name, file_key):
    credentials = service_account.Credentials.from_service_account_file(file_key)
    # Cria uma instancia do client storage
    storage_client = storage.Client(credentials=credentials, project=project_name)
    # Pega o bucket desejado
    bucket = storage_client.get_bucket(bucket_name)
    # Lista os arquivos da pasta dre/
    folder = 'dre/'
    blobs = bucket.list_blobs(prefix=folder)

    read_files(blobs)

def init():
    project_name = "teste-gcp-py-chrystian"
    bucket_name = "bucket-testetidados-chrystian"
    file_key = 'data\\teste-gcp-py-chrystian-f4cffb90d1ae.json'

    try:
        connection(project_name, bucket_name, file_key)
        print('Processo finalizado!')

    except Exception as e:
        print(e)