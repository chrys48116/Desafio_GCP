import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage, bigquery
import os


def func():
    # Configurações do projeto GCP
    project_id = 'teste-gcp-py-chrystian'
    bucket_name = 'bucket-testetidados-chrystian'
    folder_name = 'dre'

    # Carrega as credenciais do GCP a partir do arquivo JSON
    credentials = service_account.Credentials.from_service_account_file('Teste_GCP\\Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json')

    # Conecta-se ao serviço de armazenamento do GCP
    storage_client = storage.Client(project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)

    # Obtém a lista de blobs (arquivos) dentro da pasta DRE
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name+'/')

    # Loop para processar cada arquivo
    for blob in blobs:
        print(blob)
        print(blobs)
        print(blob.name)
        # Faz o download do arquivo Excel sem baixar para a máquina local
        byte_content = blob.download_as_bytes()
        print(byte_content)
        
        # Lê o arquivo Excel usando o pandas
        df = pd.read_excel(byte_content, engine='xlrd')
        
        # Identifica as colunas a serem normalizadas
        colunas_a_normalizar = df.columns[(df.columns > 'Nome') & (df.columns < 'Total')]

        # Normaliza as colunas selecionadas
        df_normalizado = pd.melt(df, id_vars=['Nome', 'Total'], value_vars=colunas_a_normalizar,
                                var_name='Coluna', value_name='Valor')

        # Extrai o mês e cria a coluna DataRef
        df_normalizado['DataRef'] = pd.to_datetime(df_normalizado['Coluna'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')

        # Extrai a unidade e cria a coluna Unidade
        df_normalizado['Unidade'] = df_normalizado['Coluna'].str.split().str[-1]

        # Cria a coluna Id da Conta
        df_normalizado['Id da Conta'] = df_normalizado['Nome'].str.split().str[0]

        # Identifica Grupos, Subgrupos e Contas
        df_normalizado['Grupo'] = df_normalizado['Id da Conta'].str.contains('.', regex=False)
        df_normalizado['Conta'] = df_normalizado['Id da Conta'].apply(lambda x: not df_normalizado['Id da Conta'].str.startswith(x + '.', na=False).any())
        df_normalizado['Subgrupo'] = ~(df_normalizado['Grupo'] | df_normalizado['Conta'])

        # Aplica o trim na coluna Nome
        df_normalizado['Nome'] = df_normalizado['Nome'].str.strip()

        # Remove colunas em excesso
        df_final = df_normalizado[['IdConta', 'Grupo', 'Subgrupo', 'Conta', 'Nome', 'DataRef', 'Unidade', 'Valor']]

        # Salva o arquivo transformado em formato CSV
        csv_bytes = df_final.to_csv(index=False).encode()
        new_blob_name = blob.name.replace('.xlsx', '.csv')
        new_blob = bucket.blob(new_blob_name)
        new_blob.upload_from_string(csv_bytes, content_type='text/csv')

        print(f"Arquivo transformado '{new_blob_name}' salvo com sucesso no bucket.")

    print("Processamento concluído.")

def teste():
    # query = """
    # select * from `bucket-testetidados-chrystian.dre`
    # """
    # credentials = service_account.Credentials.from_service_account_file(filename='Teste_GCP\\Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json',
    #                                                                     scopes=['https://www.googleapis.com/auth/cloud-platform'])
    # pd.read_gbq(query=query, credentials=credentials)
    # Configurações do projeto e do bucket
    projeto_gcp = "teste-gcp-py-chrystian"
    bucket_nome = "bucket-testetidados-chrystian"
    credentials = service_account.Credentials.from_service_account_file('Teste_GCP\\Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json')

    # Cria o cliente do Storage
    cliente_storage = storage.Client(project=projeto_gcp, credentials=credentials)

    # Obtém a referência para o bucket
    bucket = cliente_storage.get_bucket(bucket_nome)

    # Lista os arquivos dentro do diretório "dre"
    diretorio = "dre/"
    blobs = bucket.list_blobs(prefix=diretorio)

    # Configurações do BigQuery
    projeto_bigquery = "teste-gcp-py-chrystian"
    conjunto_dados = "dw_chrystian"
    tabela_destino = "nome_tabela_destino"

    # Cria o cliente do BigQuery
    cliente_bigquery = bigquery.Client(project=projeto_bigquery, credentials=credentials)

    # Loop para ler os arquivos Excel e carregar no BigQuery
    for blob in blobs:
        print(blob.name)
        print(blob)
        # Verifica se o arquivo é um Excel (.xlsx)
        if blob.name.endswith(".xlsx"):
            # Lê o arquivo Excel sem baixar para a máquina local
            blob_byte_range = blob.download_as_bytes(start=0, end=1024)  # Define a quantidade de bytes a serem lidos (1024 neste exemplo)
            df = pd.read_excel(blob_byte_range)
            
            # Carrega o DataFrame no BigQuery
            tabela_ref = f"{projeto_bigquery}.{conjunto_dados}.{tabela_destino}"
            job_config = bigquery.LoadJobConfig(destination=tabela_ref, write_disposition="WRITE_APPEND")
            job = cliente_bigquery.load_table_from_dataframe(df, tabela_ref, job_config=job_config)
            job.result()  # Aguarda a conclusão do job
            
            print(f"Arquivo {blob.name} carregado no BigQuery com sucesso!")


teste()