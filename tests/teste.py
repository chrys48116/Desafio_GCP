import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage

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
    projeto_gcp = "teste-gcp-py-chrystian"
    bucket_nome = "bucket-testetidados-chrystian"
    credentials = service_account.Credentials.from_service_account_file('Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json')

    # Cria o cliente do Storage
    cliente_storage = storage.Client(project=projeto_gcp, credentials=credentials)

    # Obtém a referência para o bucket
    bucket = cliente_storage.get_bucket(bucket_nome)

    # Lista os arquivos dentro do diretório "dre"
    diretorio = "dre/"
    blobs = bucket.list_blobs(prefix=diretorio)

    # Loop para ler os arquivos Excel e carregar no BigQuery
    for blob in blobs:
        print(blob.name)
        # Verifica se o arquivo é um Excel (.xlsx)
        if blob.name.endswith(".xlsx"):
            # Lê o arquivo Excel sem baixar para a máquina local
            file_name = blob.name.split('/')[1]
            download = blob.download_to_filename('results\\' +file_name+ '.xlsx')

            blob_byte_range = blob.download_as_bytes(start=0, end=1024)  # Define a quantidade de bytes a serem lidos (1024 neste exemplo)
            df = pd.read_excel(blob_byte_range)

            
            
            # Carrega o DataFrame no BigQuery
            # tabela_ref = f"{projeto_bigquery}.{conjunto_dados}.{tabela_destino}"
            # job_config = bigquery.LoadJobConfig(destination=tabela_ref, write_disposition="WRITE_APPEND")
            # job = cliente_bigquery.load_table_from_dataframe(df, tabela_ref, job_config=job_config)
            # job.result()  # Aguarda a conclusão do job
            
            print(f"Arquivo {blob.name} carregado no BigQuery com sucesso!")

def read_excel(directory):
    # Lista os arquivos no diretório
    files = os.listdir(directory)

    # Loop para ler os arquivos Excel
    for file in files:
        print(f'Arquivos encontrados: {files}')
        if file.endswith('.xlsx'):
            # Caminho completo do arquivo
            file_path = os.path.join(directory, file)

            # Lê o arquivo Excel com pandas
            df = pd.read_excel(file_path)

            # Normaliza as colunas
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


            # Exemplo: imprime as primeiras linhas do dataframe
            final_df.to_excel(f'results/dados normalizados{file}.xlsx', index=False)
            print(final_df.head())


# Diretório onde os arquivos Excel estão localizados
#directory = 'data'

# Executa a função para ler os arquivos Excel
#read_excel(directory)

def connection():
    # Substitua o caminho pelo caminho para o seu arquivo JSON de chave de serviço
    path_to_keyfile = 'Teste_GCP\\Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json'

    # Crie um objeto cliente de armazenamento com base no arquivo de chave
    client = storage.Client.from_service_account_json(path_to_keyfile)

    # Listar os baldes existentes
    buckets = client.list_buckets()

    for bucket in buckets:
        print(bucket.name)
        if bucket.name == 'bucket-testetidados-chrystian':
            bucket_name = 'bucket-testetidados-chrystian'
            bucket = client.get_bucket(bucket_name)
            folder = 'dre/'

            blob = bucket.blob(folder)
            
    #blob.download_to_file('Teste_GCP\\data\\')
    # blob.upload_from_filename(file)

            blobs = bucket.list_blobs(delimiter='/', prefix=folder)
            for blob in blobs:
                print(blob.name)
                print(blob.size)
                if blob.size > 0:
                    blob.download_to_filename(blob.name)
                    print(blob.name)
                print(blob.public_url)
                print(blob.content_type)
#connection()