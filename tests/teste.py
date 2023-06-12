import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage


def func():
    # Configurações do projeto GCP
    project_id = 'teste-gcp-py-chrystian'
    bucket_name = 'bucket-testetidados-leonel'
    folder_name = 'DRE'

    # Carrega as credenciais do GCP a partir do arquivo JSON
    credentials = service_account.Credentials.from_service_account_file('caminho/para/chave.json')

    # Conecta-se ao serviço de armazenamento do GCP
    storage_client = storage.Client(project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)

    # Obtém a lista de blobs (arquivos) dentro da pasta DRE
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name+'/')

    # Loop para processar cada arquivo
    for blob in blobs:
        # Faz o download do arquivo Excel sem baixar para a máquina local
        byte_content = blob.download_as_bytes()
        
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

import pandas as pd

# Ler o arquivo Excel
df = pd.read_excel('data/DRE 01-2022.xlsx')

# Identificar as colunas a serem normalizadas
colunas_a_normalizar = df.columns[(df.columns > 'Nome') & (df.columns < 'Total')]

# Copiar as colunas a serem normalizadas e adicionar uma coluna de índice
df_normalizado = df[colunas_a_normalizar].copy()
df_normalizado.insert(0, 'Index', df.index)

# Normalizar as colunas selecionadas
df_normalizado = df_normalizado.melt(id_vars='Index', var_name='Coluna', value_name='Valor')

# Remover as linhas com valores ausentes
df_normalizado = df_normalizado.dropna(subset=['Valor'])

# Combinar os dados normalizados com o DataFrame original
df_final = pd.concat([df[['Nome', 'Total']], df_normalizado], axis=1)
df_final.to_excel('results/teste2.xlsx')

# Exibir o resultado
print(df_final)

