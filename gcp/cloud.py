from google.oauth2 import service_account
from google.cloud import storage, bigquery
import pandas as pd

def connection():
    key_path = 'Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json'
    credentials = service_account.Credentials.from_service_account_file(key_path)
    storage_client = storage.Client(credentials=credentials)
    bigquery_client = bigquery.Client(credentials=credentials)
    # Conecte-se ao serviço do Cloud Storage
    bucket_name = 'bucket-testetidados-chrystian'
    folder = 'dre/'
    bucket = storage_client.get_bucket(bucket_name)

    read_files(bucket, folder)

def read_files(bucket, folder):
    # Liste os arquivos dentro do bucket
    blobs = bucket.list_blobs(prefix=folder)

    # Itere sobre os blobs e imprima seus nomes
    for blob in blobs:
        # Verifica se é um arquivo Excel
        if blob.name.endswith('.xlsx'):
            # Obtém os bytes do blob
            blob_bytes = blob.download_as_bytes()

            # Lê o arquivo Excel diretamente a partir dos bytes
            df = pd.read_excel(blob_bytes)

            # Faz o processamento desejado com o dataframe
            # ...

            # Exemplo: imprime as primeiras linhas do dataframe
            print(df.head())


connection()