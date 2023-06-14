import os
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import storage

project_name = "teste-gcp-py-chrystian"
bucket_name = "bucket-testetidados-chrystian_formove"
file_key = 'data\\teste-gcp-py-chrystian-f4cffb90d1ae.json'

credentials = service_account.Credentials.from_service_account_file(file_key)
# Cria uma instancia do client storage
storage_client = storage.Client(credentials=credentials, project=project_name)
# Pega o bucket desejado
bucket = storage_client.get_bucket(bucket_name)
# Lista os arquivos da pasta dre/
folder = 'dre/'
blobs = bucket.list_blobs(prefix=folder)

for file in blobs:
    print(file.name)
