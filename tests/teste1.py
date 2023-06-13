from google.cloud import storage
def connection():
    # Substitua o caminho pelo caminho para o seu arquivo JSON de chave de serviço
    path_to_keyfile = 'Teste_GCP\Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json'

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
connection()

def delete():
    path_to_keyfile = 'Teste_GCP\Arquivos de Apoio\\teste-gcp-py-chrystian-f4cffb90d1ae.json'

    bucket_name = 'bucket-testetidados-chrystian'
    # Crie um objeto cliente de armazenamento com base no arquivo de chave
    client = storage.Client.from_service_account_json(path_to_keyfile)
    bucket = client.get_bucket(bucket_name)
    folder = 'dre/'
    blob = bucket.blob(folder)
    blob.delete()
