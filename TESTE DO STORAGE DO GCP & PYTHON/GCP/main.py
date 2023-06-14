from funcoes_auxiliares import connection

if __name__ == '__main__':
    project_name = "teste-gcp-py-chrystian"
    bucket_name = "bucket-testetidados-chrystian"
    file_key = 'data\\teste-gcp-py-chrystian-f4cffb90d1ae.json'

    try:
        connection(project_name, bucket_name, file_key)
        print('Processo finalizado!')

    except Exception as e:
        print(e)