from google.cloud import bigquery, storage
from datetime import datetime
from google.oauth2 import service_account
import pandas as pd
import io

# Configurar o cliente do BigQuery
file_key = 'Teste_GCP\\TESTE DO BIGQUERY & PYTHON\\data\\teste-gcp-py-chrystian-f4cffb90d1ae.json'
credentials = service_account.Credentials.from_service_account_file(file_key)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Nome do conjunto de dados
dataset_name = 'dw_chrystian'

# Criação das dimensões
def create_dimension_table(table_name, schema):
    table_id = f'{client.project}.{dataset_name}.{table_name}'
    table = bigquery.Table(table_id, schema=schema)
    #print(table)
    #table = client.delete_table(table, not_found_ok=True)
    table = client.create_table(table)
    print(f'Tabela {table.table_id} criada com sucesso.')

# Criação da tabela de fatos
def create_fact_table(table_name, schema):
    table_id = f'{client.project}.{dataset_name}.{table_name}'
    table = bigquery.Table(table_id, schema=schema)
    #table = client.delete_table(table, not_found_ok=True)
    table = client.create_table(table)
    print(f'Tabela {table.table_id} criada com sucesso.')

# Função para obter o ID incremental de uma dimensão
def get_dimension_id(dimension_name, value):
    # Consulta o BigQuery para obter o próximo ID disponível
    table_id = f'{client.project}.{dataset_name}.{dimension_name}'
    query = f"""
        SELECT MAX(Id) as MaxId
        FROM `{table_id}`
    """
    query_job = client.query(query)
    results = query_job.result()
    max_id = [row.MaxId for row in results][0]
    if max_id is None:
        return 1
    else:
        return max_id + 1

# Função para inserir registros em uma dimensão
def insert_dimension_row(table_name, values):
    # Obtém o próximo ID disponível
    dimension_id = get_dimension_id(table_name, values[0])
    
    # Insere o registro na dimensão
    row = [dimension_id] + values
    table_id = f'{client.project}.{dataset_name}.{table_name}'
    table = client.get_table(table_id)
    errors = client.insert_rows(table, [row])
    
    if errors == []:
        print(f'Registro inserido na tabela {table.table_id} com sucesso.')
    else:
        print(f'Erro ao inserir registro na tabela {table.table_id}.')

# Função para inserir registros na tabela de fatos
def insert_fact_row(dataref, valor, id_conta, id_unidade, id_tipo):
    table_id = f'{client.project}.{dataset_name}.fato'
    table = client.get_table(table_id)
    row = [id_conta, id_unidade, id_tipo, dataref, valor]
    errors = client.insert_rows(table, [row])

    if errors == []:
        print(f'Registro inserido na tabela {table.table_id} com sucesso.')
    else:
        print(f'Erro ao inserir registro na tabela {table.table_id}.')

# Esquema das tabelas
conta_schema = [
    bigquery.SchemaField('Id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('Nome', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Grupo', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Subgrupo', 'STRING', mode='REQUIRED')
]

unidade_schema = [
    bigquery.SchemaField('Id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('Unidade', 'STRING', mode='REQUIRED')
]

calendario_schema = [
    bigquery.SchemaField('Id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('DataRef', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('DiaDaSemana', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Mes', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Ano', 'INT64', mode='REQUIRED')
]

tipo_schema = [
    bigquery.SchemaField('Id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('Tipo', 'STRING', mode='REQUIRED')
]

fato_schema = [
    bigquery.SchemaField('IdConta', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('IdUnidade', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('IdTipo', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('DataRef', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('Valor', 'FLOAT64', mode='REQUIRED')
]


try:
    # Criar tabelas de dimensões
    create_dimension_table('conta', conta_schema)
    create_dimension_table('unidade', unidade_schema)
    create_dimension_table('calendario', calendario_schema)
    create_dimension_table('tipo', tipo_schema)

# Criar tabela de fatos
    create_fact_table('fato', fato_schema)
except Exception as e:
    print(e)

# Ler os arquivos CSV e inserir os registros nas tabelas correspondentes
file_key = 'Teste_GCP\\TESTE DO BIGQUERY & PYTHON\\data\\teste-gcp-py-chrystian-f4cffb90d1ae.json'
credentials = service_account.Credentials.from_service_account_file(file_key)
storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket_name = 'bucket-testetidados-chrystian_formove'
bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs(prefix='dre/')

for blob in blobs:
    if blob.name.endswith('.csv'):
        print("Lendo arquivo:", blob.name)
        filename = blob.name.split('/')[1]
        try:
            download = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(download))
            print(df.head())
        except Exception as e:
            print(e)
        
        # Inserir registros nas dimensões
        for _, row in df.iterrows():
            conta_id = get_dimension_id('conta', row['IdConta'])
            unidade_id = get_dimension_id('unidade', row['IdUnidade'])
            tipo_id = get_dimension_id('tipo', row['IdTipo'])
            
            insert_dimension_row('conta', [row['IdConta'], row['Nome'], row['Grupo'], row['Subgrupo']])
            insert_dimension_row('unidade', [row['IdUnidade'], row['Unidade']])
            insert_dimension_row('tipo', [row['IdTipo'], row['Tipo']])
            
            # Converter a coluna 'DataRef' para o formato de data do BigQuery
            dataref = datetime.strptime(row['DataRef'], '%Y-%m-%d').date()
            dia_da_semana = dataref.strftime('%A')
            mes = dataref.strftime('%B')
            ano = dataref.year
            
            insert_dimension_row('calendario', [dataref, dia_da_semana, mes, ano])
            
            # Inserir registro na tabela de fatos
            insert_fact_row(dataref, row['Valor'], conta_id, unidade_id, tipo_id)
