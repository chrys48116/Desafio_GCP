from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# Configurações do projeto e do conjunto de dados
project_id = 'seu-id-do-projeto'
dataset_id = 'seu-id-do-conjunto-de-dados'

# Caminho para o arquivo de credenciais do Google Cloud
credentials_path = '/caminho/para/arquivo-de-credenciais.json'

# Criar cliente do BigQuery e do Storage
credentials = service_account.Credentials.from_service_account_file(credentials_path)
bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
storage_client = storage.Client(project=project_id, credentials=credentials)

# Função para criar uma tabela de dimensão com ID incremental
def create_dimension_table(table_name, schema):
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table)
    print(f'Tabela {table.table_id} criada com sucesso.')

# Função para criar a tabela de fatos
def create_fact_table(table_name, schema):
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table)
    print(f'Tabela {table.table_id} criada com sucesso.')

# Função para carregar um arquivo CSV em uma tabela do BigQuery
def load_csv_to_table(table_name, file_path):
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    table_ref = bigquery_client.dataset(dataset_id).table(table_name)
    job_config = bigquery.LoadJobConfig(
        schema=table_ref.schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )
    with open(file_path, 'rb') as source_file:
        job = bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()  # Aguarda a conclusão do carregamento do arquivo
    print(f'Arquivo {file_path} carregado na tabela {table_id} com sucesso.')

# Função para gerar IDs incrementais para uma tabela
def generate_incremental_ids(table_name):
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    query = f'''
        CREATE OR REPLACE TABLE {table_id} AS
        SELECT *, ROW_NUMBER() OVER(ORDER BY Id) AS IncrementalId
        FROM {table_id}
    '''
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_id
    job = bigquery_client.query(query, job_config=job_config)
    job.result()  # Aguarda a conclusão da geração de IDs
    print(f'IDs incrementais gerados para a tabela {table_id}.')

# Função para criar as tabelas de dimensões e a tabela de fatos
def create_star_schema():
    # Criar tabela de dimensão 'Conta'
    schema_conta = [
        bigquery.SchemaField('Id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('Nome', 'STRING', mode='REQUIRED')
    ]
    create_dimension_table('conta', schema_conta)

    # Criar tabela de dimensão 'Unidade'
    schema_unidade = [
        bigquery.SchemaField('Id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('Unidade', 'STRING', mode='REQUIRED')
    ]
    create_dimension_table('unidade', schema_unidade)

    # Criar tabela de dimensão 'Calendario'
    schema_calendario = [
        bigquery.SchemaField('DataRef', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('DiaDaSemana', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('Mes', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('Ano', 'INT64', mode='REQUIRED')
    ]
    create_dimension_table('calendario', schema_calendario)

    # Criar tabela de dimensão 'Tipo'
    schema_tipo = [
        bigquery.SchemaField('Id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('Tipo', 'STRING', mode='REQUIRED')
    ]
    create_dimension_table('tipo', schema_tipo)

    # Criar tabela de fatos 'Fato'
    schema_fato = [
        bigquery.SchemaField('IdConta', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('IdUnidade', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('IdTipo', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('DataRef', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('Valor', 'FLOAT64', mode='REQUIRED')
    ]
    create_fact_table('fato', schema_fato)

    # Carregar arquivos CSV nas tabelas
    load_csv_to_table('conta', '/caminho/para/arquivo-conta.csv')
    load_csv_to_table('unidade', '/caminho/para/arquivo-unidade.csv')
    load_csv_to_table('calendario', '/caminho/para/arquivo-calendario.csv')
    load_csv_to_table('tipo', '/caminho/para/arquivo-tipo.csv')
    load_csv_to_table('fato', '/caminho/para/arquivo-fato.csv')

    # Gerar IDs incrementais para as tabelas de dimensões
    generate_incremental_ids('conta')
    generate_incremental_ids('unidade')
    generate_incremental_ids('calendario')
    generate_incremental_ids('tipo')

# Executar a criação do esquema de estrela
create_star_schema()
