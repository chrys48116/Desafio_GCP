import os
import pandas as pd
import numpy as np

def read_excel_files(directory):
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


            # Cria a coluna Id da Conta
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
directory = 'data'

# Executa a função para ler os arquivos Excel
read_excel_files(directory)
