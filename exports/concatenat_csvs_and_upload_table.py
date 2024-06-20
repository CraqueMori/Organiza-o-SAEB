import pandas as pd
import os
import saeb.global_aux as global_aux
import saeb.global_keys as global_keys
import time
import sqlalchemy

CREDENTIALS = global_keys.get_database_credentials()
BASE_URL = global_keys.get_base_url('PROD')

SCHEMA = 'teste_scripts_06_04'
IF_EXISTS = 'replace'

start_time = time.time()
engine = sqlalchemy.create_engine(
    'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**CREDENTIALS),
    pool_pre_ping=True
)

directory = 'Export_10_05'
# Lista para armazenar cada DataFrame lido
lista_dfs = []

# Loop para ler cada arquivo CSV na pasta
for arquivo in os.listdir(directory):
    caminho_completo = os.path.join(directory, arquivo)
    if arquivo.endswith('.csv'):
        df = pd.read_csv(caminho_completo)
        print(arquivo)
        lista_dfs.append(df)

# Concatena todos os DataFrames na lista
df_concatenado = pd.concat(lista_dfs, ignore_index=True)
print(df_concatenado)

global_aux.write2sql(
        dataframe=df_concatenado, 
        table_name='d_dashboard_teste', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )

end_time = time.time()
print('CONCLUIDO')
print(f'Tempo total do script: {round((end_time-start_time)/60, 4)} min')
time.sleep(10)
print(start_time)

# Salva o DataFrame concatenado em um novo arquivo CSV
# df_concatenado.to_csv('dados_concatenados.csv', index=False)

# df_simulado_104 = df_concatenado[df_concatenado['Ano'] == "4 ANO"]
# df_simulado_108 = df_concatenado[df_concatenado['Ano'] == "8 ANO"]

# df_simulado_104.to_excel('dados_4_ano.xlsx', engine="openpyxl")
# df_simulado_108.to_excel('dados_8_ano.xlsx', engine="openpyxl")