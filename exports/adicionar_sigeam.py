import pandas as pd
import time
import sqlalchemy
import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np
import re
import minerva.minerva as minerva

# Carregando funções redefinidas
importlib.reload(global_aux)

# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'
IF_EXISTS = 'replace'

start_time = time.time()
engine = sqlalchemy.create_engine(
    f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
    pool_pre_ping=True
)

# Carregar os dados do banco de dados
d_escolas_novo = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_escolas_novo;", engine)
d_dashboard = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_dashboard_teste;", engine)

# Verificar as colunas dos DataFrames
print(d_escolas_novo.columns)
print(d_dashboard.columns)

# Realizar o merge com base na coluna 'escola_nome'
merged_df = pd.merge(d_dashboard, d_escolas_novo[['escola_nome', 'escola_id_sigeam']], left_on='escola', right_on='escola_nome', how='left')

# Verificar as colunas do DataFrame resultante
print(merged_df.columns)

# Remover a coluna 'escola_nome' se necessário
merged_df = merged_df.drop(columns=['escola_nome'])

# Exibir o DataFrame resultante
print(merged_df.head())

global_aux.write2sql(dataframe=merged_df, table_name=f"d_dashboard_novo", database_connection=engine,
                                database_schema=SCHEMA, if_exists='replace')