import pandas as pd
import time
import sqlalchemy
import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np
import re

# Carregando funções redefinidas
importlib.reload(global_aux)

# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'
SCHEMA2 = 'backup_saeb_2024'

start_time = time.time()
engine = sqlalchemy.create_engine(
    f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
    pool_pre_ping=True
)
engine2 = sqlalchemy.create_engine(
    f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
    pool_pre_ping=True
)

# Carregar os dados dos bancos de dados
d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)
d_estudantes_backup = pd.read_sql(f"SELECT * FROM {SCHEMA2}.d_estudantes;", engine2)

# Encontrar as linhas que estão em d_estudantes_backup mas não estão em d_estudantes baseado na coluna 'codigo'
df_diff = pd.merge(d_estudantes[['codigo']], d_estudantes_backup[['codigo']], on='codigo', how='outer', indicator=True)

# Filtrar apenas as linhas que estão em d_estudantes_backup mas não estão em d_estudantes
df_diff_in_estudantes_backup = df_diff[df_diff['_merge'] == 'right_only'][['codigo']]

# Recuperar todas as colunas das linhas que estão em d_estudantes_backup mas não estão em d_estudantes
alunos_faltantes = pd.merge(df_diff_in_estudantes_backup, d_estudantes_backup, on='codigo', how='left')

# Adicionar os alunos faltantes ao DataFrame d_estudantes
d_estudantes_atualizado = pd.concat([d_estudantes, alunos_faltantes], ignore_index=True)

# Exibir o DataFrame atualizado
print("DataFrame d_estudantes atualizado com alunos faltantes adicionados:")
print(d_estudantes_atualizado)

d_estudantes_atualizado.to_excel('validação_alunos.xlsx')

global_aux.write2sql(dataframe=d_estudantes_atualizado, table_name=f"d_estudantes", database_connection=engine,
                                database_schema=SCHEMA, if_exists='replace')
