import pandas as pd
import time
import sqlalchemy

import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np


# Carregando funções redefinidas
importlib.reload(global_aux)

# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'
start_time = time.time()
engine = sqlalchemy.create_engine(
    f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
    pool_pre_ping=True
)



d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)
f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)


# Mesclar os DataFrames pelo estudante_id
merged_df = pd.merge(f_resultados, d_estudantes, on='estudante_id', how='inner')
merged_df.to_excel('dados com cartão.xlsx', engine='openpyxl')