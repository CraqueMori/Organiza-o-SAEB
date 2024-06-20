import pandas as pd
import time
import sqlalchemy
import sys 
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np
import re
import os

# Carregando funções redefinidas
importlib.reload(global_aux)

# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'saeb_2024_testes'
IF_EXISTS = 'replace'

start_time = time.time()
engine = sqlalchemy.create_engine(
    f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
    pool_pre_ping=True
)


# Leitura dos dados das duas folhas
df1 = pd.read_excel('export_elloi_4_ano.xlsx', sheet_name=0)
df2 = pd.read_excel('export_elloi_8_ano.xlsx', sheet_name=0)

# Adicionar colunas 'Media_HI' e 'Media_GE' na primeira DataFrame com valores zero
df1['Media_HI'] = 0
df1['Media_GE'] = 0

# Combinar as duas DataFrames
df_combined = pd.concat([df1, df2], ignore_index=True)


global_aux.write2sql(
        dataframe=df_combined, 
        table_name='d_dashboard_novo', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )
