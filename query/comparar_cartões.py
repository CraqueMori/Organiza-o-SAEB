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

start_time = time.time()
engine = sqlalchemy.create_engine(
        f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
        pool_pre_ping=True
    )

# Carregamento de dados
ids_desejados = ("2", "3", "4")
d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)
alunos_faltantes= pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados WHERE presenca_id IN {ids_desejados};", engine)
alunos_presentes = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados WHERE presenca_id = '0';", engine)
d_dashboard = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_dashboard_teste", engine)

print(alunos_presentes.head())
print(d_estudantes.head())
resultado_concatenado = pd.merge(alunos_presentes, d_estudantes, on='estudante_id', how='left').to_excel('dados_alunos_presentes.xlsx', engine='openpyxl')


#Emissão de Samples
# d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes LIMIT 5;", engine).to_csv('d_estudantes_sample.csv')
# alunos_faltantes_samples = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados WHERE presenca_id IN {ids_desejados} LIMIT 5;", engine).to_csv('f_resultados_faltosos_sample.csv')
# alunos_presentes_samples  = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados WHERE presenca_id = '0'LIMIT 5 ;", engine).to_csv('f_resultados_presentes_sample.csv')
# d_dashboard_samples  = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_dashboard_teste LIMIT 5", engine).to_csv('d_dashboard_sample.csv')




