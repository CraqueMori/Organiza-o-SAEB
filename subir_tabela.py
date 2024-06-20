import pandas as pd
import sqlalchemy
import psycopg2

import saeb.global_aux as global_aux
import saeb.global_keys as global_keys

CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'
IF_EXISTS = 'replace'

engine = sqlalchemy.create_engine(
    'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**CREDENTIALS),
    pool_pre_ping=True
)



d_dashboard = pd.read_excel('d_dashboard.xlsx', engine="openpyxl")
f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)


# Realizar o merge das tabelas com base na coluna comum resultado_id
d_dashboard_merged = pd.merge(d_dashboard, f_resultados[['resultado_id', 'cartao_resposta']], on='resultado_id', how='left')

# Verificar se a coluna foi adicionada corretamente
print(d_dashboard_merged.head())
d_dashboard_merged.rename(columns={
    'cartao_resposta': 'Cartão'
})


# Salvar o DataFrame atualizado de volta para um arquivo Excel, se necessário
global_aux.write2sql(dataframe=d_dashboard_merged, table_name='d_dashboard_teste', database_connection=engine, database_schema=SCHEMA, if_exists=IF_EXISTS)

