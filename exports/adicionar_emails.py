import pandas as pd
import time
import sqlalchemy
import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux

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
d_dashboards_novo = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_dashboard_novo;", engine)
d_escolas_novo = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_escolas_novo;", engine)

# Verificar as colunas dos DataFrames
print(d_dashboards_novo.columns)
print(d_escolas_novo.columns)

# Realizar o merge com base na coluna 'escola' em d_dashboards_novo e 'escola_nome' em d_escolas_novo
merged_df = pd.merge(d_dashboards_novo, d_escolas_novo[['escola_nome', "Email"]], left_on='escola', right_on='escola_nome', how='left')

# Criar a coluna 'email' no DataFrame d_dashboards_novo e preenchê-la com os valores de 'mail'
d_dashboards_novo['email'] = merged_df['Email']

# Verificar as colunas do DataFrame resultante
print(d_dashboards_novo.columns)

# Exibir o DataFrame resultante
print(d_dashboards_novo.head())

# Salvar o DataFrame atualizado no banco de dados
d_dashboards_novo.to_sql('d_dashboard_novo', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)

print(f"Tempo de execução: {time.time() - start_time} segundos")
