import pandas as pd
import sqlalchemy
import time
import saeb.global_keys as global_keys

# Conectar ao banco de dados PostgreSQL
# Substitua 'username', 'password', 'host', 'port' e 'database' pelos valores reais de sua configuração
# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'

start_time = time.time()
engine = sqlalchemy.create_engine(
        f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
        pool_pre_ping=True
    )


alunos_em_computacao_manual = pd.read_excel('Computação Manual - 03.06.2024.xlsx', engine='openpyxl')



# Capturar a coluna 'estudante_id'
estudante_id_comp_manual = alunos_em_computacao_manual['codigo'].tolist()

#resultado_id_alunos_417 = alunos_em_computacao_manual['estudante_id'].tolist()

#procurar alunos em F_resultados
# Criar a query de seleção
query = f"SELECT * FROM {SCHEMA}.d_estudantes WHERE codigo IN ({','.join(map(lambda x: f'\'{x}\'', estudante_id_comp_manual))})"

# Executar a query de seleção e armazenar os resultados em um DataFrame
with engine.connect() as connection:
    result_df = pd.read_sql_query(query, connection)

print(result_df)

estudante_id = result_df['estudante_id'].to_list()

query2 = f"SELECT * FROM {SCHEMA}.f_resultados WHERE estudante_id IN ({','.join(map(lambda x: f'\'{x}\'', estudante_id))})"

# Executar a query de seleção e armazenar os resultados em um DataFrame
with engine.connect() as connection:
    result_df2 = pd.read_sql_query(query2, connection)
print(result_df2)
alternativas_escolhidas = result_df2['resultado_id'].to_list()

# Criar a query de deletar usando SQLAlchemy
query_del1 = sqlalchemy.text(f"DELETE FROM {SCHEMA}.d_estudantes WHERE estudante_id IN :ids")
query_del2 = sqlalchemy.text(f"DELETE FROM {SCHEMA}.f_resultados WHERE resultado_id IN :ids")
query_del3 = sqlalchemy.text(f"DELETE FROM {SCHEMA}.f_resultados_respostas WHERE resultado_id IN :ids")

# Executar as queries de deletar
with engine.connect() as connection:
    with connection.begin() as transaction:
        connection.execute(query_del1, {'ids': tuple(estudante_id)})
        connection.execute(query_del2, {'ids': tuple(alternativas_escolhidas)})
        connection.execute(query_del3, {'ids': tuple(alternativas_escolhidas)})
        transaction.commit()

print("Registros deletados com sucesso.")