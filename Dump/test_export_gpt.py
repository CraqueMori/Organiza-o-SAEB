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

def main(simulado):
    start_time = time.time()
    engine = sqlalchemy.create_engine(
        f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
        pool_pre_ping=True
    )

    # Carregamento de dados
    d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)
    f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)
    f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)

    d_estudantes = d_estudantes.drop(columns=['estudante_registro_id', 'sexo', 'telefone', 'necessidades', 'data_nascimento', 'escola_id', 'escola_inep', 'estudante_inep'])

    merged_df = pd.merge(d_estudantes, f_resultados[['estudante_id', 'resultado_id']], on='estudante_id', how='left')

    # Pivoteando os resultados das respostas
    resultado_pivot = f_resultados_respostas.pivot_table(index='resultado_id', columns='questao_id', values='alternativa_id', aggfunc='first').reset_index()

    # Renomeando colunas para incluir um prefixo e evitar conflitos de nome
    resultado_pivot.columns = ['resultado_id'] + [f'questao_{col}' for col in resultado_pivot.columns if col != 'resultado_id']

    # Merge do pivot com merged_df
    merged_df = pd.merge(merged_df, resultado_pivot, on='resultado_id', how='left')
    merged_df.to_excel('teste_comp2.xlsx', engine="openpyxl")
    # Mostrando o DataFrame final
    print(merged_df)

    end_time = time.time()
    print('CONCLUIDO')
    print(f'Tempo total do script: {round((end_time - start_time) / 60, 4)} min')

    # Se necessário, salve ou processe os dados
    # global_aux.write2sql(dataframe=merged_df, table_name='d_estudantes_expandido', database_connection=engine, database_schema=SCHEMA, if_exists=IF_EXISTS)

    return

if __name__ == '__main__':
    simulado = 'Simulado 1'
    print('Simulado:', simulado)
    main(simulado)
