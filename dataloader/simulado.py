#TODO Criador de Simulado
import sys 
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import time
import sqlalchemy

import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import re
import numpy as np
import re
import pandas as pd


CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'saeb_2024_testes'
IF_EXISTS = 'append'

def main(simulado):
    start_time = time.time()
    engine = sqlalchemy.create_engine(
        f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
        pool_pre_ping=True
    )

    #d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes WHERE simulado_id IN (204, 208);", engine)
    d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes", engine)
    f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)
    f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)
    d_avaliacoes =  pd.read_sql(f"SELECT * FROM {SCHEMA}.d_avaliacoes;", engine)
    d_cursos = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_cursos;", engine)
    d_questoes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_questoes;", engine)
    d_questoes_alternativas = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_questoes_alternativas;", engine)
    d_questoes_eixos_aux = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_questoes_eixos_aux;", engine)
    d_simulados = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_simulados;", engine)


    #Tratamento de Troca de Simulado_ID
    d_avaliacoes['simulado_id'] = d_avaliacoes['simulado_id'].replace({'104': '204', '108': '208'})
    print(d_avaliacoes.head())
    d_estudantes['simulado_id']= d_estudantes['simulado_id'].replace({'104': '204', '108': '208'})
    f_resultados['simulado_id']= f_resultados['simulado_id'].replace({'104': '204', '108': '208'})
    f_resultados_respostas['simulado_id']= f_resultados_respostas['simulado_id'].replace({'104': '204', '108': '208'})
    d_avaliacoes ['simulado_id'] = d_avaliacoes ['simulado_id'].replace({"104": "204", "108": "208"})
    d_cursos['simulado_id']= d_cursos['simulado_id'].replace({'104': '204', '108': '208'})
    d_questoes['simulado_id']= d_questoes['simulado_id'].replace({'104': '204', '108': '208'})
    d_questoes_alternativas['simulado_id'] = d_questoes_alternativas['simulado_id'].replace({'104': '204', '108': '208'})
    d_questoes_eixos_aux['simulado_id'] = d_questoes_eixos_aux['simulado_id'].replace({'104': '204', '108': '208'})
    d_simulados['simulado_id'] = d_simulados['simulado_id'].replace({'104': '204', '108': '208'})

    d_avaliacoes.to_sql('d_avaliacoes', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    d_estudantes.to_sql('d_estudantes', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    f_resultados.to_sql('f_resultados', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    f_resultados_respostas.to_sql('f_resultados_respostas', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    d_cursos.to_sql('d_cursos', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    d_questoes.to_sql('d_questoes', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    d_questoes_alternativas.to_sql('d_questoes_alternativas', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    d_questoes_eixos_aux.to_sql('d_questoes_eixos_aux', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    d_simulados.to_sql('d_simulados', engine, schema=SCHEMA, if_exists=IF_EXISTS, index=False)
    
    return

if __name__ == '__main__':
    simulado = 'Simulado 1'
    print('Simulado:', simulado)
    main(simulado)