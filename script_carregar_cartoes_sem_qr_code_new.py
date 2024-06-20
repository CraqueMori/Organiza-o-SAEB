import pandas as pd
import numpy as np
import math
import re
import sqlalchemy
import psycopg2

import api_data_process

import saeb.global_keys as global_keys
import saeb.global_aux as global_aux 

"""
Computação manual: Escolas.
"""

# global ---------------------------------------------------------------
CREDENTIALS = global_keys.get_database_credentials()
BASE_URL = global_keys.get_base_url('PROD')

SCHEMA = 'teste_09_04'
IF_EXISTS = 'append'

engine = sqlalchemy.create_engine(
        'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**CREDENTIALS),
        pool_pre_ping=True
    )


# functions
def read_excel(filepath):
    return pd.read_excel(filepath, skiprows=1)

def get_files(simulado):
    files = [
        'DDZ CENTRO-SUL.xlsx', 'DDZ LESTE 1.xlsx', 'DDZ LESTE 2.xlsx', 'DDZ NORTE.xlsx',
        'DDZ OESTE.xlsx', 'DDZ RURAL.xlsx', 'DDZ SUL.xlsx'
    ]
    dt = pd.DataFrame([])
    path = f"202304_nova_estrutura\cartoes_sem_qr_code\{simulado}"
    
    try:
        for file in files:
            dt_aux = read_excel(f"{path}/{file}")
            dt_aux.columns = dt_aux.columns.str.strip()
            dt = pd.concat([dt, dt_aux])
            dt = dt.reset_index(drop=True)
    except:
        file = 'PLANILHA ANÁLISE MASTER.xlsx'
        # file = 'PLANILHA ANÁLISE MASTER - testes.xlsx'
        dt_aux = read_excel(f"{path}/{file}")
        dt_aux.columns = dt_aux.columns.str.strip()
        dt = pd.concat([dt, dt_aux])
        dt = dt.reset_index(drop=True)

    return dt



def set_alternativa_id(dataframe):
    x = dataframe
    if x['respostas_omr_n_markedtargets'] == 0:
        rslt = f"{x['questao_id']}"
    elif x['respostas_omr_n_markedtargets'] == 1:
        rslt = f"{x['questao_id']}{x['respostas_resposta_letra']}"
    elif x['respostas_omr_n_markedtargets'] > 1:
        rslt = f"{x['questao_id']}N"
    else:
        rslt = None
    return rslt


#Alterar default para 0
def set_presenca_id(dataframe):
    x = dataframe
    if x['informacao_presenca'] in ['Presente','presente']:
        rslt = 0
    elif x['informacao_presenca'] in ['Faltou','ausencia']:
        rslt = 3
    elif x['informacao_presenca'] in ['Deficiente','deficiente']:
        rslt = 4
    elif x['informacao_presenca'] in ['Abandonou','abandono']:
        rslt = 77
    elif x['informacao_presenca'] in ['Transferido','transferencia']:
        rslt = 2
    else:
        rslt = None
    return rslt


def get_estudantes(data):
    d_estudantes_aux = pd.read_sql(
        sql=f"select * from {SCHEMA}.d_estudantes limit 5",
        con=engine
    )

    data_aux = data \
        .drop_duplicates('estudante_registro_id') \
        .reset_index(drop=True)
    
    # data_aux['codigo'] = None
    data_aux['sexo'] = None
    data_aux['telefone'] = None
    data_aux['data_nascimento'] = None
    data_aux['ano_matricula'] = None
    # data_aux['escola_id'] = None
    data_aux['escola_inep'] = None
    data_aux['estudante_inep'] = None
    data_aux['necessidades'] = None
    # data_aux['turno'] = None
    data_aux['curso_id'] = None

    data_aux = data_aux[d_estudantes_aux.columns]

    return data_aux


def get_resultados(data):
    f_resultados_aux = pd.read_sql(
        sql=f"select * from {SCHEMA}.f_resultados limit 5",
        con=engine
    )

    data_aux = data \
        .drop_duplicates('resultado_id') \
        .reset_index(drop=True)
    
    data_aux['curso_id'] = None
    data_aux['avaliacao_id'] = None
    data_aux['cartao_resposta'] = None
    data_aux['informacoes_presenca_markedtargets'] = None
    data_aux['informacoes_presenca_n_markedtargets'] = None
    data_aux['informacoes_presenca_one_markedtarget'] = None
    data_aux['deficiencia_id'] = None
    data_aux['codigos_deficiencia_markedtargets'] = None
    data_aux['codigos_deficiencia_n_markedtargets'] = None
    data_aux['codigos_deficiencia_one_markedtarget'] = None

    data_aux = data_aux[f_resultados_aux.columns]

    return data_aux


def get_resultados_respostas(data):
    f_resultados_respostas_aux = pd.read_sql(
        sql=f"select * from {SCHEMA}.f_resultados_respostas limit 5",
        con=engine
    )

    data_aux = data
    
    data_aux['data'] = None
    data_aux['pergunta_id'] = None
    data_aux['resposta_id'] = None
    data_aux['respostas_omr_markedtargets'] = None
    data_aux['respostas_omr_n_markedtargets'] = None
    data_aux['respostas_omr_one_markedtarget'] = None
    data_aux['resultado_resposta_registro_id'] = None

    data_aux = data_aux[f_resultados_respostas_aux.columns]

    return data_aux


def main(simulado):
    print('SCHEMA:', SCHEMA)
    print('IF_EXISTS:', IF_EXISTS)
    print()

    dt = get_files(simulado) \
        .astype({
            'fase':'str'
        })
    nro_questoes = int(re.search('\d.',dt.columns[-1]).group())+1


    ''' 
    Ponto central de mudanças, alterando os registros de Simulado_ID, Estudante Registro ID para bater com o padrão de 2024
    
    '''
    dt['simulado_id'] = dt \
        .apply(lambda x: f"{simulado[-1]}0"+re.search('\d',x['fase']).group() if pd.notnull(re.search('\d',x['fase'])) else f"{simulado[-1]}0",
               axis=1)
    #Estudante registro ID
    dt['estudante_registro_id'] = [f"sim0{simulado[-1]}-{i}" for i in range(230, 230 + len(dt))]
    dt['estudante_id'] = dt['estudante_registro_id']
    dt['resultado_id'] = dt['estudante_registro_id']
    dt['fase'] = dt['fase'].apply(lambda x: api_data_process.corrige_fase(x))


    variaveis_id_presenca = [
        'resultado_id', 'simulado_id', 'estudante_id',
        'estudante_registro_id', 'nome', 
        'distrito', 'turma', 'fase', 'escola_id',
        'codigo', 'turno'
    ]

    variaveis_id_questoes = [
        'resultado_id', 'simulado_id'
    ]

    # presencas
    dt_presenca = pd.melt(
        dt, 
        id_vars=variaveis_id_presenca, 
        value_vars=['Presente', 'Faltou', 'Deficiente', 'Abandonou', 'Transferido']
    )
    dt_presenca['informacao_presenca'] = np.where(pd.notnull(dt_presenca['value']), dt_presenca['variable'], None)
    dt_presenca.dropna(subset=['value'], inplace=True)
    dt_presenca = dt_presenca.drop(columns=['variable','value'])


    # questoes
    dt_questoes = pd.DataFrame([])
    
    for i in range(nro_questoes):
        num_questao = i + 1
        if i == 0:
            i = ''
        else:
            i = '.'+str(i)
        dt2 = pd.melt(
            dt, 
            id_vars=variaveis_id_questoes, 
            value_vars=['A'+i, 'B'+i, 'C'+i, 'D'+i]
        )
        dt2['nro_questao'] = num_questao
        dt_questoes = pd.concat([dt_questoes,dt2])
    dt_questoes['variable'] = dt_questoes['variable'].str.slice(0,1)

    dt_questoes['respostas_resposta_letra'] = np.where(pd.notnull(dt_questoes['value']), dt_questoes['variable'], '')
    dt_questoes = dt_questoes \
        .groupby(variaveis_id_questoes+['nro_questao']) \
        .agg({"respostas_resposta_letra": lambda x: list(x)}) \
        .reset_index()
    dt_questoes['respostas_resposta_letra'] = dt_questoes['respostas_resposta_letra'].apply(
        lambda x: list(filter(None, x))
    )
    dt_questoes['respostas_resposta_letra'] = dt_questoes['respostas_resposta_letra'].apply(
        lambda x: ','.join(x)
    )
    dt_questoes['respostas_omr_n_markedtargets'] = dt_questoes['respostas_resposta_letra'].apply(
        lambda x: math.ceil(len(x)/2)
    )

    dt_questoes['questao_id'] = dt_questoes \
        .apply(lambda x: f"{x['simulado_id']}0{x['nro_questao']}",
               axis=1)
    dt_questoes['alternativa_id'] = dt_questoes \
        .apply(lambda x: set_alternativa_id(x), 
               axis=1)

    
    # dt_final
    dt_final = pd.merge(
        dt_questoes, dt_presenca, how='left', on=variaveis_id_questoes
    ) \
        .astype({
            'escola_id':'str'
        })

    dt_final['presenca_id'] = dt_final \
        .apply(lambda x: set_presenca_id(x), axis=1)
    dt_final['presenca_id'] = dt_final['presenca_id'].astype('Int64')


    d_escolas = pd.read_sql(f"select escola_id_sigeam, escola_nome from {SCHEMA}.d_escolas", con=engine)
    dt_final = pd.merge(
        left=dt_final, right=d_escolas, how='left',
        left_on=['escola_id'], right_on=['escola_id_sigeam']
    ) \
        .rename(columns={
            'escola_nome':'escola'
        })
    dt_final['escola_id'] = dt_final['escola_id'].astype('float').astype('Int64')

    
    d_estudantes = get_estudantes(dt_final)
    global_aux.write2sql(
        dataframe=d_estudantes, 
        table_name='d_estudantes', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )


    f_resultados = get_resultados(dt_final)
    global_aux.write2sql(
        dataframe=f_resultados, 
        table_name='f_resultados', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )
    

    f_resultados_respostas = get_resultados_respostas(dt_final)
    global_aux.write2sql(
        dataframe=f_resultados_respostas, 
        table_name='f_resultados_respostas', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )

    return

if __name__ == '__main__':
    main("Simulado Teste")