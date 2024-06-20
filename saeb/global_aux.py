import pandas as pd
from datetime import datetime
from tqdm.auto import tqdm
import sqlalchemy
#import pandas_gbq

import saeb.api_extractor as api_extractor


def get_ids_notnull(d_cursos, d_simulados, number_ids):
    
    curso_ids = []
    avaliacao_ids = []
    for i, simulado_id in enumerate(tqdm(d_simulados['simulado_id'][0:4])):
        nro_ids = 0
        for curso_id in d_cursos[d_cursos['simulado_id'] == simulado_id]['curso_id']:
            avaliacao_id = api_extractor.get_avaliacoes_curso(curso_id)['avaliacao_id'][0]
            resultado = api_extractor.get_resultados_curso_avaliacao(
                curso_id=curso_id, avaliacao_id=avaliacao_id
            )
            if len(resultado) > 0:
                curso_ids.append(curso_id)
                avaliacao_ids.append(avaliacao_id)
                
                nro_ids += 1

            if nro_ids == number_ids:
                break

    return curso_ids, avaliacao_ids


def write2sql(dataframe, table_name, database_connection, database_schema, if_exists):
    if if_exists in ('replace', 'append'):
        dataframe.to_sql(
            name=table_name, 
            con=database_connection, 
            schema=database_schema, 
            if_exists=if_exists, 
            index=False
        )

def delete_from_table(table_name, database_connection, database_schema, where=None):
    database_connection.execute(f" \
        delete from {database_schema}.{table_name} where {where}   \
    ")

    return

def copy_tables(from_schema, to_schema, engine):
    tabelas_lista = [
        'd_avaliacoes',
        'd_cursos',
        'd_deficiencia',
        'd_eixos_aux',
        'd_eixos_cognitivos',
        'd_escolas',
        'd_estudantes',
        'd_perguntas',
        'd_perguntas_respostas',
        'd_presenca',
        'd_questoes',
        'd_questoes_alternativas',
        'd_questoes_aux',
        'd_questoes_eixos_aux',
        'd_simulados',
        'f_resultados',
        'f_resultados_respostas'
    ]
    for i, tabela_nome in enumerate(tqdm(tabelas_lista)):
        # drop table (if exists)
        try:
            engine.execute(
                f"drop table {to_schema}.{tabela_nome}"
            )
        except:
            pass
        # create table
        engine.execute(
            f"create table {to_schema}.{tabela_nome} as select * from {from_schema}.{tabela_nome}"
        )
        
    if to_schema in ['schema_backup_prod','schema_backup_testes','schema_backup_dev','schema_backup']:
        tabela = pd.DataFrame(
            [
                [from_schema, datetime.today().strftime('%Y-%m-%d %H:%M:%S')],
            ],
            columns=['backup_schema', 'updated_date']
        )
        tabela_nome = 'backup_status'
        write2sql(
            dataframe=tabela, 
            table_name=tabela_nome, 
            database_connection=engine,
            database_schema=to_schema,
            if_exists='replace'
        )
    
    return


# GBQ
# def write2gbq(dataframe, table_name, database_connection, database_schema, if_exists):
#     if if_exists=='replace':
#         dataframe.to_sql(
#             name=table_name, 
#             con=database_connection, 
#             schema=database_schema, 
#             if_exists=if_exists, 
#             index=False
#         )
#     elif if_exists=='append':
#         dataframe.to_sql(
#             name=table_name, 
#             con=database_connection, 
#             schema=database_schema, 
#             if_exists=if_exists, 
#             index=False
#         )
#     else:
#         None
#     return