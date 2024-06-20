# libraries
import pandas as pd
import time
import sqlalchemy
import psycopg2
import sys

import re
from bs4 import BeautifulSoup
import json
import html

import saeb.api_extractor as api_extractor
import api_data_process
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux 

import importlib
importlib.reload(api_data_process)


# global ---------------------------------------------------------------
CREDENTIALS = global_keys.get_database_credentials()
BASE_URL = global_keys.get_base_url('PROD')

SCHEMA = 'teste_scripts_06_04'
IF_EXISTS = 'replace'

def main(simulado):
    start_time = time.time()
    engine = sqlalchemy.create_engine(
        'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**CREDENTIALS),
        pool_pre_ping=True
    )


    # # Cursos
    # # d_cursos_raw = Dataframe de /GET CURSOS results.data
    d_cursos_raw = api_extractor.get_cursos(BASE_URL)
    d_cursos, d_simulados = api_data_process.process_cursos(d_cursos_raw)
    
    #d_cursos = pd.read_sql(f"select * from {SCHEMA}.d_cursos", con=engine)
    print('---- d_cursos ----')
    d_cursos = d_cursos[d_cursos['simulado'] == simulado].reset_index(drop=True)
    d_simulados = d_simulados[d_simulados['simulado'] == simulado].reset_index(drop=True)
   
    global_aux.write2sql(
        dataframe=d_cursos, 
        table_name='d_cursos', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )
    global_aux.write2sql(
        dataframe=d_simulados, 
        table_name='d_simulados', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )
 
    # Estudantes
    d_estudantes_raw = api_extractor.get_estudantes(BASE_URL, d_cursos)
    d_estudantes_raw = d_estudantes_raw[d_estudantes_raw.nome != 'LÍVIO CARVALHO']

    d_escolas = pd.read_sql(f"select * from {SCHEMA}.d_escolas_novo", con=engine)


    print("---------- escolas -------------")
    #d_escolas.to_excel("escolas.xlsx", engine="openpyxl")
    d_estudantes = api_data_process.process_estudantes(d_estudantes_raw, d_cursos, d_escolas)
    #d_estudantes = pd.read_sql(f"select * from {SCHEMA}.d_estudantes", con=engine)



    global_aux.write2sql(
        dataframe=d_estudantes, 
        table_name='d_estudantes', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )
    
    # Avaliações
    
    print("------------ avaliacoes -----------")
    d_avaliacoes_raw = api_extractor.get_avaliacoes(BASE_URL, d_cursos)

    # print(type(d_avaliacoes_raw))
    # print(d_avaliacoes_raw)
 
    # global_aux.write2sql(
    #     dataframe=d_avaliacoes_raw, 
    #     table_name='d_avaliacoes_raw', 
    #     database_connection=engine,
    #     database_schema=SCHEMA,
    #     if_exists=IF_EXISTS
    # )
 
    
    d_avaliacoes, d_perguntas_raw, d_perguntas_respostas_raw = api_data_process.process_avaliacoes(d_avaliacoes_raw, d_cursos)

    #d_avaliacoes, d_perguntas_raw, d_perguntas_respostas_raw =pd.read_sql(f"select * from {SCHEMA}.d_cursos", con=engine)

    #d_avaliacoes = pd.read_excel("avaliacoes.xlsx", sheet_name='Sheet1')
    #d_perguntas_raw = pd.read_excel("avaliacoes.xlsx", sheet_name='Sheet1')
    #d_perguntas_respostas_raw = pd.read_excel("avaliacoes.xlsx", sheet_name='Sheet1')


    # global_aux.write2sql(
    #     dataframe=d_avaliacoes, 
    #     table_name='d_avaliacoes', 
    #     database_connection=engine,
    #     database_schema=SCHEMA,
    #     if_exists=IF_EXISTS
    # )

    # Resultados

    # d_avaliacoes = pd.read_sql("select * from schema_para_dev.d_avaliacoes where simulado_id like '90%%'", con=engine) #provisorio
    f_resultados_raw = api_extractor.get_resultados(BASE_URL, d_avaliacoes)

    print(f_resultados_raw.head())
    f_resultados, f_resultados_respostas_raw = api_data_process.process_resultados(f_resultados_raw, d_cursos)
    print('process resultados')


    global_aux.write2sql(
        dataframe=f_resultados, 
        table_name='f_resultados', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )

    # perguntas / questoes
    print('PERGUNTAS / QUESTOES')

    print(d_perguntas_raw)
    print(f_resultados_respostas_raw)


    d_perguntas, d_questoes = api_data_process.process_perguntas(d_perguntas_raw, f_resultados_respostas_raw)

    
  
    global_aux.write2sql(
        dataframe=d_perguntas, 
        table_name='d_perguntas', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )
    global_aux.write2sql(
        dataframe=d_questoes, 
        table_name='d_questoes', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )


    # perguntas_respostas / questoes_alternativas
    print('PERGUNTAS_RESPOSTAS / QUESTOES_ALTERNATIVAS')
    d_perguntas_respostas, d_questoes_alternativas = api_data_process.process_perguntas_respostas(d_perguntas_respostas_raw, f_resultados_respostas_raw, d_perguntas)


    global_aux.write2sql(
        dataframe=d_perguntas_respostas, 
        table_name='d_perguntas_respostas', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )
    global_aux.write2sql(
        dataframe=d_questoes_alternativas, 
        table_name='d_questoes_alternativas', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )


    # resultados_respostas
    print('RESULTADOS_RESPOSTAS')
    d_perguntas = pd.read_sql("select * from teste_scripts_06_04.d_perguntas where simulado_id like '10%%'", con=engine) #provisorio
    d_perguntas_respostas = pd.read_sql("select * from teste_scripts_06_04.d_perguntas_respostas where simulado_id like '10%%'", con=engine) #provisorio
    f_resultados_respostas = api_data_process.process_resultados_respostas(f_resultados_respostas_raw, d_perguntas, d_perguntas_respostas)


    global_aux.write2sql(
        dataframe=f_resultados_respostas, 
        table_name='f_resultados_respostas', 
        database_connection=engine,
        database_schema=SCHEMA,
        if_exists=IF_EXISTS
    )


    end_time = time.time()

    print('CONCLUIDO')
    print(f'Tempo total do script: {round((end_time-start_time)/60, 4)} min')
    time.sleep(10)

    return


# execute script
if __name__ == '__main__':
    simulado = 'Simulado 1'
    print('Simulado:', simulado)
    print()
    main(simulado)