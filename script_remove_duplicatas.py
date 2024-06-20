import pandas as pd
import sqlalchemy
import psycopg2

import saeb.global_aux as global_aux
import saeb.global_keys as global_keys

CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'schema_para_dev'

def main(simulado):
    engine = sqlalchemy.create_engine(
        'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**CREDENTIALS),
        pool_pre_ping=True
    )

    print('REMOVENDO DUPLICATAS')
    print('Simulado: ', simulado)
    print('SCHEMA: ', SCHEMA)

    # Estudantes
    query_estudantes = f"""
        SELECT 
            estudante_id, nome, codigo as matricula, distrito, 
            escola_id, escola, fase, turma, turno, 
            curso_id, simulado_id,
            CONCAT(nome, '_', fase, '_', turma, '_', escola_id) AS estudante_id_chave,
            CONCAT(estudante_id,'_',curso_id) AS estudante_curso_id,
            CASE 
                WHEN estudante_id ~~ '%%sim%%' THEN 1 ELSE 0 
            END AS computacao_manual,
            ROW_NUMBER() OVER (
                PARTITION BY nome, escola, distrito, simulado_id
                ORDER BY nome
            ) AS num_registros
        FROM {SCHEMA}.d_estudantes
        WHERE 
            simulado_id ~~ ('%%{simulado[-1]}0%%') and 
            nome is not null
        ORDER BY 
            nome asc
    """

    d_estudantes = pd.read_sql(sql=query_estudantes, con=engine)

    # Resultados
    query_resultados = f"""
        SELECT 
            resultado_id, estudante_id,  
            curso_id, simulado_id,
            CONCAT(estudante_id,'_',curso_id) AS estudante_curso_id,
            CASE WHEN estudante_id ~~ '%%sim%%' THEN 1 ELSE 0 END AS computacao_manual
        FROM {SCHEMA}.f_resultados
        WHERE 
            simulado_id ~~ ('%%{simulado[-1]}0%%') and
            estudante_id in {tuple(d_estudantes['estudante_id'])}
        ORDER BY 
            resultado_id asc
    """
    f_resultados = pd.read_sql(sql=query_resultados, con=engine)
    
    # estudantes com curso id duplo
    d_estudantes_curso_id_duplicado = d_estudantes[d_estudantes['num_registros'] > 1]
    # d_estudantes_curso_id_duplicado.to_excel('d_estudantes_curso_id_duplicado.xlsx', index=False)

    # curso_id presente na d_estudantes mas nao na f_resultados (registro duplicado de sistema) # ERRATA: precisa fixar o estudante, pois o curso_id pode ter sido utilizado
    # este problema do curso_id será resolvido direto no sistema, depois é só puxar novamente os dados da API
    d_estudantes['estudante_curso_id_valido'] = 99
    d_estudantes.loc[~ d_estudantes['estudante_curso_id'].isin(f_resultados['estudante_curso_id']), 'estudante_curso_id_valido'] = 0 
    d_estudantes.loc[d_estudantes['estudante_curso_id'].isin(f_resultados['estudante_curso_id']), 'estudante_curso_id_valido'] = 1 
    # d_estudantes.to_excel('d_estudantes_curso_id_valido.xlsx', index=False)


    # isolando duplicatas sistema + manual (falta isolar f_resultados_respostas)
    estudante_id_chaves = d_estudantes[d_estudantes['computacao_manual']==1]['estudante_id_chave'].to_list()
    estudante_id_chaves = [s.strip() for s in estudante_id_chaves]
    d_estudantes_2 = d_estudantes[
        (d_estudantes['estudante_id_chave'].isin(estudante_id_chaves)) & 
        (d_estudantes['computacao_manual']==0)
    ]

    # query_estudantes2 = f"""
    #     SELECT 
    #         estudante_id, nome, distrito, 
    #         escola_id, escola, fase, turma, turno, 
    #         curso_id, simulado_id,
    #         CONCAT(nome, '_', fase, '_', turma, '_', escola_id) AS estudante_id_chave,
    #         CONCAT(estudante_id,'_',curso_id) as estudante_curso_id
    #     FROM {SCHEMA}.d_estudantes
    #     WHERE 
    #         simulado_id ~~ ('%%{simulado[-1]}0%%') and 
    #         CONCAT(estudante_id,'_',curso_id) in {tuple(d_estudantes_2['estudante_curso_id'])}
    #     ORDER BY 
    #         nome asc
    # """
    # d_estudantes2 = pd.read_sql(sql=query_estudantes2, con=engine)
    
    query_resultados2 = f"""
        SELECT 
            resultado_id, estudante_id,  
            curso_id, simulado_id,
            CONCAT(estudante_id,'_',curso_id) as estudante_curso_id
        FROM {SCHEMA}.f_resultados
        WHERE 
            simulado_id ~~ ('%%{simulado[-1]}0%%') and 
            CONCAT(estudante_id,'_',curso_id) in {tuple(d_estudantes_2['estudante_curso_id'])}
        ORDER BY 
            resultado_id asc
    """
    f_resultados2 = pd.read_sql(sql=query_resultados2, con=engine)

    # query_resultados_respostas = f"""
    #     SELECT 
    #         resultado_id, questao_id, alternativa_id,
    #         simulado_id
    #     FROM {SCHEMA}.f_resultados_respostas
    #     WHERE 
    #         simulado_id ~~ ('%%{simulado[-1]}0%%') and 
    #         resultado_id in {tuple(f_resultados2['resultado_id'])}
    #     ORDER BY 
    #         resultado_id asc
    # """
    # f_resultados_respostas = pd.read_sql(sql=query_resultados_respostas, con=engine)

    # Apagando duplicatas
    try:
        global_aux.delete_from_table(
            table_name='d_estudantes',
            database_connection=engine,
            database_schema=SCHEMA,
            where=f"""
                simulado_id ~~ ('%%{simulado[-1]}0%%') and 
                CONCAT(estudante_id,'_',curso_id) in {tuple(d_estudantes_2['estudante_curso_id'])}
            """
        )
    except Exception as e:
        print(f"ERRO: {e}")

    
    try:
        global_aux.delete_from_table(
            table_name='f_resultados',
            database_connection=engine,
            database_schema=SCHEMA,
            where=f"""
                simulado_id ~~ ('%%{simulado[-1]}0%%') and 
                CONCAT(estudante_id,'_',curso_id) in {tuple(d_estudantes_2['estudante_curso_id'])}
            """
        )
    except Exception as e:
        print(f"ERRO: {e}")


    try:
        global_aux.delete_from_table(
            table_name='f_resultados_respostas',
            database_connection=engine,
            database_schema=SCHEMA,
            where=f"""
                simulado_id ~~ ('%%{simulado[-1]}0%%') and 
                resultado_id in {tuple(f_resultados2['resultado_id'])}
            """
        )
    except Exception as e:
        print(f"ERRO: {e}")



if __name__ == '__main__':
    main()