import pandas as pd
import re
from bs4 import BeautifulSoup
import json
import html


# cursos e simulados
def extract_simulado_curso(txt):
    extraction = re.search(r'([\d].* [\w]imulado - [\d].* (ano|xpt))|([\d].* [\w]imulado [\d].* (ano|xpt))', 
        txt, flags=re.IGNORECASE)
    rslt = extraction.group().title() if pd.notnull(extraction) else None
    return rslt

    
    
def process_cursos(cursos):
    cursos['titulo'] = cursos['titulo'] \
        .replace('1° Simulado 5° Ano', '1° Simulado - 5° Ano') \
        .replace('º', '°') \
        .replace('E.M. SAO PEDRO -RIBEIRINHA_5° ano A', 'E.M. SAO PEDRO RIBEIRINHA - 1° simulado - 5° ano A') \
        .replace('E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT_4_A', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_A') \
        .replace('E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT_4_B', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_B') \
        .replace('E.M. PROF.ª SILVIA HELENA COSTA DE OLIVEIRA BONETTI_4_G', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_G') \
        .replace('E.M. PROF.ª SILVIA HELENA COSTA DE OLIVEIRA BONETTI_4_H', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_H') \
        .replace('E.M. PROF.ª SILVIA HELENA COSTA DE OLIVEIRA BONETTI_4_I', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_I') \
        .replace('E.M. PROF.ª SILVIA HELENA COSTA DE OLIVEIRA BONETTI_4_J', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_J') \
        .replace('E.M. PROF.ª SILVIA HELENA COSTA DE OLIVEIRA BONETTI_4_K', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_K') \
        .replace('E.M. PROF.ª SILVIA HELENA COSTA DE OLIVEIRA BONETTI_4_L', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_L') \
        .replace('E.M. PROF.ª SILVIA HELENA COSTA DE OLIVEIRA BONETTI_4_M', 'E.M. PROF. SERGIO AUGUSTO PARA BITTENCOURT - 1° Simulado - 4° ano_M') \
        .replace('E.M. SANTO AGOSTINHO -1° Simulado -  4° ano - A', 'E.M. SANTO AGOSTINHO - 1° Simulado - 4° ano - A') \
        .replace('E.M. SAO VICENTE DE PAULA - 1° simulado -  4° ano - A', 'E.M. SAO VICENTE DE PAULA - 1° Simulado - 4° ano - A') \
        .replace('E.M. TEREZINHA MOURA BRASIL -  1° Simulado- 4° ano - B', 'E.M. TEREZINHA MOURA BRASIL -  1° Simulado - 4° ano - B') \
        .replace('ESC. MUL. SAO JOSE - 1° simulado -  4° ano - C', 'ESC. MUL. SAO JOSE - 1° simulado - 4° ano - C') \
        .replace('E.M. PROF.ª PERCILIA DO NASCIMENTO SOUZA - 1° simulado -  4° ano - D', 'E.M. PROF.ª PERCILIA DO NASCIMENTO SOUZA - 1° simulado - 4° ano - D')
    print("----- 1 ------")
    print(cursos)
    cursos['simulado_ano'] = cursos['titulo'].apply(lambda x: extract_simulado_curso(x))
    print("----- 2 ------")
    print(cursos)
    cursos['simulado_id'] = cursos['simulado_ano'].apply(lambda x: '0'.join(re.findall(r'\d', x)) if pd.notnull(x) else x)
    print("----- 3 ------")
    print(cursos)
    cursos['simulado'] = cursos['simulado_id'].apply(lambda x: f"Simulado {x[0]}" if pd.notnull(x) else x)
    print("----- 4 ------")
    print(cursos)
    cursos.to_excel('cursos_durante_process.xlsx', index = False)
    cursos = cursos[pd.notnull(cursos['simulado_id'])]
    print(cursos)
    cursos = cursos.astype({
        'curso_id':'str',
        'status':'str'
    })

    simulados = cursos[['simulado_id','simulado_ano','simulado']] \
        .drop_duplicates('simulado_id') \
        .sort_values(by='simulado_id') \
        .reset_index(drop=True)

    return cursos, simulados



# estudantes e alunos
def corrige_fase(fase):
    try: 
        nro_fase = int(re.search(r'\d',fase).group())
    except:
        nro_fase = None

    if nro_fase == 4:
        rslt = '4 ANO'
    elif nro_fase == 8:
        rslt = '8 ANO'
    else:
        rslt = fase
    return rslt


def replace_empty_string_with_none(string):
    conv = lambda x: x or None
    return conv(string)


def process_estudantes(estudantes, cursos, escolas):
    # LEFT JOIN - Mantém todos os estudantes e agrega os "cursos" (escola-simulado-ano) com base no curso_id
    estudantes['escola_id'] = estudantes['escola_id'].astype(str)

    estudantes_aux = pd.merge(
        left=estudantes,
        right=cursos,
        on=['curso_id'],
        how='left'
    )
    
    estudantes_aux = pd.merge(
        left=estudantes_aux,
        right=escolas,
        how='left',
        left_on=['escola_id'],
        right_on=['escola_id_sigeam']
    )

    estudantes_aux = estudantes_aux \
        .astype({
            'estudante_id':'int64',
            'ano_matricula':'int64',
            'escola_id':'int64' # modificado em 30/09/2023
        }) \
        .astype({
            'estudante_id':'str',
            'ano_matricula':'str',
            'escola_id':'str'
        })  


    estudantes_aux['estudante_registro_id'] = estudantes_aux.apply(lambda x: f"{x['estudante_id']}{x['simulado_id']}", axis=1)
    
    estudantes_aux['distrito'] = estudantes_aux['distrito'].apply(lambda x: re.sub(r'\d - ', '', x) if pd.notnull(x) else x)
    estudantes_aux['distrito'] = estudantes_aux['distrito'].apply(lambda x: replace_empty_string_with_none(x))
    
    estudantes_aux['fase'] = estudantes_aux['fase'].apply(lambda x: corrige_fase(x))
    estudantes_aux['fase'] = estudantes_aux['fase'].apply(lambda x: replace_empty_string_with_none(x))
    
    estudantes_aux['codigo'] = estudantes_aux['codigo'].apply(lambda x: replace_empty_string_with_none(x))
    estudantes_aux['nome'] = estudantes_aux['nome'].apply(lambda x: replace_empty_string_with_none(x))
    estudantes_aux['escola_id'] = estudantes_aux['escola_id'].apply(lambda x: replace_empty_string_with_none(x))
    estudantes_aux['escola_inep'] = estudantes_aux['escola_inep'].apply(lambda x: replace_empty_string_with_none(x))
    estudantes_aux['estudante_inep'] = estudantes_aux['estudante_inep'].apply(lambda x: replace_empty_string_with_none(x))
    estudantes_aux['necessidades'] = estudantes_aux['necessidades'].apply(lambda x: replace_empty_string_with_none(x))
    estudantes_aux['turma'] = estudantes_aux['turma'].apply(lambda x: replace_empty_string_with_none(x))
    estudantes_aux['turno'] = estudantes_aux['turno'].apply(lambda x: replace_empty_string_with_none(x))


    estudantes_aux = estudantes_aux[[
        'estudante_registro_id', 'estudante_id', 'codigo', 'nome', 'sexo', 'telefone',
        'data_nascimento', 'ano_matricula', 'distrito', 'escola_id', 'escola_inep', 'escola_nome',
        'estudante_inep', 'necessidades', 'fase', 'turma', 'turno', 'curso_id', 'simulado_id'
    ]] \
        .rename(columns={
            'escola_nome':'escola'
        }) \
        .sort_values(by='estudante_registro_id') \
        .reset_index(drop=True)

    return estudantes_aux



# avaliacoes
def clean_string(string):
    return re.sub(
        r'u([0-9a-f]{4})', 
        lambda m: chr(int(m.group(1), 16)), string) if pd.notnull(string) else None


def clean_html_string(string):
    return html.unescape(BeautifulSoup(str(string), features='lxml').get_text()) if pd.notnull(string) else string


def process_avaliacoes(avaliacoes, cursos):
    #estudar esse merge de informações, procurando vazios ou incompatibilidade de nomes
    avaliacoes_aux = pd.merge(
        left=avaliacoes,
        right=cursos,
        on=['curso_id'],
        how='left'
    )[[
        'avaliacao_id', 'perguntas', 'simulado_id', 'curso_id'
    ]]
    
    avaliacoes_aux = avaliacoes_aux \
        .astype({
            'avaliacao_id':'str'
        })

    # perguntas
    perguntas = pd.DataFrame()
    for simulado_id, avaliacao_id, avaliacao in zip(avaliacoes_aux['simulado_id'], avaliacoes_aux['avaliacao_id'], avaliacoes_aux['perguntas']):
        perguntas_aux = pd.json_normalize(avaliacao)
        perguntas_aux['avaliacao_id'] = avaliacao_id
        perguntas_aux['simulado_id'] = simulado_id
        perguntas = pd.concat([perguntas, perguntas_aux])
    
    perguntas.rename(columns={'id':'pergunta_id'}, inplace=True)
    perguntas.reset_index(drop=True, inplace=True)
    
    perguntas = pd.concat([
        perguntas, 
        pd.json_normalize(
            perguntas['banco_questoes_dados'].apply(
                lambda x: json.loads(x) if pd.notnull(x) else x)
            )
        ], axis=1)
    
    perguntas['componente_curricular'] = perguntas['componente_curricular'].apply(lambda x: clean_string(x))
    perguntas['pergunta'] = perguntas['pergunta'].apply(lambda x: clean_html_string(x))
    
    perguntas = perguntas \
        .astype({
            'pergunta_id':'str',
            'banco_questoes_id':'str',
            'eixo_cognitivo':'str'
        })

    # respostas
    respostas = pd.DataFrame()
    for pergunta_id, pergunta in zip(perguntas['pergunta_id'], perguntas['respostas']):
        respostas_aux = pd.json_normalize(pergunta)
        respostas_aux['pergunta_id'] = pergunta_id
        respostas = pd.concat([respostas, respostas_aux])
    
    respostas.rename(columns={'id':'resposta_id', 'resposta_correta':'se_resposta_correta'}, inplace=True)
    respostas.reset_index(drop=True, inplace=True)
    
    respostas['resposta'] = respostas['resposta'].apply(lambda x: clean_html_string(x))

    respostas = respostas \
        .astype({
            'resposta_id':'str'
        })

    # organiza output
    avaliacoes_aux = avaliacoes_aux.drop(['perguntas'], axis=1)
    perguntas = perguntas[[
        'pergunta_id','pergunta','banco_questoes_id',
        'eixo_cognitivo','componente_curricular','bncc',
        'avaliacao_id', 'simulado_id'
    ]]


    return avaliacoes_aux, perguntas, respostas



# resultados
def get_one_selected_markedtarget(array, position):
    try:
        rslt = array[int(position)]
    except:
        rslt = None
    return rslt


def selected_json_normalize(data_column_vector):
    rslt = []
    for row in data_column_vector: 
        groupname = row['groupname'] if 'groupname' in row.keys() else None
        # markedtargets = row['markedtargets'] if 'markedtargets' in row.keys() else None
        targets = row['targets'] if 'targets' in row.keys() else None
        markedtargets = [] # new markedtargets
        for x in targets:
            # if x['percentblack'] >= 18: #x['percentblack'] >= 20:
            #     markedtargets.append(x['id'])
            if x['ismarked']:
                if x['percentblack'] >= 18:
                    markedtargets.append(x['id'])
        one_marked_selected_position = row['one_marked_selected']['position'] if 'one_marked_selected' in row.keys() else None
        rslt.append({
            'groupname': groupname, 
            'markedtargets': markedtargets,
            'one_marked_selected.position': one_marked_selected_position
        })
    
    return rslt


def set_presenca_id(resultados):
    x = resultados
    if int(x['n_presenca_id']) == 0:
        rslt = '0'
    elif int(x['n_presenca_id']) == 1:
        rslt = x['presenca_id']
    elif int(x['n_presenca_id']) > 1:
        rslt = '99'
    else:
        rslt = None
    return rslt


def process_resultados(resultados, cursos):
    cursos['curso_id'] = cursos['curso_id'].astype(str)  # Garantindo que curso_id seja do tipo string
    #resultados = pd.read_excel("f_resultados_raw.xlsx", sheet_name="Sheet1")
    resultados_aux = resultados \
        .rename(columns={'curso':'curso_id'}) \
        .astype({
            'resultado_id':'str',
            'estudante_id':'str',
            'curso_id':'str',
            'avaliacao_id':'str'
        })

    resultados_aux = pd.merge(
        left=resultados_aux,
        right=cursos[['curso_id','simulado_id']],
        on=['curso_id'],
        how='left'
    )[[
        'resultado_id', 'estudante_id', 'curso_id', 'avaliacao_id', 'simulado_id',
        'cartao_resposta', 'respostas', 'respostas_omr',
        'informacoes_presenca', 'codigos_deficiencia'
    ]]
    
    resultados_respostas = pd.DataFrame()

    # respostas e respostas_omr
    for simulado_id, resultado_id, resposta, resposta_omr in zip(resultados_aux['simulado_id'], resultados_aux['resultado_id'], resultados_aux['respostas'], resultados_aux['respostas_omr']):
        #resposta = resposta.strip('][').split(', ')
        respostas_aux = pd.json_normalize(resposta)
        #resposta_omr = resposta_omr.strip('][').split(', ')
        respostas_omr_aux = pd.json_normalize(selected_json_normalize(resposta_omr))
        resultado_aux = pd.concat([respostas_aux, respostas_omr_aux], axis=1)
        resultado_aux['resultado_id'] = resultado_id
        resultado_aux['simulado_id'] = simulado_id
        resultados_respostas = pd.concat([resultados_respostas, resultado_aux])


    # resultados_respostas.reset_index(drop=True, inplace=True)
    resultados_respostas['n_markedtargets'] = [len(x) for x in resultados_respostas['markedtargets']]
    try:
        resultados_respostas['one_markedtarget'] = [get_one_selected_markedtarget(x,y) for x, y in zip(resultados_respostas['markedtargets'], resultados_respostas['one_marked_selected.position'])]
    except:
        resultados_respostas['one_markedtarget'] = None
    resultados_respostas['markedtargets'] = resultados_respostas['markedtargets'].apply(lambda x: ','.join(map(str, x))) 
    

    # informacoes_presenca
    try:
        informacoes_presenca = pd.json_normalize(selected_json_normalize(resultados_aux['informacoes_presenca']))
    except:
        informacoes_presenca = pd.json_normalize([{'groupname':x['groupname'], 'markedtargets':x['markedtargets']} for x in resultados_aux['informacoes_presenca']])
    informacoes_presenca['n_markedtargets'] = [len(x) for x in informacoes_presenca['markedtargets']]
    try:
        informacoes_presenca['one_markedtarget'] = [get_one_selected_markedtarget(x,y) for x, y in zip(informacoes_presenca['markedtargets'], informacoes_presenca['one_marked_selected.position'])]
    except:
        informacoes_presenca['one_markedtarget'] = None
    informacoes_presenca['markedtargets'] = informacoes_presenca['markedtargets'].apply(lambda x: ','.join(map(str, x))) 
    informacoes_presenca.rename(columns={'markedtargets':'informacoes_presenca_markedtargets', 'n_markedtargets':'informacoes_presenca_n_markedtargets', 'one_markedtarget':'informacoes_presenca_one_markedtarget'}, inplace=True)
    informacoes_presenca = informacoes_presenca.drop(['groupname'], axis=1)
    resultados_aux = pd.concat([resultados_aux, informacoes_presenca], axis=1)
    
    # codigos_deficiencia
    try:
        codigos_deficiencia = pd.json_normalize(selected_json_normalize(resultados_aux['codigos_deficiencia']))
    except:
        codigos_deficiencia = pd.json_normalize([{'groupname':x['groupname'], 'markedtargets':x['markedtargets']} for x in resultados_aux['codigos_deficiencia']])
    codigos_deficiencia['n_markedtargets'] = [len(x) for x in codigos_deficiencia['markedtargets']]
    try:
        codigos_deficiencia['one_markedtarget'] = [get_one_selected_markedtarget(x,y) for x, y in zip(codigos_deficiencia['markedtargets'], codigos_deficiencia['one_marked_selected.position'])]
    except:
        codigos_deficiencia['one_markedtarget'] = None
    codigos_deficiencia['markedtargets'] = codigos_deficiencia['markedtargets'].apply(lambda x: ','.join(map(str, x)))
    codigos_deficiencia.rename(columns={'markedtargets':'codigos_deficiencia_markedtargets', 'n_markedtargets':'codigos_deficiencia_n_markedtargets', 'one_markedtarget':'codigos_deficiencia_one_markedtarget'}, inplace=True)
    codigos_deficiencia = codigos_deficiencia.drop(['groupname'], axis=1)
    resultados_aux = pd.concat([resultados_aux, codigos_deficiencia], axis=1)

    # organiza output   
    resultados_aux = resultados_aux.drop(
        ['respostas','respostas_omr','informacoes_presenca','codigos_deficiencia', 'one_marked_selected.position'], 
        axis=1
    )
    # resultados_aux['presenca_id'] = resultados_aux \
    #     .apply(lambda x: x['informacoes_presenca_one_markedtarget'] 
    #             if ((x['informacoes_presenca_one_markedtarget'] != '') and 
    #                (pd.notnull(x['informacoes_presenca_one_markedtarget']))) 
    #             else x['informacoes_presenca_markedtargets'],
    #     axis=1)
    
    resultados_aux['presenca_id'] = resultados_aux['informacoes_presenca_markedtargets']
    resultados_aux['n_presenca_id'] = resultados_aux \
        .apply(lambda x: len(re.findall(',', str(x['presenca_id']))) + 1 if x['presenca_id'] != '' else 0,
               axis=1)
    resultados_aux['presenca_id'] = resultados_aux \
        .apply(lambda x: set_presenca_id(x), 
               axis=1)

    
    # resultados_aux['deficiencia_id'] = resultados_aux \
    #     .apply(lambda x: x['codigos_deficiencia_one_markedtarget'] 
    #             if ((x['codigos_deficiencia_one_markedtarget'] != '') and 
    #                (pd.notnull(x['codigos_deficiencia_one_markedtarget']))) 
    #             else x['codigos_deficiencia_markedtargets'],
    #     axis=1)
    resultados_aux['deficiencia_id'] = resultados_aux['codigos_deficiencia_markedtargets']

    # organiza output
    resultados_respostas = resultados_respostas[[
            'data', 'questao', 'resposta', 'pontuacao', 
            'groupname', 'markedtargets', 'n_markedtargets', 'one_markedtarget', 'resultado_id', 'simulado_id']] \
        .rename(columns={
            'questao':'pergunta_id', 'resposta':'resposta_id', 'pontuacao':'peso', 'groupname':'nro_questao',
            'markedtargets':'respostas_omr_markedtargets', 'n_markedtargets':'respostas_omr_n_markedtargets', 'one_markedtarget':'respostas_omr_one_markedtarget'
        }) \
        .reset_index(drop=True)
    
    resultados_respostas = resultados_respostas \
        .astype({
            'pergunta_id':'str',
            'resposta_id':'str',
            'peso':'str',
            'nro_questao':'str',
            'respostas_omr_n_markedtargets':'str'
        })
    resultados_respostas = resultados_respostas \
        .astype({
            'respostas_omr_one_markedtarget':'Int64'
        }) \
        .astype({
            'respostas_omr_one_markedtarget':'str'
        }) \
        .replace('<NA>','')  


    resultados_aux['estudante_registro_id'] = resultados_aux.apply(lambda x: f"{x['estudante_id']}{x['simulado_id']}", axis=1)
    resultados_aux = resultados_aux[[
        'resultado_id', 'simulado_id', 'curso_id', 'avaliacao_id', 'estudante_registro_id', 'estudante_id', 'cartao_resposta',
        'presenca_id', 
        'informacoes_presenca_markedtargets', 'informacoes_presenca_n_markedtargets', 'informacoes_presenca_one_markedtarget',
        'deficiencia_id',
        'codigos_deficiencia_markedtargets', 'codigos_deficiencia_n_markedtargets', 'codigos_deficiencia_one_markedtarget'
    ]] 

    resultados_aux = resultados_aux \
        .astype({
            'informacoes_presenca_n_markedtargets':'str',
            'codigos_deficiencia_n_markedtargets':'str'
        })
    resultados_aux = resultados_aux \
        .astype({
            'informacoes_presenca_one_markedtarget':'Int64'
        }) \
        .astype({
            'informacoes_presenca_one_markedtarget':'str'
        }) \
        .replace('<NA>','')
    

    return resultados_aux, resultados_respostas


# perguntas
def process_perguntas(perguntas, resultados_respostas):
    perguntas_aux = pd.merge(
        left=resultados_respostas[['pergunta_id', 'peso', 'nro_questao']],
        right=perguntas,
        on=['pergunta_id'],
        how='left'
    ) \
        .drop_duplicates('pergunta_id') \
        .reset_index(drop=True)
    perguntas_aux['questao_id'] = perguntas_aux.apply(lambda x: f"{x['simulado_id']}0{x['nro_questao']}", axis=1)

    perguntas_aux = perguntas_aux \
        .rename(columns={
            'pergunta':'questao', 
            'eixo_cognitivo':'eixo_cognitivo_id'
        })

    questoes = perguntas_aux \
        .drop_duplicates('questao_id')[[
            'questao_id', 'questao', 'nro_questao', 'peso', 
            'componente_curricular', 'eixo_cognitivo_id', 
            'bncc', 'simulado_id'
        ]] \
        .reset_index(drop=True)
    

    return perguntas_aux, questoes

"""
def process_perguntas(perguntas, resultados_respostas):
    # Verificando se há duplicatas em 'pergunta_id' no DataFrame 'd_perguntas_raw'
    if perguntas.duplicated('pergunta_id').any():
        print("Duplicatas encontradas em d_perguntas_raw. Removendo duplicatas...")
        perguntas = perguntas.drop_duplicates('pergunta_id')

    # Verificando se há duplicatas em 'pergunta_id' no DataFrame 'f_resultados_respostas_raw'
    if resultados_respostas.duplicated('pergunta_id').any():
        print("Duplicatas encontradas em f_resultados_respostas_raw. Removendo duplicatas...")
        resultados_respostas = resultados_respostas.drop_duplicates('pergunta_id')

    perguntas.to_excel("pergundas_fodas.xlsx", engine = "openpyxl")
    resultados_respostas.to_excel("resultados_respostas_fodas.xlsx", engine = "openpyxl")

    # Operação de mesclagem com os DataFrames limpos
    perguntas_aux = pd.merge(
        left=resultados_respostas[['pergunta_id', 'peso', 'nro_questao']],
        right=perguntas,
        on='pergunta_id',
        how='left'
    ).drop_duplicates('pergunta_id').reset_index(drop=True)
    
    # Criação da coluna 'questao_id'
    perguntas_aux['questao_id'] = perguntas_aux.apply(
        lambda x: f"{x['simulado_id']}0{x['nro_questao']}", axis=1
    )
    
    # Renomeando colunas conforme necessário
    perguntas_aux = perguntas_aux.rename(columns={
        'pergunta': 'questao', 
        'eixo_cognitivo': 'eixo_cognitivo_id'
    })
    
    # Selecionando e renomeando colunas para o DataFrame final 'questoes'
    questoes = perguntas_aux.drop_duplicates('questao_id')[[
        'questao_id', 'questao', 'nro_questao', 'peso', 
        'componente_curricular', 'eixo_cognitivo_id', 
        'bncc', 'simulado_id'
    ]].reset_index(drop=True)
    
    return perguntas_aux, questoes
"""


# process perguntas_respostas / alternativas
def set_alternativa_id(perguntas_respostas_aux):
    x = perguntas_respostas_aux
    if int(x['n_resposta_id']) == 0:
        rslt = f"{x['questao_id']}"
    elif int(x['n_resposta_id']) == 1:
        rslt = f"{x['questao_id']}{x['letra']}"
    elif int(x['n_resposta_id']) > 1:
        rslt = f"{x['questao_id']}N"
    else:
        rslt = None
    return rslt

def process_perguntas_respostas(perguntas_respostas, resultados_respostas, perguntas):
    perguntas_respostas_aux = pd.merge(
        left=resultados_respostas[['resposta_id', 'respostas_omr_markedtargets', 'respostas_omr_n_markedtargets', 'respostas_omr_one_markedtarget']],
        right=perguntas_respostas,
        on=['resposta_id'],
        how='left'
    )
    
    perguntas_respostas_aux = pd.merge(
        left=perguntas_respostas_aux,
        right=perguntas,
        on=['pergunta_id'],
        how='left'
    )
    
    # perguntas_respostas_aux['resposta_id'] = perguntas_respostas_aux \
    #     .apply(lambda x: x['respostas_omr_one_markedtarget'] 
    #             if ((x['respostas_omr_one_markedtarget'] != '') and 
    #                (pd.notnull(x['respostas_omr_one_markedtarget']))) 
    #             else x['respostas_omr_markedtargets'],
    #     axis=1)
    perguntas_respostas_aux['resposta_id'] = perguntas_respostas_aux['respostas_omr_markedtargets']
    perguntas_respostas_aux['n_resposta_id'] = perguntas_respostas_aux \
        .apply(lambda x: len(re.findall(',', str(x['resposta_id']))) + 1 if x['resposta_id'] != '' else 0,
               axis=1)
    
    perguntas_respostas_aux = perguntas_respostas_aux[pd.notnull(perguntas_respostas_aux['pergunta_id'])]

    perguntas_respostas_aux['alternativa_id'] = perguntas_respostas_aux \
        .apply(lambda x: set_alternativa_id(x), 
               axis=1)
    
    # organiza output
    perguntas_respostas_aux = perguntas_respostas_aux  \
        .drop(columns=['n_resposta_id']) \
        .drop_duplicates(['resposta_id', 'pergunta_id']) \
        .reset_index(drop=True)


    questoes_alternativas = perguntas_respostas_aux[[
        'alternativa_id', 'resposta', 'letra', 
        'se_resposta_correta', 'resposta_id', 
        'pergunta_id', 'questao_id', 'simulado_id'
    ]] \
        .rename(columns={
            'resposta':'alternativa'
        }) \
        .drop_duplicates('alternativa_id') \
        .sort_values(by=['questao_id','alternativa_id']) \
        .reset_index(drop=True)
    
    questoes_alternativas['alternativa'] = questoes_alternativas \
        .apply(
            lambda x: '' if (x['alternativa_id'] == x['questao_id']) or (x['alternativa_id'][-1] == 'N') else x['alternativa'],
            axis=1
        )
    questoes_alternativas['letra'] = questoes_alternativas \
        .apply(
            lambda x: '' if (x['alternativa_id'] == x['questao_id']) or (x['alternativa_id'][-1] == 'N') else x['letra'],
            axis=1
        )
    questoes_alternativas['se_resposta_correta'] = questoes_alternativas \
        .apply(
            lambda x: 'n' if (x['alternativa_id'] == x['questao_id']) or (x['alternativa_id'][-1] == 'N') else x['se_resposta_correta'],
            axis=1
        )
    
    return perguntas_respostas_aux, questoes_alternativas


# process resultados respostas
def set_alternativa_id_resultados(resultados_respostas_aux):
    x = resultados_respostas_aux
    if int(x['respostas_omr_n_markedtargets']) == 0:
        rslt = f"{x['questao_id']}"
    elif int(x['respostas_omr_n_markedtargets']) == 1:
        rslt = f"{x['alternativa_id']}"
    elif int(x['respostas_omr_n_markedtargets']) > 1:
        rslt = f"{x['questao_id']}N"
    else:
        rslt = None
    return rslt

def process_resultados_respostas(resultados_respostas, perguntas, perguntas_respostas):
    resultados_respostas_aux = resultados_respostas
    # resultados_respostas_aux['resposta_id'] = resultados_respostas_aux \
    #     .apply(lambda x: x['respostas_omr_one_markedtarget'] 
    #             if ((x['respostas_omr_one_markedtarget'] != '') and 
    #                (pd.notnull(x['respostas_omr_one_markedtarget']))) 
    #             else x['respostas_omr_markedtargets'],
    #     axis=1)
    
    resultados_respostas_aux = pd.merge(
        left=resultados_respostas_aux,
        right=perguntas[['pergunta_id', 'questao_id']],
        on=['pergunta_id'],
        how='left'
    )
    
    aux1 = resultados_respostas_aux[resultados_respostas_aux['resposta_id'] == '']
    aux2 = resultados_respostas_aux[resultados_respostas_aux['resposta_id'] != '']
    aux3 = pd.merge(
        left=aux2,
        right=perguntas_respostas[['resposta_id', 'alternativa_id']].astype({'resposta_id':'str'}),
        on=['resposta_id'],
        how='left'
    )
    aux1['alternativa_id'] = aux1['questao_id'] 
    resultados_respostas_aux = pd.concat([aux3,aux1])

    resultados_respostas_aux['alternativa_id'] = resultados_respostas_aux \
        .apply(lambda x: set_alternativa_id_resultados(x),
               axis=1)

    # organiza output
    # resultados_respostas_aux = resultados_respostas_aux \
    #     .drop_duplicates(['resultado_id','questao_id']) \
    #     .reset_index(drop=True)
    resultados_respostas_aux['resultado_resposta_registro_id'] = resultados_respostas_aux \
        .apply(
            lambda x: f"{x['resultado_id']}0{x['nro_questao']}",
            axis=1
        )
    resultados_respostas_aux.drop(columns=['peso','nro_questao'], inplace=True)
    

    return resultados_respostas_aux