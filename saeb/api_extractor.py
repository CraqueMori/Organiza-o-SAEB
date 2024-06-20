import pandas as pd
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


import saeb.api_requests as api_requests


# cursos
def get_cursos(base_url): 
    url = f'{base_url}/cursos'
    cursos = api_requests.get_data(url)
    cursos = pd.DataFrame(cursos)
    cursos.columns = ['curso_id', 'titulo', 'status']
    #.query("curso_id == 12773 or curso_id == 11850")
    return cursos


# avaliacoes
def get_avaliacoes_curso(base_url, curso_id):
    url = f'{base_url}/curso/{curso_id}/avaliacoes'
    avaliacoes = api_requests.get_data(url)
    avaliacoes = pd.DataFrame(avaliacoes)
    avaliacoes['curso_id'] = curso_id
    avaliacoes.columns = ['avaliacao_id', 'titulo', 'perguntas', 'curso_id']
    return avaliacoes


def get_avaliacoes(base_url, cursos=None):
    if cursos is None:
        cursos = get_cursos()

    dt = pd.DataFrame()
    #Enumerate Cursos = 1 
    for i, curso_id in enumerate(tqdm(cursos['curso_id'])):
        try:
            dt2 = get_avaliacoes_curso(base_url, curso_id)
            dt = pd.concat([dt, dt2])
        except ValueError:
            pass

    dt.reset_index(drop=True, inplace=True)
    return dt

# def get_avaliacoes_curso(base_url, curso_id):
#     url = f'{base_url}/curso/{curso_id}/avaliacoes'
#     avaliacoes = api_requests.get_data(url)
#     avaliacoes = pd.DataFrame(avaliacoes)
#     avaliacoes['curso_id'] = curso_id
#     avaliacoes.columns = ['avaliacao_id', 'titulo', 'perguntas', 'curso_id']
#     return avaliacoes



# resultados
def get_resultados_curso_avaliacao(base_url, curso_id, avaliacao_id):
    url = f'{base_url}/curso/{curso_id}/avaliacao/{avaliacao_id}'
    resultados = api_requests.get_data(url)
    resultados = pd.DataFrame(resultados)
    resultados.rename(columns={'id':'resultado_id', 'aluno':'estudante_id', 'avaliacao':'avaliacao_id'}, inplace=True)

    return resultados


def get_resultados(base_url, avaliacoes=None):
    print("entrou em resultados GET")
    if avaliacoes is None:
        print("entrou no if none")
        avaliacoes = get_avaliacoes()

    dt = pd.DataFrame()

    """ QUE COLUNAS TEM QUE TER """
    
    dt_aux = avaliacoes[['curso_id', 'avaliacao_id']].drop_duplicates()
    for curso_id, avaliacao_id in tqdm(zip(dt_aux['curso_id'], dt_aux['avaliacao_id']), total=len(dt_aux)):
        try:
            print(f'esse é o valor de {avaliacao_id} e de curso id {curso_id}')
            dt2 = get_resultados_curso_avaliacao(base_url, curso_id, avaliacao_id)
            dt = pd.concat([dt, dt2])
        except ValueError:
            pass

    dt.reset_index(drop=True, inplace=True)
    return dt 


# estudantes
def get_estudantes_curso(base_url, curso_id):
    url = f'{base_url}/curso/{curso_id}/alunos'
    estudantes = api_requests.get_data(url)
    estudantes = pd.DataFrame(estudantes)
    estudantes['curso_id'] = curso_id
    estudantes.rename(columns={
        'id':'estudante_id', 'cod_escola':'escola_id', 'cod_escola_inep':'escola_inep',
        'cod_aluno_inep':'estudante_inep'}, 
        inplace=True)
    return estudantes


def get_estudantes(base_url, cursos=None):
    if cursos is None:
        cursos = get_cursos()

    dt = pd.DataFrame()
    for i, curso_id in enumerate(tqdm(cursos['curso_id'])):
        try:
            dt2 = get_estudantes_curso(base_url, curso_id)
            dt = pd.concat([dt, dt2])
        except ValueError:
            pass

    dt.reset_index(drop=True, inplace=True)

    return dt