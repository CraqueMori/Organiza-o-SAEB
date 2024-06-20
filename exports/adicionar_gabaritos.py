import pandas as pd
import time
import sqlalchemy
import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np
import re
import minerva.minerva as minerva

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

# Carregar dados do banco de dados
d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)
f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)
f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)

# Adicionar alternativas corrigidas
correção_dos_gabaritos = minerva.adicionar_alternativa(f_resultados_respostas)

# Remover colunas desnecessárias de d_estudantes
d_estudantes = d_estudantes.drop(columns=['estudante_registro_id', 'sexo', 'telefone', 'necessidades', 'data_nascimento', 'escola_id', 'escola_inep', 'estudante_inep'])

# Merge com os resultados gerais
merge_estudantes_cartao_respostas = pd.merge(d_estudantes, f_resultados[['estudante_id', 'resultado_id']], on='estudante_id', how='left')

# Pivotar alternativa_id
pivot_alternativa_id = f_resultados_respostas.pivot_table(
    index='resultado_id',
    columns='questao_id',
    values='alternativa_id',
    aggfunc='first'
).reset_index()

# Mesclar os DataFrames resultantes
final_merged_df = pd.merge(merge_estudantes_cartao_respostas, pivot_alternativa_id, on='resultado_id', how='left')

# Verificar a presença de 'resultado_id'
assert 'resultado_id' in final_merged_df.columns, "'resultado_id' não está presente em final_merged_df"

# DataFrame onde a fase é "4 ANO"
dados_4_ano = final_merged_df[final_merged_df['fase'] == "4 ANO"]

# DataFrame onde a fase não é "4 ANO"
dados_8_ano = final_merged_df[final_merged_df['fase'] != "4 ANO"]

# Carregar cálculos
calculos_4_ano = pd.read_excel('export_elloi_4_ano.xlsx', engine='openpyxl')
calculos_8_ano = pd.read_excel('export_elloi_8_ano.xlsx', engine='openpyxl')

# Renomear colunas duplicadas para 'resultado_id'
calculos_4_ano = calculos_4_ano.rename(columns={'resultado_id_x': 'resultado_id'})
calculos_8_ano = calculos_8_ano.rename(columns={'resultado_id_x': 'resultado_id'})

# Merge com os cálculos
final_4_ano = pd.merge(dados_4_ano, calculos_4_ano, on='codigo', how='left')
final_8_ano = pd.merge(dados_8_ano, calculos_8_ano, on='codigo', how='left')

# Renomear e remover colunas duplicadas se existirem
if 'resultado_id_y' in final_4_ano.columns:
    final_4_ano = final_4_ano.drop(columns=['resultado_id_y'])
if 'resultado_id_x' in final_4_ano.columns:
    final_4_ano = final_4_ano.rename(columns={'resultado_id_x': 'resultado_id'})

if 'resultado_id_y' in final_8_ano.columns:
    final_8_ano = final_8_ano.drop(columns=['resultado_id_y'])
if 'resultado_id_x' in final_8_ano.columns:
    final_8_ano = final_8_ano.rename(columns={'resultado_id_x': 'resultado_id'})

# Verificar a presença de 'resultado_id'
assert 'resultado_id' in final_4_ano.columns, "'resultado_id' não está presente em final_4_ano após o merge"
assert 'resultado_id' in final_8_ano.columns, "'resultado_id' não está presente em final_8_ano após o merge"

# Lista de questões para excluir
lista_de_questoes_4 = [
    '10401', '10402', '10403', '10404', '10405', '10406', '10407', '10408', '10409', '104010', '104011', 
    '104012', '104013', '104014', '104015', '104016','104017', '104018', '104019', '104020', '104021', '104022', 
    '104023', '104024', '104025', '104026', '104027','104028', '104029', '104030', '104031',
    '104032', '104033', '104034', '104035', '104036', '104037','104038', '104039', '104040', '104041', '104042', '104043', '104044'
]

lista_de_questoes_8 = [
    '10801', '10802', '10803', '10804', '10805', '10806', '10807', '10808', '10809', '108010',
    '108011', '108012', '108013', '108014', '108015', '108016', '108017', '108018', '108019', '108020',
    '108021', '108022', '108023', '108024', '108025', '108026', '108027', '108028', '108029', '108030',
    '108031', '108032', '108033', '108034', '108035', '108036', '108037',
    '108038', '108039', '108040', '108041', '108042', '108043', '108044'
]

aba_8ano = final_8_ano.drop(columns=lista_de_questoes_4)
aba_4ano = final_4_ano.drop(columns=lista_de_questoes_8)

# Verificar a presença de 'resultado_id' antes do merge final
assert 'resultado_id' in aba_4ano.columns, "'resultado_id' não está presente em aba_4ano"
assert 'resultado_id' in aba_8ano.columns, "'resultado_id' não está presente em aba_8ano"
assert 'resultado_id' in f_resultados.columns, "'resultado_id' não está presente em f_resultados"
assert 'presenca_id' in f_resultados.columns, "'presenca_id' não está presente em f_resultados"

# Verificar as colunas dos DataFrames antes de mesclar
print(aba_4ano.columns)
print(aba_8ano.columns)

# Merge com os resultados de respostas para obter presenca_id
final_4_ano_com_presenca = pd.merge(aba_4ano, f_resultados[['resultado_id', 'presenca_id']], on='resultado_id', how='left')
final_8_ano_com_presenca = pd.merge(aba_8ano, f_resultados[['resultado_id', 'presenca_id']], on='resultado_id', how='left')

# Verificar colunas após o merge
print(final_4_ano_com_presenca.columns)
print(final_8_ano_com_presenca.columns)

substituicoes_presenca = {
    "99": 'Inválida',
    "0": 'Presente',
    "3": 'Faltou',
    "4": 'Deficiente (não realizou a prova)',
    "77": 'Abandonou',
    "2": 'Transferido',
    "88": 'Não preencheu'
}

# Substituir os valores de presenca_id nos DataFrames
final_4_ano_com_presenca['presenca_id'] = final_4_ano_com_presenca['presenca_id'].replace(substituicoes_presenca)
final_8_ano_com_presenca['presenca_id'] = final_8_ano_com_presenca['presenca_id'].replace(substituicoes_presenca)

# Renomear coluna para 'status'
final_4_ano_com_presenca = final_4_ano_com_presenca.rename(columns={'presenca_id': 'status'})
final_8_ano_com_presenca = final_8_ano_com_presenca.rename(columns={'presenca_id': 'status'})

# Verificar se a substituição funcionou corretamente
print(final_4_ano_com_presenca[['resultado_id', 'status']].head(10))
print(final_4_ano_com_presenca.columns)
print(final_8_ano_com_presenca[['resultado_id', 'status']].head(10))
print(final_8_ano_com_presenca.columns)
final_4_ano_com_presenca = final_4_ano_com_presenca.drop(columns=['simulado_id','curso_id',  'nome_y', 'ano_matricula_y',
       'distrito_y', 'escola_y', 'fase_y', 'turma_y', 'turno_y'])

final_8_ano_com_presenca = final_8_ano_com_presenca.drop(columns=['simulado_id','curso_id','nome_y', 'ano_matricula_y',
       'distrito_y', 'escola_y', 'fase_y', 'turma_y', 'turno_y'])

ordem_colunas_4_ano = ['estudante_id', 'codigo', 'nome_x', 'ano_matricula_x', 'distrito_x', 'escola_x', 'fase_x', 'turma_x', 'turno_x', 'resultado_id'] + lista_de_questoes_4 + [
    'Soma_Portugues_4ano', 'Soma_Matematica_4ano', 'Soma_Ciencias_4ano',
    'Soma_Branco', 'Soma_Rasura', 'Soma_Erros_Portugues',
    'Soma_Erros_Matematica', 'Soma_Erros_Ciencias', 'Media_LP', 'Media MP',
    'Media CI', 'Media Final', 'status'
]

ordem_colunas_8_ano = ['estudante_id', 'codigo', 'nome_x', 'ano_matricula_x', 'distrito_x', 'escola_x', 'fase_x', 'turma_x', 'turno_x', 'resultado_id'] + lista_de_questoes_8 + [
    'Soma_Portugues_8ano', 'Soma_Matematica_8ano', 'Soma_Ciencias_8ano',
    'Soma_Geografia_8ano', 'Soma_Historia_8ano', 'Soma_Branco',
    'Soma_Rasura', 'Soma_Erros_Portugues', 'Soma_Erros_Matematica',
    'Soma_Erros_Ciencias', 'Soma_Erros_Geografia', 'Soma_Erros_Historia',
    'Media_LP', 'Media_MP', 'Media_CI', 'Media_HI', 'Media_GE',
    'Media Final', 'status'
]

# Reorganizar as colunas do DataFrame final_4_ano_com_presenca
final_4_ano_com_presenca = final_4_ano_com_presenca[ordem_colunas_4_ano]

# Reorganizar as colunas do DataFrame final_8_ano_com_presenca
final_8_ano_com_presenca = final_8_ano_com_presenca[ordem_colunas_8_ano]

final_4_ano_com_presenca.to_excel('elloi_com_presença_4_ano.xlsx', engine='openpyxl')
final_8_ano_com_presenca.to_excel('elloi_com_presença_8_ano.xlsx', engine='openpyxl')