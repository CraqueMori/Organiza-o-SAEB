# Vamos fazer da forma mais simples, o nosso dataframe gerado pode resolver o problema.

# Precisamos agora criar essas colunas adicionais: Acerto LP	Erro LP	Acerto MA	Erro MA	Acerto CI	Erro CI	Acerto GE	Erro GE	Acerto HI	Erro HI	Acerto GR	Erro GR	Branco GR	Rasura GR

# Para preenchelas, preciamos percorrer todas as respostas de um aluno, com essas regras:
# Se estiver tudo em branco, o aluno não respondeu a resposta, e deve ser computado tudo como zero, e com o total de brancos 44.
# Devemos saber o ano em que o aluno fez a prova, essa informação existe no dataframe, e é dada pela coluna Fase.
# Se o aluno for do "4 ANO", suas respostas vão estar entre as colunas M e BD
# Se o aluno for do "8 ANO", suas respostas vão estar entre as colunas BE e CV
# Se dentro de seu ano, estiver em branco, o aluno não respondeu a resposta, e deve ser computado tudo como zero, e com o total de brancos 44.
# Suas respostas devem ser comparadas com um gabarito, que vai validar seus pontos na prova. 
# Se alguma resposta tiver N em vez de uma letra, essa resposta é considerada como rasura, e deve ser somada a rasura.
# Se alguma resposta tiver apenas o operador da pergunta, e nenhuma letra o acompanhando, ela é considerado em branco, e deve computar para a soma de em brancos.
# As questões 10401 a 14016 são correspondentes a portugues, por tanto, computam pontos para portugues e para soma geral. 
# As questões 10417 a 10432 são correspondentes a Matematica, e devem computar pontos para matematica e para soma geral. 
# As questões 10433 a 10444 são de Ciencias, e devem commputar pontos para portugues e para a soma geral.
# As questões 10801 a 10810 são de portugues, e devem computar pontos para portugues e soma geral.
# As questões 10811 a 10820 são de matematica, e devem computar pontos para matematica e para soma geral.
# As questões 10821 a 10830 são de ciencias, e devem computar pontos para ciencias e para a soma geral.
# As questões 10831 a 10837 são de geografia, e devem computar pontos para geografia e para a soma geral.
# As questões 10838 a 10844 são de historia, e devem contar pontos para historia e soma geral.
# Existe uma lógica para a contabilização da média, cada uma das disciplinas vão de 0 a 10, e são multiplicadas no fim para gerar uma média. 
# Para o 8 ano: português, matematica e ciências tem peso 1, e geografia e historia tem peso 1,428571429, e são divididas por 5, para somar 10.
# Para o 4 ano: portugues, matematica tem peso 0,62500 e ciencias da natureza tem peso 0,83333  e são divididas por 3 para somar 10.
# Definindo as colunas para cada matéria


import sys 
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import pandas as pd
import time
import sqlalchemy

import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np
import re

import pandas as pd

# Carregar os arquivos Excel
df_4ano = pd.read_excel('folha_4_ano.xlsx', engine='openpyxl')
df_8ano = pd.read_excel('folha_8_ano.xlsx', engine='openpyxl')


METRICA_4_PORTUGUES_MATEMATICA = 0.625
METRICA_4_ANO_CI = 0.83333
METRICA_8_ANO_LP_CI_MP = 1.0
METRICA_8_ANO_GE_HI = 1.4285

# Definindo as colunas para cada matéria do 4º ano
portugues_4ano = ['10401', '10402', '10403', '10404', '10405', '10406', '10407', '10408', '10409', '104010', '104011', '104012', '104013', '104014', '104015', '104016']
matematica_4ano = ['104017', '104018', '104019', '104020', '104021', '104022', '104023', '104024', '104025', '104026', '104027', '104028', '104029', '104030', '104031','104032']
ciencias_4ano = ['104033', '104034', '104035', '104036', '104037','104038', '104039', '104040', '104041', '104042', '104043', '104044']

lista_de_questoes_4 = ['10401', '10402', '10403', '10404', '10405', '10406', '10407', '10408', '10409', '104010', '104011', 
                       '104012', '104013', '104014', '104015', '104016','104017', '104018', '104019', '104020', '104021', '104022', 
                       '104023', '104024', '104025', '104026', '104027','104028', '104029', '104030', '104031',
                        '104032', '104033', '104034', '104035', '104036', '104037','104038', '104039', '104040', '104041', '104042', '104043', '104044']

lista_de_questoes_8= ['10801', '10802', '10803', '10804', '10805', '10806', '10807', '10808', '10809', '108010',
                      '108011', '108012', '108013', '108014', '108015', '108016', '108017', '108018', '108019', '108020',
                      '108021', '108022', '108023', '108024', '108025', '108026', '108027', '108028', '108029', '108030',
                      '108031', '108032', '108033', '108034', '108035', '108036', '108037',
                      '108038', '108039', '108040', '108041', '108042', '108043', '108044']


aba_4ano = df_4ano.copy()

# Criar cópias temporárias das colunas que serão somadas
temp_portugues_4ano = aba_4ano[portugues_4ano].apply(pd.to_numeric, errors='coerce')
temp_matematica_4ano = aba_4ano[matematica_4ano].apply(pd.to_numeric, errors='coerce')
temp_ciencias_4ano = aba_4ano[ciencias_4ano].apply(pd.to_numeric, errors='coerce')

# Calcular as somas usando as cópias temporárias
aba_4ano['Soma_Portugues_4ano'] = temp_portugues_4ano.sum(axis=1)
aba_4ano['Soma_Matematica_4ano'] = temp_matematica_4ano.sum(axis=1)
aba_4ano['Soma_Ciencias_4ano'] = temp_ciencias_4ano.sum(axis=1)
aba_4ano['Soma_Branco'] = aba_4ano[lista_de_questoes_4].apply(lambda row: (row == 'B').sum(), axis=1)
aba_4ano['Soma_Rasura'] = aba_4ano[lista_de_questoes_4].apply(lambda row: (row == 'N').sum(), axis=1)


aba_4ano['Soma_Branco_Portugues_4ano'] = aba_4ano[portugues_4ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_4ano['Soma_Rasura_Portugues_4ano'] = aba_4ano[portugues_4ano].apply(lambda row: (row == 'N').sum(), axis=1)

aba_4ano['Soma_Branco_Matematica_4ano'] = aba_4ano[matematica_4ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_4ano['Soma_Rasura_Matematica_4ano'] = aba_4ano[matematica_4ano].apply(lambda row: (row == 'N').sum(), axis=1)

aba_4ano['Soma_Branco_Ciencias_4ano'] = aba_4ano[ciencias_4ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_4ano['Soma_Rasura_Ciencias_4ano'] = aba_4ano[ciencias_4ano].apply(lambda row: (row == 'N').sum(), axis=1)

aba_4ano['Soma_Erros_Portugues'] = 16 - aba_4ano['Soma_Portugues_4ano'] - aba_4ano['Soma_Branco_Portugues_4ano'] - aba_4ano['Soma_Rasura_Portugues_4ano'] 
aba_4ano['Soma_Erros_Matematica'] = 16 - aba_4ano['Soma_Matematica_4ano'] - aba_4ano['Soma_Branco_Matematica_4ano'] - aba_4ano['Soma_Rasura_Matematica_4ano']
aba_4ano['Soma_Erros_Ciencias'] = 12 - aba_4ano['Soma_Ciencias_4ano'] - aba_4ano['Soma_Branco_Ciencias_4ano'] - aba_4ano['Soma_Rasura_Ciencias_4ano']

aba_4ano['Media_LP'] = (aba_4ano['Soma_Portugues_4ano'] * METRICA_4_PORTUGUES_MATEMATICA).round(2)
aba_4ano['Media MP'] = (aba_4ano['Soma_Matematica_4ano'] * METRICA_4_PORTUGUES_MATEMATICA).round(2)
aba_4ano['Media CI'] = (aba_4ano['Soma_Ciencias_4ano'] * METRICA_4_ANO_CI).round(2)
aba_4ano['Media Final'] = (((aba_4ano['Soma_Ciencias_4ano'] + aba_4ano['Soma_Matematica_4ano'] + aba_4ano['Soma_Portugues_4ano']) / 44 ) * 10).round(2)
aba_4ano = aba_4ano.drop(columns=lista_de_questoes_4)
aba_4ano = aba_4ano.drop(columns=lista_de_questoes_8)
aba_4ano = aba_4ano.drop(columns=['Soma_Branco_Portugues_4ano',
'Soma_Rasura_Portugues_4ano',
'Soma_Branco_Matematica_4ano',
'Soma_Rasura_Matematica_4ano',
'Soma_Branco_Ciencias_4ano',
'Soma_Rasura_Ciencias_4ano', 'curso_id', 'simulado_id', 'estudante_id', 'Unnamed: 0'])


# Salvar o dataframe atualizado
aba_4ano.to_excel("export_elloi_4_ano.xlsx", engine='openpyxl', index=False)

# Definindo as colunas para cada matéria do 8º ano
portugues_8ano = ['10801', '10802', '10803', '10804', '10805', '10806', '10807', '10808', '10809', '108010']
matematica_8ano = ['108011', '108012', '108013', '108014', '108015', '108016', '108017', '108018', '108019', '108020']
ciencias_8ano = ['108021', '108022', '108023', '108024', '108025', '108026', '108027', '108028', '108029', '108030']
geografia_8ano = ['108031', '108032', '108033', '108034', '108035', '108036', '108037']
historia_8ano = ['108038', '108039', '108040', '108041', '108042', '108043', '108044']

# Processando a aba "8 ano"
aba_8ano = df_8ano.copy()

# Criar cópias temporárias das colunas que serão somadas
temp_portugues_8ano = aba_8ano[portugues_8ano].apply(pd.to_numeric, errors='coerce')
temp_matematica_8ano = aba_8ano[matematica_8ano].apply(pd.to_numeric, errors='coerce')
temp_ciencias_8ano = aba_8ano[ciencias_8ano].apply(pd.to_numeric, errors='coerce')
temp_geografia_8ano = aba_8ano[geografia_8ano].apply(pd.to_numeric, errors='coerce')
temp_historia_8ano = aba_8ano[historia_8ano].apply(pd.to_numeric, errors='coerce')

# Calcular as somas usando as cópias temporárias
aba_8ano['Soma_Portugues_8ano'] = temp_portugues_8ano.sum(axis=1)
aba_8ano['Soma_Matematica_8ano'] = temp_matematica_8ano.sum(axis=1)
aba_8ano['Soma_Ciencias_8ano'] = temp_ciencias_8ano.sum(axis=1)
aba_8ano['Soma_Geografia_8ano'] = temp_geografia_8ano.sum(axis=1)
aba_8ano['Soma_Historia_8ano'] = temp_historia_8ano.sum(axis=1)
aba_8ano['Soma_Branco'] = aba_8ano[lista_de_questoes_8].apply(lambda row: (row == 'B').sum(), axis=1)
aba_8ano['Soma_Rasura'] = aba_8ano[lista_de_questoes_8].apply(lambda row: (row == 'N').sum(), axis=1)


aba_8ano['Soma_Branco_Portugues_8ano'] = aba_8ano[portugues_8ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_8ano['Soma_Rasura_Portugues_8ano'] = aba_8ano[portugues_8ano].apply(lambda row: (row == 'N').sum(), axis=1)

aba_8ano['Soma_Branco_Matematica_8ano'] = aba_8ano[matematica_8ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_8ano['Soma_Rasura_Matematica_8ano'] = aba_8ano[matematica_8ano].apply(lambda row: (row == 'N').sum(), axis=1)

aba_8ano['Soma_Branco_Ciencias_8ano'] = aba_8ano[ciencias_8ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_8ano['Soma_Rasura_Ciencias_8ano'] = aba_8ano[ciencias_8ano].apply(lambda row: (row == 'N').sum(), axis=1)

aba_8ano['Soma_Branco_Geografia_8ano'] = aba_8ano[geografia_8ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_8ano['Soma_Rasura_Geografia_8ano'] = aba_8ano[geografia_8ano].apply(lambda row: (row == 'N').sum(), axis=1)

aba_8ano['Soma_Branco_Historia_8ano'] = aba_8ano[historia_8ano].apply(lambda row: (row == 'B').sum(), axis=1)
aba_8ano['Soma_Rasura_Historia_8ano'] = aba_8ano[historia_8ano].apply(lambda row: (row == 'N').sum(), axis=1)


aba_8ano['Soma_Erros_Portugues'] = 10 - aba_8ano['Soma_Portugues_8ano'] - aba_8ano['Soma_Branco_Portugues_8ano'] - aba_8ano['Soma_Rasura_Portugues_8ano']
aba_8ano['Soma_Erros_Matematica'] = 10 - aba_8ano['Soma_Matematica_8ano'] - aba_8ano['Soma_Branco_Matematica_8ano'] - aba_8ano['Soma_Rasura_Matematica_8ano']
aba_8ano['Soma_Erros_Ciencias'] = 10 - aba_8ano['Soma_Ciencias_8ano'] - aba_8ano['Soma_Branco_Ciencias_8ano'] - aba_8ano['Soma_Rasura_Ciencias_8ano']
aba_8ano['Soma_Erros_Geografia'] = 7 - aba_8ano['Soma_Geografia_8ano'] - aba_8ano['Soma_Branco_Geografia_8ano'] - aba_8ano['Soma_Rasura_Geografia_8ano']
aba_8ano['Soma_Erros_Historia'] = 7 - aba_8ano['Soma_Historia_8ano'] - aba_8ano['Soma_Branco_Historia_8ano'] - aba_8ano['Soma_Rasura_Historia_8ano']


aba_8ano['Media_LP'] = (aba_8ano['Soma_Portugues_8ano'] * METRICA_8_ANO_LP_CI_MP).round(2)
aba_8ano["Media_MP"] = (aba_8ano['Soma_Matematica_8ano'] * METRICA_8_ANO_LP_CI_MP).round(2)
aba_8ano["Media_CI"] = (aba_8ano['Soma_Ciencias_8ano'] * METRICA_8_ANO_LP_CI_MP ).round(2)
aba_8ano['Media_HI'] = (aba_8ano['Soma_Historia_8ano'] * METRICA_8_ANO_GE_HI).round(2)
aba_8ano['Media_GE'] = (aba_8ano['Soma_Geografia_8ano'] * METRICA_8_ANO_GE_HI).round(2)
aba_8ano['Media Final'] = (((aba_8ano['Soma_Ciencias_8ano'] + aba_8ano['Soma_Historia_8ano'] + aba_8ano['Soma_Matematica_8ano'] + aba_8ano['Soma_Geografia_8ano'] + aba_8ano['Soma_Portugues_8ano']) / 44 ) * 10).round(2)


aba_8ano = aba_8ano.drop(columns=lista_de_questoes_4)
aba_8ano = aba_8ano.drop(columns=lista_de_questoes_8)
aba_8ano = aba_8ano.drop(columns=['Soma_Branco_Historia_8ano', 'Soma_Rasura_Historia_8ano', 'Soma_Branco_Geografia_8ano', 'Soma_Rasura_Geografia_8ano',  
                         'Soma_Branco_Portugues_8ano', 'Soma_Rasura_Portugues_8ano', 'Soma_Branco_Matematica_8ano',  'Soma_Rasura_Matematica_8ano', 'Soma_Branco_Ciencias_8ano', 'Soma_Rasura_Ciencias_8ano','curso_id', 'simulado_id', 'estudante_id', 'Unnamed: 0'])
 

# Salvar o dataframe atualizado
aba_8ano.to_excel('export_elloi_8_ano.xlsx', engine='openpyxl', index=False)



print("As somas foram aplicadas e os novos arquivos Excel foram salvos com sucesso.")
