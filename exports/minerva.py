import pandas as pd
import time
import sqlalchemy

import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np
import re

def corrigir_respostas(df, gabarito):
    # Função para aplicar as regras de correção
    def aplicar_regras(alternativa_id, questao_id):
        # Checar se contém a letra N
        if 'N' in alternativa_id:
            return 'N'
        # Extrair a letra da resposta
        letra = re.findall(r'[A-D]', alternativa_id)
        if letra:
            resposta_correta = gabarito.get(questao_id, '')
            return 1 if letra[0] == resposta_correta else 0
        # Se não contém letra ou é inválido
        return 'B'
    "Configurar N e B"
    # Aplicar a função de correção no DataFrame
    df['alternativa_corrigida'] = df.apply(lambda row: aplicar_regras(row['alternativa_id'], row['questao_id']), axis=1)
    return df

def adicionar_alternativa (df):
    pattern = r'\d'
    df['alternativa_id'] = df['alternativa_id'].astype(str)
    df['alternativa_id'] = df['alternativa_id'].apply(lambda x: re.sub(pattern, '', x))

    return df

#TODO def pivot_table()
#TODO def insert_student_alternatives()
#TODO def associar_gabarito_a_alternativa()
#TODO def 

