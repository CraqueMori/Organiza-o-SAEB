import pandas as pd
import time
import sqlalchemy

import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import numpy as np
import re

# Carregando funções redefinidas
importlib.reload(global_aux)

# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'
IF_EXISTS = 'replace'

def corrigir_respostas(df, gabarito):
    # Função para aplicar as regras de correção
    def aplicar_regras(alternativa_id, questao_id):
        # Checar se contém a letra N
        print(alternativa_id)
        if 'N' in alternativa_id:
            return 'N'
        # Extrair a letra da resposta
        letra = re.findall(r'[A-D]', alternativa_id)
        print(letra)
        if letra:
            print('caiu no if letra')
            # Checar se a resposta é correta comparando com o gabarito
            print(questao_id)
            print(gabarito)
            
            resposta_correta = gabarito.get(questao_id, '')
            
            print(resposta_correta)
            return '1' if letra[0] == resposta_correta else '0'
        # Se não contém letra ou é inválido
        return 'B'
    "Configurar N e B"
    # Aplicar a função de correção no DataFrame
    df['alternativa_id'] = df.apply(lambda row: aplicar_regras(row['alternativa_id'], row['questao_id']), axis=1)
    return df



def main(simulado):
    start_time = time.time()
    engine = sqlalchemy.create_engine(
        f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
        pool_pre_ping=True
    )
    gabarito= { '10401':'B', '10402': 'D', '10403': 'C', '10404': 'A', '10405': 'D', '10406': 'B' , 
                '10407': 'B', '10408': 'A', '10409': 'C', '104010': 'B', 
                '104011': 'D', '104012': 'D' ,'104013': 'A', '104014': 'B', '104015': 'C' , '104016': 'D',
                '104017': 'D', '104018': 'B', '104019': 'C', '104020': 'A' ,'104021': 'B', '104022': 'C', 
                '104023': 'A', '104024': 'B', '104025': 'A', '104026': 'C', '104027': 'B', '104028': 'C', '104029': 'B', 
                '104030': 'A', '104031': 'D', '104032': 'C' , '104033': 'D', '104034': 'C',
                '104035': 'C', '104036': 'D', '104037': 'A', '104038': 'A' ,'104039': 'B', '104040': 'C',
                '104041': 'A', '104042': 'D', '104043': 'B', '104044': 'A', '10801': 'A', '10802': 'D', '10803': 'C',
                '10804': 'B', '10805': 'C', '10806': 'D', '10807': 'A', '10808': 'D', '10809': 'A', 
                '108010': 'D', '108011': 'C', '108012': 'B' ,'108013': 'D' ,'108014': 'A', '108015': 'D',
                '108016': 'C', '108017': 'A', '108018': 'D', '108019': 'B', 
                '108020': 'C' ,'108021': 'A', '108022': 'C', '108023': 'B' , '108024': 'C', '108025': 'C', 
                '108026': 'B', '108027': 'D', '108028': 'A', '108029': 'D', '108030': 'B', 
                '108031': 'C', '108032': 'B', '108033': 'C', 
                '108034': 'D', '108035': 'A', '108036': 'D', '108037': 'B', '108038': 'C', 
                '108039': 'D' ,'108040': 'B', '108041': 'C' ,'108042': 'A' ,'108043': 'D', '108044': 'B' 
}

    # Carregamento de dados
    d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)

    f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)

    f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)



    f_resultados_respostas= corrigir_respostas(f_resultados_respostas, gabarito)
    

    # # Remoção de colunas desnecessárias
    d_estudantes = d_estudantes.drop(columns=['estudante_registro_id', 'sexo', 'telefone', 'necessidades', 'data_nascimento', 'escola_id', 'escola_inep', 'estudante_inep'])

    # # Merge com os resultados gerais
    merged_df = pd.merge(d_estudantes, f_resultados[['estudante_id', 'resultado_id']], on='estudante_id', how='left')

    
    # # Pivoteando os resultados das respostas baseados na correção
    resultado_pivot = f_resultados_respostas.pivot_table(
        index='resultado_id', 
        columns='questao_id', 
        values='alternativa_id', 
        aggfunc='first'  # Captura a primeira ocorrência da 'alternativa_id' para cada combinação de 'resultado_id' e 'questao_id'
    ).reset_index()

    # Ajustando os nomes das colunas após o pivot
    resultado_pivot.columns = ['resultado_id'] + [f'questao_{col}' if isinstance(col, int) else col for col in resultado_pivot.columns[1:]]
    print(resultado_pivot)
    print(resultado_pivot.info())
    print(resultado_pivot.head())

    # # Merge do pivot com o DataFrame principal
    merged_df = pd.merge(merged_df, resultado_pivot, on='resultado_id', how='right')

    # DataFrame onde a fase é "4 ANO"
    df_fase_4_ano = merged_df[merged_df['fase'] == "4 ANO"]

    # DataFrame onde a fase não é "4 ANO"
    df_fase_8_ano = merged_df[merged_df['fase'] != "4 ANO"]


    with pd.ExcelWriter('export_eloi_20_05.xlsx', engine='openpyxl') as writer:
    # Escrever o primeiro DataFrame em uma folha chamada 'Folha1'
        df_fase_4_ano.to_excel(writer, sheet_name='4 ano')

    # Escrever o segundo DataFrame em uma folha chamada 'Folha2'
        df_fase_8_ano.to_excel(writer, sheet_name='8 ano')
    # Mostrando o DataFrame final
    print(merged_df)

    end_time = time.time()
    print('CONCLUIDO')
    print(f'Tempo total do script: {round((end_time - start_time) / 60, 4)} min')

    # Se necessário, salve ou processe os dados
    # global_aux.write2sql(dataframe=merged_df, table_name='d_estudantes_expandido', database_connection=engine, database_schema=SCHEMA, if_exists=IF_EXISTS)

    return

if __name__ == '__main__':
    simulado = 'Simulado 1'
    print('Simulado:', simulado)
    main(simulado)
