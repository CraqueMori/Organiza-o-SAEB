import sys 
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import time
import sqlalchemy

import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux
import re
import numpy as np
import re
import pandas as pd

def corrigir_alternativa_id(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    def processar_alternativa(alt: str) -> str:
        match = re.search(r'(\d{2})$', alt)
        if match:
            numero = match.group(0)
            return numero
        return alt
    
    # Verificar se a coluna existe no DataFrame
    if coluna in df.columns:
        # Aplicar a função à coluna especificada
        df[coluna] = df[coluna].apply(processar_alternativa)
    else:
        raise ValueError(f"Coluna '{coluna}' não encontrada no DataFrame.")
    
    return df


# Carregando funções redefinidas
importlib.reload(global_aux)

# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'

def main(simulado):
    start_time = time.time()
    engine = sqlalchemy.create_engine(
        f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
        pool_pre_ping=True
    )

    # Carregamento de dados
    d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)
    #d_estudantes.head().to_csv("csv_estudantes")
    f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)
    #f_resultados.head().to_csv('csv_resultados')
    f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)
    #f_resultados_respostas.head().to_csv("csv_respostas")
    
    d_estudantes = d_estudantes.drop(columns=['estudante_registro_id', 'sexo', 'telefone', 'necessidades', 'data_nascimento', 'escola_inep', 'estudante_inep'])
    # Primeiro, realizaremos o merge entre df_estudantes e df_resultados usando estudante_id

    merged_estudantes_resultados = pd.merge(d_estudantes, f_resultados, on="estudante_id", how="left")
    collums = merged_estudantes_resultados.columns
    merged_estudantes_resultados.drop(columns=['ano_matricula','simulado_id_y', 'curso_id_y','estudante_registro_id','informacoes_presenca_markedtargets',
       'informacoes_presenca_n_markedtargets',
       'informacoes_presenca_one_markedtarget', 'deficiencia_id',
       'codigos_deficiencia_markedtargets',
       'codigos_deficiencia_n_markedtargets',
       'codigos_deficiencia_one_markedtarget'])
    
    

    # Agora, vamos realizar o merge do resultado acima com df_respostas usando resultado_id
    
    final_merged = pd.merge(merged_estudantes_resultados, f_resultados_respostas, on="resultado_id", how="left")

    final_merged = final_merged.drop(columns=['simulado_id_x','resultado_id','curso_id_x','simulado_id_y','curso_id_y','avaliacao_id','estudante_registro_id','informacoes_presenca_markedtargets','informacoes_presenca_n_markedtargets','informacoes_presenca_one_markedtarget','deficiencia_id','codigos_deficiencia_markedtargets','codigos_deficiencia_n_markedtargets','codigos_deficiencia_one_markedtarget','pergunta_id','resposta_id','simulado_id'])

    # Mostrando as primeiras linhas do DataFrame final para verificar a integração dos dados
    
    #print(final_merged)
    pattern = r'\d'
    final_merged['alternativa_id'] = final_merged['alternativa_id'].astype(str)
    final_merged['alternativa_id'] = final_merged['alternativa_id'].apply(lambda x: re.sub(pattern, '', x))

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
                '108039': 'D' ,'108040': 'B', '108041': 'C' ,'108042': 'A' ,'108043': 'D', '108044': 'B'} 

    
    
    df_simulado_104 = final_merged[final_merged['fase'] == "4 ANO"]
    df_simulado_108 = final_merged[final_merged['fase'] == "8 ANO"]

    df_simulado_104['gabarito'] = None
    df_simulado_108['gabarito'] = None

    df_simulado_104['gabarito'] = df_simulado_104['questao_id'].map(gabarito)
    df_simulado_108['gabarito'] = df_simulado_108['questao_id'].map(gabarito)

    df_simulado_104['alternativa_id'] = df_simulado_104['alternativa_id'].replace({np.nan: 'Em Processamento', '': 'Em Branco', '   ': 'Em Branco', 'N': 'Rasura', 'nan':'Em Processamento'})
    df_simulado_108['alternativa_id'] = df_simulado_108['alternativa_id'].replace({np.nan: 'Em Processamento', '': 'Em Branco', '   ': 'Em Branco', 'N': 'Rasura', 'nan':'Em Processamento'})

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
    df_simulado_104['presenca_id'] = df_simulado_104['presenca_id'].replace(substituicoes_presenca)
    df_simulado_108['presenca_id'] = df_simulado_108['presenca_id'].replace(substituicoes_presenca)

    df_simulado_104['questao_id'] = df_simulado_104['questao_id'].astype(str)
    df_simulado_108['questao_id'] = df_simulado_108['questao_id'].astype(str)

    df_simulado_104 = corrigir_alternativa_id(df_simulado_104, 'questao_id')
    df_simulado_108 = corrigir_alternativa_id(df_simulado_108, 'questao_id')

    df_simulado_104.drop(columns=['estudante_id'], axis=1)
    df_simulado_108.drop(columns=['estudante_id'], axis=1)
    df_simulado_104.to_excel('dados_4_ano_FINAL.xlsx', engine="openpyxl")
    df_simulado_108.to_excel('dados_8_ano_FINAL.xlsx', engine="openpyxl")

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