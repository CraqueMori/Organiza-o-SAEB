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
    f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados WHERE presenca_id = '0';", engine)
    f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)

    #Aplica a função "Corrigir Respostas" do módulo Minerva no DataFrame, para a obtenção dos valores binários dos gabaritos
    correção_dos_gabaritos = minerva.corrigir_respostas(f_resultados_respostas, gabarito)
    

    #Remoção de colunas desnecessárias
    d_estudantes = d_estudantes.drop(columns=['estudante_registro_id', 'sexo', 'telefone', 'necessidades', 'data_nascimento', 'escola_id', 'escola_inep', 'estudante_inep'])

    # # Merge com os resultados gerais
    merged_df = pd.merge(d_estudantes, f_resultados[['estudante_id', 'resultado_id']], on='estudante_id', how='left')

    
    #Pivoteando os resultados das respostas baseados na correção
    resultado_pivot = correção_dos_gabaritos.pivot_table(
        index='resultado_id', 
        columns='questao_id', 
        values='alternativa_id', 
        aggfunc='first'  # Captura a primeira ocorrência da 'alternativa_id' para cada combinação de 'resultado_id' e 'questao_id'
    ).reset_index()

    # Ajustando os nomes das colunas após o pivot
    resultado_pivot.columns = ['resultado_id'] + [f'questao_{col}' if isinstance(col, int) else col for col in resultado_pivot.columns[1:]]

    # # Merge do pivot com o DataFrame principal
    merged_df = pd.merge(merged_df, resultado_pivot, on='resultado_id', how='right')

    # DataFrame onde a fase é "4 ANO"
    df_fase_4_ano = merged_df[merged_df['fase'] == "4 ANO"]

    # DataFrame onde a fase não é "4 ANO"
    df_fase_8_ano = merged_df[merged_df['fase'] != "4 ANO"]

    # Reordenando as colunas
    open_index = ['estudante_id', 'codigo', 'nome', 'ano_matricula', 'distrito', 'escola', 'fase', 'turma', 'turno', 'curso_id', 'simulado_id', 'resultado_id',
                      '10401', '10402', '10403', '10404', '10405', '10406', '10407', '10408', '10409', '104010', '104011', '104012',
                      '104013', '104014', '104015', '104016', '104017', '104018', '104019', '104020', '104021', '104022', '104023',
                      '104024', '104025', '104026', '104027', '104028', '104029', '104030', '104031', '104032', '104033', '104034', 
                      '104035', '104036', '104037',
                      '104038', '104039', '104040', '104041', '104042', '104043', '104044', '10801', '10802', '10803', '10804', 
                      '10805', '10806', '10807', '10808', '10809', '108010',
                      '108011', '108012', '108013', '108014', '108015', '108016', '108017', '108018', '108019', '108020', '108021', '108022',
                      '108023', '108024', '108025', '108026', '108027', '108028', '108029', '108030', '108031', '108032', '108033', '108034', '108035', '108036', 
                      '108037', '108038', '108039', '108040', '108041', '108042', '108043', '108044']

    # Garantindo que todas as colunas estejam presentes
    missing_cols = set(open_index) - set(merged_df.columns)
    for col in missing_cols:
        merged_df[col] = np.nan
    
    merged_df = merged_df.reindex(columns=open_index)

    #Para anular uma questão: 
    df_fase_8_ano['108035'] = 1
    
    with pd.ExcelWriter('folha_4_ano.xlsx', engine='openpyxl') as writer:
        df_fase_4_ano.to_excel(writer, sheet_name='4 ano')

    # Salvar o segundo DataFrame em outro arquivo Excel
    with pd.ExcelWriter('folha_8_ano.xlsx', engine='openpyxl') as writer:
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