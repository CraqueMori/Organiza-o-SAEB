import pandas as pd
import time
import sqlalchemy

import importlib
import saeb.global_keys as global_keys
import saeb.global_aux as global_aux

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

    # Carregamento de dados
    d_estudantes = pd.read_sql(f"SELECT * FROM {SCHEMA}.d_estudantes;", engine)
    f_resultados = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados;", engine)
    f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)

    print(f_resultados_respostas)
    gabarito = ["GABARITO", '10401B', '10402D', '10403C','10404A', '10405D', '10406B', '10407B', '10408A', '10409C', '10410B', '10411D', 
                '10412D', '10413A', '10414B', '10415C', '10416D', '10417D', '10418B', '10419C', '10420A', '10421B', '10422C', 
                '10423A', '10424B', '10425A', '10426C', '10427B', '10428C', '10429B', '10430A', '10431D', '10432C', '10433D', 
                '10434C', '10435C', '10436D', '10437A', '10438A', '10439B', '10440C', '10441A', '10442D', '10443B', '10444A', 
                '10801A', '10802D', '10803C', '10804B', '10805C', '10806D', '10807A', '10808D', '10809A', '10810D', '10811C', 
                '10812B', '10813D', '10814A', '10815D', '10816C', '10817A', '10818D', '10819B', '10820C', '10821A', '10822C', 
                '10823B', '10824C', '10825C', '10826B', '10827D', '10828A', '10829D', '10830B', '10831C', '10832B', '10833C', 
                '10834D', '10835A', '10836D', '10837B', '10838C', '10839D', '10840B', '10841C', '10842A', '10843D', '10844B']
    

    gabarito_dict = {item[:-1]: item[-1:] for item in gabarito}
    print(gabarito_dict)

    for index, row in f_resultados_respostas.iterrows():
        questao_id = row['questao_id']  # Pega o ID da questão da linha atual
        alternativa_id = row['alternativa_id'][:5]  # Pega a alternativa selecionada da linha atual
        resultado_id = row['resultado_id']
        resposta_correta = gabarito_dict.get(questao_id)  # Pega a resposta correta do dicionário de gabarito
        print(resposta_correta, "Resposta do Gabarito")
        print(resultado_id, "Resultado ID")
        print(alternativa_id, "Alternativa ID")
        

        # Verifica se a resposta está correta
        if resposta_correta:
            print(alternativa_id, "Alternativa dentro do IF")
            if alternativa_id == resposta_correta:
                print(alternativa_id, "Dentro do Primeiro IF")
                f_resultados_respostas.at[index, 'alternativa_id'] = 1  # Marca como correta
            elif alternativa_id == questao_id:
                print(alternativa_id, "Dentro do Segundo IF")
                f_resultados_respostas.at[index, 'alternativa_id'] = "Em Branco"  # Marca como em branco
            elif alternativa_id == questao_id + "N":  # Supondo que 'N' é a designação para rasura
                print(alternativa_id, "Dentro do Terceiro IF")
                f_resultados_respostas.at[index, 'alternativa_id'] = "Rasura"  # Marca como rasura
            else:
                print(alternativa_id, "Dentro do Else")
                f_resultados_respostas.at[index, 'alternativa_id'] = 0  # Marca como incorreta
        else:
            print(f"Questão {questao_id} não encontrada no gabarito.")



    d_estudantes = d_estudantes.drop(columns=['estudante_registro_id', 'sexo', 'telefone', 'necessidades', 'data_nascimento', 'escola_id', 'escola_inep', 'estudante_inep'])

    merged_df = pd.merge(d_estudantes, f_resultados[['estudante_id', 'resultado_id']], on='estudante_id', how='left')

    # Pivoteando os resultados das respostas
    resultado_pivot = f_resultados_respostas.pivot_table(index='resultado_id', columns='questao_id', values='alternativa_id', aggfunc='first').reset_index()

    # Renomeando colunas para incluir um prefixo e evitar conflitos de nome
    resultado_pivot.columns = ['resultado_id'] + [f'questao_{col}' for col in resultado_pivot.columns if col != 'resultado_id']

    # Merge do pivot com merged_df
    merged_df = pd.merge(merged_df, resultado_pivot, on='resultado_id', how='left')
    merged_df.to_excel('teste_comp_3.xlsx', engine="openpyxl")
    # Mostrando o DataFrame final
    #print(merged_df)

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
