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

    dataframe = pd.read_excel('teste_comp.xlsx')

    gabarito = ['10401B', '10402D', '10403C','10404A', '10405D', '10406B', '10407B', '10408A', '10409C', '10410B', '10411D', 
                '10412D', '10413A', '10414B', '10415C', '10416D', '10417D', '10418B', '10419C', '10420A', '10421B', '10422C', 
                '10423A', '10424B', '10425A', '10426C', '10427B', '10428C', '10429B', '10430A', '10431D', '10432C', '10433D', 
                '10434C', '10435C', '10436D', '10437A', '10438A', '10439B', '10440C', '10441A', '10442D', '10443B', '10444A', 
                '10801A', '10802D', '10803C', '10804B', '10805C', '10806D', '10807A', '10808D', '10809A', '10810D', '10811C', 
                '10812B', '10813D', '10814A', '10815D', '10816C', '10817A', '10818D', '10819B', '10820C', '10821A', '10822C', 
                '10823B', '10824C', '10825C', '10826B', '10827D', '10828A', '10829D', '10830B', '10831C', '10832B', '10833C', 
                '10834D', '10835A', '10836D', '10837B', '10838C', '10839D', '10840B', '10841C', '10842A', '10843D', '10844B']
    

    # Processar respostas
    


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
