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


def main(simulado):
    start_time = time.time()
    engine = sqlalchemy.create_engine(
        f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
        pool_pre_ping=True
    )

    f_resultados_respostas = pd.read_sql(f"SELECT * FROM {SCHEMA}.f_resultados_respostas;", engine)

    print(f_resultados_respostas.columns)

    f_resultados_respostas.drop(columns=['respostas_omr_markedtargets', 'respostas_omr_n_markedtargets', 'respostas_omr_one_markedtarget','data','resultado_resposta_registro_id'], inplace=True)
    
    
    global_aux.write2sql(dataframe=f_resultados_respostas, table_name='f_resultados_respostas', database_connection=engine, database_schema=SCHEMA, if_exists=IF_EXISTS)
    return

if __name__ == '__main__':
    simulado = 'Simulado 1'
    print('Simulado:', simulado)
    main(simulado)