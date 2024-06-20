import sys 
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
import pandas as pd
from tqdm.auto import tqdm

import saeb.global_keys as global_keys

import saeb.global_aux as global_aux

CREDENTIALS_DW = global_keys.get_database_credentials()

SCHEMA_DW = 'teste_scripts_06_04'

NEW_SCHEMA = 'saeb_2024_testes'

IF_EXISTS = 'replace'



def main():
    engine_DW = sqlalchemy.create_engine(
        'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**CREDENTIALS_DW),
        pool_pre_ping=True
    )

    tabelas_lista = [
        'd_avaliacoes',
        'd_cursos',
        'd_dashboard_teste',
        'd_dashboard_novo',
        'd_deficiencia',
        'd_eixos_aux',
        'd_eixos_cognitivos',
        'd_escolas_novo',
        'd_estudantes',
        'd_presenca',
        'd_questoes',
        'd_questoes_alternativas',
        'd_questoes_eixos_aux',
        'd_questoes_eixos_new',
        'd_simulados',
        'f_resultados',
        'f_resultados_respostas'
    ]

    for tabela_banco in tqdm(tabelas_lista):
        # Leitura da tabela no esquema original
        query = f'SELECT * FROM {SCHEMA_DW}.{tabela_banco}'

        df = pd.read_sql(query, engine_DW)

        # Escrevendo o dataframe para o novo esquema
        df.to_sql(tabela_banco, engine_DW, schema=NEW_SCHEMA, if_exists=IF_EXISTS, index=False)

    

if __name__ == "__main__":
    main()
