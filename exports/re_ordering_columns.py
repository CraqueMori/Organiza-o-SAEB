import pandas as pd
# Carregar todas as planilhas do arquivo Excel
xlsx = pd.ExcelFile('export_eloi_15_05.xlsx')

# Criar um escritor Excel usando openpyxl
with pd.ExcelWriter('tabela_dashboard.xlsx', engine='openpyxl') as writer:
    # Iterar sobre todas as planilhas
    for sheet_name in xlsx.sheet_names:
        # Carregar a planilha atual
        dataframe = pd.read_excel(xlsx, sheet_name=sheet_name)
        print(dataframe)
        # Lista de colunas esperadas (ajuste conforme necessário)
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

        # Reindexar o DataFrame com as colunas corrigidas
        dataframe = dataframe.reindex(columns=open_index)

        # Salvar o DataFrame ajustado em uma nova folha do arquivo Excel de saída
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False)  # index=False para não incluir o índice como uma coluna