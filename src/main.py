import pandas as pd
import os
import glob

# caminho para ler os arquivos
folder_path = 'src\\data\\raw'

# lista de todos oss arquivos de excel
excel_files = glob.glob(os.path.join(folder_path , '*.xlsx'))

if not excel_files:
        print("Nenhum arquivo compátivel encontrado")
else:
        
    # dataframe = tabela na memória para guardar os conteúdos dos arquivos
    dfs = []

    for excel_file in excel_files:

        try:
          
          #Leitura do arquivo excel
          df_temp = pd.read_excel(excel_file)

          #pegar o nome do arquivo
          file_name = os.path.basename(excel_file)

          df_temp['filename'] = file_name

          #Coluna chamada location
          if 'brasil' in file_name.lower():
              df_temp['location'] = 'br'
          elif 'estados unidos' in file_name.lower():
              df_temp['location'] = 'eua'
          elif 'australia' in file_name.lower():
              df_temp['location'] = 'au'
          
          #Guardando dados tratados dentro de uma dataframe comum
          dfs.append(df_temp)

     
        except     Exception as e:
            print( f"Erro ao ler o arquivo {excel_file} : {e}")

if dfs:


   #Concatena todas as tabelas salvas no dfs em uma única tabela
   result = pd.concat(dfs, ignore_index=True)

   #caminho de saída
   output_file = os.path.join('src', 'data', 'ready', 'clean.xlsx')
   
   #Motor de escrita
   writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
   
   #Leva os dados do resultado a serem  escritos no motor de excel configurado
   result.to_excel(writer, index=False)
   
   #Salva o arquivo do excel
   writer._save()
else:
  print("nenhum dado para ser salvo")