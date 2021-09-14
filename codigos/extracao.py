import os
import zipfile
from ftplib import FTP

# %%
estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR',
           'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
anos = [2015, 2016, 2017, 2018, 2019]

diretorio = '/home/sergio/Documentos/neurotech/bases'

# %%
ftp = FTP('ftp.dadosabertos.ans.gov.br')

ftp.login()

for ano in anos:
    ftp.cwd(f'/FTP/PDA/TISS/HOSPITALAR/{ano}')
    for estado in estados:
        path = os.path.join(diretorio, estado)
        with open(path + '.zip', 'wb') as arq:
            ftp.retrbinary(f'RETR {estado}.zip', arq.write)

        if not os.path.exists(path):
            os.mkdir(path)

        with zipfile.ZipFile(path + '.zip', 'r') as zip_ref:
            zip_ref.extractall(path)