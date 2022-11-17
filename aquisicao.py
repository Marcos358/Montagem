import warnings
warnings.filterwarnings('ignore')

import os
import time
import numpy as np
import pandas as pd
from datetime import datetime
from IPython.display import clear_output
from pysus.online_data.SIH import download


### Criando as pastas do diretório

estados = ['ac','al','ap','am','ba','ce','df','es','go','ma','mt','ms','mg','pa',
           'pb','pr','pe','pi','rj','rn','rs','ro','rr','sc','se','to']
if 'dados' not in os.listdir():
    os.mkdir('dados')
for e in estados:
    if e not in os.listdir('dados'):
        os.mkdir('dados/' + e)
if 'dados3' not in os.listdir():
    os.mkdir('dados3')
if 'Pessoa' not in os.listdir('dados3'):
    os.mkdir('dados3/Pessoa')
if 'Ocorrencia' not in os.listdir('dados3'):
    os.mkdir('dados3/Ocorrencia')
    
files = os.listdir('dados3/Pessoa')
pd.to_pickle(files, 'loaded_Pessoa.pkl')
files = os.listdir('dados3/Ocorrencia')
pd.to_pickle(files, 'loaded_Ocorrencia.pkl')
print('Pastas criadas')


def extrai(df):  
    '''
    função para transformar o dataframe original num formato compatível com o banco
    '''
    
    colunas = ['sexo','nascimento','cod_idade','idade','nacionalidade','raca_cor','etnia','filhos',
               'escolaridade','ocupação','municipio']
    indices = [10,9,49,50,53,83,84,59,60,67,8]
    Pessoa = df.iloc[:,indices]
    Pessoa.set_axis(colunas, axis = 1, inplace = True)
    Pessoa = Pessoa.assign(pID = list(Pessoa.index))
    Pessoa['idade'] = [Pessoa['idade'][i] if Pessoa['cod_idade'][i] == '4' else 0 for i in       range(Pessoa.shape[0])]
    Pessoa = Pessoa[['pID','municipio','nascimento','sexo','idade','raca_cor','escolaridade',
                     'filhos','ocupação','nacionalidade']]
    
    colunas = ['AIH','valor','data','saida','morte','complexidade',
           'CID','diag_sec','municipio']

    indices = [5,35,38,39,52,79,40,95,48]
    Ocorrencia = df.iloc[:,indices]
    Ocorrencia.set_axis(colunas, axis = 1, inplace = True)
    Ocorrencia = Ocorrencia.assign(pID = 0)
    Ocorrencia = Ocorrencia[['AIH','pID','municipio','CID','data','valor','diag_sec','saida',
                             'morte','complexidade']]

    DIC = {'Pessoa':Pessoa, 'Ocorrencia':Ocorrencia}

    return DIC


estados = ['ac','al','ap','am','ba','ce','df','es','go','ma','mt','ms','mg','pa',
           'pb','pr','pe','pi','rj','rn','rs','ro','rr','sc','se','to','sp']
anos = int(input('Janela de tempo em anos: ')) - 1
anos = list(range(datetime.now().year - anos, datetime.now().year + 1))
meses = list(range(1,13))

class Extracao:
    def __init__(self, pasta1 = 'dados', pasta2 = 'dados3'):
        global estados, anos, meses
        self.pasta1 = {'path':pasta1,'last_update':str(datetime.now()),
                       'folders':os.listdir(pasta1), 'size':0}
        self.pasta2 = {'path':pasta2,'last_update':str(datetime.now()),
                       'folders':os.listdir(pasta2), 'size':0}
        self.meses = []
        self.anos = []
        self.UFs_ft = [i for i in estados if i not in os.listdir(pasta1)]
        self.estados = []
        
        s = 0
        for e in os.listdir(pasta1):
            for f in os.listdir(pasta1 + '/' + e):
                s += os.path.getsize(pasta1 + '/' + e + '/' + f)
        self.pasta1['size'] = s // 10 ** 6
        
        s = 0
        for e in os.listdir(pasta2):
            for f in os.listdir(pasta2 + '/' + e):
                s += os.path.getsize(pasta2 + '/' + e + '/' + f)
        self.pasta2['size'] = s // 10 ** 6

    def aquis(self, anos = anos, meses = meses, exc = 'sp', path = None):
        
        t0 = time.time()
        print('Extração iniciada \n')
        UFs = [input('Algum estado específico? ')]
        if UFs == ['']:
            UFs = estados
            
        UFs = [i for i in UFs if i != exc]
        if path == None:
            path = self.pasta1['path']
        
        T = len(UFs) * len(anos) * len(meses)
        c, t0 = 0, time.time()
        _UFs, _anos, _meses = self.estados, self.anos, self.meses
        size = 0
        _c = 0
        for UF in UFs:
            _UFs.append(UF)
            for a in anos:
                for m in meses:
                    if a == datetime.now().year and m > datetime.now().month - 6:
                        pass
                    else:
                        clear_output(wait = True)
                        c += 1
                        s = '*' * int(50 * c/T) + '-' * (50 - int(50 * c/T))
                        print('Extração iniciada \n')
                        print('Arquivo: ' + UF + '_' + str(a) + '_'+str(m) + '\n'
                              + s + ' {:.2f}% \n{:.2f}s'.format(100 * c / T, time.time() - t0))
                        s = str(a) + '_' + str(m) + '.parquet'
                        if s not in os.listdir(path + '/' + UF):
                            _c += 1
                            cur = download(UF,a,m)
                            cur.to_parquet(path + '/' + UF + '/' + s)
                            self.pasta1['last_update'] = str(datetime.now())
                            _anos.append(a)
                            _meses.append(m)
                            size += os.path.getsize(path + '/' + UF + '/' + s)

        self.pasta1['size'] = size // 10 ** 6
        
        self.estados = list(set(_UFs))
        self.anos = list(set(_anos))
        self.meses = list(set(_meses))           
        print('Aquisiçao terminou em {:.2f}s \n{} arquivos baixados'.format(time.time() - t0, _c))
        

    def padroniza(self, src = None, dst = None):
        
        t0 = time.time()
        if src == None:
            src = self.pasta1['path']
        if dst == None:
            dst = self.pasta2['path']
            
        names = os.listdir(src)
        UFs = {}
        for e in names:
            UFs.update({e: os.listdir(src + '/' + e)})

        T = sum([len(os.listdir(src + '/' + i)) for i in names])
        size, c, _c = 0, 0, 0
        for e in names:
            for f in UFs[e]:
                clear_output(wait = True)
                c += 1
                s = '*' * int(50 * c/T) + '-' * (50 - int(50 * c/T))
                print('Padronização iniciada \n')
                print('Arquivo: ' + e + '_' + f + '\n' + s + ' {:.2f}% \n{:.2f}s'. \
                      format(100 * c / T, time.time() - t0))
                s = e + '_' + f
                if (s not in os.listdir(dst + '/Pessoa')) or (s not in os.listdir(dst + '/Ocorrencia')):
                    _c += 1
                    cur = pd.read_parquet(src + '/' + e + '/' + f)
                    DIC = extrai(cur)
                    Pessoa = DIC['Pessoa']
                    Ocorrencia = DIC['Ocorrencia']
                    Pessoa.to_parquet(dst + '/Pessoa/' + e + '_' + f)
                    Ocorrencia.to_parquet(dst + '/Ocorrencia/' + e + '_' + f)
                    size += os.path.getsize(dst + '/Pessoa/' + e + '_' + f)
                    size += os.path.getsize(dst + '/Ocorrencia/' + e + '_' + f)
                    self.pasta2['last_update'] = str(datetime.now())
            
        self.pasta2['size'] = size // 10 ** 6
        print('Padronização terminou em {:.2f}s \n{} arquivos modificados'. \
              format(time.time()-t0, _c))
        
        
bd_SIH = Extracao()
bd_SIH.aquis()
bd_SIH.padroniza()