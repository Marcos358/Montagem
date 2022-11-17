import warnings
warnings.filterwarnings('ignore')

import os
import sys
import time
import psycopg2
import numpy as np
import pandas as pd
import unidecode as ud
from IPython.display import clear_output
from pysus.online_data.SIH import download



ano, mes, dia, data = [], [], [], []
aux = [2,4,6,9,11]

for a in range(2010,2023):
    for m in range(1,13):
        for d in range(1,32):
            if m in aux and d > 30:
                pass
            elif m == 2 and d > 28:
                if a % 4 != 0 or d > 29:
                    pass
            else:
                s = str(a)
                if m < 10:
                    s += '0'
                s += str(m)
                if d < 10:
                    s += '0'
                s += str(d)
                ano.append(a)
                mes.append(m)
                dia.append(d)
                data.append(s)

Calendario = pd.DataFrame({'data':data,'ano':ano, 'mes':mes, 'dia':dia})


## Carregando os nomes dos munípios

Municipio = pd.read_excel('Municipios.xls')[['Nome_Município','Código Município Completo']]
Municipio.iloc[:,1] = [str(Municipio.iloc[x,1])[:-1] for x in range(np.shape(Municipio)[0])]

Municipio.rename(columns = {'Nome_Município':'municipio', 'Código Município Completo':'UF_ZI'}, 
           inplace = True)
Municipio = Municipio[['UF_ZI','municipio']]

Municipio = Municipio.drop_duplicates()

cods = [11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]
nomes = ['ro','ac','am','rr','pa','ap','to','ma','pi','ce','rn','pb','pe','al',
         'se','ba','mg','es','rj','sp','pr','sc','rs','ms','mg','go','df']
reg = ['norte','norte','norte','norte','norte','norte','norte','nordeste',
       'nordeste','nordeste','nordeste','nordeste','nordeste','nordeste','nordeste','nordeste',
       'sudeste','sudeste','sudeste','sudeste','sul','sul','sul','CO','CO','CO','CO']
UFS = pd.DataFrame({'cod':[str(i) for i in cods], 'sigla':nomes, 'regiao':reg})

Municipio = Municipio.assign(cod = [i[0:2] for i in Municipio['UF_ZI']])
Municipio = UFS.merge(Municipio, on = 'cod', how = 'outer')

colunas = ['cod','UF','regiao','municipio','nome']
Municipio.set_axis(colunas, axis = 1, inplace = True)
Municipio.pop('cod')
Municipio = Municipio[['municipio','nome','UF','regiao']]
Municipio['nome'] = [i.replace("'",'') for i in Municipio['nome']]


## carregando nomes das doenças

CID = pd.read_csv('cid10tab.csv')
CID.rename(columns = {'CID':'DIAG_PRINC','Nome':'doença'}, inplace = True)

x = list(set(CID['DIAG_PRINC']))
x.sort()
caps = pd.read_excel('CID10-codigos.xlsx')[['Capítulo','Códigos']]
caps['last'] = ''
caps = caps.sort_values(by = 'Códigos')
for i in range(np.shape(caps)[0]):
    caps['last'][i] = caps['Códigos'][i][4:] + '999'

cur = 0
for i in range(len(x)):
    x[i] = [x[i], caps['Capítulo'][cur]]
    if x[i][0] >= caps['last'][cur]:
        cur += 1
caps = pd.DataFrame(data = {'DIAG_PRINC':[i[0] for i in x], 'capitulo':[i[1] for i in x]})
CID = caps.merge(CID, on = 'DIAG_PRINC', how = 'inner')
colunas = ['CID','capitulo','nome']
CID.set_axis(colunas, axis = 1, inplace = True)

for i in range(CID.shape[0]):
    for j in range(CID.shape[1]):
        CID.iloc[i,j] = ud.unidecode(CID.iloc[i,j].replace("'",''))
        
for i in range(Municipio.shape[0]):
    for j in range(Municipio.shape[1]):
        Municipio.iloc[i,j] = ud.unidecode(Municipio.iloc[i,j].replace("'",''))

for i in range(Calendario.shape[0]):
    for j in range(1):
        Calendario.iloc[i,j] = ud.unidecode(Calendario.iloc[i,j].replace("'",''))

with open('senha.txt','r') as f:
    passw = f.read().replace('\n','')

class Carga:
    global passw, Calendario, CID, Municipio
    def __init__(self, src='dados3'):
        self.src      = src
        self.user     = input('Digite seu usuário: ')
        self.host     = input('Digite o host: ')
        self.passw    = passw
        self.database = input('Digite o nome do database: ')
        self.Exists   = False
        self.inserts  = 0
        self.errors   = 0
        self.con      = psycopg2.connect(host=self.host, database=self.database,
                                    user=self.user, password=self.passw)
        self.con.close()
        self.loadeds = {'Pessoa':pd.read_pickle('loaded_Pessoa.pkl'),
                        'Ocorrencia':pd.read_pickle('loaded_Ocorrencia.pkl')}
    
    def __open_connection(self):
        self.con = psycopg2.connect(host=self.host, database=self.database,
                                    user=self.user, password=self.passw)
        
    def start(self, TabNames = ['pessoa','ocorrencia']):

        _query_P = """
        CREATE TABLE IF NOT EXISTS brasil.""" + TabNames[0] + """ (
          id serial, pID int,
          municipio varchar(20),
          nascimento varchar(20),
          sexo varchar(20),
          idade int,
          raca_cor varchar(20),
          escolaridade varchar(20),
          filhos int,
          ocupação varchar(20),
          nacionalidade varchar(20)) """

        _query_O = """
        CREATE TABLE IF NOT EXISTS brasil.""" + TabNames[1] + """ (
              id serial,
              AIH varchar(20),
              pID int,
              municipio varchar(20),
              CID varchar(20),
              data varchar(20),
              valor float,
              diag_sec varchar(20),
              saida varchar(20),
              morte int,
              complexidade varchar(20)) """
        
        q_Cal = """
            create table if not exists brasil.calendario (
                data char(8) primary key,
                ano int,
                mes int,
                dia int) """
        q_CID = """
            create table if not exists brasil.cid (
                CID varchar(10) primary key,
                capitulo varchar(10),
                nome varchar(400)) """
        q_Mun = """
            create table if not exists brasil.municipio (
                municipio varchar(10) primary key,
                nome varchar(400),
                UF char(2),
                regiao varchar(20)) """
        


        
        self.__open_connection()
        _cur = self.con.cursor()
        _cur.execute(_query_P)
        _cur.execute(_query_O)
        _cur.execute(q_Cal)
        _cur.execute(q_CID)
        _cur.execute(q_Mun)
        self.con.commit()
        self.con.close()

        self.Exists = True
        print('Tabelas criadas')
        
    
    def __insere(self, df, tabela, schema = 'brasil'):

        _cols = list(df.columns)
        colunas = ''
        for i in _cols:
            colunas += i + ','
        colunas = colunas[:-1]


        cursor = self.con.cursor()
        self.inserts = 0
        self.errors = 0
        for i in range(df.shape[0]):
            registro = ''
            for j in range(df.shape[1]):
                s = df.iloc[i,j]
                if type(s) == str:
                    s = "'" + s + "'"
                else:
                    s = str(df.iloc[i,j])
                registro += s + ','

            registro = registro[:-1]
            query = """insert into {}.{}({}) values ({})""". \
                    format(schema, tabela, colunas, registro)
            
            try:
                cursor.execute(query)
                cursor.fetchone
                self.inserts += 1

            except:
                self.errors += 1
        

    def envia(self, files = None, tabelas = ['Pessoa','Ocorrencia']):
        
        t0 = time.time()
        self.__open_connection()
        src = self.src
        for sch in tabelas:
            print('Tabela: ' + sch + '\n')
            
            if files == None:
                files = os.listdir(src + '/' + sch)
                
            _t = len(files)
            _c = 0
            _erros = 0
            _ac = 0
            c = 0
            
            _aux1, _aux2 = [CID, Municipio, Calendario], ['cid', 'municipio', 'calendario']
            for _aux in range(3):
                f = _aux2[_aux]
                if f not in self.loadeds[sch]:
                    df = _aux1[_aux]
                    self.__insere(df = df, tabela = f)
                    _aux = pd.read_pickle('loaded_' + sch + '.pkl')
                    _aux.append(f)
                    pd.to_pickle(_aux,'loaded_Pessoa.pkl')
                    pd.to_pickle(_aux,'loaded_Ocorrencia.pkl')
                    self.loadeds = {'Pessoa':pd.read_pickle('loaded_Pessoa.pkl'),
                        'Ocorrencia':pd.read_pickle('loaded_Ocorrencia.pkl')}

            for f in files:
                clear_output(wait = True)
                _c += 1
                s = '*' * int(50 * _c/_t) + '-' * (50 - int(50 * _c/_t))
                print('Q: ' + f + '\n' + s + ' {:.2f}%'. \
                          format(100 * _c/_t) + '\nErros: {}, Ok: {} \n{:.2f}s \n'. \
                          format(self.errors, self.inserts, time.time() - t0))
                if f not in self.loadeds[sch]:
                    c += 1
                    df = pd.read_parquet(self.src + '/' + sch + '/' + f)
                    self.__insere(df = df, tabela = sch.lower())
                    _aux = pd.read_pickle('loaded_' + sch + '.pkl')
                    _aux.append(f)
                    pd.to_pickle(_aux,'loaded_' + sch + '.pkl')
                    self.loadeds = {'Pessoa':pd.read_pickle('loaded_Pessoa.pkl'),
                        'Ocorrencia':pd.read_pickle('loaded_Ocorrencia.pkl')}

        self.con.commit()
        self.con.close()
        print('Carga completa: {:.2f}s'.format(time.time() - t0))

        
SIH = Carga()
SIH.start()
SIH.envia()



