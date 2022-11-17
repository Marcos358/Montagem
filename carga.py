import warnings
warnings.filterwarnings('ignore')

from ftplib import FTP
import os
import pandas as pd
import numpy as np
from pysus.online_data.SIH import download
import warnings
import time
import plotly
import sys
import psycopg2
from IPython.display import clear_output


with open('senha.txt','r') as f:
    passw = f.read().replace('\n','')

class Carga:
    global passw
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
        
    def start(self, TabName = 'brasil'):

        _query_P = """
        CREATE TABLE IF NOT EXISTS pessoa.""" + TabName + """ (
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
        CREATE TABLE IF NOT EXISTS ocorrencia.""" + TabName + """ (
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

        self.__open_connection()
        _cur = self.con.cursor()
        _cur.execute(_query_P)
        _cur.execute(_query_O)
        self.con.commit()
        self.con.close()

        self.Exists = True
        
    
    def __insere(self, df, schema, tabela = 'brasil'):

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
        

    def envia(self, files = None, schemas = ['Pessoa','Ocorrencia']):
        
        self.__open_connection()
        src = self.src
        for sch in schemas:
            print('Schema: ' + sch + '\n')
            
            if files == None:
                files = os.listdir(src + '/' + sch)
                
            _t = len(files)
            _c = 0
            _erros = 0
            _ac = 0

            for f in files:
                if f not in self.loadeds[sch]:
                    clear_output(wait = True)
                    _c += 1
                    s = '*' * int(50 * _c/_t) + '-' * (50 - int(50 * _c/_t))
                    df = pd.read_parquet(self.src + '/' + sch + '/' + f)
                    self.__insere(df = df, schema = sch.lower())
                    _aux = pd.read_pickle('loaded_' + sch + '.pkl')
                    _aux.append(f)
                    pd.to_pickle(_aux,'loaded_' + sch + '.pkl')
                    self.loadeds = {'Pessoa':pd.read_pickle('loaded_Pessoa.pkl'),
                        'Ocorrencia':pd.read_pickle('loaded_Ocorrencia.pkl')}
                    print('Q: ' + f + '\n' + s + ' {:.2f}%'. \
                          format(100 * _c/_t) + '\nErros: {}, Ok: {} \n'. \
                          format(self.errors, self.inserts))

        self.con.commit()
        self.con.close()
        
SIH = Carga()
SIH.start()
SIH.envia()

print('Carga finalizada')


