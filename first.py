from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as B
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, count, regexp_extract
import mysql.connector
import pandas as pd
import numpy as np

caminho_planilha = "gs://dataproc-staging-sa-east1-38488625567-ytcttqkr/notebooks"


if __name__ == "__main__":

    # ----------------------------------------------------------
    # -- PLANILHA 2015
    # ----------------------------------------------------------
    pla_2015_1 = pd.read_csv(f"{caminho_planilha}/ca-2015-01.csv", sep=";")
    pla_2015_2 = pd.read_csv(f"{caminho_planilha}/ca-2015-02.csv", sep=";")
    pla_2015_2 = pla_2015_2.drop(0)
    pla_2015 = pd.concat([pla_2015_1, pla_2015_2])

    
    # TRATAMENTO
    pla_2015['Regiao - Sigla'] = pla_2015['Regiao - Sigla'].str.replace("'", "")
    pla_2015['Regiao - Sigla'] = pla_2015['Regiao - Sigla'].str.replace('"', '')
    pla_2015['Estado - Sigla'] = pla_2015['Estado - Sigla'].str.replace("'", "")
    pla_2015['Estado - Sigla'] = pla_2015['Estado - Sigla'].str.replace('"', '')
    pla_2015['Municipio'] = pla_2015['Municipio'].str.replace("'", "")
    pla_2015['Municipio'] = pla_2015['Municipio'].str.replace('"', '')
    pla_2015['Revenda'] = pla_2015['Revenda'].str.replace("'", "")
    pla_2015['Revenda'] = pla_2015['Revenda'].str.replace('"', '')
    pla_2015['CNPJ da Revenda'] = pla_2015['CNPJ da Revenda'].str.replace("'", "")
    pla_2015['CNPJ da Revenda'] = pla_2015['CNPJ da Revenda'].str.replace('"', '')
    pla_2015['Nome da Rua'] = pla_2015['Nome da Rua'].str.replace("'", "")
    pla_2015['Nome da Rua'] = pla_2015['Nome da Rua'].str.replace('"', '')
    pla_2015['Numero Rua'] = pla_2015['Numero Rua'].str.replace("'", "")
    pla_2015['Numero Rua'] = pla_2015['Numero Rua'].str.replace('"', '')
    pla_2015['Complemento'] = pla_2015['Complemento'].str.replace("'", "")
    pla_2015['Complemento'] = pla_2015['Complemento'].str.replace('"', '')
    pla_2015['Bairro'] = pla_2015['Bairro'].str.replace("'", "")
    pla_2015['Bairro'] = pla_2015['Bairro'].str.replace('"', '')
    pla_2015['Cep'] = pla_2015['Cep'].str.replace("'", "")
    pla_2015['Cep'] = pla_2015['Cep'].str.replace('"', '')
    pla_2015['Produto'] = pla_2015['Produto'].str.replace("'", "")
    pla_2015['Produto'] = pla_2015['Produto'].str.replace('"', '')
    pla_2015['Bandeira'] = pla_2015['Bandeira'].str.replace("'", "")
    pla_2015['Bandeira'] = pla_2015['Bandeira'].str.replace('"', '')
    pla_2015['Data da Coleta'] = pla_2015['Data da Coleta'].str.replace("'", "")
    pla_2015['Data da Coleta'] = pla_2015['Data da Coleta'].str.replace('"', '')
    pla_2015['Valor de Venda'] = pla_2015['Valor de Venda'].str.replace("'", "")
    pla_2015['Valor de Venda'] = pla_2015['Valor de Venda'].str.replace('"', '')
    pla_2015['Valor de Compra'] = pla_2015['Valor de Compra'].str.replace("'", "")
    pla_2015['Valor de Compra'] = pla_2015['Valor de Compra'].str.replace('"', '')
    pla_2015['Unidade de Medida'] = pla_2015['Unidade de Medida'].str.replace("'", "")
    pla_2015['Unidade de Medida'] = pla_2015['Unidade de Medida'].str.replace('"', '')
    

    con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
    cursor = con.cursor()
    
    
    for index, row in pla_2015.iterrows():
        
        valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])  
        sql = "INSERT INTO consumo_2015 (regiao_sigla,estado_sigla,municipio,revenda,CNPJ_revenda,nome_da_rua,numero_rua,complemento,bairro,cep,produto,data_da_coleta,valor_de_venda,valor_de_compra,unidade_de_medida,bandeira) values "+valores+";"
        cursor.execute(sql)
    
    cursor.close()
    con.commit()
    con.close()
        

    print("fim da inserção 2015")
    
    # ----------------------------------------------------------
    # -- PLANILHA 2016
    # ----------------------------------------------------------
    pla_2016_1 = pd.read_csv(f"{caminho_planilha}/ca-2016-01.csv", sep=";")
    pla_2016_2 = pd.read_csv(f"{caminho_planilha}/ca-2016-02.csv", sep=";")
    pla_2016_2 = pla_2016_2.drop(0)
    pla_2016 = pd.concat([pla_2016_1, pla_2016_2])

    
    # TRATAMENTO
    pla_2016['Regiao - Sigla'] = pla_2016['Regiao - Sigla'].str.replace("'", "")
    pla_2016['Regiao - Sigla'] = pla_2016['Regiao - Sigla'].str.replace('"', '')
    pla_2016['Estado - Sigla'] = pla_2016['Estado - Sigla'].str.replace("'", "")
    pla_2016['Estado - Sigla'] = pla_2016['Estado - Sigla'].str.replace('"', '')
    pla_2016['Municipio'] = pla_2016['Municipio'].str.replace("'", "")
    pla_2016['Municipio'] = pla_2016['Municipio'].str.replace('"', '')
    pla_2016['Revenda'] = pla_2016['Revenda'].str.replace("'", "")
    pla_2016['Revenda'] = pla_2016['Revenda'].str.replace('"', '')
    pla_2016['CNPJ da Revenda'] = pla_2016['CNPJ da Revenda'].str.replace("'", "")
    pla_2016['CNPJ da Revenda'] = pla_2016['CNPJ da Revenda'].str.replace('"', '')
    pla_2016['Nome da Rua'] = pla_2016['Nome da Rua'].str.replace("'", "")
    pla_2016['Nome da Rua'] = pla_2016['Nome da Rua'].str.replace('"', '')
    pla_2016['Numero Rua'] = pla_2016['Numero Rua'].str.replace("'", "")
    pla_2016['Numero Rua'] = pla_2016['Numero Rua'].str.replace('"', '')
    pla_2016['Complemento'] = pla_2016['Complemento'].str.replace("'", "")
    pla_2016['Complemento'] = pla_2016['Complemento'].str.replace('"', '')
    pla_2016['Bairro'] = pla_2016['Bairro'].str.replace("'", "")
    pla_2016['Bairro'] = pla_2016['Bairro'].str.replace('"', '')
    pla_2016['Cep'] = pla_2016['Cep'].str.replace("'", "")
    pla_2016['Cep'] = pla_2016['Cep'].str.replace('"', '')
    pla_2016['Produto'] = pla_2016['Produto'].str.replace("'", "")
    pla_2016['Produto'] = pla_2016['Produto'].str.replace('"', '')
    pla_2016['Bandeira'] = pla_2016['Bandeira'].str.replace("'", "")
    pla_2016['Bandeira'] = pla_2016['Bandeira'].str.replace('"', '')
    pla_2016['Data da Coleta'] = pla_2016['Data da Coleta'].str.replace("'", "")
    pla_2016['Data da Coleta'] = pla_2016['Data da Coleta'].str.replace('"', '')
    pla_2016['Valor de Venda'] = pla_2016['Valor de Venda'].str.replace("'", "")
    pla_2016['Valor de Venda'] = pla_2016['Valor de Venda'].str.replace('"', '')
    pla_2016['Valor de Compra'] = pla_2016['Valor de Compra'].str.replace("'", "")
    pla_2016['Valor de Compra'] = pla_2016['Valor de Compra'].str.replace('"', '')
    pla_2016['Unidade de Medida'] = pla_2016['Unidade de Medida'].str.replace("'", "")
    pla_2016['Unidade de Medida'] = pla_2016['Unidade de Medida'].str.replace('"', '')

    con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
    cursor = con.cursor()
    
    
    for index, row in pla_2016.iterrows():
        
        valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])  
        sql = "INSERT INTO consumo_2016 (regiao_sigla,estado_sigla,municipio,revenda,CNPJ_revenda,nome_da_rua,numero_rua,complemento,bairro,cep,produto,data_da_coleta,valor_de_venda,valor_de_compra,unidade_de_medida,bandeira) values "+valores+";"
        cursor.execute(sql)
    cursor.close()
    con.commit()
    con.close()
        

    print("fim da inserção 2016")
    
    # # ----------------------------------------------------------
    # # -- PLANILHA 2017
    # # ----------------------------------------------------------
    pla_2017_1 = pd.read_csv(f"{caminho_planilha}/ca-2017-01.csv", sep=";")
    pla_2017_2 = pd.read_csv(f"{caminho_planilha}/ca-2017-02.csv", sep=";")
    pla_2017_2 = pla_2017_2.drop(0)
    pla_2017 = pd.concat([pla_2017_1, pla_2017_2])

    
    # TRATAMENTO
    pla_2017['Regiao - Sigla'] = pla_2017['Regiao - Sigla'].str.replace("'", "")
    pla_2017['Regiao - Sigla'] = pla_2017['Regiao - Sigla'].str.replace('"', '')
    pla_2017['Estado - Sigla'] = pla_2017['Estado - Sigla'].str.replace("'", "")
    pla_2017['Estado - Sigla'] = pla_2017['Estado - Sigla'].str.replace('"', '')
    pla_2017['Municipio'] = pla_2017['Municipio'].str.replace("'", "")
    pla_2017['Municipio'] = pla_2017['Municipio'].str.replace('"', '')
    pla_2017['Revenda'] = pla_2017['Revenda'].str.replace("'", "")
    pla_2017['Revenda'] = pla_2017['Revenda'].str.replace('"', '')
    pla_2017['CNPJ da Revenda'] = pla_2017['CNPJ da Revenda'].str.replace("'", "")
    pla_2017['CNPJ da Revenda'] = pla_2017['CNPJ da Revenda'].str.replace('"', '')
    pla_2017['Nome da Rua'] = pla_2017['Nome da Rua'].str.replace("'", "")
    pla_2017['Nome da Rua'] = pla_2017['Nome da Rua'].str.replace('"', '')
    pla_2017['Numero Rua'] = pla_2017['Numero Rua'].str.replace("'", "")
    pla_2017['Numero Rua'] = pla_2017['Numero Rua'].str.replace('"', '')
    pla_2017['Complemento'] = pla_2017['Complemento'].str.replace("'", "")
    pla_2017['Complemento'] = pla_2017['Complemento'].str.replace('"', '')
    pla_2017['Bairro'] = pla_2017['Bairro'].str.replace("'", "")
    pla_2017['Bairro'] = pla_2017['Bairro'].str.replace('"', '')
    pla_2017['Cep'] = pla_2017['Cep'].str.replace("'", "")
    pla_2017['Cep'] = pla_2017['Cep'].str.replace('"', '')
    pla_2017['Produto'] = pla_2017['Produto'].str.replace("'", "")
    pla_2017['Produto'] = pla_2017['Produto'].str.replace('"', '')
    pla_2017['Bandeira'] = pla_2017['Bandeira'].str.replace("'", "")
    pla_2017['Bandeira'] = pla_2017['Bandeira'].str.replace('"', '')
    pla_2017['Data da Coleta'] = pla_2017['Data da Coleta'].str.replace("'", "")
    pla_2017['Data da Coleta'] = pla_2017['Data da Coleta'].str.replace('"', '')
    pla_2017['Valor de Venda'] = pla_2017['Valor de Venda'].str.replace("'", "")
    pla_2017['Valor de Venda'] = pla_2017['Valor de Venda'].str.replace('"', '')
    pla_2017['Valor de Compra'] = pla_2017['Valor de Compra'].str.replace("'", "")
    pla_2017['Valor de Compra'] = pla_2017['Valor de Compra'].str.replace('"', '')
    pla_2017['Unidade de Medida'] = pla_2017['Unidade de Medida'].str.replace("'", "")
    pla_2017['Unidade de Medida'] = pla_2017['Unidade de Medida'].str.replace('"', '')

    con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
    cursor = con.cursor()
    
    
    for index, row in pla_2017.iterrows():
        
        valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])  
        sql = "INSERT INTO consumo_2017 (regiao_sigla,estado_sigla,municipio,revenda,CNPJ_revenda,nome_da_rua,numero_rua,complemento,bairro,cep,produto,data_da_coleta,valor_de_venda,valor_de_compra,unidade_de_medida,bandeira) values "+valores+";"
        cursor.execute(sql)
    cursor.close()
    con.commit()
    con.close()
    
        

    print("fim da inserção 2017")
    
    # ----------------------------------------------------------
    # -- PLANILHA 2018
    # ----------------------------------------------------------
    pla_2018_1 = pd.read_csv(f"{caminho_planilha}/ca-2018-01.csv", sep=";")
    pla_2018_2 = pd.read_csv(f"{caminho_planilha}/ca-2018-02.csv", sep=";")
    pla_2018_2 = pla_2018_2.drop(0)
    pla_2018 = pd.concat([pla_2018_1, pla_2018_2])

    
    # TRATAMENTO
    pla_2018['Regiao - Sigla'] = pla_2018['Regiao - Sigla'].str.replace("'", "")
    pla_2018['Regiao - Sigla'] = pla_2018['Regiao - Sigla'].str.replace('"', '')
    pla_2018['Estado - Sigla'] = pla_2018['Estado - Sigla'].str.replace("'", "")
    pla_2018['Estado - Sigla'] = pla_2018['Estado - Sigla'].str.replace('"', '')
    pla_2018['Municipio'] = pla_2018['Municipio'].str.replace("'", "")
    pla_2018['Municipio'] = pla_2018['Municipio'].str.replace('"', '')
    pla_2018['Revenda'] = pla_2018['Revenda'].str.replace("'", "")
    pla_2018['Revenda'] = pla_2018['Revenda'].str.replace('"', '')
    pla_2018['CNPJ da Revenda'] = pla_2018['CNPJ da Revenda'].str.replace("'", "")
    pla_2018['CNPJ da Revenda'] = pla_2018['CNPJ da Revenda'].str.replace('"', '')
    pla_2018['Nome da Rua'] = pla_2018['Nome da Rua'].str.replace("'", "")
    pla_2018['Nome da Rua'] = pla_2018['Nome da Rua'].str.replace('"', '')
    pla_2018['Numero Rua'] = pla_2018['Numero Rua'].str.replace("'", "")
    pla_2018['Numero Rua'] = pla_2018['Numero Rua'].str.replace('"', '')
    pla_2018['Complemento'] = pla_2018['Complemento'].str.replace("'", "")
    pla_2018['Complemento'] = pla_2018['Complemento'].str.replace('"', '')
    pla_2018['Bairro'] = pla_2018['Bairro'].str.replace("'", "")
    pla_2018['Bairro'] = pla_2018['Bairro'].str.replace('"', '')
    pla_2018['Cep'] = pla_2018['Cep'].str.replace("'", "")
    pla_2018['Cep'] = pla_2018['Cep'].str.replace('"', '')
    pla_2018['Produto'] = pla_2018['Produto'].str.replace("'", "")
    pla_2018['Produto'] = pla_2018['Produto'].str.replace('"', '')
    pla_2018['Bandeira'] = pla_2018['Bandeira'].str.replace("'", "")
    pla_2018['Bandeira'] = pla_2018['Bandeira'].str.replace('"', '')
    pla_2018['Data da Coleta'] = pla_2018['Data da Coleta'].str.replace("'", "")
    pla_2018['Data da Coleta'] = pla_2018['Data da Coleta'].str.replace('"', '')
    pla_2018['Valor de Venda'] = pla_2018['Valor de Venda'].str.replace("'", "")
    pla_2018['Valor de Venda'] = pla_2018['Valor de Venda'].str.replace('"', '')
    pla_2018['Valor de Compra'] = pla_2018['Valor de Compra'].str.replace("'", "")
    pla_2018['Valor de Compra'] = pla_2018['Valor de Compra'].str.replace('"', '')
    pla_2018['Unidade de Medida'] = pla_2018['Unidade de Medida'].str.replace("'", "")
    pla_2018['Unidade de Medida'] = pla_2018['Unidade de Medida'].str.replace('"', '')
    
    con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
    cursor = con.cursor()
    
    
    for index, row in pla_2018.iterrows():
        
        valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])  
        sql = "INSERT INTO consumo_2018 (regiao_sigla,estado_sigla,municipio,revenda,CNPJ_revenda,nome_da_rua,numero_rua,complemento,bairro,cep,produto,data_da_coleta,valor_de_venda,valor_de_compra,unidade_de_medida,bandeira) values "+valores+";"
        cursor.execute(sql)
    cursor.close()
    con.commit()
    con.close()
        

    print("fim da inserção 2018")
    
    
    # ----------------------------------------------------------
    # -- PLANILHA 2019
    # ----------------------------------------------------------
    pla_2019_1 = pd.read_csv(f"{caminho_planilha}/ca-2019-01.csv", sep=";")
    pla_2019_2 = pd.read_csv(f"{caminho_planilha}/ca-2019-02.csv", sep=";")
    pla_2019_2 = pla_2019_2.drop(0)
    pla_2019 = pd.concat([pla_2019_1, pla_2019_2])

    
    # TRATAMENTO
    pla_2019['Regiao - Sigla'] = pla_2019['Regiao - Sigla'].str.replace("'", "")
    pla_2019['Regiao - Sigla'] = pla_2019['Regiao - Sigla'].str.replace('"', '')
    pla_2019['Estado - Sigla'] = pla_2019['Estado - Sigla'].str.replace("'", "")
    pla_2019['Estado - Sigla'] = pla_2019['Estado - Sigla'].str.replace('"', '')
    pla_2019['Municipio'] = pla_2019['Municipio'].str.replace("'", "")
    pla_2019['Municipio'] = pla_2019['Municipio'].str.replace('"', '')
    pla_2019['Revenda'] = pla_2019['Revenda'].str.replace("'", "")
    pla_2019['Revenda'] = pla_2019['Revenda'].str.replace('"', '')
    pla_2019['CNPJ da Revenda'] = pla_2019['CNPJ da Revenda'].str.replace("'", "")
    pla_2019['CNPJ da Revenda'] = pla_2019['CNPJ da Revenda'].str.replace('"', '')
    pla_2019['Nome da Rua'] = pla_2019['Nome da Rua'].str.replace("'", "")
    pla_2019['Nome da Rua'] = pla_2019['Nome da Rua'].str.replace('"', '')
    pla_2019['Numero Rua'] = pla_2019['Numero Rua'].str.replace("'", "")
    pla_2019['Numero Rua'] = pla_2019['Numero Rua'].str.replace('"', '')
    pla_2019['Complemento'] = pla_2019['Complemento'].str.replace("'", "")
    pla_2019['Complemento'] = pla_2019['Complemento'].str.replace('"', '')
    pla_2019['Bairro'] = pla_2019['Bairro'].str.replace("'", "")
    pla_2019['Bairro'] = pla_2019['Bairro'].str.replace('"', '')
    pla_2019['Cep'] = pla_2019['Cep'].str.replace("'", "")
    pla_2019['Cep'] = pla_2019['Cep'].str.replace('"', '')
    pla_2019['Produto'] = pla_2019['Produto'].str.replace("'", "")
    pla_2019['Produto'] = pla_2019['Produto'].str.replace('"', '')
    pla_2019['Bandeira'] = pla_2019['Bandeira'].str.replace("'", "")
    pla_2019['Bandeira'] = pla_2019['Bandeira'].str.replace('"', '')
    pla_2019['Data da Coleta'] = pla_2019['Data da Coleta'].str.replace("'", "")
    pla_2019['Data da Coleta'] = pla_2019['Data da Coleta'].str.replace('"', '')
    pla_2019['Valor de Venda'] = pla_2019['Valor de Venda'].str.replace("'", "")
    pla_2019['Valor de Venda'] = pla_2019['Valor de Venda'].str.replace('"', '')
    pla_2019['Valor de Compra'] = pla_2019['Valor de Compra'].str.replace("'", "")
    pla_2019['Valor de Compra'] = pla_2019['Valor de Compra'].str.replace('"', '')
    pla_2019['Unidade de Medida'] = pla_2019['Unidade de Medida'].str.replace("'", "")
    pla_2019['Unidade de Medida'] = pla_2019['Unidade de Medida'].str.replace('"', '')
    

    con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
    cursor = con.cursor()
    
    
    for index, row in pla_2019.iterrows():
        
        valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])  
        sql = "INSERT INTO consumo_2019 (regiao_sigla,estado_sigla,municipio,revenda,CNPJ_revenda,nome_da_rua,numero_rua,complemento,bairro,cep,produto,data_da_coleta,valor_de_venda,valor_de_compra,unidade_de_medida,bandeira) values "+valores+";"
        cursor.execute(sql)
        

    print("fim da inserção 2019")
    cursor.close()
    con.commit()
    con.close()
    
    
    
    # # ----------------------------------------------------------
    # # -- PLANILHA 2020
    # # ----------------------------------------------------------
    pla_2020_1 = pd.read_csv(f"{caminho_planilha}/ca-2020-01.csv", sep=";")
    pla_2020_2 = pd.read_csv(f"{caminho_planilha}/ca-2020-02.csv", sep=";")
    pla_2020_2 = pla_2020_2.drop(0)
    pla_2020 = pd.concat([pla_2020_1, pla_2020_2])

    
    # TRATAMENTO
    pla_2020['Regiao - Sigla'] = pla_2020['Regiao - Sigla'].str.replace("'", "")
    pla_2020['Regiao - Sigla'] = pla_2020['Regiao - Sigla'].str.replace('"', '')
    pla_2020['Estado - Sigla'] = pla_2020['Estado - Sigla'].str.replace("'", "")
    pla_2020['Estado - Sigla'] = pla_2020['Estado - Sigla'].str.replace('"', '')
    pla_2020['Municipio'] = pla_2020['Municipio'].str.replace("'", "")
    pla_2020['Municipio'] = pla_2020['Municipio'].str.replace('"', '')
    pla_2020['Revenda'] = pla_2020['Revenda'].str.replace("'", "")
    pla_2020['Revenda'] = pla_2020['Revenda'].str.replace('"', '')
    pla_2020['CNPJ da Revenda'] = pla_2020['CNPJ da Revenda'].str.replace("'", "")
    pla_2020['CNPJ da Revenda'] = pla_2020['CNPJ da Revenda'].str.replace('"', '')
    pla_2020['Nome da Rua'] = pla_2020['Nome da Rua'].str.replace("'", "")
    pla_2020['Nome da Rua'] = pla_2020['Nome da Rua'].str.replace('"', '')
    pla_2020['Numero Rua'] = pla_2020['Numero Rua'].str.replace("'", "")
    pla_2020['Numero Rua'] = pla_2020['Numero Rua'].str.replace('"', '')
    pla_2020['Complemento'] = pla_2020['Complemento'].str.replace("'", "")
    pla_2020['Complemento'] = pla_2020['Complemento'].str.replace('"', '')
    pla_2020['Bairro'] = pla_2020['Bairro'].str.replace("'", "")
    pla_2020['Bairro'] = pla_2020['Bairro'].str.replace('"', '')
    pla_2020['Cep'] = pla_2020['Cep'].str.replace("'", "")
    pla_2020['Cep'] = pla_2020['Cep'].str.replace('"', '')
    pla_2020['Produto'] = pla_2020['Produto'].str.replace("'", "")
    pla_2020['Produto'] = pla_2020['Produto'].str.replace('"', '')
    pla_2020['Bandeira'] = pla_2020['Bandeira'].str.replace("'", "")
    pla_2020['Bandeira'] = pla_2020['Bandeira'].str.replace('"', '')
    pla_2020['Data da Coleta'] = pla_2020['Data da Coleta'].str.replace("'", "")
    pla_2020['Data da Coleta'] = pla_2020['Data da Coleta'].str.replace('"', '')
    pla_2020['Valor de Venda'] = pla_2020['Valor de Venda'].str.replace("'", "")
    pla_2020['Valor de Venda'] = pla_2020['Valor de Venda'].str.replace('"', '')
    pla_2020['Valor de Compra'] = pla_2020['Valor de Compra'].str.replace("'", "")
    pla_2020['Valor de Compra'] = pla_2020['Valor de Compra'].str.replace('"', '')
    pla_2020['Unidade de Medida'] = pla_2020['Unidade de Medida'].str.replace("'", "")
    pla_2020['Unidade de Medida'] = pla_2020['Unidade de Medida'].str.replace('"', '')
    

    con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
    cursor = con.cursor()
    
    
    for index, row in pla_2020.iterrows():
        
        valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])  
        sql = "INSERT INTO consumo_2020 (regiao_sigla,estado_sigla,municipio,revenda,CNPJ_revenda,nome_da_rua,numero_rua,complemento,bairro,cep,produto,data_da_coleta,valor_de_venda,valor_de_compra,unidade_de_medida,bandeira) values "+valores+";"
        cursor.execute(sql)
        

    print("fim da inserção 2020")
    
    cursor.close()
    con.commit()
    con.close()
    
    
    
    
    
    