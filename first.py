import mysql.connector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

caminho_planilha = "gs://dataproc-staging-sa-east1-38488625567-ytcttqkr/notebooks"

if __name__ == "__main__":
    try:

        # ----------------------------------------------------------
        # -- PLANILHA 2015
        # ----------------------------------------------------------
        pla_2015_1 = pd.read_csv(f"{caminho_planilha}/ca-2015-01.csv", sep=";")
        pla_2015_2 = pd.read_csv(f"{caminho_planilha}/ca-2015-02.csv", sep=";")
        pla_2015_2 = pla_2015_2.drop(0)
        pla_2015 = pd.concat([pla_2015_1, pla_2015_2])

        
        # TRATAMENTO DA PLANILHA
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
        
        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # INSERCAO DOS DADOS NO MYSQL
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

        
        # TRATAMENTO DA PLANILHA
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

        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # INSERCAO DOS DADOS
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

        
        # TRATAMENTO DA PLANILHA
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

        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # INSERCAO DOS DADOS
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

        
        # TRATAMENTO DA PLANILHA
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
        
        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # INSERCAO DOS DADOS
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

        
        # TRATAMENTO DA PLANILHA
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

        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # INSERCAO DOS DADOS
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

        # TRATAMENTO DA PLANILHA
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
        
        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # INSERCAO DOS DADOS
        for index, row in pla_2020.iterrows():
            
            valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])  
            sql = "INSERT INTO consumo_2020 (regiao_sigla,estado_sigla,municipio,revenda,CNPJ_revenda,nome_da_rua,numero_rua,complemento,bairro,cep,produto,data_da_coleta,valor_de_venda,valor_de_compra,unidade_de_medida,bandeira) values "+valores+";"
            cursor.execute(sql)
            

        print("fim da inserção 2020")
        
        cursor.close()
        con.commit()
        con.close()
        
        
        # CONEXAO COM SPARK
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

        # LE ARQUIVO CSV      
        df_preco = spark.read.format("csv")\
        .option("header", "true")\
        .option("delimiter", ";")\
        .option("inferSchema", "true")\
        .option("encoding", "UTF-8")\
        .load(f"{caminho_planilha}/preço_2013-2021.csv")
        
       
        df_preco = df_preco.replace("-", "-1")
               
                        
        # TRATAMENTO DO CSV SUBSTITUINDO PONTO POR VIRGULA NAS COLUNAS COM VALORES
        converter_valor = lambda variavel: float(variavel.replace(",","."))
        udf_converter_valor = functions.udf(converter_valor,FloatType())

        
        resultado = df_preco.withColumn("PREÇO MÉDIO REVENDA", udf_converter_valor(df_preco["PREÇO MÉDIO REVENDA"]))
        resultado = resultado.withColumn("DESVIO PADRÃO REVENDA", udf_converter_valor(resultado["DESVIO PADRÃO REVENDA"]))
        resultado = resultado.withColumn("PREÇO MÍNIMO REVENDA", udf_converter_valor(resultado["PREÇO MÍNIMO REVENDA"]))
        resultado = resultado.withColumn("PREÇO MÁXIMO REVENDA", udf_converter_valor(resultado["PREÇO MÁXIMO REVENDA"]))
        resultado = resultado.withColumn("MARGEM MÉDIA REVENDA", udf_converter_valor(resultado["MARGEM MÉDIA REVENDA"]))
        resultado = resultado.withColumn("COEF DE VARIAÇÃO REVENDA", udf_converter_valor(resultado["COEF DE VARIAÇÃO REVENDA"]))
        resultado = resultado.withColumn("PREÇO MÉDIO DISTRIBUIÇÃO", udf_converter_valor(resultado["PREÇO MÉDIO DISTRIBUIÇÃO"]))           
        resultado = resultado.withColumn("DESVIO PADRÃO DISTRIBUIÇÃO", udf_converter_valor(resultado["DESVIO PADRÃO DISTRIBUIÇÃO"]))    
        resultado = resultado.withColumn("PREÇO MÍNIMO DISTRIBUIÇÃO", udf_converter_valor(resultado["PREÇO MÍNIMO DISTRIBUIÇÃO"]))
        resultado = resultado.withColumn("PREÇO MÁXIMO DISTRIBUIÇÃO", udf_converter_valor(resultado["PREÇO MÁXIMO DISTRIBUIÇÃO"]))
                
        # TRATAMENTO DA PLANILHA
        preco_combustiveis = resultado.select(\
            col("DATA INICIAL").alias("data_inicial"),\
            col("DATA FINAL").alias("data_final"),\
            col("PRODUTO").alias("produto"),\
            col("NÚMERO DE POSTOS PESQUISADOS").alias("num_postos_pesquisados"),\
            col("UNIDADE DE MEDIDA").alias("unid_medida"),\
            col("PREÇO MÉDIO REVENDA").alias("preco_medio_revenda"),\
            col("DESVIO PADRÃO REVENDA").alias("desvio_padrao_revenda"),\
            col("PREÇO MÍNIMO REVENDA").alias("preco_minimo_revenda"),\
            col("PREÇO MÁXIMO REVENDA").alias("preco_maximo_revenda"),\
            col("MARGEM MÉDIA REVENDA").alias("margem_media_revenda"),\
            col("COEF DE VARIAÇÃO REVENDA").alias("coef_var_revenda"),\
            col("PREÇO MÉDIO DISTRIBUIÇÃO").alias("preco_medio_dist"),\
            col("DESVIO PADRÃO DISTRIBUIÇÃO").alias("desvio_padrao_dist"),\
            col("PREÇO MÍNIMO DISTRIBUIÇÃO").alias("preco_min_dist"),\
            col("PREÇO MÁXIMO DISTRIBUIÇÃO").alias("preco_max_dist"))
       
        
        lista_dados = preco_combustiveis.filter("data_inicial != '0'").collect()
        
        df = pd.DataFrame(lista_dados)
        df.columns = ["data_inicial","data_final","produto","num_postos_pesquisados","unid_medida","preco_medio_revenda","desvio_padrao_revenda","preco_minimo_revenda","preco_maximo_revenda","margem_media_revenda","coef_var_revenda","preco_medio_dist","desvio_padrao_dist","preco_min_dist","preco_max_dist"]

        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
                
        # INSERCAO DOS DADOS NO MYSQL
        for index, row in df.iterrows():
        
            valores = "('{}','{}','{}',{},'{}',{},{},{},{},{},{},{},{},{},{})".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14])  
            sql = "INSERT INTO preco_combustiveis (data_inicial,data_final,produto,num_postos_pesquisados,unid_medida,preco_medio_revenda,desvio_padrao_revenda,preco_minimo_revenda,preco_maximo_revenda,margem_media_revenda,coef_var_revenda,preco_medio_dist,desvio_padrao_dist,preco_min_dist,preco_max_dist) values "+valores+";"
            cursor.execute(sql)
        
        cursor.close()
        con.commit()
        con.close()

        print("Fim da inserção tabela Preços")
        
    except Exception as e:
        print(str(e))