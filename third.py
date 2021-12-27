from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import FloatType
# import numpy as np
import pandas as pd



clstr = Cluster(['34.151.227.87'], port=9042)
session = clstr.connect('analise_combustivel')



spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

df_total = spark.read.parquet(r"gs://dataproc-staging-sa-east1-38488625567-ytcttqkr/notebooks/total_parquet")
a = df_total.filter("CNPJ_revenda != '0'").collect()
b = pd.DataFrame(a)
b.columns = ["regiao_sigla","estado_sigla","municipio","revenda","CNPJ_revenda","produto","data_da_coleta","valor_de_venda","valor_de_compra","bandeira"]
# b.fillna(-1, inplace = True)
print(b)

for index, row in b.iterrows():
      
    if pd.isnull(row.valor_de_compra):
        valores = "(uuid(),'{}','{}','{}','{}','{}','{}','{}',{},'{}')".format(row.regiao_sigla,row.estado_sigla,row.municipio, row.revenda, row.CNPJ_revenda, row.produto, row.data_da_coleta, row.valor_de_venda, row.bandeira)
        sql = "INSERT INTO CONSUMO (id,regiao_sigla,estado_sigla,municipio, razao_social, cnpj, produto, data_da_coleta, valor_de_venda, bandeira) values "+ valores +";"
    else:
        valores = "(uuid(),'{}','{}','{}','{}','{}','{}','{}',{}, {},'{}')".format(row.regiao_sigla,row.estado_sigla,row.municipio, row.revenda, row.CNPJ_revenda, row.produto, row.data_da_coleta, row.valor_de_venda, row.valor_de_compra, row.bandeira)
        sql = "INSERT INTO CONSUMO (id,regiao_sigla,estado_sigla,municipio, razao_social, cnpj, produto, data_da_coleta, valor_de_venda, valor_de_compra, bandeira) values "+ valores +";"
    session.execute(sql)
    
    
print("SHOW!")
   





