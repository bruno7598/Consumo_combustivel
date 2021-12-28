from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as B
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, count
import mysql.connector
from datetime import datetime
import pandas as pd
import numpy as np


caminho_csv = "gs://arquivo_csv"
caminho_parquet = "gs://arquivo_parquet"


con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
cursor = con.cursor()

query_2015 = "SELECT * FROM consumo_2015;"
a = cursor.execute(query_2015)
a = cursor.fetchall()
df_2015 = pd.DataFrame(a)
print("tamo aqui 23")

query_2016 = "select * from consumo_2016;"
b = cursor.execute(query_2016)
b = cursor.fetchall()
df_2016 = pd.DataFrame(b)

query_2017 = "select * from consumo_2017;"
c = cursor.execute(query_2017)
c = cursor.fetchall()
df_2017 = pd.DataFrame(c)

query_2018 = "select * from consumo_2018;"
d = cursor.execute(query_2018)
d = cursor.fetchall()
df_2018 = pd.DataFrame(d)

query_2019 = "select * from consumo_2019;"
e = cursor.execute(query_2019)
e = cursor.fetchall()
df_2019 = pd.DataFrame(e)
print("tamo aqui 44")

query_2020 = "select * from consumo_2020;"
f = cursor.execute(query_2020)
f = cursor.fetchall()
df_2020 = pd.DataFrame(f)

cursor.close()
con.commit()
con.close()

df_total = pd.concat([df_2015, df_2016, df_2017, df_2018, df_2019, df_2020])

df_total.columns = ["id_total","regiao_sigla","estado_sigla","municipio","revenda","CNPJ_revenda","nome_da_rua","numero_rua","complemento","bairro","cep","produto","data_da_coleta","valor_de_venda","valor_de_compra","unidade_de_medida","bandeira"]

df_total.drop(['id_total'], axis=1, inplace=True)

spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

df_total.to_csv(f"{caminho_csv}/concat.csv", header=True)

#Inicia sessão no spark

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("delimiter", ",")\
    .option("inferSchema", "true")\
    .load(f"{caminho_csv}/concat.csv")


converter_valor = lambda variavel: float(variavel.replace(",","."))
udf_converter_valor = B.udf(converter_valor,FloatType())



resultado = df.withColumn("valor_de_venda", udf_converter_valor(df["valor_de_venda"]))
resultado = resultado.withColumn("valor_de_compra", udf_converter_valor(resultado["valor_de_compra"]))

resultado.printSchema()


df_total = resultado.select(col("regiao_sigla")\
,col("estado_sigla")\
,col("municipio")\
,col("revenda")\
,col("CNPJ_revenda")\
,col("produto")\
,col("data_da_coleta")\
,col("valor_de_venda")\
,col("valor_de_compra")\
,col("bandeira"))
df_total.write.parquet(f"{caminho_parquet}/total_parquet")
