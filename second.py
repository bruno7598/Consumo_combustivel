from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as B
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, count
import mysql.connector
from datetime import datetime
import pandas as pd

#campos a alterar
# usuario = 
# arquivo_parquet = 


con = mysql.connector.connect(user='root', password='Eugostode@55', host='localhost', database='consumo_combustivel')
cursor = con.cursor()

query_2015 = "SELECT * FROM consumo_2015;"
a = cursor.execute(query_2015)
a = cursor.fetchall()
df_2015 = pd.DataFrame(a)

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




# #Inicia sessão no spark
spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

converter_valor = lambda variavel: float(variavel.replace(",","."))
udf_converter_valor = B.udf(converter_valor,FloatType())

resultado = df_total.withColumn("valor_de_venda", udf_converter_valor(df_total["valor_de_venda"]))
resultado = resultado.withColumn("valor_de_compra", udf_converter_valor(resultado["valor_de_compra"]))


# df_total = df_total.select(col("regiao_sigla")\
# ,col("estado_sigla")\
# ,col("municipio")\
# ,col("revenda").alias("razao_social")\
# ,col("CNPJ_revenda").alias("cnpj")\
# ,col("produto")\
# ,col("data_da_coleta")\
# ,col("valor_de_venda")\
# ,col("valor_de_compra")\
# ,col("bandeira"))
# df_total.write.parquet(r"C:\Users\isa66\Desktop\Visualcode\.vscode\Atividade_de_natal\arquivos_parquet")







