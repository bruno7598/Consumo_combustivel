from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import FloatType


clstr = Cluster()
session = clstr.connect('matriz')

spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

df_total = spark.read.parquet(r"C:\Users\isa66\Desktop\Visualcode\.vscode\Atividade_de_natal\arquivos_parquet")

for index, row in df_total.iterrows():
    
    valores = "(uuid(),'{}','{}',{})".format(row.regiao_sigla,row.estado_sigla,row.municipio, row.razao_social, row.cnpj, row.produto, row.data_da_coleta, row.valor_de_venda, row.valor_de_compra, row.bandeira)  
    sql = "INSERT INTO vendas (regiao_sigla,estado_sigla,municipio, razao_social, cnpj, produto, data_da_coleta, valor_de_venda, valor_de_compra, bandeira) values "+ valores +";"
    session.execute(sql)






