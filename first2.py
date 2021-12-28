import mysql.connector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

caminho_planilha = "gs://dataproc-staging-sa-east1-38488625567-ytcttqkr/notebooks"

if __name__ == "__main__":
    try:
        
        # CONEXAO COM SPARK
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

        # LE ARQUIVO CSV      
        df_preco = spark.read.format("xlsx")\
        .option("header", "true")\
        .option("delimiter", ",")\
        .option("inferSchema", "true")\
        .option("encoding", "ISO-8859-1")\
        .load(f"{caminho_planilha}/preço_2013-2021.xlsx")
        
        # TRATAMENTO DA PLANILHA
        preco_combustiveis = df_preco.select(
            df_preco["DATA INICIAL"].alias("data_inicial"),
            df_preco["DATA FINAL"].alias("data_final"),
            df_preco["PRODUTO"].alias("produto"),
            df_preco["NÚMERO DE POSTOS PESQUISADOS"].alias("num_postos_pesquisados"),
            df_preco["UNIDADE DE MEDIDA"].alias("unid_medida"),
            df_preco["PREÇO MÉDIO REVENDA"].alias("preco_medio_revenda"),
            df_preco["DESVIO PADRÃO REVENDA"].alias("desvio_padrao_revenda"),
            df_preco["PREÇO MÍNIMO REVENDA"].alias("preco_minimo_revenda"),
            df_preco["PREÇO MÁXIMO REVENDA"].alias("preco_maximo_revenda"),
            df_preco["MARGEM MÉDIA REVENDA"].alias("margem_media_revenda"),
            df_preco["COEF DE VARIAÇÃO REVENDA"].alias("coef_var_revenda"),
            df_preco["PREÇO MÉDIO DISTRIBUIÇÃO"].alias("preco_medio_dist"),
            df_preco["DESVIO PADRÃO DISTRIBUIÇÃO"].alias("desvio_padrao_dist"),
            df_preco["PREÇO MÍNIMO DISTRIBUIÇÃO"].alias("preco_min_dist"),
            df_preco["PREÇO MÁXIMO DISTRIBUIÇÃO"].alias("preco_max_dist"))

        df = pd.DataFrame(df_preco)
        df.columns = ["data_inicial","data_final","produto","num_postos_pesquisados","unid_medida","preco_medio_revenda","desvio_padrao_revenda","preco_minimo_revenda","preco_maximo_revenda","margem_media_revenda","coef_var_revenda","preco_medio_dist","desvio_padrao_dist","preco_min_dist","preco_max_dist"]

        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # INSERCAO DOS DADOS NO MYSQL
        for index, row in df.iterrows():
        
            valores = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14])  
            sql = "INSERT INTO preco_combustiveis (data_inicial,data_final,produto,num_postos_pesquisados,unid_medida,preco_medio_revenda,desvio_padrao_revenda,preco_minimo_revenda,preco_maximo_revenda,margem_media_revenda,coef_var_revenda,preco_medio_dist,desvio_padrao_dist,preco_min_dist,preco_max_dist) values "+valores+";"
            cursor.execute(sql)
        
        cursor.close()
        con.commit()
        con.close()

        print("fim da inserção")
        
    except Exception as e:
        print(str(e))