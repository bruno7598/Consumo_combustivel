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

        print("fim da inserção")
        
    except Exception as e:
        print(str(e))