from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
import mysql.connector
import pandas as pd

caminho_csv = "gs://arquivo_csv"
caminho_parquet = "gs://arquivo_parquet"

if __name__ == "__main__":
    try:
        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        print("conexao ok")

        # SELECT NAS PLANILHAS E SALVA COMO DATAFRAME COM PANDAS
        query_2015 = "SELECT * FROM consumo_2015;"
        query = cursor.execute(query_2015)
        query = cursor.fetchall()
        df_2015 = pd.DataFrame(query)
        
        print("2015 ok")        


        query_2016 = "select * from consumo_2016;"
        query = cursor.execute(query_2016)
        query = cursor.fetchall()
        df_2016 = pd.DataFrame(query)
        print("2016 ok")

        query_2017 = "select * from consumo_2017;"
        query = cursor.execute(query_2017)
        query = cursor.fetchall()
        df_2017 = pd.DataFrame(query)
        print("2017 ok")

        query_2018 = "select * from consumo_2018;"
        query = cursor.execute(query_2018)
        query = cursor.fetchall()
        df_2018 = pd.DataFrame(query)
        print("2018 ok")

        query_2019 = "select * from consumo_2019;"
        query = cursor.execute(query_2019)
        query = cursor.fetchall()
        df_2019 = pd.DataFrame(query)
        print("2019 ok")

        query_2020 = "select * from consumo_2020;"
        query = cursor.execute(query_2020)
        query = cursor.fetchall()
        df_2020 = pd.DataFrame(query)
        print("2020 ok")

        cursor.close()
        con.commit()
        con.close()

        # CONCATENA AS PLANILHAS
        df_total = pd.concat([df_2015, df_2016, df_2017, df_2018, df_2019, df_2020])
        print("concatenou ok")
        df_total.columns = ["id_total","regiao_sigla","estado_sigla","municipio","revenda","CNPJ_revenda","nome_da_rua","numero_rua","complemento","bairro","cep","produto","data_da_coleta","valor_de_venda","valor_de_compra","unidade_de_medida","bandeira"]
        print("colunas ok")
        
        # SALVA ARQUIVO CSV
        df_total.to_csv(f"{caminho_csv}/df_concatenado.csv", header=True)
        print("salva arquivo csv")
        
        # CONEXAO COM SPARK
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
        print("conexao spark ok")

        df = spark.read.format("csv")\
            .option("header", "true")\
            .option("delimiter", ",")\
            .option("inferSchema", "true")\
            .load(f"{caminho_csv}/df_concatenado.csv")
        
        print("le parquet")

        # TRATAMENTO DO CSV SUBSTITUINDO PONTO POR VIRGULA NAS COLUNAS COM VALORES
        converter_valor = lambda variavel: float(variavel.replace(",","."))
        udf_converter_valor = functions.udf(converter_valor,FloatType())


        resultado = df.withColumn("valor_de_venda", udf_converter_valor(df["valor_de_venda"]))
        resultado = resultado.withColumn("valor_de_compra", udf_converter_valor(resultado["valor_de_compra"]))

        print("converte , em .")

        # SALVA ARQUIVO PARQUET
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

        print("Fim da execução")

    except Exception as e:
        print(str(e))