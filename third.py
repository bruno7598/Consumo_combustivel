from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import FloatType
import pandas as pd

caminho_parquet = "gs://arquivo_parquet/total_parquet"

if __name__ == "__main__":
    try:
        # CONEXAO COM CASSANDRA
        clstr = Cluster(['34.151.227.87'], port=9042)
        session = clstr.connect('analise_combustivel')

        # CONEXAO COM SPARK
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

        # LE ARQUIVO PARQUET
        df_total = spark.read.parquet(f"{caminho_parquet}")

        lista_dados = df_total.filter("CNPJ_revenda != '0'").collect()
        df_dados = pd.DataFrame(lista_dados)
        df_dados.columns = ["regiao_sigla","estado_sigla","municipio","revenda","CNPJ_revenda","produto","data_da_coleta","valor_de_venda","valor_de_compra","bandeira"]

        # INSERCAO DOS DADOS NO CASSANDRA
        for index, row in df_dados.iterrows():
            
            if pd.isnull(row.valor_de_compra):
                valores = "(uuid(),'{}','{}','{}','{}','{}','{}','{}',{},'{}')".format(row.regiao_sigla,row.estado_sigla,row.municipio, row.revenda, row.CNPJ_revenda, row.produto, row.data_da_coleta, row.valor_de_venda, row.bandeira)
                sql = "INSERT INTO CONSUMO (id,regiao_sigla,estado_sigla,municipio, razao_social, cnpj, produto, data_da_coleta, valor_de_venda, bandeira) values "+ valores +";"
            else:
                valores = "(uuid(),'{}','{}','{}','{}','{}','{}','{}',{}, {},'{}')".format(row.regiao_sigla,row.estado_sigla,row.municipio, row.revenda, row.CNPJ_revenda, row.produto, row.data_da_coleta, row.valor_de_venda, row.valor_de_compra, row.bandeira)
                sql = "INSERT INTO CONSUMO (id,regiao_sigla,estado_sigla,municipio, razao_social, cnpj, produto, data_da_coleta, valor_de_venda, valor_de_compra, bandeira) values "+ valores +";"
            session.execute(sql)
            
        print("Fim da execução")
        
    except Exception as e:
        print(str(e))