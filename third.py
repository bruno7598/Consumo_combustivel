from cassandra.cluster import Cluster, ProfileManager
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import FloatType
import pandas as pd
import mysql.connector

caminho_csv = "gs://arquivo_csv"
caminho_parquet = "gs://arquivo_parquet"


if __name__ == "__main__":
    try:
        # CONEXAO COM O MYSQL
        con = mysql.connector.connect(user='root', password='projetoNatal', host='10.6.208.3', database='consumo_combustivel')
        cursor = con.cursor()
        
        # SELECT NA PLANILHA E SALVA COMO DATAFRAME COM PANDAS
        query = "SELECT * FROM preco_combustiveis;"
        query = cursor.execute(query)
        query = cursor.fetchall()
        df_preco = pd.DataFrame(query)
        
        print(df_preco)
        
        df_preco.columns = ["cod_pla","data_inicial","data_final","produto", "num_postos_pesquisados", "unid_medida", "preco_medio_revenda", "desvio_padrao_revenda", "preco_minimo_revenda", "preco_maximo_revenda", "margem_media_revenda", "coef_var_revenda", "preco_medio_dist", "desvio_padrao_dist", "preco_min_dist", "preco_max_dist"]
        
        print("colunas ok")
        cursor.close()
        con.commit()
        con.close()
            
        # CONEXAO COM CASSANDRA
        clstr = Cluster(['34.151.227.87'], port=9042)
        session = clstr.connect('analise_combustivel')

        print("cassandra ok")
        
        for index, row in df_preco.iterrows():
            valores = "(uuid(),'{}','{}','{}','{}','{}',{},{},{},{},{},{},{},{},{},{})".format(row.data_inicial,row.data_final, row.produto, row.num_postos_pesquisados, row.unid_medida, row.preco_medio_revenda, row.desvio_padrao_revenda, row.preco_minimo_revenda, row.preco_maximo_revenda, row.margem_media_revenda, row.coef_var_revenda, row.preco_medio_dist, row.desvio_padrao_dist, row.preco_min_dist, row.preco_max_dist)
            query = "INSERT INTO preco_combustiveis (cod_pla,data_inicial,data_final,produto, num_postos_pesquisados, unid_medida, preco_medio_revenda, desvio_padrao_revenda, preco_minimo_revenda, preco_maximo_revenda, margem_media_revenda, coef_var_revenda, preco_medio_dist, desvio_padrao_dist, preco_min_dist, preco_max_dist) values "+ valores +";"
            # print(query)
            session.execute(query)
                
            
        # CONEXAO COM SPARK
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

        # LE ARQUIVO PARQUET
        df_total = spark.read.parquet(f"{caminho_parquet}/total_parquet")

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