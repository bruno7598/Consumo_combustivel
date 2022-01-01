-- SCRIPT CASSANDRA

CREATE KEYSPACE IF NOT EXISTS analise_combustivel 
    WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

USE analise_combustivel;

CREATE TABLE IF NOT EXISTS CONSUMO(
	id int auto_increment primary key,
	regiao_sigla text,
	estado_sigla text,
	municipio text,
	razao_social text,
	cnpj text,
	produto text,
	data_da_coleta text,
	valor_de_venda text,
	valor_de_compra text,
	bandeira text);

SELECT * FROM CONSUMO;

CREATE TABLE IF NOT EXISTS preco_combustiveis(
	cod_pla int auto_increment primary key,
	data_inicial text,
	data_final text,
	produto text,
	num_postos_pesquisados text,
	unid_medida text,
	preco_medio_revenda float,
	desvio_padrao_revenda float,
	preco_minimo_revenda float,
	preco_maximo_revenda float, 
	margem_media_revenda float, 
	coef_var_revenda float, 
	preco_medio_dist float, 
	desvio_padrao_dist float,
	preco_min_dist float, 
	preco_max_dist float);
	
SELECT * FROM preco_combustiveis;
