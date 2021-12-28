-- SCRIPT CASSANDRA
CREATE KEYSPACE IF NOT EXISTS analise_combustivel 
    WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

USE analise_combustivel;

CREATE TABLE IF NOT EXISTS CONSUMO (
	id int not null auto_increment primary key,
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




