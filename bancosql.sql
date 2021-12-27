CREATE DATABASES CONSUMO_COMBUSTIVEL;



"""

Regiao - Sigla
;Estado - Sigla
;Municipio
;Revenda
;CNPJ da Revenda
;Nome da Rua
;Numero Rua
;Complemento 
;Bairro 
;Cep
;Produto
;Data da Coleta
;Valor de Venda
;Valor de Compra
;Unidade de Medida
;Bandeira

"""
-- CREATE PROCEDURE PARA LOG
delimiter //
create procedure InsereLogDados ( in datas datetime, in cnpj text, in revendedor text, in valor_revenda text)
	begin
		 insert into log_combustivel (datas,CNPJ_revenda,revendedor,valor_da_revenda) values (datas,cnpj,revendedor,valor_revenda);
	end
//


-- TRIGGERS 2020

delimiter //
CREATE TRIGGER trg_consumo_2020_Insert_AI AFTER INSERT ON CONSUMO_2020
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2020_Insert_AD AFTER DELETE ON CONSUMO_2020
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2020_Insert_AU AFTER UPDATE ON CONSUMO_2020
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2019

delimiter //
CREATE TRIGGER trg_consumo_2019_Insert_AI AFTER INSERT ON CONSUMO_2019
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2019_Insert_AD AFTER DELETE ON CONSUMO_2019
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2019_Insert_AU AFTER UPDATE ON CONSUMO_2019
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2018

#trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2018_Insert_AI AFTER INSERT ON CONSUMO_2018
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2018_Insert_AD AFTER DELETE ON CONSUMO_2018
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2018_Insert_AU AFTER UPDATE ON CONSUMO_2018
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//


-- TRIGGERS 2017

#trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2017_Insert_AI AFTER INSERT ON CONSUMO_2017
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2017_Insert_AD AFTER DELETE ON CONSUMO_2017
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2017_Insert_AU AFTER UPDATE ON CONSUMO_2017
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2016

#trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2016_Insert_AI AFTER INSERT ON CONSUMO_2016
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2016_Insert_AD AFTER DELETE ON CONSUMO_2016
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2016_Insert_AU AFTER UPDATE ON CONSUMO_2016
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//



-- TRIGGERS 2015

#trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2015_Insert_AI AFTER INSERT ON CONSUMO_2015
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2015_Insert_AD AFTER DELETE ON CONSUMO_2015
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//
#trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados
delimiter //
CREATE TRIGGER trg_consumo_2015_Insert_AU AFTER UPDATE ON CONSUMO_2015
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//





-- CRIANDO AS TABELAS NO MYSQL


create table log_combustivel (
	id_log int auto_increment primary key,
    datas datetime,
    CNPJ_revenda text,
    revendedor text,
    valor_da_revenda text);



CREATE TABLE CONSUMO_2015 (
	id_2015 int not null auto_increment primary key,
    regiao_sigla text,
    estado_sigla text,
    municipio text,
    revenda text,
    CNPJ_revenda text,
    nome_da_rua text,
    numero_rua text,
    complemento text,
    bairro text,
    cep text,
    produto text,
    data_da_coleta text,
    valor_de_venda text,
    valor_de_compra text,
    unidade_de_medida text,
    bandeira text);




CREATE TABLE CONSUMO_2016 (
	id_2016 int not null auto_increment primary key,
    regiao_sigla text,
    estado_sigla text,
    municipio text,
    revenda text,
    CNPJ_revenda text,
    nome_da_rua text,
    numero_rua text,
    complemento text,
    bairro text,
    cep text,
    produto text,
    data_da_coleta text,
    valor_de_venda text,
    valor_de_compra text,
    unidade_de_medida text,
    bandeira text);



CREATE TABLE CONSUMO_2017 (
	id_2017 int not null auto_increment primary key,
    regiao_sigla text,
    estado_sigla text,
    municipio text,
    revenda text,
    CNPJ_revenda text,
    nome_da_rua text,
    numero_rua text,
    complemento text,
    bairro text,
    cep text,
    produto text,
    data_da_coleta text,
    valor_de_venda text,
    valor_de_compra text,
    unidade_de_medida text,
    bandeira text);


CREATE TABLE CONSUMO_2018 (
	id_2018 int not null auto_increment primary key,
    regiao_sigla text,
    estado_sigla text,
    municipio text,
    revenda text,
    CNPJ_revenda text,
    nome_da_rua text,
    numero_rua text,
    complemento text,
    bairro text,
    cep text,
    produto text,
    data_da_coleta text,
    valor_de_venda text,
    valor_de_compra text,
    unidade_de_medida text,
    bandeira text);



CREATE TABLE CONSUMO_2019 (
	id_2019 int not null auto_increment primary key,
    regiao_sigla text,
    estado_sigla text,
    municipio text,
    revenda text,
    CNPJ_revenda text,
    nome_da_rua text,
    numero_rua text,
    complemento text,
    bairro text,
    cep text,
    produto text,
    data_da_coleta text,
    valor_de_venda text,
    valor_de_compra text,
    unidade_de_medida text,
    bandeira text);



CREATE TABLE CONSUMO_2020 (
	id_2020 int not null auto_increment primary key,
    regiao_sigla text,
    estado_sigla text,
    municipio text,
    revenda text,
    CNPJ_revenda text,
    nome_da_rua text,
    numero_rua text,
    complemento text,
    bairro text,
    cep text,
    produto text,
    data_da_coleta text,
    valor_de_venda text,
    valor_de_compra text,
    unidade_de_medida text,
    bandeira text);