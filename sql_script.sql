-- SCRIPT MYSQL

CREATE DATABASE consumo_combustivel;

USE consumo_combustivel;

-- CRIANDO AS TABELAS NO MYSQL

CREATE TABLE IF NOT EXISTS log_combustivel (
    id_log int auto_increment primary key,
    datas datetime,
    CNPJ_revenda text,
    revendedor text,
    valor_da_revenda text);
    
SELECT * FROM log_combustivel;

CREATE TABLE IF NOT EXISTS consumo_2015 (
    id_2015 int auto_increment primary key,
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
    
SELECT * FROM consumo_2015;

CREATE TABLE IF NOT EXISTS consumo_2016 (
    id_2016 int auto_increment primary key,
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
   
SELECT * FROM consumo_2016;

CREATE TABLE consumo_2017 (
    id_2017 int auto_increment primary key,
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

SELECT * FROM consumo_2017;

CREATE TABLE consumo_2018 (
    id_2018 int auto_increment primary key,
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

SELECT * FROM consumo_2018;

CREATE TABLE consumo_2019 (
    id_2019 int auto_increment primary key,
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

SELECT * FROM consumo_2019;

CREATE TABLE consumo_2020 (
    id_2020 int auto_increment primary key,
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

SELECT * FROM consumo_2020;

-- CRIANDO PROCEDURE PARA LOG

delimiter //
create procedure InsereLogDados (in datas datetime, in cnpj text, in revendedor text, in valor_revenda text)
	begin
		insert into log_combustivel (datas,CNPJ_revenda,revendedor,valor_da_revenda) values (datas,cnpj,revendedor,valor_revenda);
	end
//


-- TRIGGERS 2020

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2020_AI AFTER INSERT ON consumo_2020
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2020_AD AFTER DELETE ON consumo_2020
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2020_AU AFTER UPDATE ON consumo_2020
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2019

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2019_AI AFTER INSERT ON consumo_2019
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2019_AD AFTER DELETE ON consumo_2019
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2019_AU AFTER UPDATE ON consumo_2019
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2018

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2018_AI AFTER INSERT ON consumo_2018
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2018_AD AFTER DELETE ON consumo_2018
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//

-- Ttrigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2018_AU AFTER UPDATE ON consumo_2018
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2017

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2017_AI AFTER INSERT ON consumo_2017
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2017_AD AFTER DELETE ON consumo_2017
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2017_AU AFTER UPDATE ON consumo_2017
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2016

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2016_AI AFTER INSERT ON consumo_2016
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2016_AD AFTER DELETE ON consumo_2016
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2016_AU AFTER UPDATE ON consumo_2016
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

-- TRIGGERS 2015

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um insert na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2015_AI AFTER INSERT ON consumo_2015
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
		END
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2015_AD AFTER DELETE ON consumo_2015
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,old.CNPJ_revenda,old.revenda,old.valor_de_venda);
		END 
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados

delimiter //
CREATE TRIGGER trg_consumo_2015_AU AFTER UPDATE ON consumo_2015
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp,new.CNPJ_revenda,new.revenda,new.valor_de_venda);
        END;
//

CREATE TABLE log_preco_combustivel (
    id_log int auto_increment primary key,
    datas datetime);

-- CREATE PROCEDURE PARA LOG

delimiter //
create procedure InsereLogPreco ( in datas datetime)
	begin
		 insert into log_preco_combustivel (datas) values (datas);
	end
//

-- TRIGGERS

delimiter //
CREATE TRIGGER trg_preco_Insert_AI AFTER INSERT ON preco_combustiveis
	FOR EACH ROW
		BEGIN
            call InsereLogPreco(current_timestamp);
		END
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um delete na tabela dados

delimiter //
CREATE TRIGGER trg_preco_Delete_AD AFTER DELETE ON preco_combustiveis
	FOR EACH ROW
		BEGIN
            call InsereLogPreco(current_timestamp);
		END 
//

-- Trigger que insere uma ocorrencia na tabela logs_dados quando existe um update na tabela dados

delimiter //
CREATE TRIGGER trg_preco_Update_AU AFTER UPDATE ON preco_combustiveis
	FOR EACH ROW
		BEGIN
            call InsereLogDados(current_timestamp);
        END;
//
