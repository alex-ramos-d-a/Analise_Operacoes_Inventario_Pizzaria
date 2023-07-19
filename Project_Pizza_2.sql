#															Pizza Project 2.0
#   Banco de dados
create database pizza_2;
use pizza_2;

# 	Importando dados
create table pizza_sale
(
	id 				int not null auto_increment,
    order_id 		int not null,
    pizza_id 		varchar(100),
    quantity		int,
    order_date		date,
    order_time		time,
    unit_price		varchar(10),
    total_price		varchar(10),
    pizza_size		varchar(5),
    pizza_category  varchar(30),
    pizza_name		varchar(150),
    ingrediente1 	varchar(50),
	ingrediente2 	varchar(50),
	ingrediente3 	varchar(50),
	ingrediente4 	varchar(50),
	ingrediente5 	varchar(50),
	ingrediente6 	varchar(50),
	ingrediente7 	varchar(50),
	ingrediente8 	varchar(50),
    constraint pk_pizza primary key (id)
);

select * from pizza_sale;
desc pizza_sale;
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
# Parte ETL
update pizza_sale set ingrediente3 = null where ingrediente3 = "null"; -- Adicionando NULO nas celulas vazias
update pizza_sale set ingrediente4 = null where ingrediente4 = " ";
update pizza_sale set ingrediente5 = null where ingrediente5 = " ";
update pizza_sale set ingrediente6 = null where ingrediente6 = " ";
update pizza_sale set ingrediente7 = null where ingrediente7 = " ";
update pizza_sale set ingrediente8 = null where ingrediente8 = ' ' or " " or '' or "";


# Modificando a estrutura da tabela e tratando um pouco 
create table treated_pizza2 (id int primary key auto_increment);
select  
		order_id, pizza_id, quantity, order_date, order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price, -- Mudando tipo de dado e virgul para ponto
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price, -- Mudando tipo de dado e virgul para ponto
		pizza_size, pizza_category, pizza_name, 
        ingrediente1 -- SEGREDO/OURO: Vai JUNTAS TODAS COLUNAS EM APENAS UMA;
from pizza_sale
union 
select  
		order_id, pizza_id, quantity, order_date, order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price, 
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price, 
		pizza_size, pizza_category, pizza_name,
        ingrediente2
from pizza_sale
union 
select  
		order_id, pizza_id, quantity, order_date, order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price, 
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price, 
		pizza_size, pizza_category, pizza_name,
        ingrediente3
from pizza_sale
union 
select  
		order_id, pizza_id, quantity, order_date, order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price, 
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price, 
		pizza_size, pizza_category, pizza_name,
        ingrediente4
from pizza_sale
union 
select  
		order_id, pizza_id, quantity, order_date, order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price, 
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price, 
		pizza_size, pizza_category, pizza_name,
        ingrediente5
from pizza_sale
union 
select  
		order_id, pizza_id, quantity, order_date, order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price,
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price,
		pizza_size, pizza_category, pizza_name,
        ingrediente6
from pizza_sale
union 
select  
		order_id, pizza_id, quantity, order_date, order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price, 
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price, 
		pizza_size, pizza_category, pizza_name,
        ingrediente7
from pizza_sale;
select * from treated_pizza2;

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
# Modelagem
create table order_pizza (id_order_pizza int primary key auto_increment)
select 
	order_id as id_dim_tempo, -- chave estrangeira (order_id) da tab original
	pizza_id as id_dim_produto, -- chave estrangeira (pizza_id) da tab original [Mesmo sendo texto e a unica identificação]
    quantity,
    unit_price,
    total_price
from treated_pizza2;
select * from order_pizza;
desc order_pizza;


create table Tempo (id_tempo int primary key auto_increment)
select distinct 
		order_date,
        order_time
from treated_pizza2;
select * from tempo;

create table produto (id_produto int primary key auto_increment)
select distinct
		pizza_id,
        pizza_name,
        pizza_size,
        pizza_category,
        ingrediente1
from treated_pizza2
where ingrediente1 is not null;
select * from produto;

create table ingredientes (id_ingredientes int primary key auto_increment) 
select distinct ingrediente1
from treated_pizza2   
where ingrediente1 is not null;

select * from produto p join ingredientes i on p.ingrediente1 = i.ingrediente1; -- Teste conexão Produto > Ingrediente;
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
# Operacoes de venda
create view v_pizza as
select distinct -- numero de pizza por semana
	p.pizza_name,
    p.pizza_size,
    order_date,
    dayname(order_date) as day_name,
    time_format(order_time, '%h pm') as _hour, 
        -- math operations 
    total_price,
    round(count(quantity) * 4 / 4 * 1 ,0) as occupied_table, -- 1 pizza * 4 cadeira / 4 * 1 mesa
    round(count(quantity) * 4,0) as occupied_chair,
    sum(quantity)
from order_pizza op
	left join tempo t on op.id_dim_tempo = t.id_tempo 
	left join produto p on op.id_dim_produto = p.pizza_id 
where order_date between '2015-01-01' and '2015-03-31'
group by 1,2,3,4,5,6
order by 3;
select * from v_pizza;

# Operacoes de inventario
create view v_ingredientes as
select 
    p.pizza_name,
    t.order_date,
	i.ingrediente1,
    time_format(t.order_time, '%h pm') as horario
    -- math operations
from order_pizza op
	right join tempo t on op.id_dim_tempo = t.id_tempo 
	right join produto p on op.id_dim_produto = p.pizza_id 
		left join ingredientes i on p.ingrediente1 = i.ingrediente1									
where order_date between '2015-01-01' and '2015-03-31'
group by 1,2,3,4
order by 3;
select * from v_ingredientes;

select  pizza_name, i.ingredientes1 from treated_pizza t join ingredientes i on t.order_id = i.id_ingredientes group by 1,2;
