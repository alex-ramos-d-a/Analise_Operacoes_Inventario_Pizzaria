# Criando banco dados
create database pizza_db;
use pizza_db;

# Importando tabela
create table big_table4
(
	id 	        int not null auto_increment,
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
    constraint pk_pizza primary key (id)
);
describe big_table4;

# Criando tabela Tratada
create table tab_geral (id int primary key auto_increment)
select  
		order_id,
		pizza_id, 
		quantity,
		order_date,	
		order_time,
		cast(replace(unit_price,',','.') as decimal(4,2)) as unit_price, 
		cast(replace(total_price,',','.') as decimal(4,2)) as total_price,
		pizza_size,
		pizza_category,
		pizza_name
from big_table4;

--------------------------------------------------------------------------
# Modelagem

create table dim_produto (id int primary key auto_increment)
select distinct
		pizza_id,
        pizza_name,
        pizza_size,
        pizza_category
from tab_geral;
select * from dim_produto;

create table dim_Tempo (id int primary key auto_increment)
select distinct 
		order_date,
        order_time,
        extract(year from order_date) as year,
        extract(month from order_date) as month,
        extract(week from order_date) as week,
        extract(day from order_date) as day, 
		extract(hour from order_time) as hour
from tab_geral; 
select * from dim_tempo;

create table ft_Pizza (id int primary key auto_increment)
select 
    pizza_id,
    quantity,
    unit_price,
    total_price
from tab_geral;

create table ft_Pedido (id int primary key auto_increment)
select 
	order_id
    quantity,
    unit_price,
    total_price
from tab_geral;

------------------------------------------------------------------------------
# View completo do projeto

create view v_pizza as
select distinct 
	dpo.pizza_name,
    dpo.pizza_size,
    order_date,
    dayname(order_date) as day_name,
    time_format(order_time, '%h pm') as _hour,
    total_price,
    round(count(quantity) * 4 / 4 * 1 ,0) as occupied_table, 
    round(count(quantity) * 4,0) as occupied_chair,
    sum(quantity)
from ft_pizza fpi
join dim_tempo dte on fpi.id = dte.id
join dim_produto dpo on fpi.pizza_id = dpo.pizza_id
where order_date between '2015-01-01' and '2015-04-01'
group by 1,2,3,4,5,6
order by 3;
