# Stock-Market-Analysis-

#The data set is a CSV file consisting time series data of six stocks TCS, Infosys, Hero, Bajaj Auto, TVS Motors and Eicher Motors 

#imported to MYSQL worked on data cleaning, joins, creating new tables from existing tables, moving averages, window functions

#creating functions and creating procedures to generate buy , sell or hold signal to the clients.

#Importing the .csv file into MySQL( for all 6 files )

#1. Bajaj Auto

create database kishore;

use kishore;

create table bajaj_auto(
Date_of              date,
Open_Price	double,
High_Price         double,
Low_Price	double,
Close_Price       double,
WAP  double,
No_of_Shares    double,
No_of_Trades     double,
Total_Turnover      bigint,
Deliverable_Quantity    double,
Deli_Qty_to_Traded_Qty   double,
Spread_High_Low double,
Spread_Close_Open     double   );


load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Bajaj_Auto.csv'

into table bajaj_auto fields terminated by ',' lines terminated by '\n' ignore 1 rows

(Dateof,Open_price,High_Price,Low_Price,Close_Price,WAP,No_of_Shares,No_of_Trades,

Total_Turnover,Deliverable_Quantity,Deli_Qty_to_Traded_Qty,Spread_High_Low,Spread_Close_Open);


#2. Eicher Motors

create table Eicher_Motors
(
Dateof         date,
Open_Price	double,
High_Price       double,
Low_Price	double,
Close_Price    double,
WAP             double,
No_of_Shares  double,
No_of_Trades   double,
Total_Turnover   bigint,
Deliverable_Quantity   double,
Deli_Qty_to_Traded_Qty   double,
Spread_High_Low    double,
Spread_Close_Open   double);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Eicher_Motors.csv'

into table Eicher_Motors fields terminated by ',' lines terminated by '\n' ignore 1 rows

(Dateof,Open_price,High_Price,Low_Price,Close_Price,WAP,No_of_Shares,No_of_Trades,

Total_Turnover,Deliverable_Quantity,Deli_Qty_to_Traded_Qty,Spread_High_Low,Spread_Close_Open);

#3. Hero Motocorp

create table Hero_Motocorp(
Dateof date,
Open_Price	double,
High_Price double,
Low_Price	double,
Close_Price double,
WAP double,
No_of_Shares double,
No_of_Trades double,
Total_Turnover bigint,
Deliverable_Quantity double,
Deli_Qty_to_Traded_Qty double,
Spread_High_Low double,
Spread_Close_Open double);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Hero_Motocorp.csv'

into table Hero_Motocorp fields terminated by ',' lines terminated by '\n' ignore 1 rows

(Dateof,Open_price,High_Price,Low_Price,Close_Price,WAP,No_of_Shares,No_of_Trades,

Total_Turnover,Deliverable_Quantity,Deli_Qty_to_Traded_Qty,Spread_High_Low,Spread_Close_Open);

#4. Infosys

create table Infosys
(
Dateof date,
Open_Price	double,
High_Price double,
Low_Price	double,
Close_Price double,
WAP double,
No_of_Shares double,
No_of_Trades double,
Total_Turnover bigint,
Deliverable_Quantity double,
Deli_Qty_to_Traded_Qty double,
Spread_High_Low double,
Spread_Close_Open double);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Infosys.csv'

into table Infosys fields terminated by ',' lines terminated by '\n' ignore 1 rows

(Dateof,Open_price,High_Price,Low_Price,Close_Price,WAP,No_of_Shares,No_of_Trades,

Total_Turnover,Deliverable_Quantity,Deli_Qty_to_Traded_Qty,Spread_High_Low,Spread_Close_Open);

#5. TCS

create table TCS
(
Dateof date,
Open_Price	double,
High_Price double,
Low_Price	double,
Close_Price double,
WAP double,
No_of_Shares double,
No_of_Trades double,
Total_Turnover bigint,
Deliverable_Quantity double,
Deli_Qty_to_Traded_Qty double,
Spread_High_Low double,
Spread_Close_Open double);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TCS.csv'

into table TCS fields terminated by ',' lines terminated by '\n' ignore 1 rows

(Dateof,Open_price,High_Price,Low_Price,Close_Price,WAP,No_of_Shares,No_of_Trades,

Total_Turnover,Deliverable_Quantity,Deli_Qty_to_Traded_Qty,Spread_High_Low,Spread_Close_Open);

#6. TVS Motors 

create table TVS_Motors
(
Dateof date,
Open_Price	double,
High_Price double,
Low_Price	double,
Close_Price double,
WAP double,
No_of_Shares double,
No_of_Trades double,
Total_Turnover bigint,
Deliverable_Quantity double,
Deli_Qty_to_Traded_Qty double,
Spread_High_Low double,
Spread_Close_Open double);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TVS_Motors.csv'

into table TVS_Motors fields terminated by ',' lines terminated by '\n' ignore 1 rows

(Dateof,Open_price,High_Price,Low_Price,Close_Price,WAP,No_of_Shares,No_of_Trades,

Total_Turnover,Deliverable_Quantity,Deli_Qty_to_Traded_Qty,Spread_High_Low,Spread_Close_Open);


#calculating 20 day moving average and 50 day moving average using window functions


1).select Date,'close_price',
avg('close_price') over(rows 19 preceding) as '20 Day MA',  
avg('close_price') over(rows 49 preceding) as '50 Day MA'
from (select Date,'close_price',row_number() over() as day from 'bajaj_auto' order by day desc);

2). select Date,'close_price',
avg('close_price') over(rows 19 preceding) as '20DayMA',
 avg('close_price') over(rows 49 preceding) as '50DayMA' 
from (select Date,'close_price',row_number() over() as day from 'Eicher_ Motors' order by day desc);

3). select Date, 'close_price',
 avg ('close_price') over (rows 19 preceding)as '20DayMA',
 avg('close_price') over (rows 49 preceding) as '50DayMA'
 from (select Date,'close_price',row_number() over() as day from 'Hero_Motocorp' order by day desc);

4). select Date,'close_price', 
avg('close_price') over(rows 19 preceding)as '20DayMA',
 avg('close_price') over(rows 49 preceding) as '50DayMA'  
from (select Date,'close_price', row_number() over() as day from 'Infosys'  order by day desc);

5). select Date,'close_price', 
avg('close_price') over(rows 19 preceding)as '20DayMA', 
avg('close_price') over(rows 49 preceding) as '50DayMA'
 from (select Date,'close_price',row_number() over() as day from 'TCS' order by day desc);

6).select Date,'close_price', 
avg('close_price') over(rows 19 preceding)as '20DayMA',
 avg('close_price') over(rows 49 preceding) as '50DayMA'
 from (select Date,'close_price', row_number() over() as day from 'TVS_Motor' order by day desc);
 #to get all tables data into single table I combined them using inner join

select b.'date' as Date, b.'close price' as 'Bajaj', t.'close price' as 'TCS', tv.'close price' as 'TVS',
 i.'close price' as 'Infosys', e.'close price' as 'Eicher', h.'close price' as 'Hero`
from 'bajaj_auto' b inner join TCS t on ba.'date'=t.'date'                        
inner join TVS_Motor  tv  on  t.'date'=tm.'date'
inner join 'Infosys' i on  tm.'date'  =i.'date'
inner join 'Eicher_Motors' e on i.'date'=e.'date'
inner join 'Hero_Motocorp' h on e.'date'=h.'date';

#creating signal to buy, sell or hold 

SELECT Date,
'close_price',
CASE
WHEN day>49 and '20DayMA' > '50DayMA' THEN  'Buy'                      
WHEN day>49 and '20DayMA' < '50DayMA' THEN 'Sell'
ELSE 'Hold'
END as 'Signal'
from (select Date,'close_price','20DayMA','50DayMA',
row_number() over() as day from bajaj_auto
order by day asc);


Set global log¬_bin_trust_function_creators=1;
                                                                       

delimiter $$       #to change end of query notation  from ; to $$                                                                    
drop function if exists signal_table_of_bajaj $$                       
create function signal_table_of_bajaj (date1 text)                    
returns char(5) deterministic
begin
declare  s_var char(10);
 select 'Signal' into s_var from bajaj2 where date=date1;
 return s_var;
end $$
delimiter ;


