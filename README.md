# mysql reporter
 generate report for mysql data

used module:  
mysql-connector-python

config file example:  
#config file should be at the same path as main.py  
[db_info]  
user = yourname  
password = yourpass  
host = your mysql server ip address  
database = your db name 

[time_range]  
#time format:YYYY-MM-DD  
#skip_weekend should be 'True' or 'False'  
start = 2019-12-11  
end = 2019-12-16  
skip_weekend = True  

[sql_info]  
#bw_direction should be 'in' or 'out'  
tables = table1,table2  
column = GigabitEthernet7/3/1,TenGigabitEthernet2/1  
max_bw = 400,10000  
bw_direction = in  
