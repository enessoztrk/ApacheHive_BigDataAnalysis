# Data Analysis with Apache Hive

1. Create a table to avoid query performance. 
Extract the schema of the Hive table to be created by examining the csv file. 
Create a table.

beeline -u jdbc:hive2://localhost:10000

drop table test1.flo_transactions ;

create table if not exists test1.flo_transactions
(master_id string,
order_channel string,
platform_type string,
last_order_channel string,
first_order_date string,
last_order_date string,
last_order_date_online string,
last_order_date_offline string,
order_num_total_ever_online int,
order_num_total_ever_offline int,
customer_value_total_ever_offline float,
customer_value_total_ever_online float,
interested_in_categories_12 array<string>,
online_product_group_amount_top_name_12 string,
offline_product_group_name_12 string,
last_order_date_new date,
store_type string )
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1');


2. Upload the dataset to the table you created.Load the csv file into the table.
It directly overwrites the existing table.
Data in linux subdirectory, not LOCAL hdfs

LOAD DATA LOCAL INPATH '/home/train/datasets/flo100k.csv' 
OVERWRITE INTO TABLE test1.flo_transactions;


LOAD DATA INPATH transfers data over hdfs to hive

LOAD DATA INPATH "/user/train/flo_odev/flo100k.csv" 
INTO TABLE test1.flo_transactions;


Post-installation check.

SELECT * FROM test1.flo_transactions LIMIT 10;

master_id                           |order_channel|platform_type|last_order_channel|first_order_date             |last_order_date              |last_order_date_online       |last_order_date_offline      |order_num_total_ever_online|order_num_total_ever_offline|customer_value_total_ever_offline|customer_value_total_ever_online|interested_in_categories_12|online_product_group_amount_top_name_12|offline_product_group_name_12|last_order_date_new|store_type|
------------------------------------|-------------|-------------|------------------|-----------------------------|-----------------------------|-----------------------------|-----------------------------|---------------------------|----------------------------|---------------------------------|--------------------------------|---------------------------|---------------------------------------|-----------------------------|-------------------|----------|
b3ace094-a17f-11e9-a2fc-000d3a38a36f|Offline      |Offline      |Offline           |2019-02-23T12:59:17.000+03:00|2019-02-23T12:59:17.000+03:00|                             |2019-02-23T12:59:17.000+03:00|                           |                           1|                           212.98|                             0.0|[]                         |                                       |                             |         2019-02-23|A         |
c57d7c4c-a950-11e9-a2fc-000d3a38a36f|Offline      |OmniChannel  |Offline           |2019-12-01T16:48:09.000+03:00|2019-12-01T16:48:09.000+03:00|                             |2019-12-01T16:48:09.000+03:00|                           |                           1|                           199.98|                             0.0|[]                         |                                       |                             |         2019-12-01|A         |
602897a6-cdac-11ea-b31f-000d3a38a36f|Offline      |Offline      |Offline           |2020-07-24T15:49:47.000+03:00|2020-07-24T15:49:47.000+03:00|                             |2020-07-24T15:49:47.000+03:00|                           |                           1|                           140.49|                             0.0|["[ERKEK]"]                |                                       |ERKEK                        |         2020-07-24|A         |
388e4c4e-af86-11e9-a2fc-000d3a38a36f|Mobile       |Online       |Mobile            |2018-12-31T07:22:07.000+03:00|2018-12-31T07:22:07.000+03:00|2018-12-31T07:22:07.000+03:00|                             |                          1|                            |                              0.0|                          174.99|[]                         |                                       |                             |         2018-12-31|A         |
80664354-adf0-11eb-8f64-000d3a299ebf|Desktop      |Online       |Desktop           |2021-05-05T21:07:02.000+03:00|2021-05-05T22:39:36.000+03:00|2021-05-05T22:39:36.000+03:00|                             |                          2|                            |                              0.0|                          283.95|["[]"]                     |                                       |                             |         2021-05-05|A         |
47511f36-aeb4-11e9-a2fc-000d3a38a36f|Android App  |Online       |Android App       |2018-11-11T11:26:51.000+03:00|2018-11-11T11:26:51.000+03:00|2018-11-11T11:26:51.000+03:00|                             |                          1|                            |                              0.0|                          139.98|[]                         |                                       |                             |         2018-11-11|A         |
77f7c318-3407-11eb-9a15-000d3a38a36f|Android App  |Online       |Android App       |2020-12-01T20:38:41.000+03:00|2020-12-01T20:38:41.000+03:00|2020-12-01T20:38:41.000+03:00|                             |                          1|                            |                              0.0|                           269.9|["[AKTIFSPOR]"]            |AKTIFSPOR                              |                             |         2020-12-01|A         |
399d6dd2-ecf1-11ea-9369-000d3a38a36f|Offline      |Offline      |Offline           |2020-09-02T10:51:09.000+03:00|2020-09-02T10:51:09.000+03:00|                             |2020-09-02T10:51:09.000+03:00|                           |                           1|                            95.73|                             0.0|["[KADIN, AKTIFSPOR]"]     |                                       |AKTIFSPOR                    |         2020-09-02|A         |
b3d4a6f2-a368-11e9-a2fc-000d3a38a36f|Offline      |Offline      |Offline           |2019-03-29T16:53:07.000+03:00|2020-09-24T17:37:25.000+03:00|                             |2020-09-24T17:37:25.000+03:00|                           |                           3|                           207.94|                             0.0|["[ERKEK, KADIN]"]         |                                       |ERKEK                        |         2020-09-24|A         |
42fdccd4-f1d1-11ea-bde9-000d3a38a36f|Desktop      |Online       |Desktop           |2020-10-18T22:59:55.000+03:00|2020-10-18T22:59:55.000+03:00|2020-10-18T22:59:55.000+03:00|                             |                          1|                            |                              0.0|                          134.95|["[]"]                     |                                       |                             |         2020-10-18|A         |


Lets create ORC table using existing table
Lets check before converting to ORC

select
cast(concat(substr(first_order_date, 1,10), ' ',
substr(first_order_date,12,8)) as timestamp)
from test1.flo_transactions LIMIT 10;


create table test1.flo_transactions_orc stored as orc
as select
master_id,
order_channel,
platform_type,
last_order_channel,
cast(concat(substr(first_order_date, 1,10), ' ',
substr(first_order_date,12,8)) as timestamp) first_order_date,
cast(concat(substr(last_order_date, 1,10), ' ',
substr(last_order_date,12,8)) as timestamp) last_order_date,
cast(concat(substr(last_order_date_online, 1,10), ' ',
substr(last_order_date_online,12,8)) as timestamp) last_order_date_online,
cast(concat(substr(last_order_date_offline, 1,10), ' ',
substr(last_order_date_offline,12,8)) as timestamp) last_order_date_offline,
order_num_total_ever_online,
order_num_total_ever_offline,
customer_value_total_ever_offline,
customer_value_total_ever_online,
interested_in_categories_12,
online_product_group_amount_top_name_12,
offline_product_group_name_12,
last_order_date_new,
store_type
from test1.flo_transactions ;


describe test1.flo_transactions_orc;


SELECT * FROM test1.flo_transactions_orc LIMIT 10;


3. Prepare the query that finds the number of transactions according to the store types (store_type).

SELECT store_type, COUNT(1) as total_count
FROM test1.flo_transactions_orc
GROUP BY store_type
ORDER BY total_count DESC;

store_type|total_count|
----------|-----------|
A         |      89225|
A,B       |       8497|
B         |       1491|
A,C       |        702|
A,B,C     |         75|
B,C       |         10|


4. Prepare the query that finds the number of transactions according to the order channels (order_channel).

SELECT order_channel, COUNT(1) as total_count
FROM test1.flo_transactions_orc
GROUP BY order_channel
ORDER BY total_count DESC;

order_channel|total_count|
-------------|-----------|
Offline      |      70784|
Android App  |      11989|
Mobile       |       8512|
Desktop      |       4751|
Ios App      |       3964|


5. Prepare the query that finds the number of transactions by year based on the first_order_year.

set hive.groupby.orderby.position.alias=true;

SELECT YEAR (first_order_date) AS first_order_year, COUNT(1) as total_count
FROM test1.flo_transactions_orc
GROUP BY 1
ORDER BY total_count DESC;

first_order_year|total_count|
----------------|-----------|
            2019|      51604|
            2020|      28605|
            2021|      11807|
            2018|       3737|
            2017|       1742|
            2016|       1070|
            2015|        771|
            2014|        445|
            2013|        219|

            
6. Prepare the query that finds the 15 customers who bring the most value in Omni-channel sales.

SELECT master_id, customer_value_total_ever_offline,
customer_value_total_ever_online,
(customer_value_total_ever_offline + customer_value_total_ever_online) as
customer_value
FROM test1.flo_transactions_orc
WHERE customer_value_total_ever_offline > 0.0 AND
customer_value_total_ever_online > 0.0
ORDER BY customer_value DESC
LIMIT 15;

master_id                           |customer_value_total_ever_offline|customer_value_total_ever_online|customer_value|
------------------------------------|---------------------------------|--------------------------------|--------------|
da9942a6-cc1c-11ea-b305-000d3a38a36f|                          1002.66|                        17903.09|      18905.75|
19162418-9ec9-11e9-9897-000d3a38a36f|                          2017.47|                         6578.17|       8595.64|
41b08e82-b179-11e9-89fa-000d3a38a36f|                          8039.93|                           74.99|     8114.9204|
3a844e8e-29b2-11eb-b280-000d3a38a36f|                           6386.1|                         1632.15|       8018.25|
24d87fbc-1c55-11ea-a490-000d3a38a36f|                           159.99|                         6815.17|       6975.16|
1ee925bc-9d8d-11e9-9897-000d3a38a36f|                           509.69|                         6185.42|       6695.11|
0d4d4104-ac5f-11e9-a2fc-000d3a38a36f|                           612.85|                         6077.17|       6690.02|
5741dc4a-ae47-11e9-a2fc-000d3a38a36f|                            84.98|                         6106.58|       6191.56|
a77606c4-a0f9-11e9-a2fc-000d3a38a36f|                          2753.02|                         3191.38|        5944.4|
d60e142a-62b6-11ea-9861-000d3a38a36f|                           220.73|                         5163.38|       5384.11|
3671954c-a5d1-11e9-a2fc-000d3a38a36f|                            79.99|                         5259.14|     5339.1304|
e1a6bee0-2832-11ea-a043-000d3a38a36f|                          1114.33|                         4065.07|        5179.4|
f5a15560-a92d-11e9-a2fc-000d3a38a36f|                           159.97|                         4976.61|       5136.58|
aef94c1e-74fa-11ea-bb48-000d3a38a36f|                           597.43|                         4354.25|       4951.68|
b0409a3a-9f4b-11e9-9897-000d3a38a36f|                            19.99|                         4871.98|       4891.97|


