drop table Graph1;
drop table Graph2;

create table Graph1 (key int, value int) 
row format delimited fields terminated by ',' stored as textfile;

create table Graph2 (key int, value int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Graph1;

insert overwrite table Graph2 select g1.key, count(g1.value) from Graph1 as g1 group by g1.key;

select g2.value, count(g2.value) from Graph2 as g2 group by g2.value;
