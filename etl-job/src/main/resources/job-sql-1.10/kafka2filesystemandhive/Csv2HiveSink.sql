create table csv( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with (  'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',
 'format.type' = 'csv')

-- table user_ino_no_part is a hive table, we create hive table first, and then use hivecatalog to load hive table, then flink can insert
-- data to hive table, the hive create table command is:
-- hive> create table user_ino_no_part(user_name string, is_new boolean, content string)  row format delimited fields terminated by '\t';

 insert into user_ino_no_part select user_name, is_new, content from csv

