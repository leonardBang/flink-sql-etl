create table csv( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with (
 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',
 'format.type' = 'csv')

-- table user_info_partition is a hive partition table, we create hive table first, and then use hivecatalog to load hive table, then flink can insert
-- data to hive table, the hive create table command is:
-- create table user_info_partition(user_name string, is_new boolean, content string) PARTITIONED BY (date_col string) row format delimited fields terminated by '\t';
