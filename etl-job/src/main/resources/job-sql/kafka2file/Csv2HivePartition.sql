create table csv( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with (  'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',
 'format.type' = 'csv',
 'format.fields.0.name' = 'user_name',
 'format.fields.0.data-type' = 'STRING',
 'format.fields.1.name' = 'is_new',
 'format.fields.1.data-type' = 'BOOLEAN',
 'format.fields.2.name' = 'content',
 'format.fields.2.data-type' = 'STRING')
-- create table user_info_partition(user_name string, is_new boolean, content string) PARTITIONED BY (date_col string) row format delimited fields terminated by '\t';
