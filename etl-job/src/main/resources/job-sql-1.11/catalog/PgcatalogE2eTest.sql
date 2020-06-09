# create table by pgadmin
CREATE TABLE public.primitive_table(id integer);
CREATE TABLE bang.primitive_table(id integer);
CREATE TABLE public.primitive_table(int integer, bytea bytea, short smallint, long bigint, real real, double_precision double precision, numeric numeric(10, 5), decimal decimal(10, 1), boolean boolean, text text, char char, character character(3), character_varying character varying(20), timestamp timestamp(5), date date,time time(0), default_numeric numeric, CONSTRAINT test_pk PRIMARY KEY (short, int));
CREATE TABLE public.primitive_table(int_arr integer[], bytea_arr bytea[], short_arr smallint[], long_arr bigint[], real_arr real[], double_precision_arr double precision[], numeric_arr numeric(10, 5)[], numeric_arr_default numeric[], decimal_arr decimal(10,2)[], boolean_arr boolean[], text_arr text[], char_arr char[], character_arr character(3)[], character_varying_arr character varying(20)[], timestamp_arr timestamp(5)[], date_arr date[], time_arr time(0)[]);
CREATE TABLE public.primitive_table(f0 smallserial, f1 serial, f2 serial2, f3 serial4, f4 serial8, f5 bigserial);
CREATE TABLE public.primitive_table2(int integer, bytea bytea, short smallint, long bigint, real real, double_precision double precision, numeric numeric(10, 5), decimal decimal(10, 1), boolean boolean, text text, char char, character character(3), character_varying character varying(20), timestamp timestamp(5), date date,time time(0), default_numeric numeric, CONSTRAINT test_pk1 PRIMARY KEY (short, int));

# insert test data
insert into public.t1 values (1);
insert into primitive_table values (1,'2',3,4,5.5,6.6,7.7,8.8,true,'a','b','c','d','2016-06-22 19:10:25','2015-01-01','00:51:02.746572', 500);
insert into array_table values ('{1,2,3}','{2,3,4}','{3,4,5}','{4,5,6}','{5.5,6.6,7.7}','{6.6,7.7,8.8}','{7.7,8.8,9.9}','{8.8,9.9,10.10}','{9.9,10.10,11.11}','{true,false,true}','{a,b,c}','{b,c,d}','{b,c,d}','{b,c,d}','{"2016-06-22 19:10:25", "2019-06-22 19:10:25"}','{"2015-01-01", "2020-01-01"}','{"00:51:02.746572", "00:59:02.746572"}');
insert into serial_table values (32767,2147483647,32767,2147483647,9223372036854775807,9223372036854775807);

# test in sql-client

(1) config conf/sql-client-defaults.yaml
catalogs:
  - name: mypg
    type: jdbc
    default-database: mydb
    username: postgres
    password: postgres
    base-url: jdbc:postgresql://localhost/

(2) add necessary dependency to /lib
flink-connector-jdbc_2.11-1.12-SNAPSHOT.jar
postgresql-42.2.9.jar

(3) sql-client test
Flink SQL> show tables;
bang.primitive_table
public.primitive_arr_table
public.primitive_serial_table
public.primitive_table
public.primitive_table2
public.simple_t1

# test read/write
Flink SQL> insert into `public.primitive_table2` select * from `public.primitive_table`;
[INFO] Submitting SQL update statement to the cluster...
[INFO] Table update statement has been successfully submitted to the cluster:
Job ID: aa953b785dea9903acaf4caafa50987a

#check result
Flink SQL> select * from `public.primitive_table2`;
[INFO] Result retrieval cancelled.
-- int                     bytea                     short                      long                      real          double_precision                   numeric
-- 1                      [50]                         3                         4                       5.5                       6.6      7.700000000000000000

-- See FLINK-17948, sql-client bug
Flink SQL> select * from `public.primitive_arr_table`;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.planner.codegen.CodeGenException: Unsupported cast from 'ARRAY<DECIMAL(10, 5)>' to 'ARRAY<DECIMAL(38, 18)>'.