create table csv( id INT, note STRING, country STRING, record_time TIMESTAMP(4), doub_val DECIMAL(6, 2)) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/Improve/flinkstream/src/main/resources/test.csv',
 'format.type' = 'csv')

create table csvSink( jnlno STRING,
  taskid char(4),
   hit VARCHAR ) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/Improve/flinkstream/src/main/resources/test12312.csv',
 'format.type' = 'csv')

insert into  csvSink select a.country,'111111qeq','false' from csv a