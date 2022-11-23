use ${db};
CREATE EXTERNAL TABLE IF NOT EXISTS ${table}(
`App_ID` string,
`App_name` string,
`User` string,
`Job_state` string,
`Start_time` timestamp,
`End_time` timestamp,
`Elapsed_time` string,
`Final_status` string,
`Memory_consumed` bigint,
`No_cpu_consumed` bigint,
`Queue_name` string)
PARTITIONED BY (
`reporting_date` date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
"${db_location}"
