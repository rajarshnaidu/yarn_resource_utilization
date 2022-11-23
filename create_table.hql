use ${hiveconf:db};
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:table}(
`App_ID` string,
`User` string,
`Start_Time` timestamp,
`End_Time` timestamp,
`Memory_consumed` bigint,
`No_cpu_consumed` bigint,
`Queue_name` string)
PARTITIONED BY (
`load_date` date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
"${hiveconf:db_location}"
