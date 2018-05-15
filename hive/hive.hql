CREATE DATABASE amazon_reviews WITH DBPROPERTIES ("creator" = "Antonio Matinata", "date" = "2018-05-05");
CREATE TABLE IF NOT EXISTS amazon_reviews.reviews (
	id INT COMMENT "Review unique id",
	productId STRING COMMENT "Unique identifier for the product",
	userId STRING COMMENT "Unique identifier for the user",
	profileName STRING COMMENT "Account name of the user",
	helpfulnessNumerator FLOAT COMMENT "Number of users who found the review helpful",
	helpfulnessDenominator FLOAT COMMENT"Number of users who graded the review",
	score FLOAT COMMENT "Rating between 1 and 5",
	time DATE COMMENT "Tmestamp of the review expressed in Unix Time",
	summary STRING COMMENT "Summary of the review",
	text STRING COMMENT "Text of the review"	
	)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY '\n'
	STORED AS TEXTFILE;
# PARTITIONED BY (productId STRING, userId STRING);

LOAD DATA INPATH '/user/hive/files/Reviews_test.csv' OVERWRITE INTO TABLE amazon_reviews.reviews; 

# 1.1
INSERT OVERWRITE DIRECTORY '/user/hive/files/output/1'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
SELECT ranked.year, ranked.word, ranked.frequency
FROM (SELECT temp.year, temp.word, temp.frequency,  rank() over (PARTITION BY temp.year ORDER BY temp.frequency DESC) as rank 
	FROM (  SELECT YEAR(Time) as year, t.t1 as word, count(*) as frequency
		FROM  amazon_reviews.reviews LATERAL VIEW EXPLODE(SPLIT(Text, " ")) t as t1 
		GROUP BY t.t1, YEAR(Time)
		ORDER BY year ASC, frequency DESC, t.t1) temp
		) ranked
WHERE ranked.rank < 10;

# row_number() over (PARTITION BY temp.year) as row_id,

Query ID = cloudera_20180515154949_caeb7b59-1a99-424b-b9f2-2fc1381eaa63
Total jobs = 4
Launching Job 1 out of 4
Number of reduce tasks not specified. Estimated from input data size: 2
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0023, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0023/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0023
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 2
2018-05-15 15:49:57,873 Stage-1 map = 0%,  reduce = 0%
2018-05-15 15:50:13,134 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.62 sec
2018-05-15 15:50:26,232 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 6.85 sec
2018-05-15 15:50:27,265 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.05 sec
MapReduce Total cumulative CPU time: 8 seconds 50 msec
Ended Job = job_1526414710154_0023
Launching Job 2 out of 4
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0024, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0024/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0024
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2018-05-15 15:50:36,489 Stage-2 map = 0%,  reduce = 0%
2018-05-15 15:50:42,917 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 0.83 sec
2018-05-15 15:50:50,336 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 1.91 sec
MapReduce Total cumulative CPU time: 1 seconds 910 msec
Ended Job = job_1526414710154_0024
Launching Job 3 out of 4
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0025, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0025/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0025
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2018-05-15 15:50:58,509 Stage-3 map = 0%,  reduce = 0%
2018-05-15 15:51:06,961 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 0.83 sec
2018-05-15 15:51:15,475 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 2.31 sec
MapReduce Total cumulative CPU time: 2 seconds 310 msec
Ended Job = job_1526414710154_0025
Launching Job 4 out of 4
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0026, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0026/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0026
Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 1
2018-05-15 15:51:24,495 Stage-4 map = 0%,  reduce = 0%
2018-05-15 15:51:30,931 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 0.84 sec
2018-05-15 15:51:40,445 Stage-4 map = 100%,  reduce = 100%, Cumulative CPU 2.48 sec
MapReduce Total cumulative CPU time: 2 seconds 480 msec
Ended Job = job_1526414710154_0026
Moving data to: /user/hive/files/output/1
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 2  Reduce: 2   Cumulative CPU: 8.05 sec   HDFS Read: 288108925 HDFS Write: 1251 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 1.91 sec   HDFS Read: 6002 HDFS Write: 1155 SUCCESS
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 2.31 sec   HDFS Read: 7364 HDFS Write: 1195 SUCCESS
Stage-Stage-4: Map: 1  Reduce: 1   Cumulative CPU: 2.48 sec   HDFS Read: 9188 HDFS Write: 117 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 750 msec
OK
Time taken: 111.857 seconds





# 1.2
INSERT OVERWRITE DIRECTORY '/user/hive/files/output/2' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
SELECT YEAR(Time) as year, ProductId as product, AVG(Score) as mean_score 
FROM amazon_reviews.reviews 
WHERE YEAR(Time) >= 2003 AND YEAR(Time) <= 2011 
GROUP BY YEAR(Time), ProductId; 

Query ID = cloudera_20180515022929_3a276c4a-97c3-4292-8662-3822bee1c66e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 2
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526307565070_0002, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526307565070_0002/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526307565070_0002
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 2
2018-05-15 02:29:50,612 Stage-1 map = 0%,  reduce = 0%
2018-05-15 02:30:10,257 Stage-1 map = 50%,  reduce = 0%, Cumulative CPU 3.18 sec
2018-05-15 02:30:13,484 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 9.07 sec
2018-05-15 02:30:30,432 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 15.38 sec
MapReduce Total cumulative CPU time: 15 seconds 380 msec
Ended Job = job_1526307565070_0002
Moving data to: /user/hive/files/output/2
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 2  Reduce: 2   Cumulative CPU: 15.38 sec   HDFS Read: 288104669 HDFS Write: 2167994 SUCCESS
Total MapReduce CPU Time Spent: 15 seconds 380 msec
OK
Time taken: 51.741 seconds


# 1.3
INSERT OVERWRITE DIRECTORY '/user/hive/files/output/3'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
SELECT t.p1 as product_1, t.p2 as product_2, SUM(t.agg) as common_reviewers
FROM (SELECT r1.ProductId as p1, r2.ProductId as p2, r1.UserId u1, COUNT(1) as agg
		FROM amazon_reviews.reviews r1 JOIN amazon_reviews.reviews r2
		WHERE r1.ProductId < r2.ProductId AND r1.UserId = r2.UserId 
        GROUP BY r1.ProductId, r2.ProductId, r1.UserId
        ORDER BY r1.ProductId ASC, r2.ProductId ASC
     ) AS t
GROUP BY t.p1, t.p2
ORDER BY common_reviewers DESC, t.p1 ASC, t.p2 ASC

Query ID = cloudera_20180515132626_7816f552-c2a4-4345-8815-d7d6fd1fb9c1
Total jobs = 4
Stage-1 is selected by condition resolver.
Launching Job 1 out of 4
Number of reduce tasks not specified. Estimated from input data size: 2
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0001, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0001/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0001
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 2
2018-05-15 13:26:46,213 Stage-1 map = 0%,  reduce = 0%
2018-05-15 13:27:02,149 Stage-1 map = 50%,  reduce = 0%, Cumulative CPU 2.47 sec
2018-05-15 13:27:07,509 Stage-1 map = 83%,  reduce = 0%, Cumulative CPU 7.49 sec
2018-05-15 13:27:09,826 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 9.06 sec
2018-05-15 13:27:28,835 Stage-1 map = 100%,  reduce = 82%, Cumulative CPU 19.13 sec
2018-05-15 13:27:32,122 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 22.55 sec
MapReduce Total cumulative CPU time: 22 seconds 550 msec
Ended Job = job_1526414710154_0001
Launching Job 2 out of 4
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0002, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0002/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0002
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2018-05-15 13:27:44,587 Stage-2 map = 0%,  reduce = 0%
2018-05-15 13:28:01,909 Stage-2 map = 38%,  reduce = 0%, Cumulative CPU 11.0 sec
2018-05-15 13:28:07,322 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 15.69 sec
2018-05-15 13:28:28,539 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 24.72 sec
MapReduce Total cumulative CPU time: 24 seconds 720 msec
Ended Job = job_1526414710154_0002
Launching Job 3 out of 4
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0003, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0003/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0003
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2018-05-15 13:28:37,967 Stage-3 map = 0%,  reduce = 0%
2018-05-15 13:28:55,015 Stage-3 map = 87%,  reduce = 0%, Cumulative CPU 11.15 sec
2018-05-15 13:28:56,062 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 11.87 sec
2018-05-15 13:29:12,015 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 20.06 sec
MapReduce Total cumulative CPU time: 20 seconds 60 msec
Ended Job = job_1526414710154_0003
Launching Job 4 out of 4
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1526414710154_0004, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1526414710154_0004/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1526414710154_0004
Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 1
2018-05-15 13:29:20,524 Stage-4 map = 0%,  reduce = 0%
2018-05-15 13:29:38,626 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 11.59 sec
2018-05-15 13:29:52,461 Stage-4 map = 100%,  reduce = 100%, Cumulative CPU 17.6 sec
MapReduce Total cumulative CPU time: 17 seconds 600 msec
Ended Job = job_1526414710154_0004
Moving data to: /user/hive/files/output/3
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 2  Reduce: 2   Cumulative CPU: 22.55 sec   HDFS Read: 288104997 HDFS Write: 154639953 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 24.72 sec   HDFS Read: 154645133 HDFS Write: 102767556 SUCCESS
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 20.06 sec   HDFS Read: 102772329 HDFS Write: 69173211 SUCCESS
Stage-Stage-4: Map: 1  Reduce: 1   Cumulative CPU: 17.6 sec   HDFS Read: 69178219 HDFS Write: 41111049 SUCCESS
Total MapReduce CPU Time Spent: 1 minutes 24 seconds 930 msec
OK
Time taken: 199.944 seconds

