```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf "spark.workflow.name=Estimate_Pi" \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 3  \
  --num-executors 3 \
  calculatePi.py \ 
> results.txt 2>&1
```
```
yarn logs --applicationId application_1638236186530_0028

hdfs dfs -cp file:///opt/cloudera/parcels/CDH/jars/spark-examples_2.11-2.4.8.7.2.12.1-1.jar s3a://goes-se-sandbox01/kdavis-talend-demo/

cat jobconf.json

{
"className":"org.apache.spark.examples.SparkPi",
"args": [1000],
"file":"s3a://<S3 ENDPOINT>/kdavis-talend-demo/spark-examples_2.11-2.4.8.7.2.12.1-1.jar",
"driverMemory": "2G",
"driverCores": 1,
"executorCores": 2,
"executorMemory": "4G",
"numExecutors": 3,
"queue": "default"
}
```
