### What's this repo?

Hello, this repo includes Java-app, which I've developed for Arenadata.
It's related to the testing purposes of ADB Spark connector.
Also, my Java-app is able to work with vanilla PostgreSQL/Greenplum without ADB Spark connector.
It was done for the test aims. Comparison between OLTP (vanilla PostgreSQL) & OLAP (Greenplum).

Used:
- openjdk8
- maven3
- intellij idea

### 'Must have' part, define before the Spark-job start:

```bash
export SPARK_MAJOR_VERSION="2"
export SPARK_LOCAL_IP="127.0.0.1"
export GSC_JAR="/tmp/adb-spark-connector-assembly-1.0.4-spark-2.3.x.jar"
```

### Working with HDFS, locally:
```bash
hdfs dfs -ls /tmp/
hdfs dfs -rmr "/tmp/test.parquet*"
```

### Or remotely:
```bash
sudo -u spark /usr/bin/hdfs dfs -ls /tmp/test_dir
sudo -u spark /usr/bin/hdfs dfs -rmr "/tmp/test_dir/test.parquet*"
```

### Build project:
```bash
mvn clean package -X
```

### Local test execution for transfer from PostgreSQL/Greenplum/ADB to Hadoop/ADH:
```bash
hdfs dfs -rmr "/tmp/test.parquet*"
/opt/spark/bin/spark-submit \
  --master spark://localhost:7077 \
  --jars $GSC_JAR \
  --class com.oorlov.sandbox1.Main \
  /tmp/sparkDbToHdfs-1.0-SNAPSHOT-jar-with-dependencies.jar \
  jdbc_db_connstr=jdbc:postgresql://localhost:5432/test_adb_connector_v1 db_user=<user> db_pwd=<pwd> db_test_schema=public db_test_table=test_table db_count_alias=total_count db_driver=org.postgresql.Driver hdfs_host=hdfs://localhost:9000 hdfs_input_path=/tmp/test_dir hdfs_output_path=/tmp/test_dir/test.parquet tool_action=fromhdfstordbms spark_app_name=DbToHdfsTransfers slice_delta_value=500 spark_master_host=local[*]
```

### Remote test execution for transfer from Hadoop/ADH to PostgreSQL/Greenplum/ADB:
```bash
sudo -u spark /usr/bin/hdfs dfs -rmr "/tmp/test_dir/test.parquet*"
sudo rm -f /tmp/report.txt
sudo -u spark /usr/bin/spark-submit \
  --master spark://localhost:7077 \
  --jars $GSC_JAR \
  --class com.oorlov.sandbox1.Main \
  /tmp/sparkDbToHdfs-1.0-SNAPSHOT-jar-with-dependencies.jar \
  jdbc_db_connstr=jdbc:postgresql://<remote-greenplum-host>:5432/test_adb_connector_v1 db_user=<user> db_pwd=<pwd> db_test_schema=public db_test_table=test_table db_count_alias=total_count db_driver=org.postgresql.Driver use_adb_connector=true hdfs_host=hdfs://localhost:9000 hdfs_input_path=/tmp/test_dir hdfs_output_path=/tmp/test_dir/test.parquet tool_action=fromhdfstordbms spark_app_name=DbToHdfsTransfers slice_delta_value=25000 spark_master_host=local[*]
```

### JMX options:
```bash
-Dcom.sun.management.jmxremote
-Dcom.sun.management.jmxremote.port=9178
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false

/usr/lib/jvm/java-1.8.0-openjdk-amd64/bin/jconsole localhost:9178
```

### View all classes in the compiled JAR & grep the certain Java-class:
```bash
jar tvf /tmp/sparkDbToHdfs-1.0-SNAPSHOT-jar-with-dependencies.jar | grep -in "oorlov"
```
